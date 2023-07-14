use crate::{ExplicitKey, Key};

use super::{Arg, AtomicWriteOperation, AtomicWriteSubOperation, BatchOperation, BatchSubOperation, Bound, Error, Result, Value};
use rand::RngCore;
use rusoto_core::RusotoError;
use rusoto_dynamodb::{
    AttributeDefinition, AttributeValue, ConsumedCapacity, Delete, DynamoDb, DynamoDbClient, KeySchemaElement, LocalSecondaryIndex, Projection, Put,
    TransactWriteItem, Update,
};
use simple_error::SimpleError;
use std::{
    collections::HashMap,
    sync::mpsc::{self, SyncSender},
};
use tracing::{field::Empty, info_span, Span};

fn add_err_to_span(e: &(dyn std::error::Error + Sync + Send + 'static)) {
    let span = Span::current();
    span.record("otel.status_code", "ERROR");
    span.record("err.msg", e);
}

trait ErrExt {
    fn spanify(self) -> Self;
}

impl<E: std::error::Error + Sync + Send + 'static> ErrExt for E {
    fn spanify(self) -> Self {
        add_err_to_span(&self);
        self
    }
}

trait ResultExt {
    fn spanify_err(self) -> Self;
}

impl<T, E: std::error::Error + Sync + Send + 'static> ResultExt for std::result::Result<T, E> {
    fn spanify_err(self) -> Self {
        if let Err(ref e) = self {
            add_err_to_span(e);
        }
        self
    }
}

#[derive(Clone)]
pub struct Backend {
    pub allow_eventually_consistent_reads: bool,
    pub client: DynamoDbClient,
    pub table_name: String,
}

const NO_SORT_KEY: &str = "_";

fn new_item<'h, 's, S: Into<Arg<'s>> + Send, A: IntoIterator<Item = (&'static str, AttributeValue)>>(
    hash: &ExplicitKey<'h>,
    sort: S,
    attrs: A,
) -> HashMap<String, AttributeValue> {
    let mut ret: HashMap<_, _> = attrs.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
    ret.insert("hk".to_string(), attribute_value(hash.unredacted.as_bytes()));
    ret.insert("rk".to_string(), attribute_value(sort));
    ret
}

fn composite_key<'k, 's, S: Into<Arg<'s>> + Send>(hash: &ExplicitKey<'k>, sort: S) -> HashMap<String, AttributeValue> {
    let mut ret = HashMap::new();
    ret.insert("hk".to_string(), attribute_value(hash.unredacted.as_bytes()));
    ret.insert("rk".to_string(), attribute_value(sort));
    ret
}

fn attribute_value<'a, V: Into<Arg<'a>>>(v: V) -> AttributeValue {
    let v = v.into();
    let mut ret = AttributeValue::default();
    ret.b = Some(bytes::Bytes::copy_from_slice(v.as_bytes()));
    ret
}

fn float_sort_key(f: f64) -> [u8; 8] {
    let mut n = f.to_bits();
    if (n & (1 << 63)) != 0 {
        n ^= 0xffffffffffffffff
    } else {
        n ^= 0x8000000000000000
    }
    n.to_be_bytes()
}

fn float_sort_key_after(f: f64) -> Option<[u8; 8]> {
    let mut n = f.to_bits();
    if (n & (1 << 63)) != 0 {
        n ^= 0xffffffffffffffff
    } else {
        n ^= 0x8000000000000000
    }
    n += 1;
    if n == 0 {
        None
    } else {
        Some(n.to_be_bytes())
    }
}

enum ArgBound<'a> {
    Inclusive(Arg<'a>),
    Unbounded,
}

impl<'a> ArgBound<'a> {
    fn inclusive_from(arg: &'a Bound<Arg<'a>>) -> Self {
        match arg {
            Bound::Included(arg) => Self::Inclusive(arg.as_bytes().into()),
            Bound::Excluded(arg) => Self::Inclusive(arg.as_bytes().into()),
            Bound::Unbounded => Self::Unbounded,
        }
    }
}

impl<'a> Into<Bound<Arg<'a>>> for ArgBound<'a> {
    fn into(self) -> Bound<Arg<'a>> {
        match self {
            Self::Inclusive(a) => Bound::Included(a),
            Self::Unbounded => Bound::Unbounded,
        }
    }
}

fn query_condition<'k, K: Key<'k>>(key: K, min: ArgBound<'_>, max: ArgBound<'_>, secondary_index: bool) -> (String, HashMap<String, AttributeValue>) {
    let mut attribute_values = HashMap::new();
    attribute_values.insert(":hash".to_string(), attribute_value(key.into().unredacted));

    if let ArgBound::Inclusive(min) = &min {
        attribute_values.insert(":minSort".to_string(), attribute_value(min));
    }

    if let ArgBound::Inclusive(max) = &max {
        attribute_values.insert(":maxSort".to_string(), attribute_value(max));
    }

    let range_key = match secondary_index {
        false => "rk",
        true => "rk2",
    };

    let condition = match (min, max) {
        (ArgBound::Inclusive(_), ArgBound::Inclusive(_)) => format!("hk = :hash AND {} BETWEEN :minSort AND :maxSort", range_key),
        (ArgBound::Inclusive(_), ArgBound::Unbounded) => format!("hk = :hash AND {} >= :minSort", range_key),
        (ArgBound::Unbounded, ArgBound::Inclusive(_)) => format!("hk = :hash AND {} <= :maxSort", range_key),
        (ArgBound::Unbounded, ArgBound::Unbounded) => "hk = :hash".to_string(),
    };

    (condition, attribute_values)
}

fn score_bounds(min: f64, max: f64) -> (ArgBound<'static>, ArgBound<'static>) {
    let min = float_sort_key(min);
    let max = float_sort_key_after(max);
    (
        ArgBound::Inclusive(min.to_vec().into()),
        match &max {
            Some(max) => ArgBound::Inclusive(max.to_vec().into()),
            None => ArgBound::Unbounded,
        },
    )
}

fn map_bound<T, U, F: FnOnce(T) -> U>(b: Bound<T>, f: F) -> Bound<U> {
    match b {
        Bound::Included(v) => Bound::Included(f(v)),
        Bound::Excluded(v) => Bound::Excluded(f(v)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn lex_bound<'a, T: Into<Arg<'a>>>(score: f64, b: Bound<T>) -> Bound<Vec<u8>> {
    let prefix = float_sort_key(score);
    map_bound(b, |v| [&prefix, v.into().as_bytes()].concat())
}

fn lex_bounds<'m, 'n, M: Into<Arg<'m>>, N: Into<Arg<'n>>>(score: f64, min: Bound<M>, max: Bound<N>) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    (lex_bound(score, min), lex_bound(score, max))
}

fn encode_field_name(name: &[u8]) -> String {
    "~".to_string() + &base64::encode_config(name, base64::URL_SAFE_NO_PAD)
}

fn decode_field_name(name: &String) -> Option<Vec<u8>> {
    if name.starts_with('~') {
        base64::decode_config(&name[1..], base64::URL_SAFE_NO_PAD).ok()
    } else {
        None
    }
}

impl Backend {
    async fn zh_range_impl<'k, 'm, 'n, M: Into<Arg<'m>>, N: Into<Arg<'n>>>(
        &self,
        key: ExplicitKey<'k>,
        min: Bound<M>,
        max: Bound<N>,
        limit: usize,
        reverse: bool,
        secondary_index: bool,
    ) -> Result<Vec<Value>> {
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));

        let min = map_bound(min, |v| v.into());
        let max = map_bound(max, |v| v.into());

        let inclusive_min = ArgBound::inclusive_from(&min);
        let inclusive_max = ArgBound::inclusive_from(&max);

        if let (ArgBound::Inclusive(min), ArgBound::Inclusive(max)) = (&inclusive_min, &inclusive_max) {
            if min.as_bytes() > max.as_bytes() {
                return Ok(vec![]);
            }
        }

        let (condition, attribute_values) = query_condition(key, inclusive_min, inclusive_max, secondary_index);

        let mut query = rusoto_dynamodb::QueryInput::default();
        query.table_name = self.table_name.clone();
        query.consistent_read = Some(!self.allow_eventually_consistent_reads);
        query.key_condition_expression = Some(condition.clone());
        query.expression_attribute_values = Some(attribute_values.clone());
        query.scan_index_forward = Some(!reverse);
        query.index_name = if secondary_index { Some("rk2".to_string()) } else { None };
        query.return_consumed_capacity = Some("TOTAL".to_owned());

        let mut members = vec![];
        let mut cap = TotalConsumedCapacity::default();

        while limit == 0 || members.len() < limit {
            let mut q = query.clone();
            if limit > 0 {
                q.limit = Some((limit - members.len() + 1) as _);
            }
            let result = self.client.query(q).await.spanify_err()?;
            if let Some(c) = result.consumed_capacity {
                cap.add(&c);
            }
            if let Some(mut items) = result.items {
                let mut skip = 0;

                // dynamodb can't express exclusive ranges so we did an inclusive query. that means
                // we may need to skip the first or last result
                if !items.is_empty() {
                    if members.is_empty() {
                        if let Bound::Excluded(excluded_key) = if reverse { &max } else { &min } {
                            if let Some(first_item_key) = items[0].get(if secondary_index { "rk2" } else { "rk" }).and_then(|v| v.b.as_ref()) {
                                if excluded_key.as_bytes() == first_item_key {
                                    skip = 1;
                                }
                            }
                        }
                    }

                    if result.last_evaluated_key.is_none() {
                        if let Bound::Excluded(excluded_key) = if reverse { &min } else { &max } {
                            if let Some(last_item_key) = items
                                .last()
                                .and_then(|item| item.get(if secondary_index { "rk2" } else { "rk" }).and_then(|v| v.b.as_ref()))
                            {
                                if excluded_key.as_bytes() == last_item_key {
                                    items.pop();
                                }
                            }
                        }
                    }
                }

                members.extend(
                    items
                        .into_iter()
                        .skip(skip)
                        .filter_map(|mut v| v.remove("v").and_then(|v| v.b).map(|v| v.to_vec().into())),
                );
            }
            match result.last_evaluated_key {
                Some(key) => query.exclusive_start_key = Some(key),
                None => break,
            }
        }

        while limit > 0 && members.len() > limit {
            members.pop();
        }
        cap.record_to(&span);
        Ok(members)
    }

    async fn zh_add_impl<'a, 'b, 'c, F: Into<Arg<'b>> + Send, V: Into<Arg<'c>> + Send>(
        &self,
        key: ExplicitKey<'a>,
        field: F,
        value: V,
        score: f64,
    ) -> Result<()> {
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));
        let field = field.into();
        let value = value.into();
        let mut put = rusoto_dynamodb::PutItemInput::default();
        put.table_name = self.table_name.clone();
        put.item = new_item(
            &key,
            &field,
            vec![
                ("v", attribute_value(&value)),
                ("rk2", attribute_value(&[&float_sort_key(score), field.as_bytes()].concat())),
            ],
        );
        put.return_consumed_capacity = Some("TOTAL".to_owned());
        let result = self.client.put_item(put).await.spanify_err()?;
        record_cap(&result.consumed_capacity, &span);
        Ok(())
    }

    async fn zh_rem_impl<'a, 'b, F: Into<Arg<'b>> + Send>(&self, key: ExplicitKey<'a>, field: F) -> Result<()> {
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));
        let mut delete = rusoto_dynamodb::DeleteItemInput::default();
        delete.table_name = self.table_name.clone();
        delete.key = composite_key(&key, field);
        delete.return_consumed_capacity = Some("TOTAL".to_owned());
        let result = self.client.delete_item(delete).await.spanify_err()?;
        record_cap(&result.consumed_capacity, &span);
        Ok(())
    }

    async fn zh_count_impl<'a>(&self, key: ExplicitKey<'a>, min: f64, max: f64) -> Result<usize> {
        if min > max {
            return Ok(0);
        }

        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));
        let (min, max) = score_bounds(min, max);
        let (condition, attribute_values) = query_condition(key, min, max, true);

        let mut query = rusoto_dynamodb::QueryInput::default();
        query.table_name = self.table_name.clone();
        query.consistent_read = Some(!self.allow_eventually_consistent_reads);
        query.key_condition_expression = Some(condition.clone());
        query.expression_attribute_values = Some(attribute_values.clone());
        query.index_name = Some("rk2".to_string());
        query.select = Some("COUNT".to_string());
        query.return_consumed_capacity = Some("TOTAL".to_owned());

        let mut count = 0;
        let mut cap = TotalConsumedCapacity::default();

        loop {
            let result = self.client.query(query.clone()).await.spanify_err()?;
            if let Some(n) = result.count {
                count += n as usize;
            }
            if let Some(c) = result.consumed_capacity {
                cap.add(&c);
            }
            match result.last_evaluated_key {
                Some(key) => query.exclusive_start_key = Some(key),
                None => break,
            }
        }

        cap.record_to(&span);
        Ok(count)
    }
}

#[async_trait]
impl super::Backend for Backend {
    #[tracing::instrument(skip_all, fields(key, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn get<'a, K: Key<'a>>(&self, key: K) -> Result<Option<Value>> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));
        let mut get = rusoto_dynamodb::GetItemInput::default();
        get.consistent_read = Some(!self.allow_eventually_consistent_reads);
        get.key = composite_key(&key, NO_SORT_KEY);
        get.table_name = self.table_name.clone();
        get.return_consumed_capacity = Some("TOTAL".to_owned());
        let result = self.client.get_item(get).await.spanify_err()?;
        record_cap(&result.consumed_capacity, &span);
        Ok(result.item.and_then(|mut item| item.remove("v")).and_then(|v| v.b).map(|v| v.to_vec().into()))
    }

    #[tracing::instrument(skip_all, fields(key, consumed_wcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn set<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));
        let mut put = rusoto_dynamodb::PutItemInput::default();
        put.table_name = self.table_name.clone();
        put.item = new_item(&key, NO_SORT_KEY, vec![("v", attribute_value(value))]);
        put.return_consumed_capacity = Some("TOTAL".to_owned());
        let result = self.client.put_item(put).await.spanify_err()?;
        record_cap(&result.consumed_capacity, &span);
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(key, consumed_wcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn set_eq<'a, 'b, 'c, K: Key<'a>, V: Into<Arg<'b>> + Send, OV: Into<Arg<'c>> + Send>(&self, key: K, value: V, old_value: OV) -> Result<bool> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));
        let mut put = rusoto_dynamodb::PutItemInput::default();
        put.table_name = self.table_name.clone();
        put.item = new_item(&key, NO_SORT_KEY, vec![("v", attribute_value(value))]);
        put.condition_expression = Some("v = :v".to_string());
        put.expression_attribute_values = Some(vec![(":v".to_string(), attribute_value(old_value))].into_iter().collect());
        put.return_consumed_capacity = Some("TOTAL".to_owned());
        match self.client.put_item(put).await {
            Ok(result) => {
                record_cap(&result.consumed_capacity, &span);
                Ok(true)
            }
            Err(RusotoError::Service(rusoto_dynamodb::PutItemError::ConditionalCheckFailed(_))) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    #[tracing::instrument(skip_all, fields(key, consumed_wcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn set_nx<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<bool> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));
        let mut put = rusoto_dynamodb::PutItemInput::default();
        put.table_name = self.table_name.clone();
        put.item = new_item(&key, NO_SORT_KEY, vec![("v", attribute_value(value))]);
        put.condition_expression = Some("attribute_not_exists(v)".to_string());
        put.return_consumed_capacity = Some("TOTAL".to_owned());
        match self.client.put_item(put).await {
            Ok(result) => {
                record_cap(&result.consumed_capacity, &span);
                Ok(true)
            }
            Err(RusotoError::Service(rusoto_dynamodb::PutItemError::ConditionalCheckFailed(_))) => Ok(false),
            Err(e) => Err(e.spanify().into()),
        }
    }

    #[tracing::instrument(skip_all, fields(key, consumed_wcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn delete<'a, K: Key<'a>>(&self, key: K) -> Result<bool> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));
        let mut delete = rusoto_dynamodb::DeleteItemInput::default();
        delete.table_name = self.table_name.clone();
        delete.key = composite_key(&key, NO_SORT_KEY);
        delete.return_values = Some("ALL_OLD".to_string());
        delete.return_consumed_capacity = Some("TOTAL".to_owned());
        let result = self.client.delete_item(delete).await.spanify_err()?;
        record_cap(&result.consumed_capacity, &span);
        Ok(result.attributes.is_some())
    }

    #[tracing::instrument(skip_all, err(Display), fields(key, consumed_wcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn s_add<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));
        let value = value.into();
        let mut v = AttributeValue::default();
        v.bs = Some(vec![bytes::Bytes::copy_from_slice(value.as_bytes())]);
        let mut update = rusoto_dynamodb::UpdateItemInput::default();
        update.key = composite_key(&key, NO_SORT_KEY);
        update.table_name = self.table_name.clone();
        update.update_expression = Some("ADD v :v".to_string());
        update.expression_attribute_values = Some(vec![(":v".to_string(), v)].into_iter().collect());
        update.return_consumed_capacity = Some("TOTAL".to_owned());
        let result = self.client.update_item(update).await.spanify_err()?;
        record_cap(&result.consumed_capacity, &span);
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(key, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn s_members<'a, K: Key<'a>>(&self, key: K) -> Result<Vec<Value>> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));
        let mut get = rusoto_dynamodb::GetItemInput::default();
        get.consistent_read = Some(!self.allow_eventually_consistent_reads);
        get.key = composite_key(&key, NO_SORT_KEY);
        get.table_name = self.table_name.clone();
        get.return_consumed_capacity = Some("TOTAL".to_owned());
        let result = self.client.get_item(get).await.spanify_err()?;
        record_cap(&result.consumed_capacity, &span);
        Ok(result
            .item
            .and_then(|mut item| item.remove("v"))
            .and_then(|v| v.bs)
            .map(|v| v.iter().map(|v| v.to_vec().into()).collect())
            .unwrap_or(vec![]))
    }

    #[tracing::instrument(skip_all, fields(key, consumed_wcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn n_incr_by<'a, K: Key<'a>>(&self, key: K, n: i64) -> Result<i64> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));
        let mut v = AttributeValue::default();
        v.n = Some(n.to_string());
        let mut update = rusoto_dynamodb::UpdateItemInput::default();
        update.key = composite_key(&key, NO_SORT_KEY);
        update.table_name = self.table_name.clone();
        update.update_expression = Some("ADD v :n".to_string());
        update.expression_attribute_values = Some(vec![(":n".to_string(), v)].into_iter().collect());
        update.return_values = Some("ALL_NEW".to_string());
        update.return_consumed_capacity = Some("TOTAL".to_owned());
        let output = self.client.update_item(update).await.spanify_err()?;
        record_cap(&output.consumed_capacity, &span);
        let new_value = match output.attributes.and_then(|mut h| h.remove("v")).and_then(|v| v.n) {
            Some(v) => v,
            None => return Err(SimpleError::new("new value not returned by dynamodb").spanify().into()),
        };
        Ok(new_value.parse().spanify_err()?)
    }

    #[tracing::instrument(skip_all, fields(key, consumed_wcu, otel.status, error.msg, otel.span_kind = "client"))]
    async fn h_set<'a, 'b, 'c, K: Key<'a>, F: Into<Arg<'b>> + Send, V: Into<Arg<'c>> + Send, I: IntoIterator<Item = (F, V)> + Send>(
        &self,
        key: K,
        fields: I,
    ) -> Result<()> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));
        let (names, values): (HashMap<_, _>, HashMap<_, _>) = fields
            .into_iter()
            .enumerate()
            .map(|(i, f)| {
                let mut v = AttributeValue::default();
                v.b = Some(f.1.into().into_vec().into());
                ((format!("#n{}", i), encode_field_name(f.0.into().as_bytes())), (format!(":n{}", i), v))
            })
            .unzip();
        let mut update = rusoto_dynamodb::UpdateItemInput::default();
        update.key = composite_key(&key, NO_SORT_KEY);
        update.table_name = self.table_name.clone();
        update.update_expression = Some(format!(
            "SET {}",
            (0..names.len()).map(|i| format!("#n{} = :n{}", i, i)).collect::<Vec<_>>().join(", ")
        ));
        update.expression_attribute_values = Some(values);
        update.expression_attribute_names = Some(names);
        update.return_consumed_capacity = Some("TOTAL".to_owned());
        let result = self.client.update_item(update).await.spanify_err()?;
        record_cap(&result.consumed_capacity, &span);
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(key, consumed_wcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn h_del<'a, 'b, K: Key<'a>, F: Into<Arg<'b>> + Send, I: IntoIterator<Item = F> + Send>(&self, key: K, fields: I) -> Result<()> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));
        let names: HashMap<_, _> = fields
            .into_iter()
            .enumerate()
            .map(|(i, f)| (format!("#n{}", i), encode_field_name(f.into().as_bytes())))
            .collect();
        let mut update = rusoto_dynamodb::UpdateItemInput::default();
        update.key = composite_key(&key, NO_SORT_KEY);
        update.table_name = self.table_name.clone();
        update.update_expression = Some(format!(
            "REMOVE {}",
            (0..names.len()).map(|i| format!("#n{}", i)).collect::<Vec<_>>().join(", ")
        ));
        update.expression_attribute_names = Some(names);
        update.return_consumed_capacity = Some("TOTAL".to_owned());
        let result = self.client.update_item(update).await.spanify_err()?;
        record_cap(&result.consumed_capacity, &span);
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(key, consumed_rcu, otel.status, error.msg, otel.span_kind = "client"))]
    async fn h_get<'a, 'b, K: Key<'a>, F: Into<Arg<'b>> + Send>(&self, key: K, field: F) -> Result<Option<Value>> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));
        let mut get = rusoto_dynamodb::GetItemInput::default();
        get.consistent_read = Some(!self.allow_eventually_consistent_reads);
        get.key = composite_key(&key, NO_SORT_KEY);
        get.table_name = self.table_name.clone();
        get.return_consumed_capacity = Some("TOTAL".to_owned());
        let result = self.client.get_item(get).await.spanify_err()?;
        record_cap(&result.consumed_capacity, &span);
        Ok(result
            .item
            .and_then(|mut item| item.remove(&encode_field_name(field.into().as_bytes())))
            .and_then(|v| v.b)
            .map(|v| v.to_vec().into()))
    }

    #[tracing::instrument(skip_all, fields(key, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn h_get_all<'a, K: Key<'a>>(&self, key: K) -> Result<HashMap<Vec<u8>, Value>> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));
        let mut get = rusoto_dynamodb::GetItemInput::default();
        get.consistent_read = Some(!self.allow_eventually_consistent_reads);
        get.key = composite_key(&key, NO_SORT_KEY);
        get.table_name = self.table_name.clone();
        get.return_consumed_capacity = Some("TOTAL".to_owned());
        let result = self.client.get_item(get).await.spanify_err()?;
        record_cap(&result.consumed_capacity, &span);
        Ok(result
            .item
            .map(|item| {
                item.into_iter()
                    .filter_map(|(name, v)| decode_field_name(&name).and_then(|name| v.b.map(|v| (name, (*v).into()))))
                    .collect()
            })
            .unwrap_or(HashMap::new()))
    }

    #[tracing::instrument(skip_all, fields(key, consumed_wcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn z_add<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V, score: f64) -> Result<()> {
        let v = value.into();
        self.zh_add_impl(key.into(), &v, &v, score).await
    }

    #[tracing::instrument(skip_all, fields(key, consumed_wcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn zh_add<'a, 'b, 'c, K: Key<'a>, F: Into<Arg<'b>> + Send, V: Into<Arg<'c>> + Send>(&self, key: K, field: F, value: V, score: f64) -> Result<()> {
        self.zh_add_impl(key.into(), field, value, score).await
    }

    #[tracing::instrument(skip_all, fields(key, consumed_wcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn zh_rem<'a, 'b, K: Key<'a>, F: Into<Arg<'b>> + Send>(&self, key: K, field: F) -> Result<()> {
        self.zh_rem_impl(key.into(), field).await
    }

    #[tracing::instrument(skip_all, fields(key, consumed_wcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn z_rem<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        self.zh_rem_impl(key.into(), value).await
    }

    #[tracing::instrument(skip_all, fields(key, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn z_count<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64) -> Result<usize> {
        self.zh_count_impl(key.into(), min, max).await
    }

    #[tracing::instrument(skip_all, fields(key, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn zh_count<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64) -> Result<usize> {
        self.zh_count_impl(key.into(), min, max).await
    }

    #[tracing::instrument(skip_all, fields(key, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn z_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        let (min, max) = score_bounds(min, max);
        self.zh_range_impl(key.into(), min.into(), max.into(), limit, false, true).await
    }

    #[tracing::instrument(skip_all, fields(key, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn zh_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        let (min, max) = score_bounds(min, max);
        self.zh_range_impl(key.into(), min.into(), max.into(), limit, false, true).await
    }

    #[tracing::instrument(skip_all, fields(key, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn z_rev_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        let (min, max) = score_bounds(min, max);
        self.zh_range_impl(key.into(), min.into(), max.into(), limit, true, true).await
    }

    #[tracing::instrument(skip_all, fields(key, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn zh_rev_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        let (min, max) = score_bounds(min, max);
        self.zh_range_impl(key.into(), min.into(), max.into(), limit, true, true).await
    }

    #[tracing::instrument(skip_all, fields(key, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn z_range_by_lex<'a, 'b, 'c, K: Key<'a>, M: Into<Arg<'b>> + Send, N: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        min: Bound<M>,
        max: Bound<N>,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let (min, max) = lex_bounds(0.0, min, max);
        self.zh_range_impl(key.into(), min, max, limit, false, true).await
    }

    #[tracing::instrument(skip_all, fields(key, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn z_rev_range_by_lex<'a, 'b, 'c, K: Key<'a>, M: Into<Arg<'b>> + Send, N: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        min: Bound<M>,
        max: Bound<N>,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let (min, max) = lex_bounds(0.0, min, max);
        self.zh_range_impl(key.into(), min, max, limit, true, true).await
    }

    #[tracing::instrument(skip_all, fields(consumed_rcu, error.msg, otel.status_code, otel.span_kind = "client"))]
    async fn exec_batch(&self, op: BatchOperation<'_>) -> Result<()> {
        if op.ops.is_empty() {
            return Ok(());
        }

        let mut keys: Vec<_> = op
            .ops
            .iter()
            .map(|op| match op {
                BatchSubOperation::Get(key, _) => composite_key(&key, NO_SORT_KEY),
            })
            .collect();

        struct Op<'a> {
            tx: &'a mpsc::SyncSender<Value>,
            _span: Span,
        }

        let mut ops: HashMap<&[u8], Op> = op
            .ops
            .iter()
            .map(|op| match op {
                BatchSubOperation::Get(key, tx) => {
                    let span = info_span!("get", ?key);
                    (key.unredacted.as_bytes(), Op { tx, _span: span })
                }
            })
            .collect();

        const MAX_BATCH_SIZE: usize = 100;

        let mut cap = TotalConsumedCapacity::default();
        while !keys.is_empty() {
            let batch = keys.split_off(if keys.len() > MAX_BATCH_SIZE { keys.len() - MAX_BATCH_SIZE } else { 0 });
            let mut get = rusoto_dynamodb::BatchGetItemInput::default();
            let mut keys_and_attributes = rusoto_dynamodb::KeysAndAttributes::default();
            keys_and_attributes.consistent_read = Some(!self.allow_eventually_consistent_reads);
            keys_and_attributes.keys = batch;
            get.request_items.insert(self.table_name.clone(), keys_and_attributes);
            get.return_consumed_capacity = Some("TOTAL".to_owned());
            let result = self.client.batch_get_item(get).await.spanify_err()?;
            for c in result.consumed_capacity.iter().flatten() {
                cap.add(&c);
            }

            if let Some(items) = result.responses.and_then(|mut r| r.remove(&self.table_name)) {
                for mut item in items {
                    if let Some(v) = item.remove("v").and_then(|v| v.b).map(|b| b.to_vec().into()) {
                        if let Some(op) = item.remove("hk").and_then(|hk| hk.b).and_then(|b| ops.remove(&b as &[u8])) {
                            let _ = op.tx.try_send(v);
                        }
                    }
                }
            }

            if let Some(unprocessed) = result.unprocessed_keys.and_then(|mut k| k.remove(&self.table_name)) {
                keys.extend(unprocessed.keys);
            }
        }
        cap.record_to(&Span::current());

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(consumed_wcu, error.msg, otel.status_code, otel.kind = "client"))]
    async fn exec_atomic_write(&self, op: AtomicWriteOperation<'_>) -> Result<bool> {
        let mut token = Vec::new();
        token.resize(20, 0u8);
        rand::thread_rng().fill_bytes(&mut token);
        let token = base64::encode_config(token, base64::URL_SAFE_NO_PAD);

        struct SubOpState<'a> {
            key: ExplicitKey<'a>,
            failure_tx: Option<SyncSender<bool>>,
            span: Span,
        }

        let mut tx = rusoto_dynamodb::TransactWriteItemsInput::default();
        tx.client_request_token = Some(token);
        tx.return_consumed_capacity = Some("TOTAL".to_owned());
        let (transact_items, states): (Vec<_>, Vec<_>) = op
            .ops
            .into_iter()
            .map(|op| match op {
                AtomicWriteSubOperation::Set(key, value) => {
                    let span = info_span!(
                        "set",
                        ?key,
                        error.code = Empty,
                        error.msg = Empty,
                        otel.status_code = Empty,
                        otel.kind = "client"
                    );
                    let mut put = Put::default();
                    put.table_name = self.table_name.clone();
                    put.item = new_item(&key, NO_SORT_KEY, vec![("v", attribute_value(value))]);
                    let mut item = TransactWriteItem::default();
                    item.put = Some(put);
                    (
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: None,
                            span,
                        },
                    )
                }
                AtomicWriteSubOperation::SetEQ(key, value, old_value, tx) => {
                    let span = info_span!(
                        "set_eq",
                        ?key,
                        error.code = Empty,
                        error.msg = Empty,
                        otel.status_code = Empty,
                        otel.kind = "client"
                    );
                    let mut put = Put::default();
                    put.table_name = self.table_name.clone();
                    put.item = new_item(&key, NO_SORT_KEY, vec![("v", attribute_value(value))]);
                    put.condition_expression = Some("v = :v".to_string());
                    put.expression_attribute_values = Some(vec![(":v".to_string(), attribute_value(old_value))].into_iter().collect());
                    let mut item = TransactWriteItem::default();
                    item.put = Some(put);
                    (
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: Some(tx),
                            span,
                        },
                    )
                }
                AtomicWriteSubOperation::SetNX(key, value, tx) => {
                    let span = info_span!(
                        "set_nx",
                        ?key,
                        error.code = Empty,
                        error.msg = Empty,
                        otel.status_code = Empty,
                        otel.kind = "client"
                    );
                    let mut put = Put::default();
                    put.table_name = self.table_name.clone();
                    put.item = new_item(&key, NO_SORT_KEY, vec![("v", attribute_value(value))]);
                    put.condition_expression = Some("attribute_not_exists(v)".to_string());
                    let mut item = TransactWriteItem::default();
                    item.put = Some(put);
                    (
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: Some(tx),
                            span,
                        },
                    )
                }
                AtomicWriteSubOperation::Delete(key) => {
                    let span = info_span!(
                        "delete",
                        ?key,
                        error.code = Empty,
                        error.msg = Empty,
                        otel.status_code = Empty,
                        otel.kind = "client"
                    );
                    let mut delete = Delete::default();
                    delete.table_name = self.table_name.clone();
                    delete.key = composite_key(&key, NO_SORT_KEY);
                    let mut item = TransactWriteItem::default();
                    item.delete = Some(delete);
                    (
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: None,
                            span,
                        },
                    )
                }
                AtomicWriteSubOperation::DeleteXX(key, tx) => {
                    let span = info_span!(
                        "delete_xx",
                        ?key,
                        error.code = Empty,
                        error.msg = Empty,
                        otel.status_code = Empty,
                        otel.kind = "client"
                    );
                    let mut delete = Delete::default();
                    delete.table_name = self.table_name.clone();
                    delete.key = composite_key(&key, NO_SORT_KEY);
                    delete.condition_expression = Some("attribute_exists(v)".to_string());
                    let mut item = TransactWriteItem::default();
                    item.delete = Some(delete);
                    (
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: Some(tx),
                            span,
                        },
                    )
                }
                AtomicWriteSubOperation::SAdd(key, value) => {
                    let span = info_span!(
                        "s_add",
                        ?key,
                        error.code = Empty,
                        error.msg = Empty,
                        otel.status_code = Empty,
                        otel.kind = "client"
                    );
                    let mut v = AttributeValue::default();
                    v.bs = Some(vec![bytes::Bytes::copy_from_slice(value.as_bytes())]);
                    let mut update = Update::default();
                    update.table_name = self.table_name.clone();
                    update.key = composite_key(&key, NO_SORT_KEY);
                    update.update_expression = "ADD v :v".to_string();
                    update.expression_attribute_values = Some(vec![(":v".to_string(), v)].into_iter().collect());
                    let mut item = TransactWriteItem::default();
                    item.update = Some(update);
                    (
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: None,
                            span,
                        },
                    )
                }
                AtomicWriteSubOperation::SRem(key, value) => {
                    let span = info_span!(
                        "s_rem",
                        ?key,
                        error.code = Empty,
                        error.msg = Empty,
                        otel.status_code = Empty,
                        otel.kind = "client"
                    );
                    let mut v = AttributeValue::default();
                    v.bs = Some(vec![bytes::Bytes::copy_from_slice(value.as_bytes())]);
                    let mut update = Update::default();
                    update.table_name = self.table_name.clone();
                    update.key = composite_key(&key, NO_SORT_KEY);
                    update.update_expression = "DELETE v :v".to_string();
                    update.expression_attribute_values = Some(vec![(":v".to_string(), v)].into_iter().collect());
                    let mut item = TransactWriteItem::default();
                    item.update = Some(update);
                    (
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: None,
                            span,
                        },
                    )
                }
                AtomicWriteSubOperation::ZAdd(key, value, score) => {
                    let span = info_span!(
                        "z_add",
                        ?key,
                        error.code = Empty,
                        error.msg = Empty,
                        otel.status_code = Empty,
                        otel.kind = "client"
                    );
                    let mut put = Put::default();
                    put.table_name = self.table_name.clone();
                    put.item = new_item(
                        &key,
                        &value,
                        vec![
                            ("v", attribute_value(&value)),
                            ("rk2", attribute_value(&[&float_sort_key(score), value.as_bytes()].concat())),
                        ],
                    );
                    let mut item = TransactWriteItem::default();
                    item.put = Some(put);
                    (
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: None,
                            span,
                        },
                    )
                }
                AtomicWriteSubOperation::ZHAdd(key, field, value, score) => {
                    let span = info_span!(
                        "zh_add",
                        ?key,
                        error.code = Empty,
                        error.msg = Empty,
                        otel.status_code = Empty,
                        otel.kind = "client"
                    );
                    let mut put = Put::default();
                    put.table_name = self.table_name.clone();
                    put.item = new_item(
                        &key,
                        &field,
                        vec![
                            ("v", attribute_value(&value)),
                            ("rk2", attribute_value(&[&float_sort_key(score), field.as_bytes()].concat())),
                        ],
                    );
                    let mut item = TransactWriteItem::default();
                    item.put = Some(put);
                    (
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: None,
                            span,
                        },
                    )
                }
                AtomicWriteSubOperation::ZRem(key, value) => {
                    let span = info_span!(
                        "z_rem",
                        ?key,
                        error.code = Empty,
                        error.msg = Empty,
                        otel.status_code = Empty,
                        otel.kind = "client"
                    );
                    let mut delete = Delete::default();
                    delete.table_name = self.table_name.clone();
                    delete.key = composite_key(&key, value);
                    let mut item = TransactWriteItem::default();
                    item.delete = Some(delete);
                    (
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: None,
                            span,
                        },
                    )
                }
                AtomicWriteSubOperation::ZHRem(key, field) => {
                    let span = info_span!(
                        "zh_rem",
                        ?key,
                        error.code = Empty,
                        error.msg = Empty,
                        otel.status_code = Empty,
                        otel.kind = "client"
                    );
                    let mut delete = Delete::default();
                    delete.table_name = self.table_name.clone();
                    delete.key = composite_key(&key, field);
                    let mut item = TransactWriteItem::default();
                    item.delete = Some(delete);
                    (
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: None,
                            span,
                        },
                    )
                }
                AtomicWriteSubOperation::HSet(key, fields) => {
                    let span = info_span!(
                        "h_set",
                        ?key,
                        error.code = Empty,
                        error.msg = Empty,
                        otel.status_code = Empty,
                        otel.kind = "client"
                    );
                    let (names, values): (HashMap<_, _>, HashMap<_, _>) = fields
                        .into_iter()
                        .enumerate()
                        .map(|(i, f)| {
                            let mut v = AttributeValue::default();
                            v.b = Some(f.1.into_vec().into());
                            ((format!("#n{}", i), encode_field_name(f.0.as_bytes())), (format!(":n{}", i), v))
                        })
                        .unzip();
                    let mut update = Update::default();
                    update.table_name = self.table_name.clone();
                    update.key = composite_key(&key, NO_SORT_KEY);
                    update.update_expression = format!("SET {}", (0..names.len()).map(|i| format!("#n{} = :n{}", i, i)).collect::<Vec<_>>().join(", "));
                    update.expression_attribute_values = Some(values);
                    update.expression_attribute_names = Some(names);
                    let mut item = TransactWriteItem::default();
                    item.update = Some(update);
                    (
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: None,
                            span,
                        },
                    )
                }
                AtomicWriteSubOperation::HSetNX(key, field, value, tx) => {
                    let span = info_span!(
                        "h_set_nx",
                        ?key,
                        error.code = Empty,
                        error.msg = Empty,
                        otel.status_code = Empty,
                        otel.kind = "client"
                    );
                    let mut update = Update::default();
                    update.table_name = self.table_name.clone();
                    update.key = composite_key(&key, NO_SORT_KEY);
                    update.condition_expression = Some("attribute_not_exists(#f)".to_string());
                    update.update_expression = "SET #f = :v".to_string();
                    let mut v = AttributeValue::default();
                    v.b = Some(value.into_vec().into());
                    update.expression_attribute_values = Some(vec![(":v".to_string(), v)].into_iter().collect());
                    update.expression_attribute_names = Some(vec![("#f".to_string(), encode_field_name(field.as_bytes()))].into_iter().collect());
                    let mut item = TransactWriteItem::default();
                    item.update = Some(update);
                    (
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: Some(tx),
                            span,
                        },
                    )
                }
                AtomicWriteSubOperation::HDel(key, fields) => {
                    let span = info_span!(
                        "h_del",
                        ?key,
                        error.code = Empty,
                        error.msg = Empty,
                        otel.status_code = Empty,
                        otel.kind = "client"
                    );
                    let names: HashMap<_, _> = fields
                        .into_iter()
                        .enumerate()
                        .map(|(i, f)| (format!("#n{}", i), encode_field_name(f.as_bytes())))
                        .collect();
                    let mut update = Update::default();
                    update.key = composite_key(&key, NO_SORT_KEY);
                    update.table_name = self.table_name.clone();
                    update.update_expression = format!("REMOVE {}", (0..names.len()).map(|i| format!("#n{}", i)).collect::<Vec<_>>().join(", "));
                    update.expression_attribute_names = Some(names);
                    let mut item = TransactWriteItem::default();
                    item.update = Some(update);
                    (
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: None,
                            span,
                        },
                    )
                }
            })
            .unzip();
        tx.transact_items = transact_items;

        match self.client.transact_write_items(tx).await {
            Err(RusotoError::Service(rusoto_dynamodb::TransactWriteItemsError::TransactionCanceled { reasons, .. })) => {
                let mut err = None;
                for (i, reason) in reasons.into_iter().enumerate() {
                    let Some(code) = reason.code.as_deref() else {
                        continue;
                    };
                    let state = &states[i];
                    match code {
                        "None" => continue,
                        "ConditionalCheckFailed" => {
                            if let Some(ref tx) = state.failure_tx {
                                let _ = tx.try_send(true);
                            }
                        }
                        "AtomicWriteConflict" => {
                            err.get_or_insert_with(|| Error::AtomicWriteConflict(state.key.clone()));
                        }
                        _ => {
                            err.get_or_insert_with(|| Error::Other(format!("{:?} failed with {}", &state.key, code).into()));
                        }
                    }
                    state.span.record("otel.status_code", "ERROR");
                    state.span.record("error.code", &code);
                    if let Some(msg) = reason.message {
                        state.span.record("error.msg", msg);
                    }
                }
                let span = Span::current();
                span.record("otel.status_code", "ERROR");
                span.record("error.msg", "transaction canceled");
                return err.map(Err).unwrap_or(Ok(false));
            }
            Err(e) => Err(e.spanify().into()),
            Ok(r) => {
                let mut cap = TotalConsumedCapacity::default();
                for c in r.consumed_capacity.iter().flatten() {
                    cap.add(&c);
                }
                cap.record_to(&Span::current());
                Ok(true)
            }
        }
    }
}

/// Tracks total consumed capacity by repeated/batch operations.
#[derive(Default)]
struct TotalConsumedCapacity {
    total_rcu: Option<f64>,
    total_wcu: Option<f64>,
}

impl TotalConsumedCapacity {
    fn add(&mut self, c: &ConsumedCapacity) {
        if let Some(rcu) = c.read_capacity_units {
            *self.total_rcu.get_or_insert(0.) += rcu;
        }
        if let Some(wcu) = c.write_capacity_units {
            *self.total_wcu.get_or_insert(0.) += wcu;
        }
    }

    /// Records to the given span, which must already have empty `consumed_rcu` and `consumed_wcu`
    /// attributes or recording those will do nothing.
    fn record_to(&self, span: &Span) {
        if let Some(rcu) = self.total_rcu {
            span.record("consumed_rcu", rcu);
        }
        if let Some(wcu) = self.total_wcu {
            span.record("consumed_wcu", wcu);
        }
    }
}

/// Records capacity used by a non-batch operation to the given span.
fn record_cap(capacity: &Option<ConsumedCapacity>, span: &Span) {
    let Some(capacity) = capacity else { return };
    if let Some(rcu) = capacity.read_capacity_units {
        span.record("consumed_rcu", rcu);
    }
    if let Some(wcu) = capacity.write_capacity_units {
        span.record("consumed_wcu", wcu);
    }
}

pub async fn create_default_table(client: &DynamoDbClient, table_name: &str) -> Result<()> {
    let mut create = rusoto_dynamodb::CreateTableInput::default();
    create.attribute_definitions = vec![
        AttributeDefinition {
            attribute_name: "hk".to_string(),
            attribute_type: "B".to_string(),
        },
        AttributeDefinition {
            attribute_name: "rk".to_string(),
            attribute_type: "B".to_string(),
        },
        AttributeDefinition {
            attribute_name: "rk2".to_string(),
            attribute_type: "B".to_string(),
        },
    ];
    create.key_schema = vec![
        KeySchemaElement {
            attribute_name: "hk".to_string(),
            key_type: "HASH".to_string(),
        },
        KeySchemaElement {
            attribute_name: "rk".to_string(),
            key_type: "RANGE".to_string(),
        },
    ];
    create.local_secondary_indexes = Some(vec![LocalSecondaryIndex {
        index_name: "rk2".to_string(),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "hk".to_string(),
                key_type: "HASH".to_string(),
            },
            KeySchemaElement {
                attribute_name: "rk2".to_string(),
                key_type: "RANGE".to_string(),
            },
        ],
        projection: Projection {
            non_key_attributes: None,
            projection_type: Some("ALL".to_string()),
        },
    }]);
    create.table_name = table_name.to_string();
    create.billing_mode = Some("PAY_PER_REQUEST".to_string());
    client.create_table(create).await?;
    Ok(())
}

#[cfg(test)]
mod test {
    mod backend {
        use crate::{rusoto_dynamodbstore, test_backend};
        use rusoto_core::{region::Region, request::HttpClient, RusotoError};
        use rusoto_credential::StaticProvider;
        use rusoto_dynamodb::{DescribeTableError, DynamoDb, DynamoDbClient};
        use tokio::time;

        test_backend!(|| async {
            // expects DynamoDB local to be running: docker run -p 8000:8000 --rm -it amazon/dynamodb-local
            let endpoint = std::env::var("DYNAMODB_ENDPOINT").unwrap_or("http://localhost:8000".to_string());
            let client = DynamoDbClient::new_with(
                HttpClient::new().unwrap(),
                StaticProvider::new_minimal("ACCESSKEYID".to_string(), "SECRET".to_string()),
                Region::Custom {
                    name: "test".to_string(),
                    endpoint,
                },
            );

            let table_name = "BackendTest".to_string();

            if let Ok(_) = client
                .delete_table(rusoto_dynamodb::DeleteTableInput {
                    table_name: table_name.clone(),
                })
                .await
            {
                for _ in 0..10u32 {
                    match client
                        .describe_table(rusoto_dynamodb::DescribeTableInput {
                            table_name: table_name.clone(),
                        })
                        .await
                    {
                        Err(RusotoError::Service(DescribeTableError::ResourceNotFound(_))) => break,
                        _ => time::sleep(time::Duration::from_millis(200)).await,
                    }
                }
            }

            rusoto_dynamodbstore::create_default_table(&client, &table_name)
                .await
                .expect("failed to create table");

            rusoto_dynamodbstore::Backend {
                allow_eventually_consistent_reads: false,
                client,
                table_name: table_name.clone(),
            }
        });
    }
}
