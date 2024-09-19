use crate::{ExplicitKey, ResultExt};

use super::{Arg, AtomicWriteOperation, AtomicWriteSubOperation, BatchOperation, BatchSubOperation, Bound, Error, Key, Result, Value};
use aws_sdk_dynamodb::{
    client::Client,
    operation::{put_item::PutItemError, transact_write_items::TransactWriteItemsError},
    primitives::Blob,
    types::{
        AttributeDefinition, AttributeValue, BillingMode, ConsumedCapacity, Delete, KeySchemaElement, KeyType, KeysAndAttributes, LocalSecondaryIndex,
        Projection, ProjectionType, Put, ReturnValue, ScalarAttributeType, Select, TransactWriteItem, Update,
    },
};
use itertools::Itertools as _;
use rand::RngCore;
use simple_error::SimpleError;
use std::{collections::HashMap, sync::mpsc};
use tracing::{field::Empty, info_span, Span};

#[derive(Clone)]
pub struct Backend {
    pub allow_eventually_consistent_reads: bool,
    pub client: Client,
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

fn composite_key<'h, 's, S: Into<Arg<'s>> + Send>(hash: &ExplicitKey<'h>, sort: S) -> HashMap<String, AttributeValue> {
    let mut ret = HashMap::new();
    ret.insert("hk".to_string(), attribute_value(hash.unredacted.as_bytes()));
    ret.insert("rk".to_string(), attribute_value(sort));
    ret
}

fn attribute_value<'a, V: Into<Arg<'a>>>(v: V) -> AttributeValue {
    AttributeValue::B(Blob::new(v.into().into_vec()))
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

fn query_condition(key: ExplicitKey<'_>, min: ArgBound<'_>, max: ArgBound<'_>, secondary_index: bool) -> (String, HashMap<String, AttributeValue>) {
    let mut attribute_values = HashMap::new();
    attribute_values.insert(":hash".to_string(), attribute_value(key.unredacted));

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
    async fn z_range_impl<'k, 'm, 'n, K: Key<'k>, M: Into<Arg<'m>>, N: Into<Arg<'n>>>(
        &self,
        key: K,
        min: Bound<M>,
        max: Bound<N>,
        limit: usize,
        reverse: bool,
        secondary_index: bool,
    ) -> Result<Vec<Value>> {
        let key = key.into();

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

        let (condition, attribute_values) = query_condition(key.into(), inclusive_min, inclusive_max, secondary_index);

        let mut query = self
            .client
            .query()
            .table_name(self.table_name.clone())
            .consistent_read(!self.allow_eventually_consistent_reads)
            .key_condition_expression(condition.clone())
            .set_expression_attribute_values(Some(attribute_values.clone()))
            .scan_index_forward(!reverse)
            .set_index_name(if secondary_index { Some("rk2".to_string()) } else { None });

        let mut members = vec![];

        while limit == 0 || members.len() < limit {
            let mut q = query.clone();
            if limit > 0 {
                q = q.limit((limit - members.len() + 1) as _);
            }
            let result = q.send().await.spanify_err()?;
            if let Some(mut items) = result.items {
                let mut skip = 0;

                // dynamodb can't express exclusive ranges so we did an inclusive query. that means
                // we may need to skip the first or last result
                if !items.is_empty() {
                    if members.is_empty() {
                        if let Bound::Excluded(excluded_key) = if reverse { &max } else { &min } {
                            if let Some(first_item_key) = items[0].get(if secondary_index { "rk2" } else { "rk" }).and_then(|v| v.as_b().ok()) {
                                if excluded_key.as_bytes() == first_item_key.as_ref() {
                                    skip = 1;
                                }
                            }
                        }
                    }

                    if result.last_evaluated_key.is_none() {
                        if let Bound::Excluded(excluded_key) = if reverse { &min } else { &max } {
                            if let Some(last_item_key) = items
                                .last()
                                .and_then(|item| item.get(if secondary_index { "rk2" } else { "rk" }).and_then(|v| v.as_b().ok()))
                            {
                                if excluded_key.as_bytes() == last_item_key.as_ref() {
                                    items.pop();
                                }
                            }
                        }
                    }
                }

                members.extend(items.into_iter().skip(skip).filter_map(|mut v| {
                    v.remove("v").and_then(|v| match v {
                        AttributeValue::B(blob) => Some(blob.into_inner().into()),
                        _ => None,
                    })
                }));
            }
            match result.last_evaluated_key {
                Some(key) => query = query.set_exclusive_start_key(Some(key)),
                None => break,
            }
        }

        while limit > 0 && members.len() > limit {
            members.pop();
        }
        Ok(members)
    }
}

#[async_trait]
impl super::Backend for Backend {
    #[tracing::instrument(skip_all, fields(key, consistent = !self.allow_eventually_consistent_reads, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn get<'a, K: Key<'a>>(&self, key: K) -> Result<Option<Value>> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));

        let result = self
            .client
            .get_item()
            .consistent_read(!self.allow_eventually_consistent_reads)
            .set_key(Some(composite_key(&key, NO_SORT_KEY)))
            .table_name(self.table_name.clone())
            .send()
            .await
            .spanify_err()?;

        record_rcu(&result.consumed_capacity, &span);

        Ok(result.item.and_then(|mut item| item.remove("v")).and_then(|v| match v {
            AttributeValue::B(b) => Some(b.into_inner().into()),
            _ => None,
        }))
    }

    #[tracing::instrument(skip_all, fields(key, consumed_wcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn set<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));

        let result = self
            .client
            .put_item()
            .table_name(self.table_name.clone())
            .set_item(Some(new_item(&key, NO_SORT_KEY, vec![("v", attribute_value(value))])))
            .send()
            .await
            .spanify_err()?;

        record_wcu(&result.consumed_capacity, &span);

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(key, consumed_wcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn set_eq<'a, 'b, 'c, K: Key<'a>, V: Into<Arg<'b>> + Send, OV: Into<Arg<'c>> + Send>(&self, key: K, value: V, old_value: OV) -> Result<bool> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));

        match self
            .client
            .put_item()
            .table_name(self.table_name.clone())
            .set_item(Some(new_item(&key, NO_SORT_KEY, vec![("v", attribute_value(value))])))
            .condition_expression("v = :v")
            .expression_attribute_values(":v", attribute_value(old_value))
            .send()
            .await
            .map_err(|e| e.into_service_error())
        {
            Ok(result) => {
                record_wcu(&result.consumed_capacity, &span);

                Ok(true)
            }
            Err(PutItemError::ConditionalCheckFailedException(_)) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    #[tracing::instrument(skip_all, fields(key, consumed_wcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn set_nx<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<bool> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));

        match self
            .client
            .put_item()
            .table_name(self.table_name.clone())
            .set_item(Some(new_item(&key, NO_SORT_KEY, vec![("v", attribute_value(value))])))
            .condition_expression("attribute_not_exists(v)")
            .send()
            .await
            .map_err(|e| e.into_service_error())
        {
            Ok(result) => {
                record_wcu(&result.consumed_capacity, &span);

                Ok(true)
            }
            Err(PutItemError::ConditionalCheckFailedException(_)) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    #[tracing::instrument(skip_all, fields(key, consumed_wcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn delete<'a, K: Key<'a>>(&self, key: K) -> Result<bool> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));

        let result = self
            .client
            .delete_item()
            .table_name(self.table_name.clone())
            .set_key(Some(composite_key(&key, NO_SORT_KEY)))
            .return_values(ReturnValue::AllOld)
            .send()
            .await
            .spanify_err()?;

        record_wcu(&result.consumed_capacity, &span);

        Ok(result.attributes.is_some())
    }

    #[tracing::instrument(skip_all, err(Display), fields(key, consumed_wcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn s_add<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));

        let value = value.into();
        let v = AttributeValue::Bs(vec![Blob::new(value.into_vec())]);

        let result = self
            .client
            .update_item()
            .set_key(Some(composite_key(&key, NO_SORT_KEY)))
            .table_name(self.table_name.clone())
            .update_expression("ADD v :v")
            .expression_attribute_values(":v", v)
            .send()
            .await
            .spanify_err()?;

        record_wcu(&result.consumed_capacity, &span);

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(key, consistent = !self.allow_eventually_consistent_reads, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn s_members<'a, K: Key<'a>>(&self, key: K) -> Result<Vec<Value>> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));

        let result = self
            .client
            .get_item()
            .consistent_read(!self.allow_eventually_consistent_reads)
            .set_key(Some(composite_key(&key, NO_SORT_KEY)))
            .table_name(self.table_name.clone())
            .send()
            .await
            .spanify_err()?;

        record_rcu(&result.consumed_capacity, &span);

        Ok(result
            .item
            .and_then(|mut item| item.remove("v"))
            .and_then(|v| match v {
                AttributeValue::Bs(bs) => Some(bs.into_iter().map(|v| v.into_inner().into()).collect()),
                _ => None,
            })
            .unwrap_or(vec![]))
    }

    #[tracing::instrument(skip_all, fields(key, consumed_wcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn n_incr_by<'a, K: Key<'a>>(&self, key: K, n: i64) -> Result<i64> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));

        let output = self
            .client
            .update_item()
            .set_key(Some(composite_key(&key, NO_SORT_KEY)))
            .table_name(self.table_name.clone())
            .update_expression("ADD v :n")
            .expression_attribute_values(":n", AttributeValue::N(n.to_string()))
            .return_values(ReturnValue::AllNew)
            .send()
            .await
            .spanify_err()?;
        record_wcu(&output.consumed_capacity, &span);

        let new_value = output
            .attributes
            .and_then(|mut h| h.remove("v"))
            .and_then(|v| match v {
                AttributeValue::N(n) => Some(n),
                _ => None,
            })
            .ok_or_else(|| SimpleError::new("new value not returned by dynamodb"))?;
        Ok(new_value.parse()?)
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
                let v = AttributeValue::B(Blob::new(f.1.into().into_vec()));
                ((format!("#n{}", i), encode_field_name(f.0.into().as_bytes())), (format!(":n{}", i), v))
            })
            .unzip();
        let result = self
            .client
            .update_item()
            .set_key(Some(composite_key(&key, NO_SORT_KEY)))
            .table_name(self.table_name.clone())
            .update_expression(format!(
                "SET {}",
                (0..names.len()).map(|i| format!("#n{} = :n{}", i, i)).collect::<Vec<_>>().join(", ")
            ))
            .set_expression_attribute_values(Some(values))
            .set_expression_attribute_names(Some(names))
            .send()
            .await
            .spanify_err()?;

        record_wcu(&result.consumed_capacity, &span);

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
        let result = self
            .client
            .update_item()
            .set_key(Some(composite_key(&key, NO_SORT_KEY)))
            .table_name(self.table_name.clone())
            .update_expression(format!(
                "REMOVE {}",
                (0..names.len()).map(|i| format!("#n{}", i)).collect::<Vec<_>>().join(", ")
            ))
            .set_expression_attribute_names(Some(names))
            .send()
            .await
            .spanify_err()?;

        record_wcu(&result.consumed_capacity, &span);

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(key, consistent = !self.allow_eventually_consistent_reads, consumed_rcu, otel.status, error.msg, otel.span_kind = "client"))]
    async fn h_get<'a, 'b, K: Key<'a>, F: Into<Arg<'b>> + Send>(&self, key: K, field: F) -> Result<Option<Value>> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));

        let result = self
            .client
            .get_item()
            .consistent_read(!self.allow_eventually_consistent_reads)
            .set_key(Some(composite_key(&key, NO_SORT_KEY)))
            .table_name(self.table_name.clone())
            .send()
            .await
            .spanify_err()?;

        record_rcu(&result.consumed_capacity, &span);

        Ok(result
            .item
            .and_then(|mut item| item.remove(&encode_field_name(field.into().as_bytes())))
            .and_then(|v| match v {
                AttributeValue::B(b) => Some(b.into_inner().into()),
                _ => None,
            }))
    }

    #[tracing::instrument(skip_all, fields(key, consistent = !self.allow_eventually_consistent_reads, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn h_get_all<'a, K: Key<'a>>(&self, key: K) -> Result<HashMap<Vec<u8>, Value>> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));

        let result = self
            .client
            .get_item()
            .consistent_read(!self.allow_eventually_consistent_reads)
            .set_key(Some(composite_key(&key, NO_SORT_KEY)))
            .table_name(self.table_name.clone())
            .send()
            .await
            .spanify_err()?;

        record_rcu(&result.consumed_capacity, &span);

        Ok(result
            .item
            .map(|item| {
                item.into_iter()
                    .filter_map(|(name, v)| {
                        decode_field_name(&name).and_then(|name| match v {
                            AttributeValue::B(b) => Some((name, b.into_inner().into())),
                            _ => None,
                        })
                    })
                    .collect()
            })
            .unwrap_or(HashMap::new()))
    }

    #[tracing::instrument(skip_all, fields(key, consumed_wcu, otel.status_code, error.msg, otel.span_kind = "client"))]

    async fn z_add<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V, score: f64) -> Result<()> {
        let v = value.into();
        self.zh_add(key, &v, &v, score).await
    }

    #[tracing::instrument(skip_all, fields(key, consumed_wcu, otel.status_code, error.msg, otel.span_kind = "client"))]

    async fn zh_add<'a, 'b, 'c, K: Key<'a>, F: Into<Arg<'b>> + Send, V: Into<Arg<'c>> + Send>(&self, key: K, field: F, value: V, score: f64) -> Result<()> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));

        let field = field.into();
        let value = value.into();
        let result = self
            .client
            .put_item()
            .table_name(self.table_name.clone())
            .set_item(Some(new_item(
                &key,
                &field,
                vec![
                    ("v", attribute_value(&value)),
                    ("rk2", attribute_value(&[&float_sort_key(score), field.as_bytes()].concat())),
                ],
            )))
            .send()
            .await
            .spanify_err()?;

        record_wcu(&result.consumed_capacity, &span);

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(key, consumed_wcu, otel.status_code, error.msg, otel.span_kind = "client"))]

    async fn zh_rem<'a, 'b, K: Key<'a>, F: Into<Arg<'b>> + Send>(&self, key: K, field: F) -> Result<()> {
        let key = key.into();
        let span = tracing::Span::current();
        span.record("key", tracing::field::debug(&key));

        let result = self
            .client
            .delete_item()
            .table_name(self.table_name.clone())
            .set_key(Some(composite_key(&key, field)))
            .send()
            .await
            .spanify_err()?;

        record_wcu(&result.consumed_capacity, &span);

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(key, consumed_wcu, otel.status_code, error.msg, otel.span_kind = "client"))]

    async fn z_rem<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        self.zh_rem(key, value).await
    }

    #[tracing::instrument(skip_all, fields(key, consistent = !self.allow_eventually_consistent_reads, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]

    async fn z_count<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64) -> Result<usize> {
        if min > max {
            return Ok(0);
        }

        let (min, max) = score_bounds(min, max);
        let (condition, attribute_values) = query_condition(key.into(), min, max, true);

        let mut query = self
            .client
            .query()
            .table_name(self.table_name.clone())
            .consistent_read(!self.allow_eventually_consistent_reads)
            .key_condition_expression(condition.clone())
            .set_expression_attribute_values(Some(attribute_values.clone()))
            .index_name("rk2")
            .select(Select::Count);

        let mut count = 0;

        loop {
            let result = query.clone().send().await.spanify_err()?;
            count += result.count as usize;
            match result.last_evaluated_key {
                Some(key) => query = query.set_exclusive_start_key(Some(key)),
                None => break,
            }
        }

        Ok(count)
    }

    #[tracing::instrument(skip_all, fields(key, consistent = !self.allow_eventually_consistent_reads, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]

    async fn zh_count<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64) -> Result<usize> {
        self.z_count(key, min, max).await
    }

    #[tracing::instrument(skip_all, fields(key, consistent = !self.allow_eventually_consistent_reads, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]

    async fn z_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        let (min, max) = score_bounds(min, max);
        self.z_range_impl(key, min.into(), max.into(), limit, false, true).await
    }

    #[tracing::instrument(skip_all, fields(key, consistent = !self.allow_eventually_consistent_reads, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]

    async fn zh_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        self.z_range_by_score(key, min, max, limit).await
    }

    #[tracing::instrument(skip_all, fields(key, consistent = !self.allow_eventually_consistent_reads, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn z_rev_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        let (min, max) = score_bounds(min, max);
        self.z_range_impl(key, min.into(), max.into(), limit, true, true).await
    }

    #[tracing::instrument(skip_all, fields(key, consistent = !self.allow_eventually_consistent_reads, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn zh_rev_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        self.z_rev_range_by_score(key, min, max, limit).await
    }

    #[tracing::instrument(skip_all, fields(key, consistent = !self.allow_eventually_consistent_reads, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn z_range_by_lex<'a, 'b, 'c, K: Key<'a>, M: Into<Arg<'b>> + Send, N: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        min: Bound<M>,
        max: Bound<N>,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let (min, max) = lex_bounds(0.0, min, max);
        self.z_range_impl(key, min, max, limit, false, true).await
    }

    #[tracing::instrument(skip_all, fields(key, consistent = !self.allow_eventually_consistent_reads, consumed_rcu, otel.status_code, error.msg, otel.span_kind = "client"))]
    async fn z_rev_range_by_lex<'a, 'b, 'c, K: Key<'a>, M: Into<Arg<'b>> + Send, N: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        min: Bound<M>,
        max: Bound<N>,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let (min, max) = lex_bounds(0.0, min, max);
        self.z_range_impl(key, min, max, limit, true, true).await
    }

    #[tracing::instrument(skip_all, fields(consumed_rcu, consistent = !self.allow_eventually_consistent_reads, error.msg, otel.status_code, otel.span_kind = "client"))]
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

        let txs: HashMap<&[u8], _> = op
            .ops
            .iter()
            .map(|op| match op {
                BatchSubOperation::Get(key, tx) => (key.unredacted.as_bytes(), tx),
            })
            .collect();

        const MAX_BATCH_SIZE: usize = 100;

        let mut cap = TotalConsumedCapacity::default();
        while !keys.is_empty() {
            let batch = keys.split_off(if keys.len() > MAX_BATCH_SIZE { keys.len() - MAX_BATCH_SIZE } else { 0 });
            let keys_and_attributes = KeysAndAttributes::builder()
                .consistent_read(!self.allow_eventually_consistent_reads)
                .set_keys(Some(batch))
                .build()?;
            let result = self
                .client
                .batch_get_item()
                .request_items(self.table_name.clone(), keys_and_attributes)
                .send()
                .await
                .spanify_err()?;

            for c in result.consumed_capacity.iter().flatten() {
                cap.add_as_rcu(&c);
            }
            if let Some(items) = result.responses.and_then(|mut r| r.remove(&self.table_name)) {
                for mut item in items {
                    if let Some(v) = item.remove("v").and_then(|v| match v {
                        AttributeValue::B(b) => Some(b.into_inner().into()),
                        _ => None,
                    }) {
                        match item
                            .remove("hk")
                            .and_then(|hk| match hk {
                                AttributeValue::B(b) => txs.get(b.as_ref()),
                                _ => None,
                            })
                            .map(|tx| tx.try_send(v))
                        {
                            Some(Ok(_)) => {}
                            Some(Err(mpsc::TrySendError::Disconnected(_))) => {}
                            Some(Err(e)) => return Err(e.into()),
                            None => {}
                        }
                    }
                }
            }

            if let Some(unprocessed) = result.unprocessed_keys.and_then(|mut k| k.remove(&self.table_name)).map(|kv| kv.keys) {
                keys.extend(unprocessed);
            }
        }

        cap.record_to(&Span::current());

        Ok(())
    }

    async fn exec_atomic_write(&self, op: AtomicWriteOperation<'_>) -> Result<bool> {
        let mut token = Vec::new();
        token.resize(20, 0u8);
        rand::thread_rng().fill_bytes(&mut token);
        let token = base64::encode_config(token, base64::URL_SAFE_NO_PAD);

        struct SubOpState {
            key: ExplicitKey<'static>,
            failure_tx: Option<mpsc::SyncSender<bool>>,
            span: Span,
        }

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

                    let item = TransactWriteItem::builder()
                        .put(
                            Put::builder()
                                .table_name(self.table_name.clone())
                                .set_item(Some(new_item(&key, NO_SORT_KEY, vec![("v", attribute_value(value))])))
                                .build()?,
                        )
                        .build();
                    Ok::<_, crate::Error>((
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: None,
                            span,
                        },
                    ))
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

                    let item = TransactWriteItem::builder()
                        .put(
                            Put::builder()
                                .table_name(self.table_name.clone())
                                .set_item(Some(new_item(&key, NO_SORT_KEY, vec![("v", attribute_value(value))])))
                                .condition_expression("v = :v")
                                .expression_attribute_values(":v", attribute_value(old_value))
                                .build()?,
                        )
                        .build();
                    Ok((
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: Some(tx),
                            span,
                        },
                    ))
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

                    let item = TransactWriteItem::builder()
                        .put(
                            Put::builder()
                                .table_name(self.table_name.clone())
                                .set_item(Some(new_item(&key, NO_SORT_KEY, vec![("v", attribute_value(value))])))
                                .condition_expression("attribute_not_exists(v)")
                                .build()?,
                        )
                        .build();
                    Ok((
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: Some(tx),
                            span,
                        },
                    ))
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

                    let item = TransactWriteItem::builder()
                        .delete(
                            Delete::builder()
                                .table_name(self.table_name.clone())
                                .set_key(Some(composite_key(&key, NO_SORT_KEY)))
                                .build()?,
                        )
                        .build();
                    Ok((
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: None,
                            span,
                        },
                    ))
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

                    let item = TransactWriteItem::builder()
                        .delete(
                            Delete::builder()
                                .table_name(self.table_name.clone())
                                .set_key(Some(composite_key(&key, NO_SORT_KEY)))
                                .condition_expression("attribute_exists(v)")
                                .build()?,
                        )
                        .build();
                    Ok((
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: Some(tx),
                            span,
                        },
                    ))
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

                    let v = AttributeValue::Bs(vec![Blob::new(value.into_vec())]);
                    let item = TransactWriteItem::builder()
                        .update(
                            Update::builder()
                                .table_name(self.table_name.clone())
                                .set_key(Some(composite_key(&key, NO_SORT_KEY)))
                                .update_expression("ADD v :v")
                                .expression_attribute_values(":v", v)
                                .build()?,
                        )
                        .build();
                    Ok((
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: None,
                            span,
                        },
                    ))
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

                    let v = AttributeValue::Bs(vec![Blob::new(value.into_vec())]);
                    let item = TransactWriteItem::builder()
                        .update(
                            Update::builder()
                                .table_name(self.table_name.clone())
                                .set_key(Some(composite_key(&key, NO_SORT_KEY)))
                                .update_expression("DELETE v :v")
                                .expression_attribute_values(":v", v)
                                .build()?,
                        )
                        .build();
                    Ok((
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: None,
                            span,
                        },
                    ))
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

                    let item = TransactWriteItem::builder()
                        .put(
                            Put::builder()
                                .table_name(self.table_name.clone())
                                .set_item(Some(new_item(
                                    &key,
                                    &value,
                                    vec![
                                        ("v", attribute_value(&value)),
                                        ("rk2", attribute_value(&[&float_sort_key(score), value.as_bytes()].concat())),
                                    ],
                                )))
                                .build()?,
                        )
                        .build();
                    Ok((
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: None,
                            span,
                        },
                    ))
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

                    let item = TransactWriteItem::builder()
                        .put(
                            Put::builder()
                                .table_name(self.table_name.clone())
                                .set_item(Some(new_item(
                                    &key,
                                    &field,
                                    vec![
                                        ("v", attribute_value(&value)),
                                        ("rk2", attribute_value(&[&float_sort_key(score), field.as_bytes()].concat())),
                                    ],
                                )))
                                .build()?,
                        )
                        .build();
                    Ok((
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: None,
                            span,
                        },
                    ))
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

                    let item = TransactWriteItem::builder()
                        .delete(
                            Delete::builder()
                                .table_name(self.table_name.clone())
                                .set_key(Some(composite_key(&key, value)))
                                .build()?,
                        )
                        .build();
                    Ok((
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: None,
                            span,
                        },
                    ))
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

                    let item = TransactWriteItem::builder()
                        .delete(
                            Delete::builder()
                                .table_name(self.table_name.clone())
                                .set_key(Some(composite_key(&key, field)))
                                .build()?,
                        )
                        .build();
                    Ok((
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: None,
                            span,
                        },
                    ))
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
                            let v = AttributeValue::B(Blob::new(f.1.into_vec()));
                            ((format!("#n{}", i), encode_field_name(f.0.as_bytes())), (format!(":n{}", i), v))
                        })
                        .unzip();
                    let item = TransactWriteItem::builder()
                        .update(
                            Update::builder()
                                .table_name(self.table_name.clone())
                                .set_key(Some(composite_key(&key, NO_SORT_KEY)))
                                .update_expression(format!(
                                    "SET {}",
                                    (0..names.len()).map(|i| format!("#n{} = :n{}", i, i)).collect::<Vec<_>>().join(", ")
                                ))
                                .set_expression_attribute_values(Some(values))
                                .set_expression_attribute_names(Some(names))
                                .build()?,
                        )
                        .build();
                    Ok((
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: None,
                            span,
                        },
                    ))
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

                    let v = AttributeValue::B(Blob::new(value.into_vec()));
                    let item = TransactWriteItem::builder()
                        .update(
                            Update::builder()
                                .table_name(self.table_name.clone())
                                .set_key(Some(composite_key(&key, NO_SORT_KEY)))
                                .condition_expression("attribute_not_exists(#f)")
                                .update_expression("SET #f = :v")
                                .expression_attribute_values(":v", v)
                                .expression_attribute_names("#f", encode_field_name(field.as_bytes()))
                                .build()?,
                        )
                        .build();
                    Ok((
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: Some(tx),
                            span,
                        },
                    ))
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
                    let item = TransactWriteItem::builder()
                        .update(
                            Update::builder()
                                .set_key(Some(composite_key(&key, NO_SORT_KEY)))
                                .table_name(self.table_name.clone())
                                .update_expression(format!(
                                    "REMOVE {}",
                                    (0..names.len()).map(|i| format!("#n{}", i)).collect::<Vec<_>>().join(", ")
                                ))
                                .set_expression_attribute_names(Some(names))
                                .build()?,
                        )
                        .build();
                    Ok((
                        item,
                        SubOpState {
                            key: key.into_owned(),
                            failure_tx: None,
                            span,
                        },
                    ))
                }
            })
            .process_results(|i| i.unzip())?;
        match self
            .client
            .transact_write_items()
            .client_request_token(token)
            .set_transact_items(Some(transact_items))
            .send()
            .await
            .map_err(|e| e.into_service_error())
        {
            Err(TransactWriteItemsError::TransactionCanceledException(cancel)) => {
                let mut err = None;
                for (i, reason) in cancel.cancellation_reasons().iter().enumerate() {
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
                        "TransactionConflict" => {
                            err.get_or_insert_with(|| Error::AtomicWriteConflict(state.key.clone()));
                        }
                        _ => {
                            err.get_or_insert_with(|| Error::Other(format!("{:?} failed with {}", &state.key, code).into()));
                        }
                    }

                    state.span.record("otel.status_code", "ERROR");
                    state.span.record("error.code", &code);
                    if let Some(msg) = &reason.message {
                        state.span.record("error.msg", msg.clone());
                    }
                }

                let span = Span::current();
                span.record("otel.status_code", "ERROR");
                span.record("error.msg", "transaction canceled");
                return err.map(Err).unwrap_or(Ok(false));
            }
            Err(e) => Err(e.into()),
            Ok(r) => {
                let mut cap = TotalConsumedCapacity::default();
                for c in r.consumed_capacity.iter().flatten() {
                    cap.add_rcu_and_wcu(&c);
                }
                cap.record_to(&Span::current());
                Ok(true)
            }
        }
    }
}

pub async fn create_default_table(client: &Client, table_name: &str) -> Result<()> {
    client
        .create_table()
        .set_attribute_definitions(Some(vec![
            AttributeDefinition::builder()
                .attribute_name("hk")
                .attribute_type(ScalarAttributeType::B)
                .build()?,
            AttributeDefinition::builder()
                .attribute_name("rk")
                .attribute_type(ScalarAttributeType::B)
                .build()?,
            AttributeDefinition::builder()
                .attribute_name("rk2")
                .attribute_type(ScalarAttributeType::B)
                .build()?,
        ]))
        .set_key_schema(Some(vec![
            KeySchemaElement::builder().attribute_name("hk").key_type(KeyType::Hash).build()?,
            KeySchemaElement::builder().attribute_name("rk").key_type(KeyType::Range).build()?,
        ]))
        .local_secondary_indexes(
            LocalSecondaryIndex::builder()
                .index_name("rk2")
                .set_key_schema(Some(vec![
                    KeySchemaElement::builder().attribute_name("hk").key_type(KeyType::Hash).build()?,
                    KeySchemaElement::builder().attribute_name("rk2").key_type(KeyType::Range).build()?,
                ]))
                .projection(Projection::builder().projection_type(ProjectionType::All).build())
                .build()?,
        )
        .table_name(table_name)
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .spanify_err()?;
    Ok(())
}

/// Tracks total consumed capacity by repeated/batch operations.
#[derive(Default)]
struct TotalConsumedCapacity {
    total_rcu: Option<f64>,
    total_wcu: Option<f64>,
}

impl TotalConsumedCapacity {
    /// Adds individually reported RCU and WCU.
    ///
    /// This may be just done for `transact_*`.
    fn add_rcu_and_wcu(&mut self, c: &ConsumedCapacity) {
        if let Some(rcu) = c.read_capacity_units {
            *self.total_rcu.get_or_insert(0.) += rcu;
        }
        if let Some(wcu) = c.write_capacity_units {
            *self.total_wcu.get_or_insert(0.) += wcu;
        }
    }

    fn add_as_rcu(&mut self, c: &ConsumedCapacity) {
        if let Some(rcu) = c.capacity_units {
            *self.total_rcu.get_or_insert(0.) += rcu;
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

/// Records wcu used by a non-batch write operation to the given span.
fn record_wcu(capacity: &Option<ConsumedCapacity>, span: &Span) {
    if let Some(wcu) = capacity.as_ref().and_then(|c| c.capacity_units) {
        span.record("consumed_wcu", wcu);
    }
}

/// Records rcu used by a non-batch read operation to the given span.
fn record_rcu(capacity: &Option<ConsumedCapacity>, span: &Span) {
    if let Some(rcu) = capacity.as_ref().and_then(|c| c.capacity_units) {
        span.record("consumed_rcu", rcu);
    }
}

#[cfg(test)]
mod test {
    mod backend {
        use crate::{aws_sdk_dynamodbstore, test_backend};
        use aws_sdk_dynamodb::{
            config::{Credentials, Region},
            operation::describe_table::DescribeTableError,
            Client,
        };
        use tokio::time;

        test_backend!(|| async {
            // expects DynamoDB local to be running: docker run -p 8000:8000 --rm -it amazon/dynamodb-local
            let endpoint = std::env::var("DYNAMODB_ENDPOINT").unwrap_or("http://localhost:8000".to_string());
            let creds = Credentials::new("ACCESSKEYID", "SECRET", None, None, "dummy");
            let config = aws_sdk_dynamodb::Config::builder()
                .behavior_version_latest()
                .credentials_provider(creds)
                .endpoint_url(endpoint)
                .region(Region::from_static("test"))
                .build();
            let client = Client::from_conf(config);

            let table_name = "BackendTest";

            if let Ok(_) = client.delete_table().table_name(table_name).send().await {
                for _ in 0..10u32 {
                    match client.describe_table().table_name(table_name).send().await.map_err(|e| e.into_service_error()) {
                        Err(DescribeTableError::ResourceNotFoundException(_)) => break,
                        _ => time::sleep(time::Duration::from_millis(200)).await,
                    }
                }
            }

            aws_sdk_dynamodbstore::create_default_table(&client, &table_name)
                .await
                .expect("failed to create table");

            aws_sdk_dynamodbstore::Backend {
                allow_eventually_consistent_reads: false,
                client,
                table_name: table_name.to_owned(),
            }
        });
    }
}
