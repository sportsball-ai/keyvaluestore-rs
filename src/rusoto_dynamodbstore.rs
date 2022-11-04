use super::{Arg, AtomicWriteOperation, AtomicWriteSubOperation, BatchOperation, BatchSubOperation, Bound, Error, Result, Value};
use rand::RngCore;
use rusoto_core::RusotoError;
use rusoto_dynamodb::{
    AttributeDefinition, AttributeValue, Delete, DynamoDb, DynamoDbClient, KeySchemaElement, LocalSecondaryIndex, Projection, Put, TransactWriteItem, Update,
};
use simple_error::SimpleError;
use std::{collections::HashMap, sync::mpsc};

#[derive(Clone)]
pub struct Backend {
    pub allow_eventually_consistent_reads: bool,
    pub client: DynamoDbClient,
    pub table_name: String,
}

const NO_SORT_KEY: &str = "_";

fn new_item<'h, 's, H: Into<Arg<'h>> + Send, S: Into<Arg<'s>> + Send, A: IntoIterator<Item = (&'static str, AttributeValue)>>(
    hash: H,
    sort: S,
    attrs: A,
) -> HashMap<String, AttributeValue> {
    let mut ret: HashMap<_, _> = attrs.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
    ret.insert("hk".to_string(), attribute_value(hash));
    ret.insert("rk".to_string(), attribute_value(sort));
    ret
}

fn composite_key<'h, 's, H: Into<Arg<'h>> + Send, S: Into<Arg<'s>> + Send>(hash: H, sort: S) -> HashMap<String, AttributeValue> {
    let mut ret = HashMap::new();
    ret.insert("hk".to_string(), attribute_value(hash));
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

fn query_condition<'k, K: Into<Arg<'k>>>(key: K, min: ArgBound<'_>, max: ArgBound<'_>, secondary_index: bool) -> (String, HashMap<String, AttributeValue>) {
    let mut attribute_values = HashMap::new();
    attribute_values.insert(":hash".to_string(), attribute_value(key));

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
    async fn z_range_impl<'k, 'm, 'n, K: Into<Arg<'k>>, M: Into<Arg<'m>>, N: Into<Arg<'n>>>(
        &self,
        key: K,
        min: Bound<M>,
        max: Bound<N>,
        limit: usize,
        reverse: bool,
        secondary_index: bool,
    ) -> Result<Vec<Value>> {
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

        let mut members = vec![];

        while limit == 0 || members.len() < limit {
            let mut q = query.clone();
            if limit > 0 {
                q.limit = Some((limit - members.len() + 1) as _);
            }
            let result = self.client.query(q).await?;
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
        Ok(members)
    }
}

#[async_trait]
impl super::Backend for Backend {
    async fn get<'a, K: Into<Arg<'a>> + Send>(&self, key: K) -> Result<Option<Value>> {
        let mut get = rusoto_dynamodb::GetItemInput::default();
        get.consistent_read = Some(!self.allow_eventually_consistent_reads);
        get.key = composite_key(key, NO_SORT_KEY);
        get.table_name = self.table_name.clone();
        let result = self.client.get_item(get).await?;
        Ok(result.item.and_then(|mut item| item.remove("v")).and_then(|v| v.b).map(|v| v.to_vec().into()))
    }

    async fn set<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        let mut put = rusoto_dynamodb::PutItemInput::default();
        put.table_name = self.table_name.clone();
        put.item = new_item(key, NO_SORT_KEY, vec![("v", attribute_value(value))]);
        self.client.put_item(put).await?;
        Ok(())
    }

    async fn set_eq<'a, 'b, 'c, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send, OV: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        value: V,
        old_value: OV,
    ) -> Result<bool> {
        let mut put = rusoto_dynamodb::PutItemInput::default();
        put.table_name = self.table_name.clone();
        put.item = new_item(key, NO_SORT_KEY, vec![("v", attribute_value(value))]);
        put.condition_expression = Some("v = :v".to_string());
        put.expression_attribute_values = Some(vec![(":v".to_string(), attribute_value(old_value))].into_iter().collect());
        match self.client.put_item(put).await {
            Ok(_) => Ok(true),
            Err(RusotoError::Service(rusoto_dynamodb::PutItemError::ConditionalCheckFailed(_))) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn set_nx<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<bool> {
        let mut put = rusoto_dynamodb::PutItemInput::default();
        put.table_name = self.table_name.clone();
        put.item = new_item(key, NO_SORT_KEY, vec![("v", attribute_value(value))]);
        put.condition_expression = Some("attribute_not_exists(v)".to_string());
        match self.client.put_item(put).await {
            Ok(_) => Ok(true),
            Err(RusotoError::Service(rusoto_dynamodb::PutItemError::ConditionalCheckFailed(_))) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn delete<'a, K: Into<Arg<'a>> + Send>(&self, key: K) -> Result<bool> {
        let mut delete = rusoto_dynamodb::DeleteItemInput::default();
        delete.table_name = self.table_name.clone();
        delete.key = composite_key(key, NO_SORT_KEY);
        delete.return_values = Some("ALL_OLD".to_string());
        let result = self.client.delete_item(delete).await?;
        Ok(result.attributes.is_some())
    }

    async fn s_add<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        let value = value.into();
        let mut v = AttributeValue::default();
        v.bs = Some(vec![bytes::Bytes::copy_from_slice(value.as_bytes())]);
        let mut update = rusoto_dynamodb::UpdateItemInput::default();
        update.key = composite_key(key, NO_SORT_KEY);
        update.table_name = self.table_name.clone();
        update.update_expression = Some("ADD v :v".to_string());
        update.expression_attribute_values = Some(vec![(":v".to_string(), v)].into_iter().collect());
        self.client.update_item(update).await?;
        Ok(())
    }

    async fn s_members<'a, K: Into<Arg<'a>> + Send>(&self, key: K) -> Result<Vec<Value>> {
        let mut get = rusoto_dynamodb::GetItemInput::default();
        get.consistent_read = Some(!self.allow_eventually_consistent_reads);
        get.key = composite_key(key, NO_SORT_KEY);
        get.table_name = self.table_name.clone();
        let result = self.client.get_item(get).await?;
        Ok(result
            .item
            .and_then(|mut item| item.remove("v"))
            .and_then(|v| v.bs)
            .map(|v| v.iter().map(|v| v.to_vec().into()).collect())
            .unwrap_or(vec![]))
    }

    async fn n_incr_by<'a, K: Into<Arg<'a>> + Send>(&self, key: K, n: i64) -> Result<i64> {
        let mut v = AttributeValue::default();
        v.n = Some(n.to_string());
        let mut update = rusoto_dynamodb::UpdateItemInput::default();
        update.key = composite_key(key, NO_SORT_KEY);
        update.table_name = self.table_name.clone();
        update.update_expression = Some("ADD v :n".to_string());
        update.expression_attribute_values = Some(vec![(":n".to_string(), v)].into_iter().collect());
        update.return_values = Some("ALL_NEW".to_string());
        let output = self.client.update_item(update).await?;
        let new_value = match output.attributes.and_then(|mut h| h.remove("v")).and_then(|v| v.n) {
            Some(v) => v,
            None => return Err(SimpleError::new("new value not returned by dynamodb").into()),
        };
        Ok(new_value.parse()?)
    }

    async fn h_set<'a, 'b, 'c, K: Into<Arg<'a>> + Send, F: Into<Arg<'b>> + Send, V: Into<Arg<'c>> + Send, I: IntoIterator<Item = (F, V)> + Send>(
        &self,
        key: K,
        fields: I,
    ) -> Result<()> {
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
        update.key = composite_key(key, NO_SORT_KEY);
        update.table_name = self.table_name.clone();
        update.update_expression = Some(format!(
            "SET {}",
            (0..names.len()).map(|i| format!("#n{} = :n{}", i, i)).collect::<Vec<_>>().join(", ")
        ));
        update.expression_attribute_values = Some(values);
        update.expression_attribute_names = Some(names);
        self.client.update_item(update).await?;
        Ok(())
    }

    async fn h_del<'a, 'b, K: Into<Arg<'a>> + Send, F: Into<Arg<'b>> + Send, I: IntoIterator<Item = F> + Send>(&self, key: K, fields: I) -> Result<()> {
        let names: HashMap<_, _> = fields
            .into_iter()
            .enumerate()
            .map(|(i, f)| (format!("#n{}", i), encode_field_name(f.into().as_bytes())))
            .collect();
        let mut update = rusoto_dynamodb::UpdateItemInput::default();
        update.key = composite_key(key, NO_SORT_KEY);
        update.table_name = self.table_name.clone();
        update.update_expression = Some(format!(
            "REMOVE {}",
            (0..names.len()).map(|i| format!("#n{}", i)).collect::<Vec<_>>().join(", ")
        ));
        update.expression_attribute_names = Some(names);
        self.client.update_item(update).await?;
        Ok(())
    }

    async fn h_get<'a, 'b, K: Into<Arg<'a>> + Send, F: Into<Arg<'b>> + Send>(&self, key: K, field: F) -> Result<Option<Value>> {
        let mut get = rusoto_dynamodb::GetItemInput::default();
        get.consistent_read = Some(!self.allow_eventually_consistent_reads);
        get.key = composite_key(key, NO_SORT_KEY);
        get.table_name = self.table_name.clone();
        let result = self.client.get_item(get).await?;
        Ok(result
            .item
            .and_then(|mut item| item.remove(&encode_field_name(field.into().as_bytes())))
            .and_then(|v| v.b)
            .map(|v| v.to_vec().into()))
    }

    async fn h_get_all<'a, K: Into<Arg<'a>> + Send>(&self, key: K) -> Result<HashMap<Vec<u8>, Value>> {
        let mut get = rusoto_dynamodb::GetItemInput::default();
        get.consistent_read = Some(!self.allow_eventually_consistent_reads);
        get.key = composite_key(key, NO_SORT_KEY);
        get.table_name = self.table_name.clone();
        let result = self.client.get_item(get).await?;
        Ok(result
            .item
            .map(|item| {
                item.into_iter()
                    .filter_map(|(name, v)| decode_field_name(&name).and_then(|name| v.b.map(|v| (name, (*v).into()))))
                    .collect()
            })
            .unwrap_or(HashMap::new()))
    }

    async fn z_add<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&self, key: K, value: V, score: f64) -> Result<()> {
        let v = value.into();
        self.zh_add(key, &v, &v, score).await
    }

    async fn zh_add<'a, 'b, 'c, K: Into<Arg<'a>> + Send, F: Into<Arg<'b>> + Send, V: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        field: F,
        value: V,
        score: f64,
    ) -> Result<()> {
        let field = field.into();
        let value = value.into();
        let mut put = rusoto_dynamodb::PutItemInput::default();
        put.table_name = self.table_name.clone();
        put.item = new_item(
            key,
            &field,
            vec![
                ("v", attribute_value(&value)),
                ("rk2", attribute_value(&[&float_sort_key(score), field.as_bytes()].concat())),
            ],
        );
        self.client.put_item(put).await?;
        Ok(())
    }

    async fn zh_rem<'a, 'b, K: Into<Arg<'a>> + Send, F: Into<Arg<'b>> + Send>(&self, key: K, field: F) -> Result<()> {
        let mut delete = rusoto_dynamodb::DeleteItemInput::default();
        delete.table_name = self.table_name.clone();
        delete.key = composite_key(key, field);
        self.client.delete_item(delete).await?;
        Ok(())
    }

    async fn z_rem<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        self.zh_rem(key, value).await
    }

    async fn z_count<'a, K: Into<Arg<'a>> + Send>(&self, key: K, min: f64, max: f64) -> Result<usize> {
        if min > max {
            return Ok(0);
        }

        let (min, max) = score_bounds(min, max);
        let (condition, attribute_values) = query_condition(key, min, max, true);

        let mut query = rusoto_dynamodb::QueryInput::default();
        query.table_name = self.table_name.clone();
        query.consistent_read = Some(!self.allow_eventually_consistent_reads);
        query.key_condition_expression = Some(condition.clone());
        query.expression_attribute_values = Some(attribute_values.clone());
        query.index_name = Some("rk2".to_string());
        query.select = Some("COUNT".to_string());

        let mut count = 0;

        loop {
            let result = self.client.query(query.clone()).await?;
            if let Some(n) = result.count {
                count += n as usize;
            }
            match result.last_evaluated_key {
                Some(key) => query.exclusive_start_key = Some(key),
                None => break,
            }
        }

        Ok(count)
    }

    async fn zh_count<'a, K: Into<Arg<'a>> + Send>(&self, key: K, min: f64, max: f64) -> Result<usize> {
        self.z_count(key, min, max).await
    }

    async fn z_range_by_score<'a, K: Into<Arg<'a>> + Send>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        let (min, max) = score_bounds(min, max);
        self.z_range_impl(key, min.into(), max.into(), limit, false, true).await
    }

    async fn zh_range_by_score<'a, K: Into<Arg<'a>> + Send>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        self.z_range_by_score(key, min, max, limit).await
    }

    async fn z_rev_range_by_score<'a, K: Into<Arg<'a>> + Send>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        let (min, max) = score_bounds(min, max);
        self.z_range_impl(key, min.into(), max.into(), limit, true, true).await
    }

    async fn zh_rev_range_by_score<'a, K: Into<Arg<'a>> + Send>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        self.z_rev_range_by_score(key, min, max, limit).await
    }

    async fn z_range_by_lex<'a, 'b, 'c, K: Into<Arg<'a>> + Send, M: Into<Arg<'b>> + Send, N: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        min: Bound<M>,
        max: Bound<N>,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let (min, max) = lex_bounds(0.0, min, max);
        self.z_range_impl(key, min, max, limit, false, true).await
    }

    async fn z_rev_range_by_lex<'a, 'b, 'c, K: Into<Arg<'a>> + Send, M: Into<Arg<'b>> + Send, N: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        min: Bound<M>,
        max: Bound<N>,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let (min, max) = lex_bounds(0.0, min, max);
        self.z_range_impl(key, min, max, limit, true, true).await
    }

    async fn exec_batch(&self, op: BatchOperation<'_>) -> Result<()> {
        if op.ops.is_empty() {
            return Ok(());
        }

        let mut keys: Vec<_> = op
            .ops
            .iter()
            .map(|op| match op {
                BatchSubOperation::Get(key, _) => composite_key(key, NO_SORT_KEY),
            })
            .collect();

        let txs: HashMap<&[u8], _> = op
            .ops
            .iter()
            .map(|op| match op {
                BatchSubOperation::Get(key, tx) => (key.as_bytes(), tx),
            })
            .collect();

        const MAX_BATCH_SIZE: usize = 100;

        while !keys.is_empty() {
            let batch = keys.split_off(if keys.len() > MAX_BATCH_SIZE { keys.len() - MAX_BATCH_SIZE } else { 0 });
            let mut get = rusoto_dynamodb::BatchGetItemInput::default();
            let mut keys_and_attributes = rusoto_dynamodb::KeysAndAttributes::default();
            keys_and_attributes.consistent_read = Some(!self.allow_eventually_consistent_reads);
            keys_and_attributes.keys = batch;
            get.request_items.insert(self.table_name.clone(), keys_and_attributes);

            let result = self.client.batch_get_item(get).await?;

            if let Some(items) = result.responses.and_then(|mut r| r.remove(&self.table_name)) {
                for mut item in items {
                    if let Some(v) = item.remove("v").and_then(|v| v.b).map(|b| b.to_vec().into()) {
                        match item
                            .remove("hk")
                            .and_then(|hk| hk.b)
                            .and_then(|b| txs.get(&b as &[u8]))
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

            if let Some(unprocessed) = result.unprocessed_keys.and_then(|mut k| k.remove(&self.table_name)) {
                keys.extend(unprocessed.keys);
            }
        }

        Ok(())
    }

    async fn exec_atomic_write(&self, op: AtomicWriteOperation<'_>) -> Result<bool> {
        let mut token = Vec::new();
        token.resize(20, 0u8);
        rand::thread_rng().fill_bytes(&mut token);
        let token = base64::encode_config(token, base64::URL_SAFE_NO_PAD);

        let mut tx = rusoto_dynamodb::TransactWriteItemsInput::default();
        tx.client_request_token = Some(token);
        let (transact_items, failure_txs): (Vec<_>, Vec<_>) = op
            .ops
            .into_iter()
            .map(|op| match op {
                AtomicWriteSubOperation::Set(key, value) => {
                    let mut put = Put::default();
                    put.table_name = self.table_name.clone();
                    put.item = new_item(key, NO_SORT_KEY, vec![("v", attribute_value(value))]);
                    let mut item = TransactWriteItem::default();
                    item.put = Some(put);
                    (item, None)
                }
                AtomicWriteSubOperation::SetEQ(key, value, old_value, tx) => {
                    let mut put = Put::default();
                    put.table_name = self.table_name.clone();
                    put.item = new_item(key, NO_SORT_KEY, vec![("v", attribute_value(value))]);
                    put.condition_expression = Some("v = :v".to_string());
                    put.expression_attribute_values = Some(vec![(":v".to_string(), attribute_value(old_value))].into_iter().collect());
                    let mut item = TransactWriteItem::default();
                    item.put = Some(put);
                    (item, Some(tx))
                }
                AtomicWriteSubOperation::SetNX(key, value, tx) => {
                    let mut put = Put::default();
                    put.table_name = self.table_name.clone();
                    put.item = new_item(key, NO_SORT_KEY, vec![("v", attribute_value(value))]);
                    put.condition_expression = Some("attribute_not_exists(v)".to_string());
                    let mut item = TransactWriteItem::default();
                    item.put = Some(put);
                    (item, Some(tx))
                }
                AtomicWriteSubOperation::Delete(key) => {
                    let mut delete = Delete::default();
                    delete.table_name = self.table_name.clone();
                    delete.key = composite_key(key, NO_SORT_KEY);
                    let mut item = TransactWriteItem::default();
                    item.delete = Some(delete);
                    (item, None)
                }
                AtomicWriteSubOperation::DeleteXX(key, tx) => {
                    let mut delete = Delete::default();
                    delete.table_name = self.table_name.clone();
                    delete.key = composite_key(key, NO_SORT_KEY);
                    delete.condition_expression = Some("attribute_exists(v)".to_string());
                    let mut item = TransactWriteItem::default();
                    item.delete = Some(delete);
                    (item, Some(tx))
                }
                AtomicWriteSubOperation::SAdd(key, value) => {
                    let mut v = AttributeValue::default();
                    v.bs = Some(vec![bytes::Bytes::copy_from_slice(value.as_bytes())]);
                    let mut update = Update::default();
                    update.table_name = self.table_name.clone();
                    update.key = composite_key(key, NO_SORT_KEY);
                    update.update_expression = "ADD v :v".to_string();
                    update.expression_attribute_values = Some(vec![(":v".to_string(), v)].into_iter().collect());
                    let mut item = TransactWriteItem::default();
                    item.update = Some(update);
                    (item, None)
                }
                AtomicWriteSubOperation::SRem(key, value) => {
                    let mut v = AttributeValue::default();
                    v.bs = Some(vec![bytes::Bytes::copy_from_slice(value.as_bytes())]);
                    let mut update = Update::default();
                    update.table_name = self.table_name.clone();
                    update.key = composite_key(key, NO_SORT_KEY);
                    update.update_expression = "DELETE v :v".to_string();
                    update.expression_attribute_values = Some(vec![(":v".to_string(), v)].into_iter().collect());
                    let mut item = TransactWriteItem::default();
                    item.update = Some(update);
                    (item, None)
                }
                AtomicWriteSubOperation::ZAdd(key, value, score) => {
                    let mut put = Put::default();
                    put.table_name = self.table_name.clone();
                    put.item = new_item(
                        key,
                        &value,
                        vec![
                            ("v", attribute_value(&value)),
                            ("rk2", attribute_value(&[&float_sort_key(score), value.as_bytes()].concat())),
                        ],
                    );
                    let mut item = TransactWriteItem::default();
                    item.put = Some(put);
                    (item, None)
                }
                AtomicWriteSubOperation::ZHAdd(key, field, value, score) => {
                    let mut put = Put::default();
                    put.table_name = self.table_name.clone();
                    put.item = new_item(
                        key,
                        &field,
                        vec![
                            ("v", attribute_value(&value)),
                            ("rk2", attribute_value(&[&float_sort_key(score), field.as_bytes()].concat())),
                        ],
                    );
                    let mut item = TransactWriteItem::default();
                    item.put = Some(put);
                    (item, None)
                }
                AtomicWriteSubOperation::ZRem(key, value) => {
                    let mut delete = Delete::default();
                    delete.table_name = self.table_name.clone();
                    delete.key = composite_key(key, value);
                    let mut item = TransactWriteItem::default();
                    item.delete = Some(delete);
                    (item, None)
                }
                AtomicWriteSubOperation::ZHRem(key, field) => {
                    let mut delete = Delete::default();
                    delete.table_name = self.table_name.clone();
                    delete.key = composite_key(key, field);
                    let mut item = TransactWriteItem::default();
                    item.delete = Some(delete);
                    (item, None)
                }
                AtomicWriteSubOperation::HSet(key, fields) => {
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
                    update.key = composite_key(key, NO_SORT_KEY);
                    update.update_expression = format!("SET {}", (0..names.len()).map(|i| format!("#n{} = :n{}", i, i)).collect::<Vec<_>>().join(", "));
                    update.expression_attribute_values = Some(values);
                    update.expression_attribute_names = Some(names);
                    let mut item = TransactWriteItem::default();
                    item.update = Some(update);
                    (item, None)
                }
                AtomicWriteSubOperation::HSetNX(key, field, value, tx) => {
                    let mut update = Update::default();
                    update.table_name = self.table_name.clone();
                    update.key = composite_key(key, NO_SORT_KEY);
                    update.condition_expression = Some("attribute_not_exists(#f)".to_string());
                    update.update_expression = "SET #f = :v".to_string();
                    let mut v = AttributeValue::default();
                    v.b = Some(value.into_vec().into());
                    update.expression_attribute_values = Some(vec![(":v".to_string(), v)].into_iter().collect());
                    update.expression_attribute_names = Some(vec![("#f".to_string(), encode_field_name(field.as_bytes()))].into_iter().collect());
                    let mut item = TransactWriteItem::default();
                    item.update = Some(update);
                    (item, Some(tx))
                }
                AtomicWriteSubOperation::HDel(key, fields) => {
                    let names: HashMap<_, _> = fields
                        .into_iter()
                        .enumerate()
                        .map(|(i, f)| (format!("#n{}", i), encode_field_name(f.as_bytes())))
                        .collect();
                    let mut update = Update::default();
                    update.key = composite_key(key, NO_SORT_KEY);
                    update.table_name = self.table_name.clone();
                    update.update_expression = format!("REMOVE {}", (0..names.len()).map(|i| format!("#n{}", i)).collect::<Vec<_>>().join(", "));
                    update.expression_attribute_names = Some(names);
                    let mut item = TransactWriteItem::default();
                    item.update = Some(update);
                    (item, None)
                }
            })
            .unzip();
        tx.transact_items = transact_items;

        match self.client.transact_write_items(tx).await {
            Err(RusotoError::Service(rusoto_dynamodb::TransactWriteItemsError::TransactionCanceled { reasons, .. })) => {
                let mut did_fail_conditional = false;
                for (i, reason) in reasons.iter().enumerate() {
                    if let Some(code) = &reason.code {
                        if code == "ConditionalCheckFailed" {
                            did_fail_conditional = true;
                            if let Some(Some(tx)) = failure_txs.get(i) {
                                match tx.try_send(true) {
                                    Ok(_) => {}
                                    Err(mpsc::TrySendError::Disconnected(_)) => {}
                                    Err(e) => return Err(e.into()),
                                }
                            }
                        }
                    }
                }
                if did_fail_conditional {
                    Ok(false)
                } else {
                    Err(Error::AtomicWriteConflict)
                }
            }
            Err(e) => Err(e.into()),
            Ok(_) => Ok(true),
        }
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
