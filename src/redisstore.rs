use crate::ExplicitKey;

use super::{Arg, AtomicWriteOperation, AtomicWriteSubOperation, BatchOperation, BatchSubOperation, Key, Result, Value, MAX_ATOMIC_WRITE_SUB_OPERATIONS};
use redis::{aio::MultiplexedConnection, AsyncCommands, Client, FromRedisValue, ParsingError, RedisWrite, Script, ToRedisArgs, ToSingleRedisArg};
use simple_error::SimpleError;
use std::{collections::HashMap, ops::Bound, sync::mpsc};

#[derive(Clone)]
pub struct Backend {
    pub client: Client,
}

impl Backend {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    async fn get_connection(&self) -> Result<MultiplexedConnection> {
        Ok(self.client.get_multiplexed_async_connection().await?)
    }
}

impl<'a> ToRedisArgs for Arg<'a> {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg(self.as_bytes())
    }
}

impl<'a> ToSingleRedisArg for Arg<'a> {}

impl<'a> ToRedisArgs for ExplicitKey<'a> {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg(self.unredacted.as_bytes())
    }
}

impl<'a> ToSingleRedisArg for ExplicitKey<'a> {}

impl FromRedisValue for Value {
    fn from_redis_value(v: redis::Value) -> std::result::Result<Self, ParsingError> {
        Ok(match v {
            redis::Value::Int(n) => n.to_string().as_bytes().to_vec().into(),
            redis::Value::BulkString(v) => v.into(),
            _ => return Err("unexpected redis value type".into()),
        })
    }
}

const ZH_HASH_KEY_PREFIX: &str = "__kvs_zh:";

impl Backend {
    async fn zh_range_by_score_impl<'a, K: Key<'a>>(
        mut conn: MultiplexedConnection,
        cmd: &'static str,
        key: K,
        start: f64,
        end: f64,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let key = key.into();

        let script = [
            "local f = redis.call('",
            cmd,
            "', KEYS[1], unpack(ARGV))\n",
            "if #f == 0 then return {} end\n",
            "for i,v in pairs(redis.call('hmget', KEYS[2], unpack(f))) do if v then f[i] = v end end\n",
            "return f",
        ];
        let script = Script::new(script.join("").as_str());

        let mut invocation = script.prepare_invoke();
        invocation.key(&key.unredacted);
        invocation.key([ZH_HASH_KEY_PREFIX.as_bytes(), key.unredacted.as_bytes()].concat());
        invocation.arg(start);
        invocation.arg(end);
        if limit != 0 {
            invocation.arg("LIMIT");
            invocation.arg(0);
            invocation.arg(limit);
        }

        Ok(invocation.invoke_async(&mut conn).await?)
    }
}

fn redis_range_arg<'a, T: Into<Arg<'a>>>(b: Bound<T>, unbounded: &str) -> Vec<u8> {
    match b {
        Bound::Included(v) => ["[".as_bytes(), v.into().as_bytes()].concat(),
        Bound::Excluded(v) => ["(".as_bytes(), v.into().as_bytes()].concat(),
        Bound::Unbounded => unbounded.as_bytes().to_vec(),
    }
}

#[async_trait]
impl super::Backend for Backend {
    async fn get<'a, K: Key<'a>>(&self, key: K) -> Result<Option<Value>> {
        Ok(self.get_connection().await?.get(key.into().unredacted).await?)
    }

    async fn set<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        Ok(self.get_connection().await?.set(key.into().unredacted, value.into()).await?)
    }

    async fn set_eq<'a, 'b, 'c, K: Key<'a>, V: Into<Arg<'b>> + Send, OV: Into<Arg<'c>> + Send>(&self, key: K, value: V, old_value: OV) -> Result<bool> {
        let mut conn = self.get_connection().await?;
        let key = key.into();
        let value = value.into();
        let old_value = old_value.into();
        loop {
            redis::cmd("WATCH").arg(&[&key]).query_async::<()>(&mut conn).await?;
            let mut pipe = redis::pipe();
            let pipe = pipe.atomic();
            let before: Option<Value> = conn.get(&key).await?;
            if let Some(before) = before {
                if before.as_bytes() == old_value.as_bytes() {
                    match pipe.set(&key, &value).ignore().query_async::<Option<()>>(&mut conn).await? {
                        None => continue,
                        Some(_) => return Ok(true),
                    }
                }
            }
            redis::cmd("UNWATCH").query_async::<()>(&mut conn).await?;
            return Ok(false);
        }
    }

    async fn set_nx<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<bool> {
        Ok(self.get_connection().await?.set_nx(key.into(), value.into()).await?)
    }

    async fn delete<'a, K: Key<'a>>(&self, key: K) -> Result<bool> {
        let deleted: i32 = self.get_connection().await?.del(key.into()).await?;
        Ok(deleted > 0)
    }

    async fn s_add<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        Ok(self.get_connection().await?.sadd(key.into(), value.into()).await?)
    }

    async fn s_rem<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        Ok(self.get_connection().await?.srem(key.into(), value.into()).await?)
    }

    async fn s_members<'a, K: Key<'a>>(&self, key: K) -> Result<Vec<Value>> {
        Ok(self.get_connection().await?.smembers(key.into()).await?)
    }

    async fn n_incr_by<'a, K: Key<'a>>(&self, key: K, n: i64) -> Result<i64> {
        Ok(self.get_connection().await?.incr(key.into(), n).await?)
    }

    async fn h_set<'a, 'b, 'c, K: Key<'a>, F: Into<Arg<'b>> + Send, V: Into<Arg<'c>> + Send, I: IntoIterator<Item = (F, V)> + Send>(
        &self,
        key: K,
        fields: I,
    ) -> Result<()> {
        let fields: Vec<_> = fields.into_iter().map(|(k, v)| (k.into(), v.into())).collect();
        Ok(self.get_connection().await?.hset_multiple(key.into(), &fields).await?)
    }

    async fn h_del<'a, 'b, K: Key<'a>, F: Into<Arg<'b>> + Send, I: IntoIterator<Item = F> + Send>(&self, key: K, fields: I) -> Result<()> {
        let fields: Vec<_> = fields.into_iter().map(|f| f.into()).collect();
        Ok(self.get_connection().await?.hdel(key.into(), fields).await?)
    }

    async fn h_get<'a, 'b, K: Key<'a>, F: Into<Arg<'b>> + Send>(&self, key: K, field: F) -> Result<Option<Value>> {
        Ok(self.get_connection().await?.hget(key.into(), field.into()).await?)
    }

    async fn h_get_all<'a, K: Key<'a>>(&self, key: K) -> Result<HashMap<Vec<u8>, Value>> {
        Ok(self.get_connection().await?.hgetall(key.into()).await?)
    }

    async fn z_add<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V, score: f64) -> Result<()> {
        Ok(self.get_connection().await?.zadd(key.into(), value.into(), score).await?)
    }

    async fn zh_add<'a, 'b, 'c, K: Key<'a>, F: Into<Arg<'b>> + Send, V: Into<Arg<'c>> + Send>(&self, key: K, field: F, value: V, score: f64) -> Result<()> {
        let key = key.into();
        let field = field.into();
        let value = value.into();
        let mut conn = self.get_connection().await?;
        redis::pipe()
            .atomic()
            .zadd(&key, &field, score)
            .hset([ZH_HASH_KEY_PREFIX.as_bytes(), key.unredacted.as_bytes()].concat(), &field, &value)
            .ignore()
            .query_async::<Option<()>>(&mut conn)
            .await?;
        Ok(())
    }

    async fn zh_rem<'a, 'b, K: Key<'a>, F: Into<Arg<'b>> + Send>(&self, key: K, field: F) -> Result<()> {
        let key = key.into();
        let field = field.into();
        let mut conn = self.get_connection().await?;
        redis::pipe()
            .atomic()
            .zrem(&key, &field)
            .hdel([ZH_HASH_KEY_PREFIX.as_bytes(), key.unredacted.as_bytes()].concat(), &field)
            .ignore()
            .query_async::<Option<()>>(&mut conn)
            .await?;
        Ok(())
    }

    async fn z_rem<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        Ok(self.get_connection().await?.zrem(key.into(), value.into()).await?)
    }

    async fn z_count<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64) -> Result<usize> {
        Ok(self.get_connection().await?.zcount(key.into(), min, max).await?)
    }

    async fn zh_count<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64) -> Result<usize> {
        self.z_count(key, min, max).await
    }

    async fn z_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        if limit > 0 {
            Ok(self
                .get_connection()
                .await?
                .zrangebyscore_limit(key.into(), min, max, 0, limit as isize)
                .await?)
        } else {
            Ok(self.get_connection().await?.zrangebyscore(key.into(), min, max).await?)
        }
    }

    async fn zh_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        Self::zh_range_by_score_impl(self.get_connection().await?, "zrangebyscore", key, min, max, limit).await
    }

    async fn z_rev_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        if limit > 0 {
            Ok(self
                .get_connection()
                .await?
                .zrevrangebyscore_limit(key.into(), max, min, 0, limit as isize)
                .await?)
        } else {
            Ok(self.get_connection().await?.zrevrangebyscore(key.into(), max, min).await?)
        }
    }

    async fn zh_rev_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        Self::zh_range_by_score_impl(self.get_connection().await?, "zrevrangebyscore", key, max, min, limit).await
    }

    async fn z_range_by_lex<'a, 'b, 'c, K: Key<'a>, M: Into<Arg<'b>> + Send, N: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        min: Bound<M>,
        max: Bound<N>,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let min = redis_range_arg(min, "-");
        let max = redis_range_arg(max, "+");
        if limit > 0 {
            Ok(self.get_connection().await?.zrangebylex_limit(key.into(), min, max, 0, limit as isize).await?)
        } else {
            Ok(self.get_connection().await?.zrangebylex(key.into(), min, max).await?)
        }
    }

    async fn z_rev_range_by_lex<'a, 'b, 'c, K: Key<'a>, M: Into<Arg<'b>> + Send, N: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        min: Bound<M>,
        max: Bound<N>,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let min = redis_range_arg(min, "-");
        let max = redis_range_arg(max, "+");
        if limit > 0 {
            Ok(self
                .get_connection()
                .await?
                .zrevrangebylex_limit(key.into(), max, min, 0, limit as isize)
                .await?)
        } else {
            Ok(self.get_connection().await?.zrevrangebylex(key.into(), max, min).await?)
        }
    }

    async fn exec_batch(&self, op: BatchOperation) -> Result<()> {
        let keys: Vec<_> = op
            .ops
            .iter()
            .map(|op| match op {
                BatchSubOperation::Get(get) => &get.0.key,
            })
            .collect();
        match keys.len() {
            0 => {}
            1 => {
                if let Some(v) = self.get_connection().await?.get(keys[0]).await? {
                    match &op.ops[0] {
                        BatchSubOperation::Get(get) => get.0.put(v),
                    }
                }
            }
            _ => {
                let values: Vec<Option<Value>> = self.get_connection().await?.mget(keys).await?;
                for op in op.ops.into_iter().zip(values) {
                    match op {
                        (BatchSubOperation::Get(get), Some(v)) => get.0.put(v),
                        (_, None) => {}
                    }
                }
            }
        }
        Ok(())
    }

    async fn exec_atomic_write(&self, op: AtomicWriteOperation<'_>) -> Result<bool> {
        if op.ops.len() > MAX_ATOMIC_WRITE_SUB_OPERATIONS {
            return Err(SimpleError::new("max sub-operation count exceeded").into());
        }

        struct SubOp<'a> {
            keys: Vec<Arg<'a>>,
            condition: &'static str,
            write: String,
            args: Vec<Arg<'a>>,
            failure_tx: Option<mpsc::SyncSender<bool>>,
        }

        let ops: Vec<_> = op
            .ops
            .into_iter()
            .map(|op| match op {
                AtomicWriteSubOperation::Set(key, value) => SubOp {
                    keys: vec![key.unredacted],
                    condition: "true",
                    write: "redis.call('set', @0, $0)".to_string(),
                    args: vec![value],
                    failure_tx: None,
                },
                AtomicWriteSubOperation::SetEQ(key, value, old_value, tx) => SubOp {
                    keys: vec![key.unredacted],
                    condition: "redis.call('get', @0) == $1",
                    write: "redis.call('set', @0, $0)".to_string(),
                    args: vec![value, old_value],
                    failure_tx: Some(tx),
                },
                AtomicWriteSubOperation::SetNX(key, value, tx) => SubOp {
                    keys: vec![key.unredacted],
                    condition: "redis.call('exists', @0) == 0",
                    write: "redis.call('set', @0, $0)".to_string(),
                    args: vec![value],
                    failure_tx: Some(tx),
                },
                AtomicWriteSubOperation::Delete(key) => SubOp {
                    keys: vec![key.unredacted],
                    condition: "true",
                    write: "redis.call('del', @0)".to_string(),
                    args: vec![],
                    failure_tx: None,
                },
                AtomicWriteSubOperation::DeleteXX(key, tx) => SubOp {
                    keys: vec![key.unredacted],
                    condition: "redis.call('exists', @0) == 1",
                    write: "redis.call('del', @0)".to_string(),
                    args: vec![],
                    failure_tx: Some(tx),
                },
                AtomicWriteSubOperation::SAdd(key, value) => SubOp {
                    keys: vec![key.unredacted],
                    condition: "true",
                    write: "redis.call('sadd', @0, $0)".to_string(),
                    args: vec![value],
                    failure_tx: None,
                },
                AtomicWriteSubOperation::SRem(key, value) => SubOp {
                    keys: vec![key.unredacted],
                    condition: "true",
                    write: "redis.call('srem', @0, $0)".to_string(),
                    args: vec![value],
                    failure_tx: None,
                },
                AtomicWriteSubOperation::ZAdd(key, value, score) => SubOp {
                    keys: vec![key.unredacted],
                    condition: "true",
                    write: "redis.call('zadd', @0, $1, $0)".to_string(),
                    args: vec![value, score.to_string().into()],
                    failure_tx: None,
                },
                AtomicWriteSubOperation::ZHAdd(key, field, value, score) => SubOp {
                    keys: vec![[ZH_HASH_KEY_PREFIX.as_bytes(), key.unredacted.as_bytes()].concat().into(), key.unredacted],
                    condition: "true",
                    write: "redis.call('zadd', @1, $1, $0)\nredis.call('hset', @0, $0, $2)".to_string(),
                    args: vec![field, score.to_string().into(), value],
                    failure_tx: None,
                },
                AtomicWriteSubOperation::ZRem(key, value) => SubOp {
                    keys: vec![key.unredacted],
                    condition: "true",
                    write: "redis.call('zrem', @0, $0)".to_string(),
                    args: vec![value],
                    failure_tx: None,
                },
                AtomicWriteSubOperation::ZHRem(key, field) => SubOp {
                    keys: vec![[ZH_HASH_KEY_PREFIX.as_bytes(), key.unredacted.as_bytes()].concat().into(), key.unredacted],
                    condition: "true",
                    write: "redis.call('zrem', @1, $0)\nredis.call('hdel', @0, $0)".to_string(),
                    args: vec![field],
                    failure_tx: None,
                },
                AtomicWriteSubOperation::HSet(key, fields) => {
                    let mut args = Vec::new();
                    for field in fields {
                        args.push(field.0);
                        args.push(field.1);
                    }
                    SubOp {
                        keys: vec![key.unredacted],
                        condition: "true",
                        write: format!(
                            "redis.call('hset', @0, {})",
                            (0..args.len()).map(|i| format!("${}", i)).collect::<Vec<_>>().join(", ")
                        ),
                        args,
                        failure_tx: None,
                    }
                }
                AtomicWriteSubOperation::HSetNX(key, field, value, tx) => SubOp {
                    keys: vec![key.unredacted],
                    condition: "redis.call('hexists', @0, $0) == 0",
                    write: "redis.call('hset', @0, $0, $1)".to_string(),
                    args: vec![field, value],
                    failure_tx: Some(tx),
                },
                AtomicWriteSubOperation::HDel(key, fields) => {
                    let args: Vec<_> = fields.into_iter().collect();
                    SubOp {
                        keys: vec![key.unredacted],
                        condition: "true",
                        write: format!(
                            "redis.call('hdel', @0, {})",
                            (0..args.len()).map(|i| format!("${}", i)).collect::<Vec<_>>().join(", ")
                        ),
                        args,
                        failure_tx: None,
                    }
                }
            })
            .collect();

        let mut keys = Vec::with_capacity(ops.len());
        let mut args = Vec::new();
        let mut failure_txs = Vec::with_capacity(ops.len());

        let mut script = vec!["local checks = {}".to_string()];
        let mut write_expressions = Vec::with_capacity(ops.len());

        for (i, mut op) in ops.into_iter().enumerate() {
            script.push(format!(
                "checks[{}] = {}",
                i + 1,
                preprocess_atomic_write_expression(op.condition, keys.len(), op.keys.len(), args.len(), op.args.len())
            ));
            write_expressions.push(preprocess_atomic_write_expression(
                &op.write,
                keys.len(),
                op.keys.len(),
                args.len(),
                op.args.len(),
            ));
            keys.append(&mut op.keys);
            args.append(&mut op.args);
            failure_txs.push(op.failure_tx);
        }

        script.push("for i, v in ipairs(checks) do".to_string());
        script.push("if not v then".to_string());
        script.push("return checks".to_string());
        script.push("end".to_string());
        script.push("end".to_string());
        script.append(&mut write_expressions);
        script.push("return checks".to_string());

        let script = Script::new(script.join("\n").as_str());
        let mut invocation = script.prepare_invoke();
        for key in keys.into_iter() {
            invocation.key(key);
        }
        for arg in args.into_iter() {
            invocation.arg(arg);
        }

        let results: Vec<Option<()>> = invocation.invoke_async(&mut self.get_connection().await?).await?;
        if results.len() != failure_txs.len() {
            return Err(SimpleError::new("not enough return values").into());
        }

        let mut ret = true;
        for (check, tx) in results.into_iter().zip(failure_txs) {
            match (check, tx) {
                (Some(_), _) => {}
                (None, Some(tx)) => {
                    ret = false;
                    match tx.try_send(true) {
                        Ok(_) => {}
                        Err(mpsc::TrySendError::Disconnected(_)) => {}
                        Err(e) => return Err(e.into()),
                    }
                }
                (None, None) => return Err(SimpleError::new("unconditional sub-op failed").into()),
            }
        }
        Ok(ret)
    }
}

fn preprocess_atomic_write_expression(expr: &str, keys_offset: usize, num_keys: usize, args_offset: usize, num_args: usize) -> String {
    let mut expr = expr.to_string();
    for i in (0..num_keys).rev() {
        expr = expr.replace(&format!("@{}", i), &format!("KEYS[{}]", keys_offset + i + 1));
    }
    for i in (0..num_args).rev() {
        expr = expr.replace(&format!("${}", i), &format!("ARGV[{}]", args_offset + i + 1));
    }
    expr
}

#[cfg(test)]
mod test {
    mod backend {
        use crate::{redisstore, test_backend};
        use redis::{Client, IntoConnectionInfo as _};

        test_backend!(|| async {
            let addr = std::env::var("REDIS_ADDRESS").unwrap_or("redis://127.0.0.1".to_string());
            let client = Client::open(addr.into_connection_info().expect("redis URL parse failure")).unwrap();
            let mut conn = client.get_connection().unwrap();
            redis::cmd("FLUSHDB").query::<()>(&mut conn).unwrap();
            redisstore::Backend::new(client)
        });
    }
}
