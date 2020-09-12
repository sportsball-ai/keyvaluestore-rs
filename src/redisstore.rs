use super::{Arg, AtomicWriteOperation, AtomicWriteSubOperation, BatchOperation, BatchSubOperation, Result, Value, MAX_ATOMIC_WRITE_SUB_OPERATIONS};
use redis::{aio::Connection, AsyncCommands, Client, FromRedisValue, RedisResult, RedisWrite, Script, ToRedisArgs};
use simple_error::SimpleError;
use std::{collections::HashMap, sync::mpsc};

pub struct Backend {
    pub client: Client,
}

impl Backend {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    async fn get_connection(&self) -> Result<Connection> {
        Ok(self.client.get_async_connection().await?)
    }
}

impl<'a> ToRedisArgs for Arg<'a> {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg(self.as_bytes())
    }
}

impl<'a> ToRedisArgs for &Arg<'a> {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg(self.as_bytes())
    }
}

impl FromRedisValue for Value {
    fn from_redis_value(v: &redis::Value) -> RedisResult<Self> {
        Ok(match v {
            redis::Value::Int(n) => n.to_string().as_bytes().to_vec().into(),
            redis::Value::Data(v) => v.clone().into(),
            _ => return Err((redis::ErrorKind::TypeError, "unexpected redis value type").into()),
        })
    }
}

#[async_trait]
impl super::Backend for Backend {
    async fn get<'a, K: Into<Arg<'a>> + Send>(&self, key: K) -> Result<Option<Value>> {
        Ok(self.get_connection().await?.get(key.into()).await?)
    }

    async fn set<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        Ok(self.get_connection().await?.set(key.into(), value.into()).await?)
    }

    async fn set_eq<'a, 'b, 'c, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send, OV: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        value: V,
        old_value: OV,
    ) -> Result<bool> {
        let mut conn = self.get_connection().await?;
        let key = key.into();
        let value = value.into();
        let old_value = old_value.into();
        loop {
            redis::cmd("WATCH").arg(&[&key]).query_async(&mut conn).await?;
            let mut pipe = redis::pipe();
            let pipe = pipe.atomic();
            let before: Option<Value> = conn.get(&key).await?;
            if let Some(before) = before {
                if before.as_bytes() == old_value.as_bytes() {
                    match pipe.set(&key, &value).ignore().query_async::<_, Option<()>>(&mut conn).await? {
                        None => continue,
                        Some(_) => return Ok(true),
                    }
                }
            }
            redis::cmd("UNWATCH").query_async(&mut conn).await?;
            return Ok(false);
        }
    }

    async fn set_nx<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<bool> {
        Ok(self.get_connection().await?.set_nx(key.into(), value.into()).await?)
    }

    async fn delete<'a, K: Into<Arg<'a>> + Send>(&self, key: K) -> Result<bool> {
        let deleted: i32 = self.get_connection().await?.del(key.into()).await?;
        Ok(deleted > 0)
    }

    async fn s_add<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        Ok(self.get_connection().await?.sadd(key.into(), value.into()).await?)
    }

    async fn s_members<'a, K: Into<Arg<'a>> + Send>(&self, key: K) -> Result<Vec<Value>> {
        Ok(self.get_connection().await?.smembers(key.into()).await?)
    }

    async fn h_set<'a, 'b, 'c, K: Into<Arg<'a>> + Send, F: Into<Arg<'b>> + Send, V: Into<Arg<'c>> + Send, I: IntoIterator<Item = (F, V)> + Send>(
        &self,
        key: K,
        fields: I,
    ) -> Result<()> {
        let fields: Vec<_> = fields.into_iter().map(|(k, v)| (k.into(), v.into())).collect();
        Ok(self.get_connection().await?.hset_multiple(key.into(), &fields).await?)
    }

    async fn h_del<'a, 'b, K: Into<Arg<'a>> + Send, F: Into<Arg<'b>> + Send, I: IntoIterator<Item = F> + Send>(&self, key: K, fields: I) -> Result<()> {
        let fields: Vec<_> = fields.into_iter().map(|f| f.into()).collect();
        Ok(self.get_connection().await?.hdel(key.into(), fields).await?)
    }

    async fn h_get<'a, 'b, K: Into<Arg<'a>> + Send, F: Into<Arg<'b>> + Send>(&self, key: K, field: F) -> Result<Option<Value>> {
        Ok(self.get_connection().await?.hget(key.into(), field.into()).await?)
    }

    async fn h_get_all<'a, K: Into<Arg<'a>> + Send>(&self, key: K) -> Result<HashMap<Vec<u8>, Value>> {
        Ok(self.get_connection().await?.hgetall(key.into()).await?)
    }

    async fn z_add<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&self, key: K, value: V, score: f64) -> Result<()> {
        Ok(self.get_connection().await?.zadd(key.into(), value.into(), score).await?)
    }

    async fn z_count<'a, K: Into<Arg<'a>> + Send>(&self, key: K, min: f64, max: f64) -> Result<usize> {
        Ok(self.get_connection().await?.zcount(key.into(), min, max).await?)
    }

    async fn z_range_by_score<'a, K: Into<Arg<'a>> + Send>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
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

    async fn z_rev_range_by_score<'a, K: Into<Arg<'a>> + Send>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
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

    async fn exec_batch(&self, op: BatchOperation<'_>) -> Result<()> {
        let keys: Vec<_> = op
            .ops
            .iter()
            .map(|op| match op {
                BatchSubOperation::Get(key, _) => key,
            })
            .collect();
        match keys.len() {
            0 => {}
            1 => {
                if let Some(v) = self.get_connection().await?.get(keys[0]).await? {
                    match &op.ops[0] {
                        BatchSubOperation::Get(_, tx) => match tx.try_send(v) {
                            Ok(_) => {}
                            Err(mpsc::TrySendError::Disconnected(_)) => {}
                            Err(e) => return Err(Box::new(e)),
                        },
                    }
                }
            }
            _ => {
                let values: Vec<Option<Value>> = self.get_connection().await?.get(keys).await?;
                for op in op.ops.into_iter().zip(values) {
                    match op {
                        (BatchSubOperation::Get(_, tx), Some(v)) => match tx.try_send(v) {
                            Ok(_) => {}
                            Err(mpsc::TrySendError::Disconnected(_)) => {}
                            Err(e) => return Err(Box::new(e)),
                        },
                        (_, None) => {}
                    }
                }
            }
        }
        Ok(())
    }

    async fn exec_atomic_write(&self, op: AtomicWriteOperation<'_>) -> Result<bool> {
        if op.ops.len() > MAX_ATOMIC_WRITE_SUB_OPERATIONS {
            return Err(Box::new(SimpleError::new("max sub-operation count exceeded")));
        }

        struct SubOp<'a> {
            key: Arg<'a>,
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
                    key,
                    condition: "true",
                    write: "redis.call('set', $@, $0)".to_string(),
                    args: vec![value],
                    failure_tx: None,
                },
                AtomicWriteSubOperation::SetNX(key, value, tx) => SubOp {
                    key,
                    condition: "redis.call('exists', $@) == 0",
                    write: "redis.call('set', $@, $0)".to_string(),
                    args: vec![value],
                    failure_tx: Some(tx),
                },
                AtomicWriteSubOperation::Delete(key) => SubOp {
                    key,
                    condition: "true",
                    write: "redis.call('del', $@)".to_string(),
                    args: vec![],
                    failure_tx: None,
                },
                AtomicWriteSubOperation::DeleteXX(key, tx) => SubOp {
                    key,
                    condition: "redis.call('exists', $@) == 1",
                    write: "redis.call('del', $@)".to_string(),
                    args: vec![],
                    failure_tx: Some(tx),
                },
                AtomicWriteSubOperation::SAdd(key, value) => SubOp {
                    key,
                    condition: "true",
                    write: "redis.call('sadd', $@, $0)".to_string(),
                    args: vec![value],
                    failure_tx: None,
                },
                AtomicWriteSubOperation::SRem(key, value) => SubOp {
                    key,
                    condition: "true",
                    write: "redis.call('srem', $@, $0)".to_string(),
                    args: vec![value],
                    failure_tx: None,
                },
                AtomicWriteSubOperation::ZAdd(key, value, score) => SubOp {
                    key,
                    condition: "true",
                    write: "redis.call('zadd', $@, $1, $0)".to_string(),
                    args: vec![value, score.to_string().into()],
                    failure_tx: None,
                },
                AtomicWriteSubOperation::ZRem(key, value) => SubOp {
                    key,
                    condition: "true",
                    write: "redis.call('zrem', $@, $0)".to_string(),
                    args: vec![value],
                    failure_tx: None,
                },
                AtomicWriteSubOperation::HSet(key, fields) => {
                    let mut args = Vec::new();
                    for field in fields {
                        args.push(field.0.into());
                        args.push(field.1.into());
                    }
                    SubOp {
                        key,
                        condition: "true",
                        write: format!(
                            "redis.call('hset', $@, {})",
                            (0..args.len()).map(|i| format!("${}", i)).collect::<Vec<_>>().join(", ")
                        ),
                        args,
                        failure_tx: None,
                    }
                }
                AtomicWriteSubOperation::HSetNX(key, field, value, tx) => SubOp {
                    key,
                    condition: "redis.call('hexists', $@, $0) == 0",
                    write: "redis.call('hset', $@, $0, $1)".to_string(),
                    args: vec![field, value],
                    failure_tx: Some(tx),
                },
                AtomicWriteSubOperation::HDel(key, fields) => {
                    let args: Vec<_> = fields.into_iter().map(|f| f.into()).collect();
                    SubOp {
                        key,
                        condition: "true",
                        write: format!(
                            "redis.call('hdel', $@, {})",
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
                preprocess_atomic_write_expression(op.condition, i + 1, args.len(), op.args.len())
            ));
            write_expressions.push(preprocess_atomic_write_expression(&op.write, i + 1, args.len(), op.args.len()));
            keys.push(op.key);
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
            return Err(Box::new(SimpleError::new("not enough return values")));
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
                        Err(e) => return Err(Box::new(e)),
                    }
                }
                (None, None) => return Err(Box::new(SimpleError::new("unconditional sub-op failed"))),
            }
        }
        Ok(ret)
    }
}

fn preprocess_atomic_write_expression(expr: &str, key_index: usize, args_offset: usize, num_args: usize) -> String {
    let mut expr = expr.replace("$@", &format!("KEYS[{}]", key_index));
    for i in (0..num_args).rev() {
        expr = expr.replace(&format!("${}", i), &format!("ARGV[{}]", args_offset + i + 1));
    }
    expr
}

#[cfg(test)]
mod test {
    mod backend {
        use crate::{redisstore, test_backend};
        use redis::{Client, ConnectionAddr, ConnectionInfo};

        test_backend!(|| async {
            let addr = std::env::var("REDIS_ADDRESS").unwrap_or("127.0.0.1".to_string());
            let addr: Vec<_> = addr.split(':').collect();
            let client = Client::open(ConnectionInfo {
                addr: Box::new(ConnectionAddr::Tcp(
                    addr[0].to_string(),
                    addr.get(1).map(|p| p.parse().unwrap()).unwrap_or(6379),
                )),
                db: 1,
                passwd: None,
            })
            .unwrap();
            let mut conn = client.get_connection().unwrap();
            redis::cmd("FLUSHDB").query::<()>(&mut conn).unwrap();
            redisstore::Backend::new(client)
        });
    }
}
