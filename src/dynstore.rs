use super::{memorystore, readcache, Arg, AtomicWriteOperation, BatchOperation, Key, Result, Value};
use std::{collections::HashMap, ops::Bound};

#[derive(Clone)]
#[non_exhaustive]
pub enum Backend {
    #[cfg(feature = "aws-sdk")]
    AwsSdkDynamoDB(crate::aws_sdk_dynamodbstore::Backend),
    Memory(memorystore::Backend),
    #[cfg(feature = "redis")]
    Redis(crate::redisstore::Backend),
    ReadCache(Box<readcache::Backend<Backend>>),
}

/// Delegates to each supported backend via identical expressions.
///
/// Rust doesn't allow macros to reference variables not passed into them or exprs passed into
/// macros to reference variables defined in the macro. Thus, `self` and `backend` must be supplied
/// as parameters.
macro_rules! dispatch {
    ($self:ident, $backend:ident, { $expansion:expr }) => {
        match $self {
            #[cfg(feature = "aws-sdk")]
            Self::AwsSdkDynamoDB($backend) => $expansion,
            Self::Memory($backend) => $expansion,
            #[cfg(feature = "redis")]
            Self::Redis($backend) => $expansion,
            Self::ReadCache($backend) => $expansion,
        }
        .await
    };
}

#[async_trait]
impl super::Backend for Backend {
    async fn get<'a, K: Key<'a>>(&self, key: K) -> Result<Option<Value>> {
        dispatch!(self, backend, { backend.get(key) })
    }

    async fn set<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        dispatch!(self, backend, { backend.set(key, value) })
    }

    async fn set_eq<'a, 'b, 'c, K: Key<'a>, V: Into<Arg<'b>> + Send, OV: Into<Arg<'c>> + Send>(&self, key: K, value: V, old_value: OV) -> Result<bool> {
        dispatch!(self, backend, { backend.set_eq(key, value, old_value) })
    }

    async fn set_nx<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<bool> {
        dispatch!(self, backend, { backend.set_nx(key, value) })
    }

    async fn delete<'a, K: Key<'a>>(&self, key: K) -> Result<bool> {
        dispatch!(self, backend, { backend.delete(key) })
    }

    async fn s_add<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        dispatch!(self, backend, { backend.s_add(key, value) })
    }

    async fn s_rem<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        dispatch!(self, backend, { backend.s_rem(key, value) })
    }

    async fn s_members<'a, K: Key<'a>>(&self, key: K) -> Result<Vec<Value>> {
        dispatch!(self, backend, { backend.s_members(key) })
    }

    async fn n_incr_by<'a, K: Key<'a>>(&self, key: K, n: i64) -> Result<i64> {
        dispatch!(self, backend, { backend.n_incr_by(key, n) })
    }

    async fn h_set<'a, 'b, 'c, K: Key<'a>, F: Into<Arg<'b>> + Send, V: Into<Arg<'c>> + Send, I: IntoIterator<Item = (F, V)> + Send>(
        &self,
        key: K,
        fields: I,
    ) -> Result<()> {
        dispatch!(self, backend, { backend.h_set(key, fields) })
    }

    async fn h_del<'a, 'b, K: Key<'a>, F: Into<Arg<'b>> + Send, I: IntoIterator<Item = F> + Send>(&self, key: K, fields: I) -> Result<()> {
        dispatch!(self, backend, { backend.h_del(key, fields) })
    }

    async fn h_get<'a, 'b, K: Key<'a>, F: Into<Arg<'b>> + Send>(&self, key: K, field: F) -> Result<Option<Value>> {
        dispatch!(self, backend, { backend.h_get(key, field) })
    }

    async fn h_get_all<'a, K: Key<'a>>(&self, key: K) -> Result<HashMap<Vec<u8>, Value>> {
        dispatch!(self, backend, { backend.h_get_all(key) })
    }

    async fn z_add<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V, score: f64) -> Result<()> {
        dispatch!(self, backend, { backend.z_add(key, value, score) })
    }

    async fn z_rem<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        dispatch!(self, backend, { backend.z_rem(key, value) })
    }

    async fn z_count<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64) -> Result<usize> {
        dispatch!(self, backend, { backend.z_count(key, min, max) })
    }

    async fn z_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        dispatch!(self, backend, { backend.z_range_by_score(key, min, max, limit) })
    }

    async fn z_rev_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        dispatch!(self, backend, { backend.z_rev_range_by_score(key, min, max, limit) })
    }

    async fn zh_add<'a, 'b, 'c, K: Key<'a>, F: Into<Arg<'b>> + Send, V: Into<Arg<'c>> + Send>(&self, key: K, field: F, value: V, score: f64) -> Result<()> {
        dispatch!(self, backend, { backend.zh_add(key, field, value, score) })
    }

    async fn zh_count<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64) -> Result<usize> {
        dispatch!(self, backend, { backend.zh_count(key, min, max) })
    }

    async fn zh_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        dispatch!(self, backend, { backend.zh_range_by_score(key, min, max, limit) })
    }

    async fn zh_rev_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        dispatch!(self, backend, { backend.zh_rev_range_by_score(key, min, max, limit) })
    }

    async fn zh_rem<'a, 'b, K: Key<'a>, F: Into<Arg<'b>> + Send>(&self, key: K, field: F) -> Result<()> {
        dispatch!(self, backend, { backend.zh_rem(key, field) })
    }

    async fn z_range_by_lex<'a, 'b, 'c, K: Key<'a>, M: Into<Arg<'b>> + Send, N: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        min: Bound<M>,
        max: Bound<N>,
        limit: usize,
    ) -> Result<Vec<Value>> {
        dispatch!(self, backend, { backend.z_range_by_lex(key, min, max, limit) })
    }

    async fn z_rev_range_by_lex<'a, 'b, 'c, K: Key<'a>, M: Into<Arg<'b>> + Send, N: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        min: Bound<M>,
        max: Bound<N>,
        limit: usize,
    ) -> Result<Vec<Value>> {
        dispatch!(self, backend, { backend.z_rev_range_by_lex(key, min, max, limit) })
    }

    async fn exec_batch(&self, op: BatchOperation) -> Result<()> {
        dispatch!(self, backend, { backend.exec_batch(op) })
    }

    async fn exec_atomic_write(&self, op: AtomicWriteOperation<'_>) -> Result<bool> {
        dispatch!(self, backend, { backend.exec_atomic_write(op) })
    }
}

#[cfg(test)]
mod test {
    mod backend {
        use crate::{dynstore, memorystore, test_backend};
        test_backend!(|| async { dynstore::Backend::Memory(memorystore::Backend::new()) });
    }
}
