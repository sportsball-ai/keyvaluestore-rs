use super::{dynamodbstore, memorystore, redisstore, Arg, AtomicWriteOperation, BatchOperation, Result, Value};

pub enum Backend {
    Memory(memorystore::Backend),
    Redis(redisstore::Backend),
    DynamoDB(dynamodbstore::Backend),
}

#[async_trait]
impl super::Backend for Backend {
    async fn get<'a, K: Into<Arg<'a>> + Send>(&self, key: K) -> Result<Option<Value>> {
        match self {
            Self::Memory(backend) => backend.get(key).await,
            Self::Redis(backend) => backend.get(key).await,
            Self::DynamoDB(backend) => backend.get(key).await,
        }
    }

    async fn set<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        match self {
            Self::Memory(backend) => backend.set(key, value).await,
            Self::Redis(backend) => backend.set(key, value).await,
            Self::DynamoDB(backend) => backend.set(key, value).await,
        }
    }

    async fn set_eq<'a, 'b, 'c, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send, OV: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        value: V,
        old_value: OV,
    ) -> Result<bool> {
        match self {
            Self::Memory(backend) => backend.set_eq(key, value, old_value).await,
            Self::Redis(backend) => backend.set_eq(key, value, old_value).await,
            Self::DynamoDB(backend) => backend.set_eq(key, value, old_value).await,
        }
    }

    async fn set_nx<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<bool> {
        match self {
            Self::Memory(backend) => backend.set_nx(key, value).await,
            Self::Redis(backend) => backend.set_nx(key, value).await,
            Self::DynamoDB(backend) => backend.set_nx(key, value).await,
        }
    }

    async fn delete<'a, K: Into<Arg<'a>> + Send>(&self, key: K) -> Result<bool> {
        match self {
            Self::Memory(backend) => backend.delete(key).await,
            Self::Redis(backend) => backend.delete(key).await,
            Self::DynamoDB(backend) => backend.delete(key).await,
        }
    }

    async fn s_add<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        match self {
            Self::Memory(backend) => backend.s_add(key, value).await,
            Self::Redis(backend) => backend.s_add(key, value).await,
            Self::DynamoDB(backend) => backend.s_add(key, value).await,
        }
    }

    async fn s_members<'a, K: Into<Arg<'a>> + Send>(&self, key: K) -> Result<Vec<Value>> {
        match self {
            Self::Memory(backend) => backend.s_members(key).await,
            Self::Redis(backend) => backend.s_members(key).await,
            Self::DynamoDB(backend) => backend.s_members(key).await,
        }
    }

    async fn z_add<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&self, key: K, value: V, score: f64) -> Result<()> {
        match self {
            Self::Memory(backend) => backend.z_add(key, value, score).await,
            Self::Redis(backend) => backend.z_add(key, value, score).await,
            Self::DynamoDB(backend) => backend.z_add(key, value, score).await,
        }
    }

    async fn z_count<'a, K: Into<Arg<'a>> + Send>(&self, key: K, min: f64, max: f64) -> Result<usize> {
        match self {
            Self::Memory(backend) => backend.z_count(key, min, max).await,
            Self::Redis(backend) => backend.z_count(key, min, max).await,
            Self::DynamoDB(backend) => backend.z_count(key, min, max).await,
        }
    }

    async fn z_range_by_score<'a, K: Into<Arg<'a>> + Send>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        match self {
            Self::Memory(backend) => backend.z_range_by_score(key, min, max, limit).await,
            Self::Redis(backend) => backend.z_range_by_score(key, min, max, limit).await,
            Self::DynamoDB(backend) => backend.z_range_by_score(key, min, max, limit).await,
        }
    }

    async fn z_rev_range_by_score<'a, K: Into<Arg<'a>> + Send>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        match self {
            Self::Memory(backend) => backend.z_rev_range_by_score(key, min, max, limit).await,
            Self::Redis(backend) => backend.z_rev_range_by_score(key, min, max, limit).await,
            Self::DynamoDB(backend) => backend.z_rev_range_by_score(key, min, max, limit).await,
        }
    }

    async fn exec_batch(&self, op: BatchOperation<'_>) -> Result<()> {
        match self {
            Self::Memory(backend) => backend.exec_batch(op).await,
            Self::Redis(backend) => backend.exec_batch(op).await,
            Self::DynamoDB(backend) => backend.exec_batch(op).await,
        }
    }

    async fn exec_atomic_write(&self, op: AtomicWriteOperation<'_>) -> Result<bool> {
        match self {
            Self::Memory(backend) => backend.exec_atomic_write(op).await,
            Self::Redis(backend) => backend.exec_atomic_write(op).await,
            Self::DynamoDB(backend) => backend.exec_atomic_write(op).await,
        }
    }
}

#[cfg(test)]
mod test {
    mod backend {
        use crate::{dynstore, memorystore, test_backend};
        test_backend!(|| async { dynstore::Backend::Memory(memorystore::Backend::new()) });
    }
}
