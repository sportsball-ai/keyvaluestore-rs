use super::{memorystore, Arg, ConditionalResult, GetResult, Result, Value};

pub enum Backend {
    Memory(memorystore::Backend),
}

pub enum BatchOperation {
    Memory(<memorystore::Backend as super::Backend>::BatchOperation),
}

impl super::BatchOperation for BatchOperation {
    fn get<'a, K: Into<Arg<'a>> + Send>(&mut self, key: K) -> GetResult {
        match self {
            Self::Memory(op) => op.get(key),
        }
    }
}

pub enum AtomicWriteOperation {
    Memory(<memorystore::Backend as super::Backend>::AtomicWriteOperation),
}

impl super::AtomicWriteOperation for AtomicWriteOperation {
    fn set<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&mut self, key: K, value: V) {
        match self {
            Self::Memory(op) => op.set(key, value),
        }
    }

    fn set_nx<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(
        &mut self,
        key: K,
        value: V,
    ) -> ConditionalResult {
        match self {
            Self::Memory(op) => op.set_nx(key, value),
        }
    }

    fn z_add<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(
        &mut self,
        key: K,
        value: V,
        score: f64,
    ) {
        match self {
            Self::Memory(op) => op.z_add(key, value, score),
        }
    }

    fn z_rem<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(
        &mut self,
        key: K,
        value: V,
    ) {
        match self {
            Self::Memory(op) => op.z_rem(key, value),
        }
    }

    fn delete<'a, K: Into<Arg<'a>> + Send>(&mut self, key: K) {
        match self {
            Self::Memory(op) => op.delete(key),
        }
    }

    fn delete_xx<'a, K: Into<Arg<'a>> + Send>(&mut self, key: K) -> ConditionalResult {
        match self {
            Self::Memory(op) => op.delete_xx(key),
        }
    }

    fn s_add<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(
        &mut self,
        key: K,
        value: V,
    ) {
        match self {
            Self::Memory(op) => op.s_add(key, value),
        }
    }

    fn s_rem<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(
        &mut self,
        key: K,
        value: V,
    ) {
        match self {
            Self::Memory(op) => op.s_rem(key, value),
        }
    }
}

#[async_trait]
impl super::Backend for Backend {
    type BatchOperation = BatchOperation;
    type AtomicWriteOperation = AtomicWriteOperation;

    async fn get<'a, K: Into<Arg<'a>> + Send>(&self, key: K) -> Result<Option<Value>> {
        match self {
            Self::Memory(backend) => backend.get(key).await,
        }
    }

    async fn set<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(
        &self,
        key: K,
        value: V,
    ) -> Result<()> {
        match self {
            Self::Memory(backend) => backend.set(key, value).await,
        }
    }

    async fn set_eq<
        'a,
        'b,
        'c,
        K: Into<Arg<'a>> + Send,
        V: Into<Arg<'b>> + Send,
        OV: Into<Arg<'c>> + Send,
    >(
        &self,
        key: K,
        value: V,
        old_value: OV,
    ) -> Result<bool> {
        match self {
            Self::Memory(backend) => backend.set_eq(key, value, old_value).await,
        }
    }

    async fn set_nx<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(
        &self,
        key: K,
        value: V,
    ) -> Result<bool> {
        match self {
            Self::Memory(backend) => backend.set_nx(key, value).await,
        }
    }

    async fn delete<'a, K: Into<Arg<'a>> + Send>(&self, key: K) -> Result<bool> {
        match self {
            Self::Memory(backend) => backend.delete(key).await,
        }
    }

    async fn s_add<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(
        &self,
        key: K,
        value: V,
    ) -> Result<()> {
        match self {
            Self::Memory(backend) => backend.s_add(key, value).await,
        }
    }

    async fn s_members<'a, K: Into<Arg<'a>> + Send>(&self, key: K) -> Result<Vec<Value>> {
        match self {
            Self::Memory(backend) => backend.s_members(key).await,
        }
    }

    async fn z_add<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(
        &self,
        key: K,
        value: V,
        score: f64,
    ) -> Result<()> {
        match self {
            Self::Memory(backend) => backend.z_add(key, value, score).await,
        }
    }

    async fn z_count<'a, K: Into<Arg<'a>> + Send>(
        &self,
        key: K,
        min: f64,
        max: f64,
    ) -> Result<usize> {
        match self {
            Self::Memory(backend) => backend.z_count(key, min, max).await,
        }
    }

    async fn z_range_by_score<'a, K: Into<Arg<'a>> + Send>(
        &self,
        key: K,
        min: f64,
        max: f64,
        limit: usize,
    ) -> Result<Vec<Value>> {
        match self {
            Self::Memory(backend) => backend.z_range_by_score(key, min, max, limit).await,
        }
    }

    async fn z_rev_range_by_score<'a, K: Into<Arg<'a>> + Send>(
        &self,
        key: K,
        min: f64,
        max: f64,
        limit: usize,
    ) -> Result<Vec<Value>> {
        match self {
            Self::Memory(backend) => backend.z_rev_range_by_score(key, min, max, limit).await,
        }
    }

    fn new_batch(&self) -> Self::BatchOperation {
        match self {
            Self::Memory(backend) => BatchOperation::Memory(backend.new_batch()),
        }
    }

    async fn exec_batch(&self, op: Self::BatchOperation) -> Result<()> {
        match self {
            Self::Memory(backend) => match op {
                BatchOperation::Memory(op) => backend.exec_batch(op).await,
            },
        }
    }

    fn new_atomic_write(&self) -> Self::AtomicWriteOperation {
        match self {
            Self::Memory(backend) => AtomicWriteOperation::Memory(backend.new_atomic_write()),
        }
    }

    async fn exec_atomic_write(&self, op: Self::AtomicWriteOperation) -> Result<bool> {
        match self {
            Self::Memory(backend) => match op {
                AtomicWriteOperation::Memory(op) => backend.exec_atomic_write(op).await,
            },
        }
    }
}
