use super::{Arg, AtomicWriteOperation, AtomicWriteSubOperation, BatchOperation, BatchSubOperation, Result, Value};
use std::{
    collections::HashMap,
    ops::Bound,
    sync::{mpsc, Arc, Mutex},
};

#[derive(Clone)]
enum Entry {
    None,
    Get(Option<Value>),
    SMembers(Vec<Value>),
    HGet(HashMap<Vec<u8>, Option<Value>>),
    HGetAll(HashMap<Vec<u8>, Value>),
}

/// This Backend implements a basic in-memory read-through cache. Writes will invalidate previous
/// reads, but otherwise the cache has no expiration mechanism. It is intended to be short-lived.
/// For example, it can be used to ensure that a single GraphQL query doesn't request the same
/// thing from a database repeatedly.
pub struct Backend<B> {
    inner: B,
    cache: Arc<Mutex<HashMap<Vec<u8>, Entry>>>,
}

impl<B> Backend<B> {
    pub fn new(inner: B) -> Self {
        Self { inner, cache: Arc::default() }
    }

    pub fn into_inner(self) -> B {
        self.inner
    }

    fn load(&self, key: &Arg<'_>) -> Option<Entry> {
        self.cache.lock().unwrap().get(key.as_bytes()).cloned()
    }

    fn store(&self, key: Arg<'_>, entry: Entry) {
        self.cache.lock().unwrap().insert(key.into_vec(), entry);
    }

    fn update<'a, F: FnOnce(&mut Entry) + 'a>(&'a self, key: Arg<'_>, f: F) {
        let mut c = self.cache.lock().unwrap();
        match c.get_mut(key.as_bytes()) {
            Some(v) => f(v),
            None => {
                let mut v = Entry::None;
                f(&mut v);
                c.insert(key.into_vec(), v);
            }
        }
    }

    fn invalidate(&self, key: &Arg<'_>) {
        self.cache.lock().unwrap().remove(key.as_bytes());
    }
}

impl<B: Clone> Clone for Backend<B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            cache: self.cache.clone(),
        }
    }
}

#[async_trait]
impl<B: super::Backend + Send + Sync> super::Backend for Backend<B> {
    async fn get<'a, K: Into<Arg<'a>> + Send>(&self, key: K) -> Result<Option<Value>> {
        let key = key.into();
        match self.load(&key) {
            Some(Entry::Get(v)) => Ok(v),
            _ => {
                let v = self.inner.get(&key).await?;
                self.store(key, Entry::Get(v.clone()));
                Ok(v)
            }
        }
    }

    async fn set<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        let key = key.into();
        let r = self.inner.set(&key, value).await;
        self.invalidate(&key);
        r
    }

    async fn set_eq<'a, 'b, 'c, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send, OV: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        value: V,
        old_value: OV,
    ) -> Result<bool> {
        let key = key.into();
        let r = self.inner.set_eq(&key, value, old_value).await;
        self.invalidate(&key);
        r
    }

    async fn set_nx<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<bool> {
        let key = key.into();
        let r = self.inner.set_nx(&key, value).await;
        self.invalidate(&key);
        r
    }

    async fn delete<'a, K: Into<Arg<'a>> + Send>(&self, key: K) -> Result<bool> {
        let key = key.into();
        let r = self.inner.delete(&key).await;
        self.invalidate(&key);
        r
    }

    async fn s_add<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        let key = key.into();
        let r = self.inner.s_add(&key, value).await;
        self.invalidate(&key);
        r
    }

    async fn s_members<'a, K: Into<Arg<'a>> + Send>(&self, key: K) -> Result<Vec<Value>> {
        let key = key.into();
        match self.load(&key) {
            Some(Entry::SMembers(v)) => Ok(v),
            _ => {
                let v = self.inner.s_members(&key).await?;
                self.store(key, Entry::SMembers(v.clone()));
                Ok(v)
            }
        }
    }

    async fn n_incr_by<'a, K: Into<Arg<'a>> + Send>(&self, key: K, n: i64) -> Result<i64> {
        let key = key.into();
        let r = self.inner.n_incr_by(&key, n).await;
        self.invalidate(&key);
        r
    }

    async fn h_set<'a, 'b, 'c, K: Into<Arg<'a>> + Send, F: Into<Arg<'b>> + Send, V: Into<Arg<'c>> + Send, I: IntoIterator<Item = (F, V)> + Send>(
        &self,
        key: K,
        fields: I,
    ) -> Result<()> {
        let key = key.into();
        let r = self.inner.h_set(&key, fields).await;
        self.invalidate(&key);
        r
    }

    async fn h_del<'a, 'b, K: Into<Arg<'a>> + Send, F: Into<Arg<'b>> + Send, I: IntoIterator<Item = F> + Send>(&self, key: K, fields: I) -> Result<()> {
        let key = key.into();
        let r = self.inner.h_del(&key, fields).await;
        self.invalidate(&key);
        r
    }

    async fn h_get<'a, 'b, K: Into<Arg<'a>> + Send, F: Into<Arg<'b>> + Send>(&self, key: K, field: F) -> Result<Option<Value>> {
        let key = key.into();
        let field = field.into();
        match self.load(&key) {
            Some(Entry::HGet(m)) => {
                if let Some(v) = m.get(field.as_bytes()) {
                    return Ok(v.clone());
                }
            }
            Some(Entry::HGetAll(m)) => return Ok(m.get(field.as_bytes()).cloned()),
            _ => {}
        }
        let v = self.inner.h_get(&key, &field).await?;
        self.update(key, |entry| match entry {
            Entry::HGetAll(_) => {}
            Entry::HGet(m) => {
                m.insert(field.into_vec(), v.clone());
            }
            _ => {
                let mut m = HashMap::new();
                m.insert(field.into_vec(), v.clone());
                *entry = Entry::HGet(m);
            }
        });
        Ok(v)
    }

    async fn h_get_all<'a, K: Into<Arg<'a>> + Send>(&self, key: K) -> Result<HashMap<Vec<u8>, Value>> {
        let key = key.into();
        match self.load(&key) {
            Some(Entry::HGetAll(v)) => Ok(v),
            _ => {
                let v = self.inner.h_get_all(&key).await?;
                self.store(key, Entry::HGetAll(v.clone()));
                Ok(v)
            }
        }
    }

    async fn z_add<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&self, key: K, value: V, score: f64) -> Result<()> {
        let key = key.into();
        let r = self.inner.z_add(&key, value, score).await;
        self.invalidate(&key);
        r
    }

    async fn z_rem<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        let key = key.into();
        let r = self.inner.z_rem(&key, value).await;
        self.invalidate(&key);
        r
    }

    async fn z_count<'a, K: Into<Arg<'a>> + Send>(&self, key: K, min: f64, max: f64) -> Result<usize> {
        let key = key.into();
        let r = self.inner.z_count(&key, min, max).await;
        self.invalidate(&key);
        r
    }

    async fn z_range_by_score<'a, K: Into<Arg<'a>> + Send>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        self.inner.z_range_by_score(key, min, max, limit).await
    }

    async fn z_rev_range_by_score<'a, K: Into<Arg<'a>> + Send>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        self.inner.z_rev_range_by_score(key, min, max, limit).await
    }

    async fn zh_add<'a, 'b, 'c, K: Into<Arg<'a>> + Send, F: Into<Arg<'b>> + Send, V: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        field: F,
        value: V,
        score: f64,
    ) -> Result<()> {
        let key = key.into();
        let r = self.inner.zh_add(&key, field, value, score).await;
        self.invalidate(&key);
        r
    }

    async fn zh_count<'a, K: Into<Arg<'a>> + Send>(&self, key: K, min: f64, max: f64) -> Result<usize> {
        self.inner.zh_count(key, min, max).await
    }

    async fn zh_range_by_score<'a, K: Into<Arg<'a>> + Send>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        self.inner.zh_range_by_score(key, min, max, limit).await
    }

    async fn zh_rev_range_by_score<'a, K: Into<Arg<'a>> + Send>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        self.inner.zh_rev_range_by_score(key, min, max, limit).await
    }

    async fn z_range_by_lex<'a, 'b, 'c, K: Into<Arg<'a>> + Send, M: Into<Arg<'b>> + Send, N: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        min: Bound<M>,
        max: Bound<N>,
        limit: usize,
    ) -> Result<Vec<Value>> {
        self.inner.z_range_by_lex(key, min, max, limit).await
    }

    async fn z_rev_range_by_lex<'a, 'b, 'c, K: Into<Arg<'a>> + Send, M: Into<Arg<'b>> + Send, N: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        min: Bound<M>,
        max: Bound<N>,
        limit: usize,
    ) -> Result<Vec<Value>> {
        self.inner.z_rev_range_by_lex(key, min, max, limit).await
    }

    async fn exec_batch(&self, op: BatchOperation<'_>) -> Result<()> {
        let mut misses = BatchOperation::new();
        let mut gets = HashMap::new();

        for op in op.ops {
            match op {
                BatchSubOperation::Get(key, tx) => match self.load(&key) {
                    Some(Entry::Get(v)) => {
                        if let Some(v) = v {
                            match tx.try_send(v) {
                                Ok(_) => {}
                                Err(mpsc::TrySendError::Disconnected(_)) => {}
                                Err(e) => return Err(e.into()),
                            }
                        }
                    }
                    _ => {
                        gets.insert(key.to_vec(), (misses.get(key), tx));
                    }
                },
            }
        }

        if misses.ops.is_empty() {
            Ok(())
        } else {
            match self.inner.exec_batch(misses).await {
                Ok(_) => {
                    for (key, (result, tx)) in gets {
                        let v = result.value();
                        self.store(key.into(), Entry::Get(v.clone()));
                        if let Some(v) = v {
                            match tx.try_send(v) {
                                Ok(_) => {}
                                Err(mpsc::TrySendError::Disconnected(_)) => {}
                                Err(e) => return Err(e.into()),
                            }
                        }
                    }
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }
    }

    async fn exec_atomic_write(&self, op: AtomicWriteOperation<'_>) -> Result<bool> {
        let mut keys = Vec::new();
        for subop in &op.ops {
            keys.push(
                match subop {
                    AtomicWriteSubOperation::Set(key, _) => key,
                    AtomicWriteSubOperation::SetEQ(key, _, _, _) => key,
                    AtomicWriteSubOperation::SetNX(key, _, _) => key,
                    AtomicWriteSubOperation::Delete(key) => key,
                    AtomicWriteSubOperation::DeleteXX(key, _) => key,
                    AtomicWriteSubOperation::SAdd(key, _) => key,
                    AtomicWriteSubOperation::SRem(key, _) => key,
                    AtomicWriteSubOperation::ZAdd(key, _, _) => key,
                    AtomicWriteSubOperation::ZHAdd(key, _, _, _) => key,
                    AtomicWriteSubOperation::ZRem(key, _) => key,
                    AtomicWriteSubOperation::ZHRem(key, _) => key,
                    AtomicWriteSubOperation::HSet(key, _) => key,
                    AtomicWriteSubOperation::HSetNX(key, _, _, _) => key,
                    AtomicWriteSubOperation::HDel(key, _) => key,
                }
                .clone(),
            );
        }
        let r = self.inner.exec_atomic_write(op).await;
        for key in keys {
            self.invalidate(&key);
        }
        r
    }
}

#[cfg(test)]
mod test {
    mod backend {
        use crate::{dynstore, memorystore, readcache, test_backend};
        test_backend!(|| async { dynstore::Backend::ReadCache(Box::new(readcache::Backend::new(dynstore::Backend::Memory(memorystore::Backend::new())))) });
    }
}
