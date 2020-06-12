#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate simple_error;

use std::convert::From;
use std::sync::mpsc;

pub mod backendtest;
pub mod memorystore;

type Error = Box<dyn std::error::Error + 'static>;
type Result<T> = std::result::Result<T, Error>;

pub trait Arg: Send {
    fn into_arg(self) -> Vec<u8>;
}

impl Arg for Vec<u8> {
    fn into_arg(self) -> Vec<u8> {
        self
    }
}

impl Arg for &str {
    fn into_arg(self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq)]
pub struct Value(Vec<u8>);

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Self(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Self(v.as_bytes().to_vec())
    }
}

impl From<Value> for Vec<u8> {
    fn from(v: Value) -> Self {
        v.0
    }
}

impl PartialEq<str> for Value {
    fn eq(&self, other: &str) -> bool {
        self.0 == other.as_bytes()
    }
}

impl PartialEq<Value> for &str {
    fn eq(&self, other: &Value) -> bool {
        other.0 == self.as_bytes()
    }
}

#[async_trait]
pub trait Backend {
    type BatchOperation: BatchOperation;
    type AtomicWriteOperation: AtomicWriteOperation;

    async fn get<K: Arg>(&self, key: K) -> Result<Option<Value>>;
    async fn set<K: Arg, V: Arg>(&self, key: K, value: V) -> Result<()>;
    async fn set_eq<K: Arg, V: Arg, OV: Arg>(
        &self,
        key: K,
        value: V,
        old_value: OV,
    ) -> Result<bool>;
    async fn set_nx<K: Arg, V: Arg>(&self, key: K, value: V) -> Result<bool>;
    async fn delete<K: Arg>(&self, key: K) -> Result<bool>;

    async fn s_add<K: Arg, V: Arg>(&self, key: K, value: V) -> Result<()>;
    async fn s_members<K: Arg>(&self, key: K) -> Result<Vec<Value>>;

    async fn z_add<K: Arg, V: Arg>(&self, key: K, value: V, score: f64) -> Result<()>;
    async fn z_count<K: Arg>(&self, key: K, min: f64, max: f64) -> Result<usize>;
    async fn z_range_by_score<K: Arg>(
        &self,
        key: K,
        min: f64,
        max: f64,
        limit: usize,
    ) -> Result<Vec<Value>>;
    async fn z_rev_range_by_score<K: Arg>(
        &self,
        key: K,
        min: f64,
        max: f64,
        limit: usize,
    ) -> Result<Vec<Value>>;

    fn new_batch(&self) -> Self::BatchOperation;
    async fn exec_batch(&self, op: Self::BatchOperation) -> Result<()>;

    fn new_atomic_write(&self) -> Self::AtomicWriteOperation;
    async fn exec_atomic_write(&self, op: Self::AtomicWriteOperation) -> Result<bool>;
}

pub struct GetResult {
    rx: mpsc::Receiver<Option<Value>>,
}

impl GetResult {
    pub fn new() -> (Self, mpsc::SyncSender<Option<Value>>) {
        let (tx, rx) = mpsc::sync_channel(1);
        (Self { rx }, tx)
    }

    pub fn value(self) -> Option<Value> {
        self.rx.recv().unwrap_or(None)
    }
}

pub trait BatchOperation {
    fn get<K: Arg + 'static>(&mut self, key: K) -> GetResult;
}

pub struct ConditionalResult {
    rx: mpsc::Receiver<bool>,
}

impl ConditionalResult {
    pub fn new() -> (Self, mpsc::SyncSender<bool>) {
        let (tx, rx) = mpsc::sync_channel(1);
        (Self { rx }, tx)
    }

    pub fn failed(self) -> bool {
        self.rx.recv().unwrap_or(false)
    }
}

pub trait AtomicWriteOperation {
    fn set<K: Arg, V: Arg>(&mut self, key: K, value: V);
    fn set_nx<K: Arg, V: Arg>(&mut self, key: K, value: V) -> ConditionalResult;
    fn z_add<K: Arg, V: Arg>(&mut self, key: K, value: V, score: f64);
    fn z_rem<K: Arg, V: Arg>(&mut self, key: K, value: V);
    fn delete<K: Arg>(&mut self, key: K);
    fn delete_xx<K: Arg>(&mut self, key: K) -> ConditionalResult;
    fn s_add<K: Arg, V: Arg>(&mut self, key: K, value: V);
    fn s_rem<K: Arg, V: Arg>(&mut self, key: K, value: V);
}

// FallbackBatchOperation provides a suitable fallback for stores that don't supported optimized
// batching.
pub struct FallbackBatchOperation {
    gets: Vec<(Vec<u8>, mpsc::SyncSender<Option<Value>>)>,
}

impl FallbackBatchOperation {
    pub async fn exec<B: Backend>(self, backend: &B) -> Result<()> {
        for (k, tx) in self.gets.into_iter() {
            let v = backend.get(k).await?;
            tx.try_send(v)?;
        }
        Ok(())
    }
}

impl FallbackBatchOperation {
    pub fn new() -> Self {
        FallbackBatchOperation { gets: vec![] }
    }
}

impl BatchOperation for FallbackBatchOperation {
    fn get<K: Arg + 'static>(&mut self, key: K) -> GetResult {
        let (ret, tx) = GetResult::new();
        self.gets.push((key.into_arg(), tx));
        ret
    }
}
