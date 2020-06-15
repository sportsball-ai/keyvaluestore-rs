#[macro_use]
extern crate async_trait;
extern crate simple_error;

use std::convert::From;
use std::sync::mpsc;

pub mod backendtest;
pub mod dynstore;
pub mod memorystore;

type Error = Box<dyn std::error::Error + Send + 'static>;
type Result<T> = std::result::Result<T, Error>;

pub enum Arg<'a> {
    Owned(Vec<u8>),
    Borrowed(&'a [u8]),
}

impl<'a> Arg<'a> {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Owned(v) => &v,
            Self::Borrowed(v) => v,
        }
    }

    pub fn to_vec(&self) -> Vec<u8> {
        match self {
            Self::Owned(v) => v.clone(),
            Self::Borrowed(v) => v.to_vec(),
        }
    }

    pub fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Owned(v) => v,
            Self::Borrowed(v) => v.to_vec(),
        }
    }
}

impl Into<Arg<'static>> for Vec<u8> {
    fn into(self) -> Arg<'static> {
        Arg::Owned(self)
    }
}

impl<'a> Into<Arg<'a>> for &'a Vec<u8> {
    fn into(self) -> Arg<'a> {
        Arg::Borrowed(&self)
    }
}

impl<'a> Into<Arg<'a>> for &'a str {
    fn into(self) -> Arg<'a> {
        Arg::Borrowed(self.as_bytes())
    }
}

impl<'a> Into<Arg<'a>> for &'a String {
    fn into(self) -> Arg<'a> {
        Arg::Borrowed(self.as_bytes())
    }
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq)]
pub struct Value(Vec<u8>);

impl<T: Into<Vec<u8>>> From<T> for Value {
    fn from(v: T) -> Self {
        Self(v.into())
    }
}

impl AsRef<[u8]> for Value {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
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

impl Value {
    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.0.clone()
    }
}

#[async_trait]
pub trait Backend {
    type BatchOperation: BatchOperation + Send;
    type AtomicWriteOperation: AtomicWriteOperation + Send;

    async fn get<'a, K: Into<Arg<'a>> + Send>(&self, key: K) -> Result<Option<Value>>;
    async fn set<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(
        &self,
        key: K,
        value: V,
    ) -> Result<()>;
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
    ) -> Result<bool>;
    async fn set_nx<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(
        &self,
        key: K,
        value: V,
    ) -> Result<bool>;
    async fn delete<'a, K: Into<Arg<'a>> + Send>(&self, key: K) -> Result<bool>;

    async fn s_add<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(
        &self,
        key: K,
        value: V,
    ) -> Result<()>;
    async fn s_members<'a, K: Into<Arg<'a>> + Send>(&self, key: K) -> Result<Vec<Value>>;

    async fn z_add<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(
        &self,
        key: K,
        value: V,
        score: f64,
    ) -> Result<()>;
    async fn z_count<'a, K: Into<Arg<'a>> + Send>(
        &self,
        key: K,
        min: f64,
        max: f64,
    ) -> Result<usize>;
    async fn z_range_by_score<'a, K: Into<Arg<'a>> + Send>(
        &self,
        key: K,
        min: f64,
        max: f64,
        limit: usize,
    ) -> Result<Vec<Value>>;
    async fn z_rev_range_by_score<'a, K: Into<Arg<'a>> + Send>(
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
    fn get<'a, K: Into<Arg<'a>> + Send>(&mut self, key: K) -> GetResult;
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
    fn set<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&mut self, key: K, value: V);
    fn set_nx<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(
        &mut self,
        key: K,
        value: V,
    ) -> ConditionalResult;
    fn z_add<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(
        &mut self,
        key: K,
        value: V,
        score: f64,
    );
    fn z_rem<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&mut self, key: K, value: V);
    fn delete<'a, K: Into<Arg<'a>> + Send>(&mut self, key: K);
    fn delete_xx<'a, K: Into<Arg<'a>> + Send>(&mut self, key: K) -> ConditionalResult;
    fn s_add<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&mut self, key: K, value: V);
    fn s_rem<'a, 'b, K: Into<Arg<'a>> + Send, V: Into<Arg<'b>> + Send>(&mut self, key: K, value: V);
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
            tx.try_send(v).map_err(|e| -> Error { Box::new(e) })?;
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
    fn get<'a, K: Into<Arg<'a>> + Send>(&mut self, key: K) -> GetResult {
        let (ret, tx) = GetResult::new();
        self.gets.push((key.into().into_vec(), tx));
        ret
    }
}
