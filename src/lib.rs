#[macro_use]
extern crate async_trait;
#[cfg(test)]
#[macro_use]
extern crate serial_test;
extern crate simple_error;

use std::{collections::HashMap, convert::From, fmt, ops::Bound, sync::mpsc};

pub mod backendtest;

#[cfg(feature = "aws-sdk")]
pub mod aws_sdk_dynamodbstore;
pub mod dynstore;
pub mod memorystore;
pub mod readcache;
#[cfg(feature = "redis")]
pub mod redisstore;
#[cfg(feature = "rusoto")]
pub mod rusoto_dynamodbstore;

// re-export these crates since we use a fork
// once this issue is resolved, we can delete the fork: https://github.com/rusoto/rusoto/issues/1774
#[cfg(feature = "rusoto")]
pub use rusoto_core;
#[cfg(feature = "rusoto")]
pub use rusoto_credential;
#[cfg(feature = "rusoto")]
pub use rusoto_dynamodb;

#[derive(Debug)]
pub enum Error {
    // AtomicWriteConflict happens when an atomic write fails due to contention (but not due to a
    // failed conditional). For example, in DynamoDB this error happens when a transaction fails
    // due to a TransactionConflict.
    AtomicWriteConflict(ExplicitKey<'static>),
    Other(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl<E: std::error::Error + Send + Sync + 'static> From<E> for Error {
    fn from(e: E) -> Self {
        Self::Other(Box::new(e))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AtomicWriteConflict(key) => write!(f, "atomic write conflict on {:?}", key),
            Self::Other(e) => e.fmt(f),
        }
    }
}

type Result<T> = std::result::Result<T, Error>;

pub trait Key<'a>: Into<ExplicitKey<'a>> + Send + Sync {}

struct UnredactedKey<'a>(Arg<'a>);

impl<'a> Key<'a> for UnredactedKey<'a> {}

impl<'a> From<UnredactedKey<'a>> for ExplicitKey<'a> {
    fn from(val: UnredactedKey<'a>) -> Self {
        Self {
            redacted: val.0.clone(),
            unredacted: val.0,
        }
    }
}

/// Returns a key which will not be redacted in trace/log output.
pub fn unredacted<'a, A: Into<Arg<'a>>>(a: A) -> impl Key<'a> {
    UnredactedKey(a.into())
}

/// Static strings (typically literals) are assumed to be non-sensitive.
impl Key<'static> for &'static str {}

impl From<&'static str> for ExplicitKey<'static> {
    fn from(val: &'static str) -> Self {
        ExplicitKey {
            redacted: val.into(),
            unredacted: val.into(),
        }
    }
}

#[derive(Clone)]
pub struct ExplicitKey<'a> {
    pub redacted: Arg<'a>,
    pub unredacted: Arg<'a>,
}

impl ExplicitKey<'_> {
    pub fn into_owned(self) -> ExplicitKey<'static> {
        ExplicitKey {
            redacted: Arg::Owned(self.redacted.into_vec()),
            unredacted: Arg::Owned(self.unredacted.into_vec()),
        }
    }
}

impl<'a> PartialEq for ExplicitKey<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.unredacted == other.unredacted
    }
}

impl<'a> Key<'a> for &'a ExplicitKey<'a> {}

impl<'a> From<&'a ExplicitKey<'a>> for ExplicitKey<'a> {
    fn from(val: &'a ExplicitKey<'a>) -> Self {
        Self {
            redacted: Arg::Borrowed(val.redacted.as_bytes()),
            unredacted: Arg::Borrowed(val.unredacted.as_bytes()),
        }
    }
}

impl<'a> Key<'a> for ExplicitKey<'a> {}

impl std::fmt::Debug for ExplicitKey<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        std::fmt::Display::fmt(&self.redacted.as_bytes().escape_ascii(), f)
    }
}

#[derive(Clone)]
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

impl<'a> Into<Arg<'a>> for &'a Arg<'a> {
    fn into(self) -> Arg<'a> {
        Arg::Borrowed(self.as_bytes())
    }
}

impl<'a> Into<Arg<'a>> for &'a Vec<u8> {
    fn into(self) -> Arg<'a> {
        Arg::Borrowed(&self)
    }
}

impl<'a> Into<Arg<'a>> for &'a [u8] {
    fn into(self) -> Arg<'a> {
        Arg::Borrowed(self)
    }
}

impl<'a> Into<Arg<'a>> for &'a str {
    fn into(self) -> Arg<'a> {
        Arg::Borrowed(self.as_bytes())
    }
}

impl Into<Arg<'static>> for String {
    fn into(self) -> Arg<'static> {
        Arg::Owned(self.into_bytes())
    }
}

impl<'a> Into<Arg<'a>> for &'a String {
    fn into(self) -> Arg<'a> {
        Arg::Borrowed(self.as_bytes())
    }
}

impl<'a> PartialEq for Arg<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Ord, Eq)]
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
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.0.clone()
    }
}

#[async_trait]
pub trait Backend {
    async fn get<'a, K: Key<'a>>(&self, key: K) -> Result<Option<Value>>;
    async fn set<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()>;
    async fn set_eq<'a, 'b, 'c, K: Key<'a>, V: Into<Arg<'b>> + Send, OV: Into<Arg<'c>> + Send>(&self, key: K, value: V, old_value: OV) -> Result<bool>;
    async fn set_nx<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<bool>;
    async fn delete<'a, K: Key<'a>>(&self, key: K) -> Result<bool>;

    async fn s_add<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()>;
    async fn s_members<'a, K: Key<'a>>(&self, key: K) -> Result<Vec<Value>>;

    /// Increments the number with the given key by some number, returning the new value. If the
    /// key doesn't exist, it's set to the given number instead. To get the current value, you
    /// can pass 0 as n.
    async fn n_incr_by<'a, K: Key<'a>>(&self, key: K, n: i64) -> Result<i64>;

    async fn h_set<'a, 'b, 'c, K: Key<'a>, F: Into<Arg<'b>> + Send, V: Into<Arg<'c>> + Send, I: IntoIterator<Item = (F, V)> + Send>(
        &self,
        key: K,
        fields: I,
    ) -> Result<()>;
    async fn h_del<'a, 'b, K: Key<'a>, F: Into<Arg<'b>> + Send, I: IntoIterator<Item = F> + Send>(&self, key: K, fields: I) -> Result<()>;
    async fn h_get<'a, 'b, K: Key<'a>, F: Into<Arg<'b>> + Send>(&self, key: K, field: F) -> Result<Option<Value>>;
    async fn h_get_all<'a, K: Key<'a>>(&self, key: K) -> Result<HashMap<Vec<u8>, Value>>;

    async fn z_add<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V, score: f64) -> Result<()>;
    async fn z_rem<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()>;
    async fn z_count<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64) -> Result<usize>;
    async fn z_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>>;
    async fn z_rev_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>>;
    async fn z_range_by_lex<'a, 'b, 'c, K: Key<'a>, M: Into<Arg<'b>> + Send, N: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        min: Bound<M>,
        max: Bound<N>,
        limit: usize,
    ) -> Result<Vec<Value>>;
    async fn z_rev_range_by_lex<'a, 'b, 'c, K: Key<'a>, M: Into<Arg<'b>> + Send, N: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        min: Bound<M>,
        max: Bound<N>,
        limit: usize,
    ) -> Result<Vec<Value>>;

    async fn zh_add<'a, 'b, 'c, K: Key<'a>, F: Into<Arg<'b>> + Send, V: Into<Arg<'c>> + Send>(&self, key: K, field: F, value: V, score: f64) -> Result<()>;
    async fn zh_rem<'a, 'b, K: Key<'a>, F: Into<Arg<'b>> + Send>(&self, key: K, field: F) -> Result<()>;
    async fn zh_count<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64) -> Result<usize>;
    async fn zh_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>>;
    async fn zh_rev_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>>;

    async fn exec_batch(&self, op: BatchOperation<'_>) -> Result<()> {
        for op in op.ops {
            match op {
                BatchSubOperation::Get(key, tx) => {
                    if let Some(v) = self.get(&key).await? {
                        match tx.try_send(v) {
                            Ok(_) => {}
                            Err(mpsc::TrySendError::Disconnected(_)) => {}
                            Err(e) => return Err(e.into()),
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn exec_atomic_write(&self, op: AtomicWriteOperation<'_>) -> Result<bool>;
}

pub struct GetResult {
    rx: mpsc::Receiver<Value>,
}

impl GetResult {
    pub fn new() -> (Self, mpsc::SyncSender<Value>) {
        let (tx, rx) = mpsc::sync_channel(1);
        (Self { rx }, tx)
    }

    pub fn value(self) -> Option<Value> {
        self.rx.try_recv().ok()
    }
}

pub enum BatchSubOperation<'a> {
    Get(ExplicitKey<'a>, mpsc::SyncSender<Value>),
}

pub struct BatchOperation<'a> {
    pub ops: Vec<BatchSubOperation<'a>>,
}

impl<'a> BatchOperation<'a> {
    pub fn new() -> Self {
        Self { ops: vec![] }
    }

    pub fn get<K: Key<'a>>(&mut self, key: K) -> GetResult {
        let (ret, value_tx) = GetResult::new();
        self.ops.push(BatchSubOperation::Get(key.into(), value_tx));
        ret
    }
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

pub enum AtomicWriteSubOperation<'a> {
    Set(ExplicitKey<'a>, Arg<'a>),
    SetEQ(ExplicitKey<'a>, Arg<'a>, Arg<'a>, mpsc::SyncSender<bool>),
    SetNX(ExplicitKey<'a>, Arg<'a>, mpsc::SyncSender<bool>),
    ZAdd(ExplicitKey<'a>, Arg<'a>, f64),
    ZHAdd(ExplicitKey<'a>, Arg<'a>, Arg<'a>, f64),
    ZRem(ExplicitKey<'a>, Arg<'a>),
    ZHRem(ExplicitKey<'a>, Arg<'a>),
    Delete(ExplicitKey<'a>),
    DeleteXX(ExplicitKey<'a>, mpsc::SyncSender<bool>),
    SAdd(ExplicitKey<'a>, Arg<'a>),
    SRem(ExplicitKey<'a>, Arg<'a>),
    HSet(ExplicitKey<'a>, Vec<(Arg<'a>, Arg<'a>)>),
    HSetNX(ExplicitKey<'a>, Arg<'a>, Arg<'a>, mpsc::SyncSender<bool>),
    HDel(ExplicitKey<'a>, Vec<Arg<'a>>),
}

// DynamoDB can't do more than 25 operations in an atomic write so all backends should enforce this
// limit.
pub const MAX_ATOMIC_WRITE_SUB_OPERATIONS: usize = 25;

pub struct AtomicWriteOperation<'a> {
    pub ops: Vec<AtomicWriteSubOperation<'a>>,
}

impl<'a> AtomicWriteOperation<'a> {
    pub fn new() -> Self {
        Self { ops: vec![] }
    }

    pub fn set<'k: 'a, 'v: 'a, K: Key<'k>, V: Into<Arg<'v>> + Send>(&mut self, key: K, value: V) {
        self.ops.push(AtomicWriteSubOperation::Set(key.into(), value.into()));
    }

    pub fn set_eq<'k: 'a, 'v: 'a, 'ov: 'a, K: Key<'k>, V: Into<Arg<'v>> + Send, OV: Into<Arg<'ov>> + Send>(
        &mut self,
        key: K,
        value: V,
        old_value: OV,
    ) -> ConditionalResult {
        let (ret, tx) = ConditionalResult::new();
        self.ops.push(AtomicWriteSubOperation::SetEQ(key.into(), value.into(), old_value.into(), tx));
        ret
    }

    pub fn set_nx<'k: 'a, 'v: 'a, K: Key<'k>, V: Into<Arg<'v>> + Send>(&mut self, key: K, value: V) -> ConditionalResult {
        let (ret, tx) = ConditionalResult::new();
        self.ops.push(AtomicWriteSubOperation::SetNX(key.into(), value.into(), tx));
        ret
    }

    pub fn z_add<'k: 'a, 'v: 'a, K: Key<'k>, V: Into<Arg<'v>> + Send>(&mut self, key: K, value: V, score: f64) {
        self.ops.push(AtomicWriteSubOperation::ZAdd(key.into(), value.into(), score));
    }

    pub fn zh_add<'k: 'a, 'f: 'a, 'v: 'a, K: Key<'k>, F: Into<Arg<'f>> + Send, V: Into<Arg<'v>> + Send>(&mut self, key: K, field: F, value: V, score: f64) {
        self.ops.push(AtomicWriteSubOperation::ZHAdd(key.into(), field.into(), value.into(), score));
    }

    pub fn z_rem<'k: 'a, 'v: 'a, K: Key<'k>, V: Into<Arg<'v>> + Send>(&mut self, key: K, value: V) {
        self.ops.push(AtomicWriteSubOperation::ZRem(key.into(), value.into()));
    }

    pub fn zh_rem<'k: 'a, 'f: 'a, K: Key<'k>, F: Into<Arg<'f>> + Send>(&mut self, key: K, field: F) {
        self.ops.push(AtomicWriteSubOperation::ZHRem(key.into(), field.into()));
    }

    pub fn delete<'k: 'a, K: Key<'k>>(&mut self, key: K) {
        self.ops.push(AtomicWriteSubOperation::Delete(key.into()));
    }

    pub fn delete_xx<'k: 'a, K: Key<'k>>(&mut self, key: K) -> ConditionalResult {
        let (ret, tx) = ConditionalResult::new();
        self.ops.push(AtomicWriteSubOperation::DeleteXX(key.into(), tx));
        ret
    }

    pub fn s_add<'k: 'a, 'v: 'a, K: Key<'k>, V: Into<Arg<'v>> + Send>(&mut self, key: K, value: V) {
        self.ops.push(AtomicWriteSubOperation::SAdd(key.into(), value.into()));
    }

    pub fn s_rem<'k: 'a, 'v: 'a, K: Key<'k> + Send, V: Into<Arg<'v>> + Send>(&mut self, key: K, value: V) {
        self.ops.push(AtomicWriteSubOperation::SRem(key.into(), value.into()));
    }

    pub fn h_set<'k: 'a, 'f: 'a, 'v: 'a, K: Key<'k>, F: Into<Arg<'f>> + Send, V: Into<Arg<'v>> + Send, I: IntoIterator<Item = (F, V)> + Send>(
        &mut self,
        key: K,
        fields: I,
    ) {
        self.ops.push(AtomicWriteSubOperation::HSet(
            key.into(),
            fields.into_iter().map(|(k, v)| (k.into(), v.into())).collect(),
        ));
    }

    pub fn h_set_nx<'k: 'a, 'f: 'a, 'v: 'a, K: Key<'k>, F: Into<Arg<'f>> + Send, V: Into<Arg<'v>> + Send>(
        &mut self,
        key: K,
        field: F,
        value: V,
    ) -> ConditionalResult {
        let (ret, tx) = ConditionalResult::new();
        self.ops.push(AtomicWriteSubOperation::HSetNX(key.into(), field.into(), value.into(), tx));
        ret
    }

    pub fn h_del<'k: 'a, 'f: 'a, K: Key<'k>, F: Into<Arg<'f>> + Send, I: IntoIterator<Item = F> + Send>(&mut self, key: K, fields: I) {
        self.ops
            .push(AtomicWriteSubOperation::HDel(key.into(), fields.into_iter().map(|k| k.into()).collect()));
    }
}
