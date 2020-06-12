use super::{Arg, ConditionalResult, FallbackBatchOperation, Result, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::mpsc;
use std::sync::Mutex;

struct SortedSet {
    scores_by_member: HashMap<Vec<u8>, f64>,
    m: BTreeMap<Vec<u8>, Vec<u8>>,
}

enum MapEntry {
    Value(Vec<u8>),
    Set(HashSet<Vec<u8>>),
    SortedSet(SortedSet),
}

pub struct Backend {
    m: Mutex<HashMap<Vec<u8>, MapEntry>>,
}

enum Condition {
    KeyExists(Vec<u8>),
    KeyDoesNotExist(Vec<u8>),
}

enum Operation {
    Set(Vec<u8>, Vec<u8>),
    ZAdd(Vec<u8>, Vec<u8>, f64),
    ZRem(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    SAdd(Vec<u8>, Vec<u8>),
    SRem(Vec<u8>, Vec<u8>),
}

pub struct AtomicWriteOperation {
    conds: Vec<(Condition, mpsc::SyncSender<bool>)>,
    ops: Vec<Operation>,
}

impl AtomicWriteOperation {
    fn new() -> Self {
        AtomicWriteOperation {
            conds: vec![],
            ops: vec![],
        }
    }
}

impl super::AtomicWriteOperation for AtomicWriteOperation {
    fn set<K: Arg, V: Arg>(&mut self, key: K, value: V) {
        self.ops
            .push(Operation::Set(key.into_arg(), value.into_arg()));
    }

    fn set_nx<K: Arg, V: Arg>(&mut self, key: K, value: V) -> ConditionalResult {
        let key = key.into_arg();
        let (ret, tx) = ConditionalResult::new();
        self.conds
            .push((Condition::KeyDoesNotExist(key.clone()), tx));
        self.ops.push(Operation::Set(key, value.into_arg()));
        ret
    }

    fn z_add<K: Arg, V: Arg>(&mut self, key: K, value: V, score: f64) {
        self.ops
            .push(Operation::ZAdd(key.into_arg(), value.into_arg(), score));
    }

    fn z_rem<K: Arg, V: Arg>(&mut self, key: K, value: V) {
        self.ops
            .push(Operation::ZRem(key.into_arg(), value.into_arg()));
    }

    fn delete<K: Arg>(&mut self, key: K) {
        self.ops.push(Operation::Delete(key.into_arg()));
    }

    fn delete_xx<K: Arg>(&mut self, key: K) -> ConditionalResult {
        let key = key.into_arg();
        let (ret, tx) = ConditionalResult::new();
        self.conds.push((Condition::KeyExists(key.clone()), tx));
        self.ops.push(Operation::Delete(key.into_arg()));
        ret
    }

    fn s_add<K: Arg, V: Arg>(&mut self, key: K, value: V) {
        self.ops
            .push(Operation::SAdd(key.into_arg(), value.into_arg()));
    }

    fn s_rem<K: Arg, V: Arg>(&mut self, key: K, value: V) {
        self.ops
            .push(Operation::SRem(key.into_arg(), value.into_arg()));
    }
}

impl Backend {
    pub fn new() -> Self {
        Self {
            m: Mutex::new(HashMap::new()),
        }
    }

    fn get<K: Arg>(m: &mut HashMap<Vec<u8>, MapEntry>, key: K) -> Option<Value> {
        match m.get(&key.into_arg()) {
            Some(MapEntry::Value(v)) => Some(v.clone().into()),
            _ => None,
        }
    }

    fn set<K: Arg, V: Arg>(m: &mut HashMap<Vec<u8>, MapEntry>, key: K, value: V) {
        m.insert(key.into_arg(), MapEntry::Value(value.into_arg()));
    }

    fn s_add<K: Arg, V: Arg>(m: &mut HashMap<Vec<u8>, MapEntry>, key: K, value: V) -> Result<()> {
        let key = key.into_arg();
        let value = value.into_arg();
        match m.get_mut(&key) {
            Some(MapEntry::Set(s)) => {
                s.insert(value);
            }
            None => {
                let mut s = HashSet::new();
                s.insert(value);
                m.insert(key, MapEntry::Set(s));
            }
            _ => bail!("attempt to add member to existing non-set value"),
        }
        Ok(())
    }

    fn s_rem<K: Arg, V: Arg>(m: &mut HashMap<Vec<u8>, MapEntry>, key: K, value: V) -> Result<()> {
        let key = key.into_arg();
        let value = value.into_arg();
        match m.get_mut(&key) {
            Some(MapEntry::Set(s)) => {
                s.remove(&value);
            }
            _ => {}
        }
        Ok(())
    }

    fn z_add<K: Arg, V: Arg>(
        m: &mut HashMap<Vec<u8>, MapEntry>,
        key: K,
        value: V,
        score: f64,
    ) -> Result<()> {
        let key = key.into_arg();
        let value = value.into_arg();
        match m.get_mut(&key) {
            Some(MapEntry::SortedSet(s)) => {
                if let Some(&previous_score) = s.scores_by_member.get(&value) {
                    s.scores_by_member.remove(&value);
                    s.m.remove(&[float_sort_key(previous_score).to_vec(), value.clone()].concat());
                }

                s.scores_by_member.insert(value.clone(), score);
                s.m.insert(
                    [float_sort_key(score).to_vec(), value.clone()].concat(),
                    value,
                );
            }
            None => {
                let mut s = SortedSet {
                    scores_by_member: HashMap::new(),
                    m: BTreeMap::new(),
                };
                s.scores_by_member.insert(value.clone(), score);
                s.m.insert(
                    [float_sort_key(score).to_vec(), value.clone()].concat(),
                    value,
                );
                m.insert(key, MapEntry::SortedSet(s));
            }
            _ => bail!("attempt to add sorted set member to existing non-sorted-set value"),
        }
        Ok(())
    }

    fn z_rem<K: Arg, V: Arg>(m: &mut HashMap<Vec<u8>, MapEntry>, key: K, value: V) -> Result<()> {
        let key = key.into_arg();
        let value = value.into_arg();
        match m.get_mut(&key) {
            Some(MapEntry::SortedSet(s)) => {
                if let Some(&previous_score) = s.scores_by_member.get(&value) {
                    s.scores_by_member.remove(&value);
                    s.m.remove(&[float_sort_key(previous_score).to_vec(), value.clone()].concat());
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn delete<K: Arg>(m: &mut HashMap<Vec<u8>, MapEntry>, key: K) -> bool {
        m.remove(&key.into_arg()).is_some()
    }
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

#[async_trait]
impl super::Backend for Backend {
    type BatchOperation = FallbackBatchOperation;
    type AtomicWriteOperation = AtomicWriteOperation;

    async fn get<K: Arg>(&self, key: K) -> Result<Option<Value>> {
        let mut m = self.m.lock().unwrap();
        Ok(Self::get(&mut m, key))
    }

    async fn set<K: Arg, V: Arg>(&self, key: K, value: V) -> Result<()> {
        let mut m = self.m.lock().unwrap();
        Ok(Self::set(&mut m, key, value))
    }

    async fn set_eq<K: Arg, V: Arg, OV: Arg>(
        &self,
        key: K,
        value: V,
        old_value: OV,
    ) -> Result<bool> {
        let mut m = self.m.lock().unwrap();
        let key = key.into_arg();
        match m.get(&key) {
            Some(MapEntry::Value(v)) => {
                if *v == old_value.into_arg() {
                    m.insert(key, MapEntry::Value(value.into_arg()));
                    return Ok(true);
                }
            }
            _ => {}
        }
        Ok(false)
    }

    async fn set_nx<K: Arg, V: Arg>(&self, key: K, value: V) -> Result<bool> {
        let mut m = self.m.lock().unwrap();
        let key = key.into_arg();
        if !m.contains_key(&key) {
            m.insert(key, MapEntry::Value(value.into_arg()));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn delete<K: Arg>(&self, key: K) -> Result<bool> {
        let mut m = self.m.lock().unwrap();
        Ok(Self::delete(&mut m, key))
    }

    async fn s_add<K: Arg, V: Arg>(&self, key: K, value: V) -> Result<()> {
        let mut m = self.m.lock().unwrap();
        Self::s_add(&mut m, key, value)
    }

    async fn s_members<K: Arg>(&self, key: K) -> Result<Vec<Value>> {
        let m = self.m.lock().unwrap();
        let key = key.into_arg();
        match m.get(&key) {
            Some(MapEntry::Set(s)) => return Ok(s.iter().map(|v| v.clone().into()).collect()),
            _ => {}
        }
        Ok(vec![])
    }

    async fn z_add<K: Arg, V: Arg>(&self, key: K, value: V, score: f64) -> Result<()> {
        let mut m = self.m.lock().unwrap();
        Self::z_add(&mut m, key, value, score)
    }

    async fn z_count<K: Arg>(&self, key: K, min: f64, max: f64) -> Result<usize> {
        Ok(self.z_range_by_score(key, min, max, 0).await?.len())
    }

    async fn z_range_by_score<K: Arg>(
        &self,
        key: K,
        min: f64,
        max: f64,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let m = self.m.lock().unwrap();
        let key = key.into_arg();

        let s = match m.get(&key) {
            Some(MapEntry::SortedSet(s)) => s,
            _ => return Ok(vec![]),
        };

        Ok(s.m
            .range((
                Included(float_sort_key(min).to_vec()),
                match float_sort_key_after(max) {
                    Some(k) => Excluded(k.to_vec()),
                    _ => Unbounded,
                },
            ))
            .take(if limit > 0 { 0 } else { s.m.len() })
            .map(|(_, v)| v.clone().into())
            .collect())
    }

    async fn z_rev_range_by_score<K: Arg>(
        &self,
        key: K,
        min: f64,
        max: f64,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let m = self.m.lock().unwrap();
        let key = key.into_arg();

        let s = match m.get(&key) {
            Some(MapEntry::SortedSet(s)) => s,
            _ => return Ok(vec![]),
        };

        Ok(s.m
            .range((
                Included(float_sort_key(min).to_vec()),
                match float_sort_key_after(max) {
                    Some(k) => Excluded(k.to_vec()),
                    _ => Unbounded,
                },
            ))
            .rev()
            .take(if limit > 0 { 0 } else { s.m.len() })
            .map(|(_, v)| v.clone().into())
            .collect())
    }

    fn new_batch(&self) -> Self::BatchOperation {
        FallbackBatchOperation::new()
    }

    async fn exec_batch(&self, op: Self::BatchOperation) -> Result<()> {
        op.exec(self).await
    }

    fn new_atomic_write(&self) -> Self::AtomicWriteOperation {
        AtomicWriteOperation::new()
    }

    async fn exec_atomic_write(&self, op: Self::AtomicWriteOperation) -> Result<bool> {
        let mut m = self.m.lock().unwrap();

        for (cond, tx) in op.conds.into_iter() {
            let failed = match cond {
                Condition::KeyExists(key) => !m.contains_key(&key),
                Condition::KeyDoesNotExist(key) => m.contains_key(&key),
            };
            if failed {
                match tx.try_send(true) {
                    Ok(_) => {}
                    Err(mpsc::TrySendError::Disconnected(_)) => {}
                    Err(e) => return Err(e.into()),
                }
                return Ok(false);
            }
        }

        for op in op.ops.into_iter() {
            match op {
                Operation::Set(key, value) => {
                    Self::set(&mut m, key, value);
                }
                Operation::Delete(key) => {
                    Self::delete(&mut m, key);
                }
                Operation::SAdd(key, value) => {
                    Self::s_add(&mut m, key, value)?;
                }
                Operation::SRem(key, value) => {
                    Self::s_rem(&mut m, key, value)?;
                }
                Operation::ZAdd(key, value, score) => {
                    Self::z_add(&mut m, key, value, score)?;
                }
                Operation::ZRem(key, value) => {
                    Self::z_rem(&mut m, key, value)?;
                }
            }
        }

        Ok(true)
    }
}

#[cfg(test)]
mod test {
    mod backend {
        use crate::{memorystore, test_backend};
        test_backend!(|| memorystore::Backend::new());
    }
}
