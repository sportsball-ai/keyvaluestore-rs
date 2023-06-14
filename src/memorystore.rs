use crate::{ExplicitKey, Key};

use super::{Arg, AtomicWriteOperation, AtomicWriteSubOperation, Result, Value, MAX_ATOMIC_WRITE_SUB_OPERATIONS};
use simple_error::SimpleError;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::sync::{mpsc, Arc, Mutex};

struct SortedSet {
    scores_by_member: HashMap<Vec<u8>, f64>,
    m: BTreeMap<Vec<u8>, Vec<u8>>,
}

enum MapEntry {
    Value(Vec<u8>),
    Set(HashSet<Vec<u8>>),
    SortedSet(SortedSet),
    Map(HashMap<Vec<u8>, Vec<u8>>),
}

#[derive(Clone)]
pub struct Backend {
    m: Arc<Mutex<HashMap<Vec<u8>, MapEntry>>>,
}

impl Backend {
    pub fn new() -> Self {
        Self {
            m: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn get<'a, K: Key<'a>>(m: &mut HashMap<Vec<u8>, MapEntry>, key: K) -> Option<Value> {
        match m.get(key.into().unredacted.as_bytes()) {
            Some(MapEntry::Value(v)) => Some(v.clone().into()),
            _ => None,
        }
    }

    fn set<'a, 'b, V: Into<Arg<'b>> + Send>(m: &mut HashMap<Vec<u8>, MapEntry>, key: ExplicitKey, value: V) {
        m.insert(key.unredacted.into_vec(), MapEntry::Value(value.into().into_vec()));
    }

    fn s_add<'a, 'b, V: Into<Arg<'b>> + Send>(m: &mut HashMap<Vec<u8>, MapEntry>, key: ExplicitKey, value: V) -> Result<()> {
        let value = value.into().into_vec();
        match m.get_mut(key.unredacted.as_bytes()) {
            Some(MapEntry::Set(s)) => {
                s.insert(value);
            }
            None => {
                let mut s = HashSet::new();
                s.insert(value);
                m.insert(key.unredacted.into_vec(), MapEntry::Set(s));
            }
            _ => return Err(SimpleError::new("attempt to add member to existing non-set value").into()),
        }
        Ok(())
    }

    fn s_rem<'a, 'b, V: Into<Arg<'b>> + Send>(m: &mut HashMap<Vec<u8>, MapEntry>, key: ExplicitKey, value: V) -> Result<()> {
        let value = value.into();
        match m.get_mut(key.unredacted.as_bytes()) {
            Some(MapEntry::Set(s)) => {
                s.remove(value.as_bytes());
            }
            _ => {}
        }
        Ok(())
    }

    fn n_incr_by<'a>(m: &mut HashMap<Vec<u8>, MapEntry>, key: ExplicitKey, n: i64) -> Result<i64> {
        match m.get_mut(key.unredacted.as_bytes()) {
            Some(MapEntry::Value(v)) => {
                let current: i64 = std::str::from_utf8(&v)?.parse()?;
                let n = current + n;
                *v = n.to_string().as_bytes().to_vec();
                Ok(n)
            }
            _ => {
                m.insert(key.unredacted.into_vec(), MapEntry::Value(n.to_string().as_bytes().to_vec()));
                Ok(n)
            }
        }
    }

    fn h_set<'a, 'b, 'c, F: Into<Arg<'b>> + Send, V: Into<Arg<'c>> + Send, I: IntoIterator<Item = (F, V)> + Send>(
        m: &mut HashMap<Vec<u8>, MapEntry>,
        key: ExplicitKey,
        fields: I,
    ) -> Result<()> {
        match m.get_mut(key.unredacted.as_bytes()) {
            Some(MapEntry::Map(m)) => {
                for field in fields.into_iter() {
                    m.insert(field.0.into().into_vec(), field.1.into().into_vec());
                }
            }
            _ => {
                m.insert(
                    key.unredacted.into_vec(),
                    MapEntry::Map(fields.into_iter().map(|(k, v)| (k.into().into_vec(), v.into().into_vec())).collect()),
                );
            }
        }
        Ok(())
    }

    fn h_del<'a, 'b, F: Into<Arg<'b>> + Send, I: IntoIterator<Item = F> + Send>(m: &mut HashMap<Vec<u8>, MapEntry>, key: ExplicitKey, fields: I) -> Result<()> {
        match m.get_mut(key.unredacted.as_bytes()) {
            Some(MapEntry::Map(m)) => {
                for field in fields.into_iter() {
                    let field = field.into();
                    m.remove(field.as_bytes());
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn zh_add<'a, 'b, 'c, F: Into<Arg<'b>> + Send, V: Into<Arg<'c>> + Send>(
        m: &mut HashMap<Vec<u8>, MapEntry>,
        key: ExplicitKey,
        field: F,
        value: V,
        score: f64,
    ) -> Result<()> {
        let field = field.into();
        let value = value.into();
        match m.get_mut(key.unredacted.as_bytes()) {
            Some(MapEntry::SortedSet(s)) => {
                if let Some(&previous_score) = s.scores_by_member.get(field.as_bytes()) {
                    s.scores_by_member.remove(field.as_bytes());
                    s.m.remove(&[&float_sort_key(previous_score), field.as_bytes()].concat());
                }

                s.scores_by_member.insert(field.to_vec(), score);
                s.m.insert([&float_sort_key(score), field.as_bytes()].concat(), value.into_vec());
            }
            None => {
                let mut s = SortedSet {
                    scores_by_member: HashMap::new(),
                    m: BTreeMap::new(),
                };
                s.scores_by_member.insert(field.to_vec(), score);
                s.m.insert([&float_sort_key(score), field.as_bytes()].concat(), value.into_vec());
                m.insert(key.unredacted.into_vec(), MapEntry::SortedSet(s));
            }
            _ => return Err(SimpleError::new("attempt to add sorted set member to existing non-sorted-set value").into()),
        }
        Ok(())
    }

    fn zh_rem<'a, 'b, F: Into<Arg<'b>> + Send>(m: &mut HashMap<Vec<u8>, MapEntry>, key: ExplicitKey, field: F) -> Result<()> {
        let field = field.into();
        match m.get_mut(key.unredacted.as_bytes()) {
            Some(MapEntry::SortedSet(s)) => {
                if let Some(&previous_score) = s.scores_by_member.get(field.as_bytes()) {
                    s.scores_by_member.remove(field.as_bytes());
                    s.m.remove(&[&float_sort_key(previous_score), field.as_bytes()].concat());
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn delete<'a>(m: &mut HashMap<Vec<u8>, MapEntry>, key: ExplicitKey) -> bool {
        m.remove(key.unredacted.as_bytes()).is_some()
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

fn map_bound<T, U, F: FnOnce(T) -> U>(b: Bound<T>, f: F) -> Bound<U> {
    match b {
        Bound::Included(v) => Bound::Included(f(v)),
        Bound::Excluded(v) => Bound::Excluded(f(v)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn bound_value<T>(b: &Bound<T>) -> Option<&T> {
    match b {
        Bound::Included(v) => Some(v),
        Bound::Excluded(v) => Some(v),
        Bound::Unbounded => None,
    }
}

#[async_trait]
impl super::Backend for Backend {
    async fn get<'a, K: Key<'a>>(&self, key: K) -> Result<Option<Value>> {
        let mut m = self.m.lock().unwrap();
        Ok(Self::get(&mut m, key))
    }

    async fn set<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        let mut m = self.m.lock().unwrap();
        Ok(Self::set(&mut m, key.into(), value))
    }

    async fn set_eq<'a, 'b, 'c, K: Key<'a>, V: Into<Arg<'b>> + Send, OV: Into<Arg<'c>> + Send>(&self, key: K, value: V, old_value: OV) -> Result<bool> {
        let mut m = self.m.lock().unwrap();
        let key = key.into();
        match m.get(key.unredacted.as_bytes()) {
            Some(MapEntry::Value(v)) => {
                if *v == old_value.into().as_bytes() {
                    m.insert(key.unredacted.into_vec(), MapEntry::Value(value.into().into_vec()));
                    return Ok(true);
                }
            }
            _ => {}
        }
        Ok(false)
    }

    async fn set_nx<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<bool> {
        let mut m = self.m.lock().unwrap();
        let key = key.into();
        if !m.contains_key(key.unredacted.as_bytes()) {
            m.insert(key.unredacted.into_vec(), MapEntry::Value(value.into().into_vec()));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn delete<'a, K: Key<'a>>(&self, key: K) -> Result<bool> {
        let mut m = self.m.lock().unwrap();
        Ok(Self::delete(&mut m, key.into()))
    }

    async fn s_add<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        let mut m = self.m.lock().unwrap();
        Self::s_add(&mut m, key.into(), value)
    }

    async fn s_members<'a, K: Key<'a>>(&self, key: K) -> Result<Vec<Value>> {
        let m = self.m.lock().unwrap();
        let key = key.into();
        Ok(match m.get(key.unredacted.as_bytes()) {
            Some(MapEntry::Set(s)) => s.iter().map(|v| v.clone().into()).collect(),
            _ => vec![],
        })
    }

    async fn n_incr_by<'a, K: Key<'a>>(&self, key: K, n: i64) -> Result<i64> {
        let mut m = self.m.lock().unwrap();
        Self::n_incr_by(&mut m, key.into(), n)
    }

    async fn h_set<'a, 'b, 'c, K: Key<'a>, F: Into<Arg<'b>> + Send, V: Into<Arg<'c>> + Send, I: IntoIterator<Item = (F, V)> + Send>(
        &self,
        key: K,
        fields: I,
    ) -> Result<()> {
        let mut m = self.m.lock().unwrap();
        Self::h_set(&mut m, key.into(), fields)
    }

    async fn h_del<'a, 'b, K: Key<'a>, F: Into<Arg<'b>> + Send, I: IntoIterator<Item = F> + Send>(&self, key: K, fields: I) -> Result<()> {
        let mut m = self.m.lock().unwrap();
        Self::h_del(&mut m, key.into(), fields)
    }

    async fn h_get<'a, 'b, K: Key<'a>, F: Into<Arg<'b>> + Send>(&self, key: K, field: F) -> Result<Option<Value>> {
        let m = self.m.lock().unwrap();
        let key = key.into();
        let field = field.into();
        Ok(match m.get(key.unredacted.as_bytes()) {
            Some(MapEntry::Map(m)) => m.get(field.as_bytes()).map(|v| v.clone().into()),
            _ => None,
        })
    }

    async fn h_get_all<'a, K: Key<'a>>(&self, key: K) -> Result<HashMap<Vec<u8>, Value>> {
        let m = self.m.lock().unwrap();
        let key = key.into();
        Ok(match m.get(key.unredacted.as_bytes()) {
            Some(MapEntry::Map(m)) => m.iter().map(|(k, v)| (k.clone(), v.clone().into())).collect(),
            _ => HashMap::new(),
        })
    }

    async fn z_add<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V, score: f64) -> Result<()> {
        let mut m = self.m.lock().unwrap();
        let v = value.into();
        Self::zh_add(&mut m, key.into(), &v, &v, score)
    }

    async fn zh_add<'a, 'b, 'c, K: Key<'a>, F: Into<Arg<'b>> + Send, V: Into<Arg<'c>> + Send>(&self, key: K, field: F, value: V, score: f64) -> Result<()> {
        let mut m = self.m.lock().unwrap();
        Self::zh_add(&mut m, key.into(), field, value, score)
    }

    async fn zh_rem<'a, 'b, K: Key<'a>, F: Into<Arg<'b>> + Send>(&self, key: K, field: F) -> Result<()> {
        let mut m = self.m.lock().unwrap();
        Self::zh_rem(&mut m, key.into(), field)
    }

    async fn z_rem<'a, 'b, K: Key<'a>, V: Into<Arg<'b>> + Send>(&self, key: K, value: V) -> Result<()> {
        self.zh_rem(key, value).await
    }

    async fn z_count<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64) -> Result<usize> {
        Ok(self.z_range_by_score(key, min, max, 0).await?.len())
    }

    async fn zh_count<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64) -> Result<usize> {
        self.z_count(key, min, max).await
    }

    async fn z_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        let m = self.m.lock().unwrap();
        let key = key.into();

        let s = match m.get(key.unredacted.as_bytes()) {
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
            .take(if limit > 0 { limit } else { s.m.len() })
            .map(|(_, v)| v.clone().into())
            .collect())
    }

    async fn zh_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        self.z_range_by_score(key, min, max, limit).await
    }

    async fn z_rev_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        let m = self.m.lock().unwrap();
        let key = key.into();

        let s = match m.get(key.unredacted.as_bytes()) {
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
            .take(if limit > 0 { limit } else { s.m.len() })
            .map(|(_, v)| v.clone().into())
            .collect())
    }

    async fn zh_rev_range_by_score<'a, K: Key<'a>>(&self, key: K, min: f64, max: f64, limit: usize) -> Result<Vec<Value>> {
        self.z_rev_range_by_score(key, min, max, limit).await
    }

    async fn z_range_by_lex<'a, 'b, 'c, K: Key<'a>, M: Into<Arg<'b>> + Send, N: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        min: Bound<M>,
        max: Bound<N>,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let m = self.m.lock().unwrap();
        let key = key.into();

        let s = match m.get(key.unredacted.as_bytes()) {
            Some(MapEntry::SortedSet(s)) => s,
            _ => return Ok(vec![]),
        };

        let min = map_bound(min, |v| (&[&float_sort_key(0.0), v.into().as_bytes()]).concat().to_vec());
        let max = map_bound(max, |v| (&[&float_sort_key(0.0), v.into().as_bytes()]).concat().to_vec());

        if let (Some(min), Some(max)) = (bound_value(&min), bound_value(&max)) {
            if min > max {
                return Ok(vec![]);
            }
        }

        Ok(s.m
            .range((min, max))
            .take(if limit > 0 { limit } else { s.m.len() })
            .map(|(_, v)| v.clone().into())
            .collect())
    }

    async fn z_rev_range_by_lex<'a, 'b, 'c, K: Key<'a>, M: Into<Arg<'b>> + Send, N: Into<Arg<'c>> + Send>(
        &self,
        key: K,
        min: Bound<M>,
        max: Bound<N>,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let m = self.m.lock().unwrap();
        let key = key.into();

        let s = match m.get(key.unredacted.as_bytes()) {
            Some(MapEntry::SortedSet(s)) => s,
            _ => return Ok(vec![]),
        };

        let min = map_bound(min, |v| (&[&float_sort_key(0.0), v.into().as_bytes()]).concat().to_vec());
        let max = map_bound(max, |v| (&[&float_sort_key(0.0), v.into().as_bytes()]).concat().to_vec());

        if let (Some(min), Some(max)) = (bound_value(&min), bound_value(&max)) {
            if min > max {
                return Ok(vec![]);
            }
        }

        Ok(s.m
            .range((min, max))
            .rev()
            .take(if limit > 0 { limit } else { s.m.len() })
            .map(|(_, v)| v.clone().into())
            .collect())
    }

    async fn exec_atomic_write(&self, op: AtomicWriteOperation<'_>) -> Result<bool> {
        if op.ops.len() > MAX_ATOMIC_WRITE_SUB_OPERATIONS {
            return Err(SimpleError::new("max sub-operation count exceeded").into());
        }

        let mut m = self.m.lock().unwrap();

        for subop in &op.ops {
            if let Some(failure_tx) = match subop {
                AtomicWriteSubOperation::Set(..) => None,
                AtomicWriteSubOperation::SetEQ(key, _, old_value, tx) => match m.get(key.unredacted.as_bytes()) {
                    Some(MapEntry::Value(v)) => {
                        if *v == old_value.as_bytes() {
                            None
                        } else {
                            Some(tx)
                        }
                    }
                    _ => Some(tx),
                },
                AtomicWriteSubOperation::SetNX(key, _, tx) => {
                    if m.contains_key(key.unredacted.as_bytes()) {
                        Some(tx)
                    } else {
                        None
                    }
                }
                AtomicWriteSubOperation::ZAdd(..) => None,
                AtomicWriteSubOperation::ZHAdd(..) => None,
                AtomicWriteSubOperation::ZRem(..) => None,
                AtomicWriteSubOperation::ZHRem(..) => None,
                AtomicWriteSubOperation::Delete(..) => None,
                AtomicWriteSubOperation::DeleteXX(key, tx) => {
                    if !m.contains_key(key.unredacted.as_bytes()) {
                        Some(tx)
                    } else {
                        None
                    }
                }
                AtomicWriteSubOperation::SAdd(..) => None,
                AtomicWriteSubOperation::SRem(..) => None,
                AtomicWriteSubOperation::HSet(..) => None,
                AtomicWriteSubOperation::HSetNX(key, field, _, tx) => match m.get(key.unredacted.as_bytes()) {
                    Some(MapEntry::Map(m)) => match m.contains_key(field.as_bytes()) {
                        true => Some(tx),
                        _ => None,
                    },
                    _ => None,
                },
                AtomicWriteSubOperation::HDel(..) => None,
            } {
                match failure_tx.try_send(true) {
                    Ok(_) => {}
                    Err(mpsc::TrySendError::Disconnected(_)) => {}
                    Err(e) => return Err(e.into()),
                }
                return Ok(false);
            }
        }

        for subop in op.ops {
            match subop {
                AtomicWriteSubOperation::Set(key, value) => {
                    Self::set(&mut m, key, value);
                }
                AtomicWriteSubOperation::SetEQ(key, value, _, _) => {
                    Self::set(&mut m, key, value);
                }
                AtomicWriteSubOperation::SetNX(key, value, _) => {
                    Self::set(&mut m, key, value);
                }
                AtomicWriteSubOperation::Delete(key) => {
                    Self::delete(&mut m, key);
                }
                AtomicWriteSubOperation::DeleteXX(key, _) => {
                    Self::delete(&mut m, key);
                }
                AtomicWriteSubOperation::SAdd(key, value) => {
                    Self::s_add(&mut m, key, value)?;
                }
                AtomicWriteSubOperation::SRem(key, value) => {
                    Self::s_rem(&mut m, key, value)?;
                }
                AtomicWriteSubOperation::ZAdd(key, value, score) => {
                    Self::zh_add(&mut m, key, &value, &value, score)?;
                }
                AtomicWriteSubOperation::ZHAdd(key, field, value, score) => {
                    Self::zh_add(&mut m, key, field, value, score)?;
                }
                AtomicWriteSubOperation::ZRem(key, value) => {
                    Self::zh_rem(&mut m, key, value)?;
                }
                AtomicWriteSubOperation::ZHRem(key, field) => {
                    Self::zh_rem(&mut m, key, field)?;
                }
                AtomicWriteSubOperation::HSet(key, fields) => {
                    Self::h_set(&mut m, key, fields)?;
                }
                AtomicWriteSubOperation::HSetNX(key, field, value, _) => {
                    Self::h_set(&mut m, key, vec![(field, value)])?;
                }
                AtomicWriteSubOperation::HDel(key, fields) => {
                    Self::h_del(&mut m, key, fields)?;
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
        test_backend!(|| async { memorystore::Backend::new() });
    }
}
