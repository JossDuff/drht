use std::collections::{hash_map::DefaultHasher, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct StripedDb<K, V> {
    stripes: Vec<Arc<Mutex<HashMap<K, V>>>>,
    num_stripes: usize,
}

impl<K, V> StripedDb<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    pub fn new(num_stripes: usize) -> Self {
        let stripes = (0..num_stripes)
            .map(|_| Arc::new(Mutex::new(HashMap::new())))
            .collect();
        Self {
            stripes,
            num_stripes,
        }
    }

    // stripe index for a key is the same across all nodes
    pub fn stripe_index(&self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_stripes
    }

    pub fn get_stripe_by_index(&self, idx: usize) -> Arc<Mutex<HashMap<K, V>>> {
        self.stripes[idx].clone()
    }

    // get a value
    pub async fn get(&self, key: &K) -> Option<V> {
        let guard = self.stripes[self.stripe_index(key)].lock().await;
        guard.get(key).cloned()
    }

    // Stores/replaces the value
    pub async fn put(&self, key: K, value: V) -> bool {
        let mut guard = self.stripes[self.stripe_index(&key)].lock().await;
        guard.insert(key, value);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_put_and_get() {
        let db = StripedDb::new(8);
        assert!(db.put(1u64, "hello").await);
        assert_eq!(db.get(&1).await, Some("hello"));
        assert_eq!(db.get(&2).await, None);
    }

    #[tokio::test]
    async fn test_put_replaces() {
        let db = StripedDb::new(8);
        assert!(db.put(1u64, "first").await);
        assert!(db.put(1u64, "second").await);
        assert_eq!(db.get(&1).await, Some("second"));
    }
}
