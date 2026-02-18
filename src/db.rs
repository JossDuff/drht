use std::collections::{hash_map::DefaultHasher, HashMap};
use std::hash::{Hash, Hasher};
use tokio::sync::Mutex;

pub struct StripedDb<K, V> {
    stripes: Vec<Mutex<HashMap<K, V>>>,
    num_stripes: usize,
}

impl<K, V> StripedDb<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    pub fn new(num_stripes: usize) -> Self {
        let stripes = (0..num_stripes)
            .map(|_| Mutex::new(HashMap::new()))
            .collect();
        Self {
            stripes,
            num_stripes,
        }
    }

    fn stripe_index(&self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_stripes
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

    // Atomically stores/replaces three key-value pairs.
    // Acquires stripes in sorted order to prevent deadlocks.
    // TODO: should I have some check here to make sure all the individual puts were successful?
    pub async fn tri_put(&self, k1: K, v1: V, k2: K, v2: V, k3: K, v3: V) -> bool {
        let s1 = self.stripe_index(&k1);
        let s2 = self.stripe_index(&k2);
        let s3 = self.stripe_index(&k3);

        let mut stripe_indices = vec![s1, s2, s3];
        stripe_indices.sort();
        stripe_indices.dedup();

        // Acquire all unique locks in sorted order
        let mut guards: Vec<_> = Vec::with_capacity(stripe_indices.len());
        for &idx in &stripe_indices {
            guards.push(self.stripes[idx].lock().await);
        }

        // Insert each key into its corresponding guard
        for (k, v, s) in [(k1, v1, s1), (k2, v2, s2), (k3, v3, s3)] {
            let pos = stripe_indices.iter().position(|&i| i == s).unwrap();
            guards[pos].insert(k, v);
        }

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

    #[tokio::test]
    async fn test_tri_put_success() {
        let db = StripedDb::new(8);
        assert!(db.tri_put(1u64, "a", 2, "b", 3, "c").await);
        assert_eq!(db.get(&1).await, Some("a"));
        assert_eq!(db.get(&2).await, Some("b"));
        assert_eq!(db.get(&3).await, Some("c"));
    }

    #[tokio::test]
    async fn test_tri_put_replaces_existing() {
        let db = StripedDb::new(8);
        assert!(db.put(2u64, "old").await);
        assert!(db.tri_put(1u64, "a", 2, "b", 3, "c").await);
        assert_eq!(db.get(&1).await, Some("a"));
        assert_eq!(db.get(&2).await, Some("b")); // replaced
        assert_eq!(db.get(&3).await, Some("c"));
    }

    #[tokio::test]
    async fn test_tri_put_same_stripe() {
        let db: StripedDb<u64, &str> = StripedDb::new(1); // all keys same stripe
        assert!(db.tri_put(1, "a", 2, "b", 3, "c").await);
        assert_eq!(db.get(&1).await, Some("a"));
        assert_eq!(db.get(&2).await, Some("b"));
        assert_eq!(db.get(&3).await, Some("c"));
    }
}
