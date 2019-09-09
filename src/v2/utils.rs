use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

pub fn hash<T: Hash>(t: &T) -> u64 {
    let mut hasher = DefaultHasher::new();

    t.hash(&mut hasher);

    hasher.finish()
}