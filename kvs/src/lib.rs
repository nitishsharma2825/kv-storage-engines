use std::collections::HashMap;

pub struct KvStore {
    memtable: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {
            memtable: HashMap::new(),
        }
    }

    pub fn get(&self, key: String) -> Option<String> {
        self.memtable.get(&key).cloned()
    }

    pub fn set(&mut self, key: String, value: String) {
        self.memtable.insert(key, value);
    }

    pub fn remove(&mut self, key: String) {
        self.memtable.remove(&key);
    }
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}
