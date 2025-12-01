use kvs::KvStore;
use std::{env::current_dir, time::Instant};

fn main() {
    let mut store = KvStore::open(current_dir().unwrap()).unwrap();

    let start = Instant::now();
    for i in 0..1000 {
        store.set(format!("key{}", i), format!("val{}", i)).unwrap();
    }

    for i in 0..1000 {
        store.set(format!("key{}", i), format!("val{}", i+1)).unwrap();
    }
    println!("1M SET took {:?}", start.elapsed());

    let start = Instant::now();
    for i in 0..1000 {
        let val = store.get(format!("key{}", i)).unwrap();
        match val {
            Some(v) => println!("{v}"),
            None => ()
        }
    }
    println!("1M GET took {:?}", start.elapsed());
}
