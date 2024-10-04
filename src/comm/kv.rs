use std::collections::BTreeMap;
use std::sync::Mutex;

static M: Mutex<BTreeMap<String, String>>= Mutex::new(BTreeMap::new());

pub fn get(k: &str) -> Option<String> {
	let kv = M.lock().unwrap();
	kv.get(k).cloned()
}

pub fn keys() -> Vec<String> {
	let kv = M.lock().unwrap();
	kv.keys().map(|s| s.to_string()).collect()
}

pub fn set(k: &str, v: &str) -> Option<String> {
	let mut kv = M.lock().unwrap();
	kv.insert(String::from(k), String::from(v))
}
