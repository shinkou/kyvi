use std::collections::BTreeMap;
use std::sync::Mutex;

use regex::{Regex, Error};

static M: Mutex<BTreeMap<String, String>>= Mutex::new(BTreeMap::new());

pub fn del(k: &str) -> Option<String> {
	let mut kv = M.lock().unwrap();
	kv.remove(k)
}

pub fn get(k: &str) -> Option<String> {
	let kv = M.lock().unwrap();
	kv.get(k).cloned()
}

pub fn keys(p: &str) -> Result<Vec<String>, Error> {
	match Regex::new(p) {
		Ok(re) => {
			let kv = M.lock().unwrap();
			Ok(
				kv.keys()
					.filter(|s| re.is_match(s))
					.map(|s| s.to_string())
					.collect()
			)
		},
		Err(e) => Err(e)
	}
}

pub fn memsize() -> usize {
	let kv = M.lock().unwrap();
	kv.iter().map(|(k, v)| k.capacity() + v.capacity()).sum()
}

pub fn set(k: &str, v: &str) -> Option<String> {
	let mut kv = M.lock().unwrap();
	kv.insert(String::from(k), String::from(v))
}
