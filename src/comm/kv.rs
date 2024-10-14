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

#[cfg(test)]
mod tests {
	use serial_test::serial;
	use super::*;

	#[test]
	#[serial]
	fn plan1() {
		set("first", "1st");
		set("second", "2nd");
		set("third", "3rd");
		assert_eq!(get("first"), Some("1st".to_string()));
		assert_eq!(get("second"), Some("2nd".to_string()));
		assert_eq!(get("third"), Some("3rd".to_string()));
		assert_eq!(keys(".*"), Ok(vec![
			"first".to_string(),
			"second".to_string(),
			"third".to_string()
		]));
		assert_eq!(memsize(), 25usize);
		del("first");
		assert_eq!(get("first"), None);
		assert_eq!(get("second"), Some("2nd".to_string()));
		assert_eq!(get("third"), Some("3rd".to_string()));
		assert_eq!(memsize(), 17usize);
		del("second");
		assert_eq!(get("first"), None);
		assert_eq!(get("second"), None);
		assert_eq!(get("third"), Some("3rd".to_string()));
		assert_eq!(memsize(), 8usize);
		del("third");
		assert_eq!(get("first"), None);
		assert_eq!(get("second"), None);
		assert_eq!(get("third"), None);
		assert_eq!(memsize(), 0usize);
	}

	#[test]
	#[serial]
	fn plan2() {
		set("one", "un");
		set("two", "deux");
		set("three", "trois");
		assert_eq!(get("one"), Some("un".to_string()));
		assert_eq!(get("two"), Some("deux".to_string()));
		assert_eq!(get("three"), Some("trois".to_string()));
		assert_eq!(keys(".*"), Ok(vec![
			"one".to_string(),
			"three".to_string(),
			"two".to_string()
		]));
		assert_eq!(memsize(), 22usize);
		del("one");
		del("two");
		del("three");
	}
}
