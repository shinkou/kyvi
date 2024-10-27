use std::collections::BTreeMap;
use std::sync::Mutex;

use regex::{Regex, Error};

use super::datatype::DataType;

static M: Mutex<BTreeMap<String, DataType>>= Mutex::new(BTreeMap::new());

pub fn del(k: &str) -> Option<DataType> {
	M.lock().unwrap().remove(k)
}

pub fn get(k: &str) -> Option<DataType> {
	M.lock().unwrap().get(k).cloned()
}

pub fn keys(p: &str) -> Result<DataType, Error> {
	let re = Regex::new(p)?;
	Ok(
		DataType::List(
			M.lock().unwrap().keys()
				.filter(|s| re.is_match(s))
				.map(|s| DataType::bulkStr(s))
				.collect()
		)
	)
}

pub fn memsize() -> usize {
	M.lock().unwrap().iter().map(|(k, v)| k.capacity() + v.capacity()).sum()
}

pub fn set(k: &str, v: &str) -> Option<DataType> {
	M.lock().unwrap().insert(String::from(k), DataType::bulkStr(v))
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
		assert_eq!(get("first"), Some(DataType::bulkStr("1st")));
		assert_eq!(get("second"), Some(DataType::bulkStr("2nd")));
		assert_eq!(get("third"), Some(DataType::bulkStr("3rd")));
		assert_eq!(keys(".*"), Ok(DataType::List(vec![
			DataType::bulkStr("first"),
			DataType::bulkStr("second"),
			DataType::bulkStr("third")
		])));
		assert_eq!(memsize(), 25usize);
		del("first");
		assert_eq!(get("first"), None);
		assert_eq!(get("second"), Some(DataType::bulkStr("2nd")));
		assert_eq!(get("third"), Some(DataType::bulkStr("3rd")));
		assert_eq!(memsize(), 17usize);
		del("second");
		assert_eq!(get("first"), None);
		assert_eq!(get("second"), None);
		assert_eq!(get("third"), Some(DataType::bulkStr("3rd")));
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
		assert_eq!(get("one"), Some(DataType::bulkStr("un")));
		assert_eq!(get("two"), Some(DataType::bulkStr("deux")));
		assert_eq!(get("three"), Some(DataType::bulkStr("trois")));
		assert_eq!(keys(".*"), Ok(DataType::List(vec![
			DataType::bulkStr("one"),
			DataType::bulkStr("three"),
			DataType::bulkStr("two")
		])));
		assert_eq!(memsize(), 22usize);
		del("one");
		del("two");
		del("three");
	}
}
