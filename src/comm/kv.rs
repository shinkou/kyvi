use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use regex::{Regex, Error};

use super::datatype::DataType;

static M: Mutex<BTreeMap<String, DataType>>= Mutex::new(BTreeMap::new());

pub fn decr(k: &str) -> Result<DataType, &str> {
	let mut m = M.lock().unwrap();
	match m.get(k) {
		Some(data) => {
			match data {
				DataType::Integer(i) => {
					let x: i64 = i - 1;
					m.insert(String::from(k), DataType::Integer(x));
					Ok(DataType::Integer(x))
				},
				DataType::BulkString(s) => {
					match s.parse::<i64>() {
						Ok(i) => {
							let x: i64 = i - 1;
							m.insert(String::from(k), DataType::Integer(x));
							Ok(DataType::Integer(x))
						},
						Err(_) => Err("Target must be integer")
					}
				},
				_ => Err("Target must be integer")
			}
		},
		None => {
			m.insert(String::from(k), DataType::Integer(-1));
			Ok(DataType::Integer(-1))
		}
	}
}

pub fn del(k: &str) -> Option<DataType> {
	M.lock().unwrap().remove(k)
}

pub fn get(k: &str) -> Option<DataType> {
	M.lock().unwrap().get(k).cloned()
}

pub fn hdel(k: &str, fs: Vec<String>) -> Result<DataType, &str> {
	let mut m = M.lock().unwrap();
	match m.get(k) {
		Some(data) => {
			match data {
				DataType::Hashset(h) => {
					let mut somehmap = h.clone();
					let cnt = fs.into_iter().map(|f| {
						match somehmap.remove(&DataType::bulkStr(&f)) {
							Some(_) => 1i64,
							None => 0i64
						}
					}).sum::<i64>();
					let hmap2save = DataType::hset(&somehmap);
					m.insert(String::from(k), hmap2save);
					Ok(DataType::Integer(cnt))
				}
				_ => Err("Key must associate with a hash")
			}
		},
		None => Err("Key must associate with a hash")
	}
}

pub fn hget<'a>(k: &'a str, f: &'a str) -> Result<DataType, &'a str> {
	let m = M.lock().unwrap();
	match m.get(k) {
		Some(data) => {
			match data {
				DataType::Hashset(h) => match h.get(&DataType::bulkStr(f)) {
					Some(v) => Ok(v.clone()),
					None => Ok(DataType::Null)
				}
				_ => Err("Key must associate with a hash")
			}
		},
		None => Err("Key must associate with a hash")
	}
}

pub fn hset(k: &str, nvs: Vec<String>) -> Result<DataType, &str> {
	if 0 != nvs.len() % 2 {
		return Err("Number of elements must a multiple of 2");
	}
	let mut m = M.lock().unwrap();
	match m.get(k) {
		Some(data) => {
			match data {
				DataType::Hashset(h) => {
					let mut somehmap = h.clone();
					nvs.chunks(2).for_each(|x| {somehmap.insert(
						DataType::bulkStr(&x[0]),
						DataType::bulkStr(&x[1])
					);});
					let hmap2save = DataType::hset(&somehmap);
					m.insert(String::from(k), hmap2save);
					Ok(DataType::Integer(somehmap.len().try_into().unwrap()))
				},
				_ => Err("Key must associate with a hash")
			}
		},
		None => {
			let mut somehmap: HashMap<DataType, DataType> = HashMap::new();
			nvs.chunks(2).for_each(|x| {somehmap.insert(
				DataType::bulkStr(&x[0]),
				DataType::bulkStr(&x[1])
			);});
			let hmap2save = DataType::hset(&somehmap);
			m.insert(String::from(k), hmap2save);
			Ok(DataType::Integer(somehmap.len().try_into().unwrap()))
		}
	}
}

pub fn incr(k: &str) -> Result<DataType, &str> {
	let mut m = M.lock().unwrap();
	match m.get(k) {
		Some(data) => {
			match data {
				DataType::Integer(i) => {
					let x: i64 = i + 1;
					m.insert(String::from(k), DataType::Integer(x));
					Ok(DataType::Integer(x))
				},
				DataType::BulkString(s) => {
					match s.parse::<i64>() {
						Ok(i) => {
							let x: i64 = i + 1;
							m.insert(String::from(k), DataType::Integer(x));
							Ok(DataType::Integer(x))
						},
						Err(_) => Err("Target must be integer")
					}
				},
				_ => Err("Target must be integer")
			}
		},
		None => {
			m.insert(String::from(k), DataType::Integer(1));
			Ok(DataType::Integer(1))
		}
	}
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
