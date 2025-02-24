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
				DataType::BulkString(s) => {
					match s.parse::<i64>() {
						Ok(i) => {
							let x: i64 = i - 1;
							m.insert(
								String::from(k),
								DataType::BulkString(x.to_string())
							);
							Ok(DataType::Integer(x))
						},
						Err(_) => Err(
							"ERR value is not an integer or out of range"
						)
					}
				},
				_ => Err(
					"WRONGTYPE Operation against a key holding the wrong \
					kind of value"
				)
			}
		},
		None => {
			m.insert(String::from(k), DataType::bulkStr("-1"));
			Ok(DataType::Integer(-1))
		}
	}
}

pub fn del(ks: &Vec<String>) -> Result<DataType, &str> {
	let mut m = M.lock().unwrap();
	let cnt: i64 = ks.into_iter().map(|k| {
		match m.remove(k) {
			Some(_) => 1i64,
			None => 0i64
		}
	}).sum::<i64>();
	Ok(DataType::Integer(cnt))
}

pub fn get(k: &str) -> Result<DataType, &str> {
	match M.lock().unwrap().get(k) {
		Some(data) => {
			match data {
				DataType::BulkString(_s) => Ok(data.clone()),
				_ => Err(
					"WRONGTYPE Operation against a key holding the wrong \
					kind of value"
				)
			}
		},
		None => Ok(DataType::Null)
	}
}

pub fn getdel(k: &str) -> Result<DataType, &str> {
	let resdata = get(k);
	match resdata {
		Ok(ref _data) => {
			let _ = del(&vec![k.to_string()]);
			resdata
		},
		Err(e) => resdata
	}
}

pub fn getset<'a>(k: &'a str, v: &'a str) -> Result<DataType, &'a str> {
	let resdata = get(k);
	match resdata {
		Ok(ref _data) => {
			let _ = set(k, v);
			resdata
		},
		Err(e) => resdata
	}
}

pub fn hdel(k: &str, fs: Vec<String>) -> Result<DataType, &str> {
	let mut m = M.lock().unwrap();
	match m.get_mut(k) {
		Some(data) => {
			match data {
				DataType::Hashset(hmap) => {
					let cnt = fs.into_iter().map(|f| {
						match hmap.remove(&DataType::bulkStr(&f)) {
							Some(_) => 1i64,
							None => 0i64
						}
					}).sum::<i64>();
					Ok(DataType::Integer(cnt))
				},
				_ => Err(
					"WRONGTYPE Operation against a key holding the wrong \
					kind of value"
				)
			}
		},
		None => Ok(DataType::Integer(0))
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
				},
				_ => Err(
					"WRONGTYPE Operation against a key holding the wrong \
					kind of value"
				)
			}
		},
		None => Ok(DataType::Null)
	}
}

pub fn hgetall(k: &str) -> Result<DataType, &str> {
	match M.lock().unwrap().get(k) {
		Some(data) => {
			match data {
				DataType::Hashset(_hmap) => Ok(data.clone()),
				_ => Err(
					"WRONGTYPE Operation against a key holding the wrong \
					kind of value"
				)
			}
		},
		None => Ok(DataType::Null)
	}
}

pub fn hset(k: &str, nvs: Vec<String>) -> Result<DataType, &str> {
	if 0 != nvs.len() % 2 {
		return Err("Number of elements must a multiple of 2");
	}
	let mut m = M.lock().unwrap();
	match m.get_mut(k) {
		Some(data) => {
			match data {
				DataType::Hashset(hmap) => {
					nvs.chunks(2).for_each(|x| {hmap.insert(
						DataType::bulkStr(&x[0]),
						DataType::bulkStr(&x[1])
					);});
					Ok(DataType::Integer(hmap.len().try_into().unwrap()))
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
	match m.get_mut(k) {
		Some(data) => {
			match data {
				DataType::BulkString(s) => {
					match s.parse::<i64>() {
						Ok(i) => {
							let x: i64 = i + 1;
							m.insert(
								String::from(k),
								DataType::BulkString(x.to_string())
							);
							Ok(DataType::Integer(x))
						},
						Err(_) => Err(
							"ERR value is not an integer or out of range"
						)
					}
				},
				_ => Err(
					"WRONGTYPE Operation against a key holding the wrong \
					kind of value"
				)
			}
		},
		None => {
			m.insert(String::from(k), DataType::bulkStr("1"));
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
		assert_eq!(get("first"), Ok(DataType::bulkStr("1st")));
		assert_eq!(get("second"), Ok(DataType::bulkStr("2nd")));
		assert_eq!(get("third"), Ok(DataType::bulkStr("3rd")));
		assert_eq!(keys(".*"), Ok(DataType::List(vec![
			DataType::bulkStr("first"),
			DataType::bulkStr("second"),
			DataType::bulkStr("third")
		])));
		assert_eq!(memsize(), 25usize);
		assert_eq!(
			del(&vec!["first".to_string()]),
			Ok(DataType::Integer(1))
		);
		assert_eq!(get("first"), Ok(DataType::Null));
		assert_eq!(get("second"), Ok(DataType::bulkStr("2nd")));
		assert_eq!(get("third"), Ok(DataType::bulkStr("3rd")));
		assert_eq!(memsize(), 17usize);
		assert_eq!(
			del(&vec!["second".to_string()]),
			Ok(DataType::Integer(1))
		);
		assert_eq!(get("first"), Ok(DataType::Null));
		assert_eq!(get("second"), Ok(DataType::Null));
		assert_eq!(get("third"), Ok(DataType::bulkStr("3rd")));
		assert_eq!(memsize(), 8usize);
		assert_eq!(
			del(&vec!["third".to_string()]),
			Ok(DataType::Integer(1))
		);
		assert_eq!(get("first"), Ok(DataType::Null));
		assert_eq!(get("second"), Ok(DataType::Null));
		assert_eq!(get("third"), Ok(DataType::Null));
		assert_eq!(memsize(), 0usize);
	}

	#[test]
	#[serial]
	fn plan2() {
		set("one", "un");
		set("two", "deux");
		set("three", "trois");
		assert_eq!(get("one"), Ok(DataType::bulkStr("un")));
		assert_eq!(get("two"), Ok(DataType::bulkStr("deux")));
		assert_eq!(get("three"), Ok(DataType::bulkStr("trois")));
		assert_eq!(keys(".*"), Ok(DataType::List(vec![
			DataType::bulkStr("one"),
			DataType::bulkStr("three"),
			DataType::bulkStr("two")
		])));
		assert_eq!(memsize(), 22usize);
		assert_eq!(
			del(&vec![
				"one".to_string(),
				"two".to_string(),
				"three".to_string()
			]),
			Ok(DataType::Integer(3))
		);
	}

	#[test]
	#[serial]
	fn plan3() {
		set("someint", "365");
		assert_eq!(get("someint"), Ok(DataType::bulkStr("365")));
		assert_eq!(incr("someint"), Ok(DataType::Integer(366)));
		assert_eq!(incr("someint"), Ok(DataType::Integer(367)));
		assert_eq!(incr("someint"), Ok(DataType::Integer(368)));
		assert_eq!(decr("someint"), Ok(DataType::Integer(367)));
		assert_eq!(get("someint"), Ok(DataType::bulkStr("367")));
		assert_eq!(
			del(&vec!["someint".to_string()]),
			Ok(DataType::Integer(1))
		);
	}

	#[test]
	#[serial]
	fn plan4() {
		let _ = hset("fieldvalues", vec![
			"field1".to_string(), "value1".to_string(),
			"field2".to_string(), "value2".to_string()
		]);
		assert_eq!(
			hget("fieldvalues", "field1"),
			Ok(DataType::bulkStr("value1"))
		);
		assert_eq!(
			hget("fieldvalues", "field2"),
			Ok(DataType::bulkStr("value2"))
		);
		let _ = hset("fieldvalues", vec![
			"field3".to_string(), "value3".to_string(),
			"field4".to_string(), "value4".to_string(),
			"field5".to_string(), "value5".to_string(),
		]);
		assert_eq!(
			hget("fieldvalues", "field1"),
			Ok(DataType::bulkStr("value1"))
		);
		assert_eq!(
			hget("fieldvalues", "field4"),
			Ok(DataType::bulkStr("value4"))
		);
		let _ = hset("fieldvalues", vec![
			"field1".to_string(), "val1".to_string(),
			"field2".to_string(), "val2".to_string()
		]);
		assert_eq!(
			hget("fieldvalues", "field5"),
			Ok(DataType::bulkStr("value5"))
		);
		assert_eq!(
			hget("fieldvalues", "field1"),
			Ok(DataType::bulkStr("val1"))
		);
		assert_eq!(
			hget("fieldvalues", "field1"),
			Ok(DataType::bulkStr("val1"))
		);
		assert_eq!(
			del(&vec!["fieldvalues".to_string()]),
			Ok(DataType::Integer(1))
		);
	}
}
