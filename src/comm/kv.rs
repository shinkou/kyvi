use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use regex::{Regex, Error};

use super::datatype::DataType;

static M: Mutex<BTreeMap<String, DataType>>= Mutex::new(BTreeMap::new());

pub fn append<'a>(k: &'a str, v: &'a str) -> Result<DataType, &'a str> {
	let mut m = M.lock().unwrap();
	match m.get(k) {
		Some(data) => {
			match data {
				DataType::BulkString(s) => {
					let a = s.to_string() + v;
					m.insert(String::from(k), DataType::bulkStr(&a));
					Ok(DataType::Integer(a.len().try_into().unwrap()))
				},
				_ => Err(
					"WRONGTYPE Operation against a key holding the wrong \
					kind of value"
				)
			}
		},
		None => {
			m.insert(String::from(k), DataType::bulkStr(v));
			Ok(DataType::Integer(v.len().try_into().unwrap()))
		}
	}
}

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
							"ERR Value is not an integer or out of range"
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

pub fn decrby<'a>(k: &'a str, v: &'a str) -> Result<DataType, &'a str> {
	let mut m = M.lock().unwrap();
	if let Err(_) = v.parse::<i64>() {
		return Err("ERR Increment by value is not an integer")
	}
	let n: i64 = v.parse::<i64>().unwrap();
	match m.get(k) {
		Some(data) => {
			match data {
				DataType::BulkString(s) => {
					match s.parse::<i64>() {
						Ok(i) => {
							let x: i64 = i - n;
							m.insert(
								String::from(k),
								DataType::BulkString(x.to_string())
							);
							Ok(DataType::Integer(x))
						},
						Err(_) => Err(
							"ERR Value is not an integer or out of range"
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
			let x: i64 = 0 - n;
			m.insert(String::from(k), DataType::BulkString(x.to_string()));
			Ok(DataType::Integer(x))
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
		Err(_e) => resdata
	}
}

pub fn getset<'a>(k: &'a str, v: &'a str) -> Result<DataType, &'a str> {
	let resdata = get(k);
	match resdata {
		Ok(ref _data) => {
			let _ = set(k, v);
			resdata
		},
		Err(_e) => resdata
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

pub fn hexists<'a>(k: &'a str, f: &'a str) -> Result<DataType, &'a str> {
	match M.lock().unwrap().get(k) {
		Some(data) => {
			match data {
				DataType::Hashset(hmap) => {
					Ok(DataType::Integer(
						if hmap.contains_key(&DataType::bulkStr(f)) {
							1i64
						} else {
							0i64
						}
					))
				},
				_ => Err(
					"WRONGTYPE Operation against a key holding the wrong \
					kind of value"
				)
			}
		},
		None => Ok(DataType::Integer(0i64))
	}
}

pub fn hget<'a>(k: &'a str, f: &'a str) -> Result<DataType, &'a str> {
	match M.lock().unwrap().get(k) {
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

pub fn hincrby<'a>(k: &'a str, f: &'a str, n: i64)
	-> Result<DataType, &'a str> {
	let mut m = M.lock().unwrap();
	match m.get_mut(k) {
		Some(data) => {
			match data {
				DataType::Hashset(hmap) => {
					match hmap.get(&DataType::bulkStr(f)) {
						Some(DataType::BulkString(somestr)) => {
							match somestr.parse::<i64>() {
								Ok(i) => {
									let x: i64 = i + n;
									hmap.insert(
										DataType::bulkStr(f),
										DataType::BulkString(x.to_string())
									);
									Ok(DataType::Integer(x))
								},
								Err(_) => Err(
									"ERR Value is not an integer or out of \
									range"
								)
							}
						},
						None => {
							hmap.insert(
								DataType::bulkStr(f),
								DataType::BulkString(n.to_string())
							);
							Ok(DataType::Integer(n))
						},
						_ => todo!() // this should never happen since we 
									 // only use DataType::BulkString for
									 // keys
					}
				},
				_ => Err(
					"WRONGTYPE Operation against a key holding the wrong \
					kind of value"
				)
			}
		},
		None => {
			let mut somehmap: HashMap<DataType, DataType> = HashMap::new();
			somehmap.insert(
				DataType::bulkStr(f),
				DataType::BulkString(n.to_string())
			);
			m.insert(String::from(k), DataType::hset(&somehmap));
			Ok(DataType::Integer(n))
		}
	}
}

pub fn hkeys(k: &str) -> Result<DataType, &str> {
	match M.lock().unwrap().get(k) {
		Some(data) => {
			match data {
				DataType::Hashset(hmap) =>
					Ok(DataType::List(
						hmap.keys().cloned().collect::<Vec<_>>()
					)),
				_ => Err(
					"WRONGTYPE Operation against a key holding the wrong \
					kind of value"
				)
			}
		},
		None => Ok(DataType::List(vec![]))
	}
}

pub fn hlen(k: &str) -> Result<DataType, &str> {
	match M.lock().unwrap().get(k) {
		Some(data) => {
			match data {
				DataType::Hashset(hmap) =>
					Ok(DataType::Integer(hmap.len().try_into().unwrap())),
				_ =>
					Err(
						"WRONGTYPE Operation against a key holding the \
						wrong kind of value"
					)
			}
		},
		None => Ok(DataType::Integer(0i64))
	}
}

pub fn hmget<'a>(k: &'a str, fs: &'a Vec<String>)
	-> Result<DataType, &'a str> {
	match M.lock().unwrap().get(k) {
		Some(data) => {
			match data {
				DataType::Hashset(hmap) => Ok(DataType::List(
					fs.into_iter().map(|f| {
						match hmap.get(&DataType::bulkStr(f)) {
							Some(somedtype) => somedtype.clone(),
							None => DataType::Null
						}
					}).collect::<Vec<_>>()
				)),
				_ => Err(
					"WRONGTYPE Operation against a key holding the wrong \
					kind of value"
				)
			}
		},
		None => Ok(DataType::List(
			fs.into_iter().map(|_| {DataType::Null}).collect::<Vec<_>>()
		))
	}
}

pub fn hset<'a>(k: &'a str, nvs: &'a Vec<String>)
	-> Result<DataType, &'a str> {
	if 0 != nvs.len() % 2 {
		return Err("ERR Number of elements must a multiple of 2");
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
				_ => Err("ERR Key must associate with a hash")
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

pub fn hvals(k: &str) -> Result<DataType, &str> {
	match M.lock().unwrap().get(k) {
		Some(data) => {
			match data {
				DataType::Hashset(hmap) =>
					Ok(DataType::List(
						hmap.values().cloned().collect::<Vec<_>>()
					)),
				_ => Err(
					"WRONGTYPE Operation against a key holding the wrong \
					kind of value"
				)
			}
		},
		None => Ok(DataType::List(vec![]))
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
							"ERR Value is not an integer or out of range"
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

pub fn incrby<'a>(k: &'a str, v: &'a str) -> Result<DataType, &'a str> {
	let mut m = M.lock().unwrap();
	if let Err(_) = v.parse::<i64>() {
		return Err("ERR Increment by value is not an integer")
	}
	let n: i64 = v.parse::<i64>().unwrap();
	match m.get(k) {
		Some(data) => {
			match data {
				DataType::BulkString(s) => {
					match s.parse::<i64>() {
						Ok(i) => {
							let x: i64 = i + n;
							m.insert(
								String::from(k),
								DataType::BulkString(x.to_string())
							);
							Ok(DataType::Integer(x))
						},
						Err(_) => Err(
							"ERR Value is not an integer or out of range"
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
			let x: i64 = 0 + n;
			m.insert(String::from(k), DataType::BulkString(x.to_string()));
			Ok(DataType::Integer(x))
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

pub fn lpush<'a>(k: &'a str, vs: &'a Vec<String>)
	-> Result<DataType, &'a str> {
	let mut m = M.lock().unwrap();
	match m.get_mut(k) {
		Some(data) => {
			match data {
				DataType::List(l) => {
					vs.into_iter().for_each(|v| {
						l.insert(0, DataType::bulkStr(&v));
					});
					Ok(DataType::Integer(l.len().try_into().unwrap()))
				},
				_ => Err(
					"WRONGTYPE Operation against a key holding the wrong \
					kind of value"
				)
			}
		},
		None => {
			let mut l: Vec<DataType> = Vec::new();
			vs.into_iter().for_each(|v| {
				l.insert(0, DataType::bulkStr(&v));
			});
			m.insert(String::from(k), DataType::List(l.clone()));
			Ok(DataType::Integer(l.len().try_into().unwrap()))
		}
	}
}

pub fn lpop<'a>(k: &'a str, n: &'a usize) -> Result<DataType, &'a str> {
	let mut m = M.lock().unwrap();
	match m.get_mut(k) {
		Some(data) => {
			match data {
				DataType::List(somevec) => {
					let mut l: Vec<DataType> = Vec::new();
					for _ in 0usize..*n {
						if 0 < somevec.len() {
							l.push(somevec.remove(0));
						}
					}
					Ok(DataType::List(l))
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

pub fn memsize() -> usize {
	M.lock().unwrap().iter().map(|(k, v)| k.capacity() + v.capacity()).sum()
}

pub fn mget(ks: &Vec<String>) -> Result<DataType, &str> {
	Ok(DataType::List(
		ks.into_iter().map(|k| {
			match M.lock().unwrap().get(k) {
				Some(data) => {
					match data {
						DataType::BulkString(_s) => data.clone(),
						_ => DataType::Null
					}
				},
				None => DataType::Null
			}
		}).collect::<Vec<_>>().to_vec()
	))
}

pub fn mset(nvs: &Vec<String>) -> Result<DataType, &str> {
	if 0 != nvs.len() % 2 {
		return Err("ERR Number of elements must a multiple of 2");
	}
	let mut m = M.lock().unwrap();
	nvs.chunks(2).for_each(|x| {
		m.insert(x[0].clone(), DataType::bulkStr(&x[1]));
	});
	Ok(DataType::str("OK"))
}

pub fn set<'a>(k: &'a str, v: &'a str) -> Result<DataType, &'a str> {
	let _ = M.lock().unwrap().insert(String::from(k), DataType::bulkStr(v));
	Ok(DataType::str("OK"))
}

#[cfg(test)]
mod tests {
	use serial_test::serial;
	use super::*;

	#[test]
	#[serial]
	fn plan1() {
		let _ = set("first", "1st");
		let _ = set("second", "2nd");
		let _ = set("third", "3rd");
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
		let _ = set("one", "un");
		let _ = set("two", "deux");
		let _ = set("three", "trois");
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
		assert_eq!(get("somestr"), Ok(DataType::Null));
		assert_eq!(append("somestr", "rust"), Ok(DataType::Integer(4)));
		assert_eq!(append("somestr", " is"), Ok(DataType::Integer(7)));
		assert_eq!(
			append("somestr", " awesome"),
			Ok(DataType::Integer(15))
		);
		assert_eq!(
			get("somestr"),
			Ok(DataType::bulkStr("rust is awesome"))
		);
		assert_eq!(
			del(&vec!["somestr".to_string()]),
			Ok(DataType::Integer(1))
		);
	}

	#[test]
	#[serial]
	fn plan4() {
		let _ = set("someint", "365");
		assert_eq!(get("someint"), Ok(DataType::bulkStr("365")));
		assert_eq!(incr("someint"), Ok(DataType::Integer(366)));
		assert_eq!(incr("someint"), Ok(DataType::Integer(367)));
		assert_eq!(incr("someint"), Ok(DataType::Integer(368)));
		assert_eq!(decr("someint"), Ok(DataType::Integer(367)));
		assert_eq!(decrby("someint", "5"), Ok(DataType::Integer(362)));
		assert_eq!(incrby("someint", "9"), Ok(DataType::Integer(371)));
		assert_eq!(get("someint"), Ok(DataType::bulkStr("371")));
		assert_eq!(decrby("newint", "4"), Ok(DataType::Integer(-4)));
		assert_eq!(get("newint"), Ok(DataType::bulkStr("-4")));
		assert_eq!(incrby("yetint", "4"), Ok(DataType::Integer(4)));
		assert_eq!(get("yetint"), Ok(DataType::bulkStr("4")));
		assert_eq!(
			del(&vec![
				"someint".to_string(),
				"newint".to_string(),
				"yetint".to_string()
			]),
			Ok(DataType::Integer(3))
		);
	}

	#[test]
	#[serial]
	fn plan5() {
		let _ = hset("fieldvalues", &vec![
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
		let _ = hset("fieldvalues", &vec![
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
		let _ = hset("fieldvalues", &vec![
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
			hexists("fieldvalues", "field5"),
			Ok(DataType::Integer(1i64))
		);
		assert_eq!(
			hexists("fieldvalues", "nonexists"),
			Ok(DataType::Integer(0i64))
		);
		assert_eq!(
			hexists("nosuchhash", "field1"),
			Ok(DataType::Integer(0i64))
		);
		assert_eq!(hlen("fieldvalues"), Ok(DataType::Integer(5)));
		if let Ok(DataType::List(somekeys)) = hkeys("fieldvalues") {
			let mut fields = somekeys.into_iter().map(|e| {
				match e {
					DataType::BulkString(s) => s,
					_ => "".to_string()
				}
			}).collect::<Vec<_>>();
			fields.sort();
			assert_eq!(
				fields,
				vec!["field1", "field2", "field3", "field4", "field5"]
			);
		}
		let _ = hset("fieldvalues", &vec![
			"field1".to_string(), "value1".to_string(),
			"field2".to_string(), "value2".to_string()
		]);
		if let Ok(DataType::List(somevals)) = hvals("fieldvalues") {
			let mut fields = somevals.into_iter().map(|e| {
				match e {
					DataType::BulkString(s) => s,
					_ => "".to_string()
				}
			}).collect::<Vec<_>>();
			fields.sort();
			assert_eq!(
				fields,
				vec!["value1", "value2", "value3", "value4", "value5"]
			);
		}
		assert_eq!(
			hmget("fieldvalues", &vec![
				"field1".to_string(),
				"field3".to_string(),
				"field5".to_string()
			]),
			Ok(DataType::List(vec![
				DataType::bulkStr("value1"),
				DataType::bulkStr("value3"),
				DataType::bulkStr("value5")
			]))
		);
		assert_eq!(
			hmget("fieldvalues", &vec![
				"field0".to_string(),
				"field2".to_string(),
				"field4".to_string()
			]),
			Ok(DataType::List(vec![
				DataType::Null,
				DataType::bulkStr("value2"),
				DataType::bulkStr("value4")
			]))
		);
		assert_eq!(
			hmget("nonexist", &vec![
				"field1".to_string(),
				"field2".to_string(),
				"field3".to_string()
			]),
			Ok(DataType::List(vec![
				DataType::Null,
				DataType::Null,
				DataType::Null
			]))
		);
		assert_eq!(
			del(&vec!["fieldvalues".to_string()]),
			Ok(DataType::Integer(1))
		);
	}

	#[test]
	#[serial]
	fn plan6() {
		assert_eq!(
			hset("fieldvalues", &vec![
				"field1".to_string(), "128".to_string(),
				"field2".to_string(), "non-numeric".to_string()
			]),
			Ok(DataType::Integer(2))
		);
		assert_eq!(
			hincrby("fieldvalues", "field1", 128),
			Ok(DataType::Integer(256))
		);
		assert_eq!(
			hincrby("fieldvalues", "field2", 64),
			Err("ERR Value is not an integer or out of range")
		);
		assert_eq!(
			del(&vec!["fieldvalues".to_string()]),
			Ok(DataType::Integer(1))
		);
	}

	#[test]
	#[serial]
	fn plan7() {
		assert_eq!(
			lpush("somekey", &vec![
				"val1".to_string(),
				"val2".to_string(),
				"val3".to_string()
			]),
			Ok(DataType::Integer(3))
		);
		assert_eq!(
			lpush("somekey", &vec![
				"val4".to_string(),
				"val5".to_string(),
				"val6".to_string()
			]),
			Ok(DataType::Integer(6))
		);
		assert_eq!(
			lpop("somekey", &6usize),
			Ok(DataType::List(vec![
				DataType::bulkStr("val6"),
				DataType::bulkStr("val5"),
				DataType::bulkStr("val4"),
				DataType::bulkStr("val3"),
				DataType::bulkStr("val2"),
				DataType::bulkStr("val1")
			]))
		);
		assert_eq!(
			del(&vec!["somekey".to_string()]),
			Ok(DataType::Integer(1))
		);
	}
}
