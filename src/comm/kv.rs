use rand::Rng;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;

use regex::Regex;

use super::datatype::DataType;

static M: Mutex<BTreeMap<String, DataType>>= Mutex::new(BTreeMap::new());

pub fn append<'a>(k: &'a str, v: &'a str) -> Result<DataType, &'a str> {
	let mut m = M.lock().unwrap();
	match m.get(k) {
		Some(DataType::BulkString(s)) => {
			let a = s.to_string() + v;
			m.insert(String::from(k), DataType::bulkStr(&a));
			Ok(DataType::Integer(a.len().try_into().unwrap()))
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => {
			m.insert(String::from(k), DataType::bulkStr(v));
			Ok(DataType::Integer(v.len().try_into().unwrap()))
		}
	}
}

pub fn decr(k: &str) -> Result<DataType, &str> {
	let mut m = M.lock().unwrap();
	match m.get(k) {
		Some(DataType::BulkString(s)) => match s.parse::<i64>() {
			Ok(i) => {
				let x: i64 = i - 1;
				m.insert(
					String::from(k),
					DataType::BulkString(x.to_string())
				);
				Ok(DataType::Integer(x))
			},
			Err(_) => Err("ERR Value is not an integer or out of range")
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => {
			m.insert(String::from(k), DataType::bulkStr("-1"));
			Ok(DataType::Integer(-1))
		}
	}
}

pub fn decrby<'a>(k: &'a str, v: &'a str) -> Result<DataType, &'a str> {
	let n: i64 = match v.parse::<i64>() {
		Ok(someint) => someint,
		Err(_) => return Err("ERR Increment by value is not an integer")
	};
	let mut m = M.lock().unwrap();
	match m.get(k) {
		Some(DataType::BulkString(s)) => match s.parse::<i64>() {
			Ok(i) => {
				let x: i64 = i - n;
				m.insert(
					String::from(k),
					DataType::BulkString(x.to_string())
				);
				Ok(DataType::Integer(x))
			},
			Err(_) => Err("ERR Value is not an integer or out of range")
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => {
			let x: i64 = 0 - n;
			m.insert(String::from(k), DataType::BulkString(x.to_string()));
			Ok(DataType::Integer(x))
		}
	}
}

pub fn del(ks: &Vec<String>) -> Result<DataType, &str> {
	let mut m = M.lock().unwrap();
	let cnt: i64 = ks.iter().map(|k| {
		match m.remove(k) {
			Some(_) => 1i64,
			None => 0i64
		}
	}).sum::<i64>();
	Ok(DataType::Integer(cnt))
}

pub fn get(k: &str) -> Result<DataType, &str> {
	let m = M.lock().unwrap();
	let data = m.get(k);
	match data {
		Some(DataType::BulkString(_)) => Ok(data.unwrap().clone()),
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::Null)
	}
}

pub fn getdel(k: &str) -> Result<DataType, &str> {
	let mut m = M.lock().unwrap();
	let data = m.get(k);
	let output = match data {
		Some(DataType::BulkString(_)) => Ok(data.unwrap().clone()),
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::Null)
	};
	if let Ok(DataType::BulkString(_)) = output {
		let _ = m.remove(k);
	};
	output
}

pub fn getset<'a>(k: &'a str, v: &'a str) -> Result<DataType, &'a str> {
	let mut m = M.lock().unwrap();
	let data = m.get_mut(k);
	let output = match data {
		Some(DataType::BulkString(_)) => Ok(data.unwrap().clone()),
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::Null)
	};
	m.insert(String::from(k), DataType::bulkStr(v));
	output
}

pub fn hdel(k: &str, fs: Vec<String>) -> Result<DataType, &str> {
	let mut m = M.lock().unwrap();
	match m.get_mut(k) {
		Some(DataType::HashMap(hmap)) => {
			let cnt = fs.iter().map(|f| {
				match hmap.remove(&DataType::bulkStr(&f)) {
					Some(_) => 1i64,
					None => 0i64
				}
			}).sum::<i64>();
			if 0 == hmap.len() {m.remove(k);}
			Ok(DataType::Integer(cnt))
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::Integer(0))
	}
}

pub fn hexists<'a>(k: &'a str, f: &'a str) -> Result<DataType, &'a str> {
	match M.lock().unwrap().get(k) {
		Some(DataType::HashMap(hmap)) => Ok(DataType::Integer(
			if hmap.contains_key(&DataType::bulkStr(f)) {
				1i64
			} else {
				0i64
			}
		)),
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::Integer(0i64))
	}
}

pub fn hget<'a>(k: &'a str, f: &'a str) -> Result<DataType, &'a str> {
	match M.lock().unwrap().get(k) {
		Some(DataType::HashMap(h)) => match h.get(&DataType::bulkStr(f)) {
			Some(v) => Ok(v.clone()),
			None => Ok(DataType::Null)
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong \
			kind of value"
		),
		None => Ok(DataType::Null)
	}
}

pub fn hgetall(k: &str) -> Result<DataType, &str> {
	let m = M.lock().unwrap();
	let data = m.get(k);
	match data {
		Some(DataType::HashMap(_)) => Ok(data.unwrap().clone()),
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::EmptyList)
	}
}

pub fn hincrby<'a>(k: &'a str, f: &'a str, n: &'a str)
	-> Result<DataType, &'a str> {
	let someint: i64 = match n.parse::<i64>() {
		Ok(v) => v,
		Err(_) => return Err("ERR Increment is not a number")
	};
	let mut m = M.lock().unwrap();
	match m.get_mut(k) {
		Some(DataType::HashMap(hmap)) => {
			match hmap.get(&DataType::bulkStr(f)) {
				Some(DataType::BulkString(somestr)) => {
					match somestr.parse::<i64>() {
						Ok(i) => {
							let x: i64 = i + someint;
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
						DataType::BulkString(someint.to_string())
					);
					Ok(DataType::Integer(someint))
				},
				Some(_) => todo!() // this should never happen since we
								   // only use DataType::BulkString for
								   // keys
			}
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => {
			let mut somehmap: HashMap<DataType, DataType> = HashMap::new();
			somehmap.insert(
				DataType::bulkStr(f),
				DataType::BulkString(someint.to_string())
			);
			m.insert(String::from(k), DataType::hmap(&somehmap));
			Ok(DataType::Integer(someint))
		}
	}
}

pub fn hkeys(k: &str) -> Result<DataType, &str> {
	match M.lock().unwrap().get(k) {
		Some(DataType::HashMap(hmap)) => Ok(DataType::List(
			hmap.keys().cloned().collect::<Vec<_>>()
		)),
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::List(vec![]))
	}
}

pub fn hlen(k: &str) -> Result<DataType, &str> {
	match M.lock().unwrap().get(k) {
		Some(DataType::HashMap(hmap)) =>
			Ok(DataType::Integer(hmap.len().try_into().unwrap())),
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::Integer(0i64))
	}
}

pub fn hmget(k: &str, fs: Vec<String>) -> Result<DataType, &str> {
	match M.lock().unwrap().get(k) {
		Some(DataType::HashMap(hmap)) => Ok(DataType::List(
			fs.iter().map(|f| {
				match hmap.get(&DataType::bulkStr(&f)) {
					Some(dtype) => dtype.clone(),
					None => DataType::Null
				}
			}).collect::<Vec<_>>()
		)),
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::List(
			fs.iter().map(|_| {DataType::Null}).collect::<Vec<_>>()
		))
	}
}

pub fn hset<'a>(k: &'a str, nvs: Vec<String>, nx: &'a bool)
	-> Result<DataType, &'a str> {
	if 0 != nvs.len() % 2 {
		return Err("ERR Number of elements must a multiple of 2");
	}
	let mut m = M.lock().unwrap();
	match m.get_mut(k) {
		Some(DataType::HashMap(hmap)) => {
			let mut cnt: i64 = 0;
			match nx {
				true => {
					nvs.chunks(2).for_each(|x| {
						if !hmap.contains_key(
							&DataType::bulkStr(&x[0])
						) {
							hmap.insert(
								DataType::bulkStr(&x[0]),
								DataType::bulkStr(&x[1])
							);
							cnt += 1;
						}
					});
				},
				false => {
					nvs.chunks(2).for_each(|x| {
						hmap.insert(
							DataType::bulkStr(&x[0]),
							DataType::bulkStr(&x[1])
						);
						cnt += 1;
					});
				}
			}
			Ok(DataType::Integer(cnt))
		},
		Some(_) => Err("ERR Key must associate with a hash"),
		None => {
			let mut somehmap: HashMap<DataType, DataType> = HashMap::new();
			nvs.chunks(2).for_each(|x| {somehmap.insert(
				DataType::bulkStr(&x[0]),
				DataType::bulkStr(&x[1])
			);});
			let hmap2save = DataType::hmap(&somehmap);
			m.insert(String::from(k), hmap2save);
			Ok(DataType::Integer(somehmap.len().try_into().unwrap()))
		}
	}
}

pub fn hvals(k: &str) -> Result<DataType, &str> {
	match M.lock().unwrap().get(k) {
		Some(DataType::HashMap(hmap)) => Ok(DataType::List(
			hmap.values().cloned().collect::<Vec<_>>()
		)),
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::List(vec![]))
	}
}

pub fn incr(k: &str) -> Result<DataType, &str> {
	let mut m = M.lock().unwrap();
	match m.get_mut(k) {
		Some(DataType::BulkString(s)) => {
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
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => {
			m.insert(String::from(k), DataType::bulkStr("1"));
			Ok(DataType::Integer(1))
		}
	}
}

pub fn incrby<'a>(k: &'a str, v: &'a str) -> Result<DataType, &'a str> {
	let mut m = M.lock().unwrap();
	let n: i64 = match v.parse::<i64>() {
		Ok(someint) => someint,
		Err(_) => return Err("ERR Increment by value is not an integer")
	};
	match m.get(k) {
		Some(DataType::BulkString(s)) => match s.parse::<i64>() {
			Ok(i) => {
				let x: i64 = i + n;
				m.insert(
					String::from(k),
					DataType::BulkString(x.to_string())
				);
				Ok(DataType::Integer(x))
			},
			Err(_) => Err("ERR Value is not an integer or out of range")
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => {
			let x: i64 = 0 + n;
			m.insert(String::from(k), DataType::BulkString(x.to_string()));
			Ok(DataType::Integer(x))
		}
	}
}

pub fn keys(p: &str) -> Result<DataType, &str> {
	match Regex::new(p) {
		Ok(re) => Ok(DataType::List(
			M.lock().unwrap().keys()
				.filter(|s| re.is_match(s))
				.map(|s| DataType::bulkStr(s))
				.collect()
		)),
		Err(e) => Ok(DataType::err(
			&e.to_string()
		))
	}
}

pub fn lindex<'a>(k: &'a str, i: &'a str) -> Result<DataType, &'a str> {
	let idx: i64 = match i.parse::<i64>() {
		Ok(v) => v,
		Err(_) => return Err("ERR Index must be an integer")
	};
	match M.lock().unwrap().get(k) {
		Some(DataType::List(somevec)) => {
			let u: usize = if idx < 0 {
				((somevec.len() as i64) + idx) as usize
			} else {
				idx as usize
			};
			match somevec.get(u) {
				Some(dtype) => Ok(dtype.clone()),
				None => Ok(DataType::Null)
			}
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::Integer(0i64))
	}
}

pub fn linsert<'a>(k: &'a str, o: &'a str, p: &'a str, e: &'a str)
	-> Result<DataType, &'a str> {
	match M.lock().unwrap().get_mut(k) {
		Some(DataType::List(l)) => {
			match l.iter().position(|v| {*v == DataType::bulkStr(p)}) {
				Some(i) => {
					let idx = match o.to_ascii_lowercase().as_str() {
						"before" => i,
						"after" => i + 1usize,
						_ => return Err("ERR Syntax error")
					};
					l.insert(idx, DataType::bulkStr(e));
					Ok(DataType::Integer(l.len() as i64))
				},
				None => return Ok(DataType::Integer(-1))
			}
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::Integer(0))
	}
}

pub fn llen(k: &str) -> Result<DataType, &str> {
	match M.lock().unwrap().get(k) {
		Some(DataType::List(l)) => Ok(DataType::Integer(
			l.len().try_into().unwrap()
		)),
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::Integer(0i64))
	}
}

pub fn lpush(k: &str, vs: Vec<String>, x: bool) -> Result<DataType, &str> {
	let mut m = M.lock().unwrap();
	match m.get_mut(k) {
		Some(DataType::List(l)) => {
			vs.iter().for_each(|v| {l.insert(0, DataType::bulkStr(&v));});
			Ok(DataType::Integer(l.len().try_into().unwrap()))
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => match x {
			true => {
				Ok(DataType::Integer(0))
			},
			false => {
				let mut l: Vec<DataType> = Vec::new();
				vs.iter().for_each(|v| {
					l.insert(0, DataType::bulkStr(&v));
				});
				m.insert(String::from(k), DataType::List(l.clone()));
				Ok(DataType::Integer(l.len().try_into().unwrap()))
			}
		}
	}
}

pub fn lpop<'a>(k: &'a str, n: &'a str) -> Result<DataType, &'a str> {
	let popsize: usize = match n.parse::<usize>() {
		Ok(v) => v,
		Err(_) => return Err("ERR Number must be a positive integer")
	};
	let mut m = M.lock().unwrap();
	match m.get_mut(k) {
		Some(DataType::List(somevec)) => {
			let mut l: Vec<DataType> = Vec::new();
			for _ in 0usize..popsize {
				if 0 < somevec.len() {
					l.push(somevec.remove(0));
				}
			}
			if 0 == somevec.len() {m.remove(k);}
			Ok(DataType::List(l))
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::Null)
	}
}

// the Redis' LRANGE specs is soooooo weird :(
pub fn lrange<'a>(k: &'a str, i: &'a str, j: &'a str)
	-> Result<DataType, &'a str> {
	let mut istart: i64 = match i.parse::<i64>() {
		Ok(v) => v,
		Err(_) => return Err("ERR Start index must be a number")
	};
	let mut istop: i64 = match j.parse::<i64>() {
		Ok(v) => v,
		Err(_) => return Err("ERR Stop index must be a number")
	};
	match M.lock().unwrap().get(k) {
		Some(DataType::List(somevec)) => {
			let veclen: i64 = somevec.len() as i64;
			// adjust -ve start and stop indexes
			if istart < 0 {
				istart = veclen + istart;
			};
			if istop < 0 {
				istop = veclen + istop;
			};
			if istop < istart || istop < 0 || istart > veclen {
				// return empty list if indexes do not qualify
				Ok(DataType::List(vec![]))
			} else {
				let ustart: usize = if 0i64 > istart {
					0usize
				} else if veclen < istart {
					somevec.len()
				} else {
					istart as usize
				};
				let ustop: usize = if 0i64 > istop {
					0usize
				} else if veclen - 1 < istop {
					// this looks weird because of the +1 below
					somevec.len() - 1usize
				} else {
					istop as usize
				};
				Ok(DataType::List(
					// always add 1 to stop index for inclusiveness
					somevec[ustart..(ustop + 1usize)].to_vec()
				))
			}
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::EmptyList)
	}
}

pub fn lrem<'a>(k: &'a str, n: &'a str, e: &'a str)
	-> Result<DataType, &'a str> {
	let cnt: i64 = match n.parse::<i64>() {
		Ok(v) => v,
		Err(_) => return Err("ERR Count must be a number")
	};
	let dte = DataType::bulkStr(e);
	let mut m = M.lock().unwrap();
	match m.get_mut(k) {
		Some(DataType::List(l)) => {
			let mut idxs: Vec<usize> = Vec::new();
			if cnt > 0 {
				for i in 0..l.len() {
					if *l.get(i).unwrap() == dte {
						idxs.push(i.clone())
					};
					if idxs.len() >= (cnt as usize) {
						break;
					};
				}
			} else if cnt < 0 {
				for i in (0..l.len()).rev() {
					if *l.get(i).unwrap() == dte {
						idxs.push(i.clone())
					};
					if idxs.len() >= ((cnt * -1) as usize) {
						break;
					};
				}
			} else {
				for i in 0..l.len() {
					if *l.get(i).unwrap() == dte {
						idxs.push(i.clone())
					};
				}
			};
			idxs.sort();
			for i in (&idxs).iter().rev() {
				let _ = l.remove(*i);
			};
			if 0 == l.len() {m.remove(k);}
			Ok(DataType::Integer(idxs.len() as i64))
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::Null)
	}
}

pub fn lset<'a>(k: &'a str, i: &'a str, e: &'a str)
	-> Result<DataType, &'a str> {
	let idx: i64 = match i.parse::<i64>() {
		Ok(v) => v,
		Err(_) => return Err("ERR Index must be an integer")
	};
	match M.lock().unwrap().get_mut(k) {
		Some(DataType::List(l)) => {
			let veclen: i64 = l.len() as i64;
			let realidx: i64 = if 0 > idx {
				(l.len() as i64) + idx
			} else {
				idx
			};
			if 0 <= realidx && realidx < veclen {
				let element: &mut DataType = l.get_mut(
					realidx as usize
				).unwrap();
				*element = DataType::bulkStr(e);
				Ok(DataType::bulkStr("OK"))
			} else {
				Err("ERR Index out of range")
			}
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Err("ERR No such key")
	}
}

pub fn ltrim<'a>(k: &'a str, i: &'a str, j: &'a str)
	-> Result<DataType, &'a str> {
	let mut istart: i64 = match i.parse::<i64>() {
		Ok(v) => v,
		Err(_) => return Err("ERR Start index must be a number")
	};
	let mut istop: i64 = match j.parse::<i64>() {
		Ok(v) => v,
		Err(_) => return Err("ERR Stop index must be a number")
	};
	match M.lock().unwrap().get_mut(k) {
		Some(DataType::List(somevec)) => {
			let veclen: i64 = somevec.len() as i64;
			// adjust -ve start and stop indexes
			if istart < 0 {
				istart = veclen + istart;
			};
			if istop < 0 {
				istop = veclen + istop;
			};
			if istop >= istart && istop >= 0 && istart <= veclen {
				let ustart: usize = if 0i64 > istart {
					0usize
				} else if veclen < istart {
					somevec.len()
				} else {
					istart as usize
				};
				let ustop: usize = if 0i64 > istop {
					0usize
				} else if veclen < istop {
					somevec.len()
				} else {
					istop as usize
				};
				somevec.drain(0..ustart);
				somevec.drain(ustop..somevec.len());
			};
			Ok(DataType::bulkStr("OK"))
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::bulkStr("OK"))
	}
}

pub fn memsize() -> usize {
	M.lock().unwrap().iter().map(|(k, v)| k.capacity() + v.capacity()).sum()
}

pub fn mget(ks: &Vec<String>) -> Result<DataType, &str> {
	Ok(DataType::List(
		ks.iter().map(|k| {
			let m = M.lock().unwrap();
			let data = m.get(k);
			match data {
				Some(DataType::BulkString(_)) => data.unwrap().clone(),
				_ => DataType::Null,
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

pub fn rpop<'a>(k: &'a str, n: &'a str) -> Result<DataType, &'a str> {
	let popsize: usize = match n.parse::<usize>() {
		Ok(v) => v,
		Err(_) => return Err("ERR Number must be a positive integer")
	};
	let mut m = M.lock().unwrap();
	match m.get_mut(k) {
		Some(DataType::List(somevec)) => {
			let mut l: Vec<DataType> = Vec::new();
			for _ in 0usize..popsize {
				if 0 < somevec.len() {
					l.push(somevec.pop().unwrap());
				}
			}
			if 0 == somevec.len() {m.remove(k);}
			Ok(DataType::List(l))
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::Null)
	}
}

pub fn rpush<'a>(k: &'a str, vs: Vec<String>, x: &'a bool)
	-> Result<DataType, &'a str> {
	let mut m = M.lock().unwrap();
	match m.get_mut(k) {
		Some(DataType::List(l)) => {
			vs.iter().for_each(|v| {l.push(DataType::bulkStr(&v));});
			Ok(DataType::Integer(l.len().try_into().unwrap()))
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => match x {
			true => {
				Ok(DataType::Integer(0))
			},
			false => {
				let mut l: Vec<DataType> = Vec::new();
				vs.iter().for_each(|v| {
					l.push(DataType::bulkStr(&v));
				});
				m.insert(String::from(k), DataType::List(l.clone()));
				Ok(DataType::Integer(l.len().try_into().unwrap()))
			}
		}
	}
}

pub fn sadd(k: &str, vs: Vec<String>) -> Result<DataType, &str> {
	let mut m = M.lock().unwrap();
	match m.get_mut(k) {
		Some(DataType::HashSet(s)) => Ok(DataType::Integer(
			vs.iter().map(|v|{
				if s.insert(DataType::bulkStr(v)){1}else{0}
			}).sum()
		)),
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => {
			let mut s: HashSet<DataType> = HashSet::new();
			let i = vs.iter().map(|v|{
				if s.insert(DataType::bulkStr(v)){1}else{0}
			}).sum();
			m.insert(String::from(k), DataType::HashSet(s.clone()));
			Ok(DataType::Integer(i))
		}
	}
}

pub fn scard(k: &str) -> Result<DataType, &str> {
	match M.lock().unwrap().get(k) {
		Some(DataType::HashSet(hset)) => Ok(DataType::Integer(
			hset.len() as i64
		)),
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::Integer(0))
	}
}

pub fn sdiff(k: &str, ks: Vec<String>) -> Result<DataType, &str> {
	let m = M.lock().unwrap();
	match m.get(k) {
		Some(DataType::HashSet(hset)) => {
			let mut vs = hset.iter().cloned().collect::<Vec<_>>();
			ks.iter().for_each(|k2| {
				match m.get(k2) {
					Some(DataType::HashSet(hset2)) => {
						vs.retain(|e| {!hset2.contains(&e)});
					},
					_ => {}
				}
			});
			Ok(DataType::List(vs.iter().cloned().collect()))
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::EmptyList)
	}
}

pub fn sdiffstore<'a>(dst: &'a str, k: &'a str, ks: Vec<String>)
	-> Result<DataType, &'a str> {
	let mut m = M.lock().unwrap();
	match m.get(k) {
		Some(DataType::HashSet(hset)) => {
			let mut vs = hset.iter().cloned()
				.collect::<HashSet<_>>();
			ks.iter().for_each(|k2| {match m.get(k2) {
				Some(DataType::HashSet(hset2)) =>
					vs.retain(|e| {!hset2.contains(&e)}),
				_ => {}
			}});
			m.insert(String::from(dst), DataType::hset(&vs));
			Ok(DataType::Integer(vs.len() as i64))
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::Integer(0))
	}
}

pub fn set<'a>(k: &'a str, v: &'a str) -> Result<DataType, &'a str> {
	let _ = M.lock().unwrap().insert(String::from(k), DataType::bulkStr(v));
	Ok(DataType::str("OK"))
}

pub fn sinter(k: &str, ks: Vec<String>) -> Result<DataType, &str> {
	let m = M.lock().unwrap();
	match m.get(k) {
		Some(DataType::HashSet(hset)) => {
			let mut vs = hset.iter().cloned().collect::<Vec<_>>();
			ks.iter().for_each(|k2| {match m.get(k2) {
				Some(DataType::HashSet(hset2)) =>
					vs.retain(|e| {hset2.contains(&e)}),
				_ => {}
			}});
			Ok(DataType::List(vs))
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::EmptyList)
	}
}

pub fn sinterstore<'a>(dst: &'a str, k: &'a str, ks: Vec<String>)
	-> Result<DataType, &'a str> {
	let mut m = M.lock().unwrap();
	match m.get(k) {
		Some(DataType::HashSet(hset)) => {
			let mut vs = hset.iter().cloned()
				.collect::<HashSet<_>>();
			ks.iter().for_each(|k2| {match m.get(k2) {
				Some(DataType::HashSet(hset2)) =>
					vs.retain(|e| {hset2.contains(&e)}),
				_ => {}
			}});
			m.insert(String::from(dst), DataType::hset(&vs));
			Ok(DataType::Integer(vs.len() as i64))
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::Integer(0))
	}
}

pub fn sismember<'a>(k: &'a str, v: &'a str) -> Result<DataType, &'a str> {
	match M.lock().unwrap().get(k) {
		Some(DataType::HashSet(hset)) => Ok(DataType::Integer(
			if hset.contains(&DataType::bulkStr(v)) {1} else {0}
		)),
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::Integer(0))
	}
}

pub fn smembers(k: &str) -> Result<DataType, &str> {
	match M.lock().unwrap().get(k) {
		Some(DataType::HashSet(hset)) =>
			Ok(DataType::HashSet(hset.clone())),
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::EmptyList)
	}
}

pub fn smove<'a>(src: &'a str, dst: &'a str, v: &'a str)
	-> Result<DataType, &'a str> {
	let mut m = M.lock().unwrap();
	let item = match m.get_mut(src) {
		Some(DataType::HashSet(hset)) => {
			let e = hset.take(&DataType::bulkStr(v));
			if 0 == hset.len() {m.remove(src);}
			e
		},
		Some(_) => return Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => return Ok(DataType::Integer(0))
	};
	match item {
		Some(DataType::BulkString(_)) => match m.get_mut(dst) {
			Some(DataType::HashSet(hset2)) => {
				hset2.insert(item.unwrap());
				Ok(DataType::Integer(1))
			},
			Some(_) => Err(
				"WRONGTYPE Operation against a key holding the wrong kind of \
				value"
			),
			None => {
				let mut hset2: HashSet<DataType> = HashSet::new();
				hset2.insert(item.unwrap());
				m.insert(String::from(dst), DataType::hset(&hset2));
				Ok(DataType::Integer(1))
			}
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::Integer(0))
	}
}

pub fn smismember(k: &str, vs: Vec<String>) -> Result<DataType, &str> {
	match M.lock().unwrap().get(k) {
		Some(DataType::HashSet(hset)) => Ok(DataType::List(
			vs.iter().map(|v| {DataType::Integer(
				if hset.contains(&DataType::bulkStr(v)) {1} else {0}
			)}).collect()
		)),
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::List(
			vs.iter().map(|_| {DataType::Integer(0)}).collect()
		))
	}
}

pub fn spop<'a>(k: &'a str, n: &'a str, single_item: bool)
	-> Result<DataType, &'a str> {
	let popsize: usize = match n.parse() {
		Ok(v) => v,
		Err(_) => return Err("ERR Number must be a positive integer")
	};
	match M.lock().unwrap().get_mut(k) {
		Some(DataType::HashSet(hset)) => {
			let h: Vec<DataType> = hset.iter().cloned().collect();
			let mut idxs: Vec<usize> = Vec::new();
			let mut rng = rand::rng();
			while idxs.len() < popsize {
				idxs.push(rng.random_range(0..h.len()));
				idxs.sort();
				idxs.dedup();
			}
			let vs = idxs.iter().map(|&idx|{h.get(idx).unwrap().clone()})
				.collect::<Vec<_>>();
			hset.retain(|e| {!vs.contains(e)});
			if single_item && 1 == vs.len() {
				Ok(vs.first().unwrap().clone())
			} else {
				Ok(DataType::List(vs))
			}
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::EmptyList)
	}
}

pub fn srandmember<'a>(k: &'a str, c: &'a str)
	-> Result<DataType, &'a str> {
	let i = match c.parse::<i64>() {
		Ok(n) => n,
		Err(_) => return Err("ERR Number must be an integer")
	};
	match M.lock().unwrap().get(k) {
		Some(DataType::HashSet(hset)) => {
			let h: Vec<DataType> = hset.iter().cloned().collect();
			let cnt: usize = if (h.len() as i64) < i {
				h.len()
			} else if 0 <= i {
				i as usize
			} else {
				(-1 * i) as usize
			};
			let mut idxs: Vec<usize> = Vec::new();
			let mut rng = rand::rng();
			loop {
				(0..cnt).for_each(|_| {idxs.push(
					rng.random_range(0..h.len())
				);});
				if 0 < i {
					idxs.sort();
					idxs.dedup();
				};
				if cnt <= idxs.len() {
					if cnt < idxs.len() {
						idxs.truncate(cnt);
					}
					break;
				}
			}
			Ok(DataType::List(
				idxs.iter().map(|&idx|{h.get(idx).unwrap().clone()})
					.collect::<Vec<_>>()
			))
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::Null)
	}
}

pub fn srem(k: &str, vs: Vec<String>) -> Result<DataType, &str> {
	let mut m = M.lock().unwrap();
	match m.get_mut(k) {
		Some(DataType::HashSet(hset)) => {
			let cnt = vs.iter().map(|s| {
				if hset.remove(&DataType::bulkStr(&s)) {
					1i64
				} else {
					0i64
				}
			}).sum::<i64>();
			if 0 == hset.len() {m.remove(k);}
			Ok(DataType::Integer(cnt))
		},
		Some(_) => Err(
			"WRONGTYPE Operation against a key holding the wrong kind of \
			value"
		),
		None => Ok(DataType::Integer(0))
	}
}

pub fn sunion(ks: Vec<String>) -> Result<DataType, &'static str> {
	let m = M.lock().unwrap();
	let mut wk: HashSet<DataType> = HashSet::new();
	for k in ks {
		if let Some(DataType::HashSet(hset)) = m.get(&k) {
			wk = wk.union(hset).cloned().collect();
		} else {
			return Err(
				"WRONGTYPE Operation against a key holding the wrong kind \
				of value"
			);
		}
	}
	Ok(DataType::List(wk.iter().cloned().collect::<Vec<_>>()))
}

pub fn sunionstore(dst: &str, ks: Vec<String>) -> Result<DataType, &str> {
	let mut m = M.lock().unwrap();
	let mut wk: HashSet<DataType> = HashSet::new();
	for k in ks {
		if let Some(DataType::HashSet(hset)) = m.get(&k) {
			wk = wk.union(hset).cloned().collect();
		} else {
			return Err(
				"WRONGTYPE Operation against a key holding the wrong kind \
				of value"
			);
		}
	}
	m.insert(String::from(dst), DataType::hset(&wk));
	Ok(DataType::Integer(wk.len() as i64))
}

#[cfg(test)]
mod tests;
