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
	let _ = hset(
		"fieldvalues",
		vec![
			"field1".to_string(), "value1".to_string(),
			"field2".to_string(), "value2".to_string()
		],
		&false
	);
	assert_eq!(
		hget("fieldvalues", "field1"),
		Ok(DataType::bulkStr("value1"))
	);
	assert_eq!(
		hget("fieldvalues", "field2"),
		Ok(DataType::bulkStr("value2"))
	);
	let _ = hset(
		"fieldvalues",
		vec![
			"field3".to_string(), "value3".to_string(),
			"field4".to_string(), "value4".to_string(),
			"field5".to_string(), "value5".to_string(),
		],
		&false
	);
	assert_eq!(
		hget("fieldvalues", "field1"),
		Ok(DataType::bulkStr("value1"))
	);
	assert_eq!(
		hget("fieldvalues", "field4"),
		Ok(DataType::bulkStr("value4"))
	);
	let _ = hset(
		"fieldvalues",
		vec![
			"field1".to_string(), "val1".to_string(),
			"field2".to_string(), "val2".to_string()
		],
		&false
	);
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
		let mut fields = somekeys.iter().map(|e| {
			match e {
				DataType::BulkString(s) => s.clone(),
				_ => "".to_string()
			}
		}).collect::<Vec<_>>();
		fields.sort();
		assert_eq!(
			fields,
			vec!["field1", "field2", "field3", "field4", "field5"]
		);
	}
	let _ = hset(
		"fieldvalues",
		vec![
			"field1".to_string(), "value1".to_string(),
			"field2".to_string(), "value2".to_string()
		],
		&false
	);
	if let Ok(DataType::List(somevals)) = hvals("fieldvalues") {
		let mut fields = somevals.iter().map(|e| {
			match e {
				DataType::BulkString(s) => s.clone(),
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
		hmget("fieldvalues", vec![
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
		hmget("fieldvalues", vec![
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
		hmget("nonexist", vec![
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
		hset(
			"fieldvalues",
			vec![
				"field1".to_string(), "128".to_string(),
				"field2".to_string(), "non-numeric".to_string()
			],
			&false
		),
		Ok(DataType::Integer(2))
	);
	assert_eq!(
		hincrby("fieldvalues", "field1", "128"),
		Ok(DataType::Integer(256))
	);
	assert_eq!(
		hincrby("fieldvalues", "field2", "64"),
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
		llen("somekey"),
		Ok(DataType::Integer(0))
	);
	assert_eq!(
		lpush(
			"somekey",
			vec![
				"val1".to_string(),
				"val2".to_string(),
				"val3".to_string()
			],
			false
		),
		Ok(DataType::Integer(3))
	);
	assert_eq!(
		llen("somekey"),
		Ok(DataType::Integer(3))
	);
	assert_eq!(
		lpush(
			"somekey",
			vec![
				"val4".to_string(),
				"val5".to_string(),
				"val6".to_string()
			],
			false
		),
		Ok(DataType::Integer(6))
	);
	assert_eq!(
		lindex("somekey", "1"),
		Ok(DataType::bulkStr("val5"))
	);
	assert_eq!(
		lindex("somekey", "-1"),
		Ok(DataType::bulkStr("val1"))
	);
	assert_eq!(
		lindex("somekey", "-6"),
		Ok(DataType::bulkStr("val6"))
	);
	assert_eq!(
		lindex("somekey", "3"),
		Ok(DataType::bulkStr("val3"))
	);
	assert_eq!(
		lrange("somekey", "1", "1"),
		Ok(DataType::List(vec![
			DataType::bulkStr("val5")
		]))
	);
	assert_eq!(
		lrange("somekey", "-6", "2"),
		Ok(DataType::List(vec![
			DataType::bulkStr("val6"),
			DataType::bulkStr("val5"),
			DataType::bulkStr("val4")
		]))
	);
	assert_eq!(
		lrange("somekey", "-100", "100"),
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
		lpop("somekey", "6"),
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
		llen("somekey"),
		Ok(DataType::Integer(0))
	);
	assert_eq!(
		rpush(
			"somekey",
			vec![
				"nval1".to_string(),
				"nval2".to_string(),
				"nval3".to_string()
			],
			&false
		),
		Ok(DataType::Integer(3))
	);
	assert_eq!(
		lrange("somekey", "0", "-1"),
		Ok(DataType::List(vec![
			DataType::bulkStr("nval1"),
			DataType::bulkStr("nval2"),
			DataType::bulkStr("nval3")
		]))
	);
	assert_eq!(
		rpop("somekey", "2"),
		Ok(DataType::List(vec![
			DataType::bulkStr("nval3"),
			DataType::bulkStr("nval2")
		]))
	);
	assert_eq!(
		lrange("somekey", "0", "-1"),
		Ok(DataType::List(vec![
			DataType::bulkStr("nval1")
		]))
	);
	assert_eq!(
		rpop("somekey", "1"),
		Ok(DataType::List(vec![
			DataType::bulkStr("nval1")
		]))
	);
	assert_eq!(
		lrange("somekey", "0", "-1"),
		Ok(DataType::EmptyList)
	);
	assert_eq!(
		rpush(
			"somekey",
			vec![
				"one".to_string(),
				"two".to_string(),
				"three".to_string(),
				"four".to_string(),
				"five".to_string()
			],
			&false
		),
		Ok(DataType::Integer(5))
	);
	assert_eq!(
		ltrim("somekey", "1", "-2"),
		Ok(DataType::bulkStr("OK"))
	);
	assert_eq!(
		lrange("somekey", "0", "-1"),
		Ok(DataType::List(vec![
			DataType::bulkStr("two"),
			DataType::bulkStr("three"),
			DataType::bulkStr("four")
		]))
	);
	assert_eq!(
		del(&vec!["somekey".to_string()]),
		Ok(DataType::Integer(1))
	);
	assert_eq!(
		rpush(
			"somekey",
			vec![
				"one".to_string(),
				"two".to_string(),
				"three".to_string(),
				"one".to_string(),
				"two".to_string(),
				"three".to_string(),
				"one".to_string(),
				"two".to_string(),
				"three".to_string()
			],
			&false
		),
		Ok(DataType::Integer(9))
	);
	assert_eq!(
		lrem("somekey", "2", "two"),
		Ok(DataType::Integer(2))
	);
	assert_eq!(
		lrange("somekey", "0", "-1"),
		Ok(DataType::List(vec![
			DataType::bulkStr("one"),
			DataType::bulkStr("three"),
			DataType::bulkStr("one"),
			DataType::bulkStr("three"),
			DataType::bulkStr("one"),
			DataType::bulkStr("two"),
			DataType::bulkStr("three")
		]))
	);
	assert_eq!(
		del(&vec!["somekey".to_string()]),
		Ok(DataType::Integer(1))
	);
	assert_eq!(
		rpush(
			"somekey",
			vec![
				"one".to_string(),
				"two".to_string(),
				"three".to_string(),
				"one".to_string(),
				"two".to_string(),
				"three".to_string(),
				"one".to_string(),
				"two".to_string(),
				"three".to_string()
			],
			&false
		),
		Ok(DataType::Integer(9))
	);
	assert_eq!(
		lrem("somekey", "-2", "two"),
		Ok(DataType::Integer(2))
	);
	assert_eq!(
		lrange("somekey", "0", "-1"),
		Ok(DataType::List(vec![
			DataType::bulkStr("one"),
			DataType::bulkStr("two"),
			DataType::bulkStr("three"),
			DataType::bulkStr("one"),
			DataType::bulkStr("three"),
			DataType::bulkStr("one"),
			DataType::bulkStr("three")
		]))
	);
	assert_eq!(
		del(&vec!["somekey".to_string()]),
		Ok(DataType::Integer(1))
	);
	assert_eq!(
		rpush(
			"somekey",
			vec![
				"one".to_string(),
				"two".to_string(),
				"three".to_string(),
				"one".to_string(),
				"two".to_string(),
				"three".to_string(),
				"one".to_string(),
				"two".to_string(),
				"three".to_string()
			],
			&false
		),
		Ok(DataType::Integer(9))
	);
	assert_eq!(
		rpush(
			"nonkey",
			vec![
				"un".to_string(),
				"deux".to_string(),
				"trois".to_string()
			],
			&true
		),
		Ok(DataType::Integer(0))
	);
	assert_eq!(
		lrange("nonkey", "0", "-1"),
		Ok(DataType::EmptyList)
	);
	assert_eq!(
		lpush(
			"nonkey",
			vec![
				"un".to_string(),
				"deux".to_string(),
				"trois".to_string()
			],
			true
		),
		Ok(DataType::Integer(0))
	);
	assert_eq!(
		lrange("nonkey", "0", "-1"),
		Ok(DataType::EmptyList)
	);
	assert_eq!(
		lrem("somekey", "0", "two"),
		Ok(DataType::Integer(3))
	);
	assert_eq!(
		lrange("somekey", "0", "-1"),
		Ok(DataType::List(vec![
			DataType::bulkStr("one"),
			DataType::bulkStr("three"),
			DataType::bulkStr("one"),
			DataType::bulkStr("three"),
			DataType::bulkStr("one"),
			DataType::bulkStr("three")
		]))
	);
	assert_eq!(
		lset("somekey", "1", "two"),
		Ok(DataType::bulkStr("OK"))
	);
	assert_eq!(
		lset("somekey", "-1", "six"),
		Ok(DataType::bulkStr("OK"))
	);
	assert_eq!(
		lset("somekey", "-4", "three"),
		Ok(DataType::bulkStr("OK"))
	);
	assert_eq!(
		lset("somekey", "4", "five"),
		Ok(DataType::bulkStr("OK"))
	);
	assert_eq!(
		lset("somekey", "3", "four"),
		Ok(DataType::bulkStr("OK"))
	);
	assert_eq!(
		lrange("somekey", "0", "-1"),
		Ok(DataType::List(vec![
			DataType::bulkStr("one"),
			DataType::bulkStr("two"),
			DataType::bulkStr("three"),
			DataType::bulkStr("four"),
			DataType::bulkStr("five"),
			DataType::bulkStr("six")
		]))
	);
	assert_eq!(
		linsert("somekey", "before", "one", "zero"),
		Ok(DataType::Integer(7))
	);
	assert_eq!(
		linsert("somekey", "after", "six", "seven"),
		Ok(DataType::Integer(8))
	);
	assert_eq!(
		linsert("somekey", "after", "ten", "eleven"),
		Ok(DataType::Integer(-1))
	);
	assert_eq!(
		linsert("nonkey", "after", "seven", "eight"),
		Ok(DataType::Integer(0))
	);
	assert_eq!(
		lrange("somekey", "0", "-1"),
		Ok(DataType::List(vec![
			DataType::bulkStr("zero"),
			DataType::bulkStr("one"),
			DataType::bulkStr("two"),
			DataType::bulkStr("three"),
			DataType::bulkStr("four"),
			DataType::bulkStr("five"),
			DataType::bulkStr("six"),
			DataType::bulkStr("seven")
		]))
	);
	assert_eq!(
		del(&vec!["somekey".to_string()]),
		Ok(DataType::Integer(1))
	);
}

#[test]
#[serial]
fn plan8() {
	assert_eq!(
		scard("someset"),
		Ok(DataType::Integer(0))
	);
	assert_eq!(
		sadd(
			"someset",
			vec![
				"one".to_string(),
				"two".to_string(),
				"three".to_string()
			]
		),
		Ok(DataType::Integer(3))
	);
	assert!(matches!(smembers("someset"), Ok(DataType::HashSet(_))));
	if let Ok(DataType::HashSet(s)) = smembers("someset") {
		let v = vec![
			DataType::bulkStr("one"),
			DataType::bulkStr("two"),
			DataType::bulkStr("three")
		];
		assert_eq!(s.len(), 3usize);
		assert_eq!(v.iter().all(|e|{s.contains(e)}), true);
	}
	assert_eq!(
		scard("someset"),
		Ok(DataType::Integer(3))
	);
	assert_eq!(
		sadd(
			"someset",
			vec![
				"three".to_string(),
				"four".to_string(),
				"five".to_string()
			]
		),
		Ok(DataType::Integer(2))
	);
	assert_eq!(
		scard("someset"),
		Ok(DataType::Integer(5))
	);
	assert_eq!(
		sadd(
			"someset",
			vec![
				"one".to_string(),
				"three".to_string(),
				"five".to_string()
			]
		),
		Ok(DataType::Integer(0))
	);
	assert!(matches!(smembers("someset"), Ok(DataType::HashSet(_))));
	if let Ok(DataType::HashSet(s)) = smembers("someset") {
		let v = vec![
			DataType::bulkStr("one"),
			DataType::bulkStr("two"),
			DataType::bulkStr("three"),
			DataType::bulkStr("four"),
			DataType::bulkStr("five")
		];
		assert_eq!(s.len(), 5usize);
		assert_eq!(v.iter().all(|e|{s.contains(e)}), true);
	}
	assert_eq!(
		smismember(
			"nonexists",
			vec![
				"one".to_string(),
				"two".to_string(),
				"three".to_string(),
				"four".to_string(),
				"five".to_string()
			]
		),
		Ok(DataType::List(vec![
			DataType::Integer(0),
			DataType::Integer(0),
			DataType::Integer(0),
			DataType::Integer(0),
			DataType::Integer(0)
		]))
	);
	assert_eq!(
		smismember(
			"someset",
			vec![
				"one".to_string(),
				"two".to_string(),
				"three".to_string(),
				"four".to_string(),
				"five".to_string()
			]
		),
		Ok(DataType::List(vec![
			DataType::Integer(1),
			DataType::Integer(1),
			DataType::Integer(1),
			DataType::Integer(1),
			DataType::Integer(1)
		]))
	);
	assert_eq!(
		scard("someset"),
		Ok(DataType::Integer(5))
	);
	assert_eq!(
		sismember("someset", "four"),
		Ok(DataType::Integer(1))
	);
	assert_eq!(
		sismember("someset", "deux"),
		Ok(DataType::Integer(0))
	);
	assert_eq!(
		sismember("someset", "two"),
		Ok(DataType::Integer(1))
	);
	assert!(matches!(srandmember("someset", "5"), Ok(DataType::List(_))));
	if let Ok(DataType::List(l)) = srandmember("someset", "5") {
		let v = vec![
			DataType::bulkStr("one"),
			DataType::bulkStr("two"),
			DataType::bulkStr("three"),
			DataType::bulkStr("four"),
			DataType::bulkStr("five")
		];
		assert_eq!(l.len(), 5usize);
		assert_eq!(v.iter().all(|e|{l.contains(e)}), true);
	}
	assert!(matches!(srandmember("someset", "6"), Ok(DataType::List(_))));
	if let Ok(DataType::List(l)) = srandmember("someset", "6") {
		let v = vec![
			DataType::bulkStr("one"),
			DataType::bulkStr("two"),
			DataType::bulkStr("three"),
			DataType::bulkStr("four"),
			DataType::bulkStr("five")
		];
		assert_eq!(l.len(), 5usize);
		assert_eq!(v.iter().all(|e|{l.contains(e)}), true);
	}
	assert!(matches!(srandmember("someset", "-4"), Ok(DataType::List(_))));
	if let Ok(DataType::List(l)) = srandmember("someset", "-4") {
		let v = vec![
			DataType::bulkStr("one"),
			DataType::bulkStr("two"),
			DataType::bulkStr("three"),
			DataType::bulkStr("four"),
			DataType::bulkStr("five")
		];
		assert_eq!(l.len(), 4usize);
		assert_eq!(l.iter().all(|e|{v.contains(e)}), true);
	}
	assert!(matches!(srandmember("someset", "-6"), Ok(DataType::List(_))));
	if let Ok(DataType::List(l)) = srandmember("someset", "-6") {
		let v = vec![
			DataType::bulkStr("one"),
			DataType::bulkStr("two"),
			DataType::bulkStr("three"),
			DataType::bulkStr("four"),
			DataType::bulkStr("five")
		];
		assert_eq!(l.len(), 6usize);
		assert_eq!(l.iter().all(|e|{v.contains(e)}), true);
	}
	assert!(matches!(srandmember("someset", "0"), Ok(DataType::List(_))));
	if let Ok(DataType::List(l)) = srandmember("someset", "0") {
		assert_eq!(l.len(), 0usize);
	}
	assert_eq!(srandmember("nonexists", "5"), Ok(DataType::Null));
	assert_eq!(
		sadd(
			"anotherset",
			vec![
				"one".to_string(),
				"three".to_string(),
				"five".to_string()
			]
		),
		Ok(DataType::Integer(3))
	);
	assert_eq!(
		sadd(
			"yetanotherset",
			vec![
				"one".to_string(),
				"two".to_string()
			]
		),
		Ok(DataType::Integer(2))
	);
	assert_eq!(
		sdiff(
			"someset",
			vec![
				"anotherset".to_string(),
				"yetanotherset".to_string()
			]
		),
		Ok(DataType::List(vec![
			DataType::bulkStr("four")
		]))
	);
	assert_eq!(
		sdiffstore(
			"newset",
			"someset",
			vec![
				"anotherset".to_string(),
				"yetanotherset".to_string()
			]
		),
		Ok(DataType::Integer(1))
	);
	assert!(matches!(smembers("newset"), Ok(DataType::HashSet(_))));
	if let Ok(DataType::HashSet(s)) = smembers("newset") {
		let v = vec![
			DataType::bulkStr("four")
		];
		assert_eq!(s.len(), 1usize);
		assert_eq!(v.iter().all(|e|{s.contains(e)}), true);
	}
	assert_eq!(
		sinter(
			"someset",
			vec![
				"anotherset".to_string(),
				"yetanotherset".to_string()
			]
		),
		Ok(DataType::List(vec![DataType::bulkStr("one")]))
	);
	assert_eq!(
		sinterstore(
			"newset",
			"someset",
			vec![
				"anotherset".to_string(),
				"yetanotherset".to_string()
			]
		),
		Ok(DataType::Integer(1))
	);
	assert!(matches!(smembers("newset"), Ok(DataType::HashSet(_))));
	if let Ok(DataType::HashSet(s)) = smembers("newset") {
		let v = vec![
			DataType::bulkStr("one")
		];
		assert_eq!(s.len(), 1usize);
		assert_eq!(v.iter().all(|e|{s.contains(e)}), true);
	}
	assert_eq!(
		del(&vec![
			"someset".to_string(),
			"anotherset".to_string(),
			"yetanotherset".to_string(),
			"newset".to_string()
		]),
		Ok(DataType::Integer(4))
	);
	assert_eq!(
		sadd(
			"fruits",
			vec![
				"apple".to_string(),
				"banana".to_string(),
				"cherry".to_string()
			]
		),
		Ok(DataType::Integer(3))
	);
	assert_eq!(smembers("meal"), Ok(DataType::EmptyList));
	assert_eq!(smove("fruits", "meal", "banana"), Ok(DataType::Integer(1)));
	assert_eq!(smove("fruits", "meal", "cherry"), Ok(DataType::Integer(1)));
	assert_eq!(smove("fruits", "meal", "banana"), Ok(DataType::Integer(0)));
	assert_eq!(smove("fruits", "meal", "apple"), Ok(DataType::Integer(1)));
	assert_eq!(smembers("fruits"), Ok(DataType::EmptyList));
	assert!(matches!(smembers("meal"), Ok(DataType::HashSet(_))));
	if let Ok(DataType::HashSet(s)) = smembers("meal") {
		let v = vec![
			DataType::bulkStr("apple"),
			DataType::bulkStr("banana"),
			DataType::bulkStr("cherry")
		];
		assert_eq!(s.len(), 3usize);
		assert_eq!(v.iter().all(|e|{s.contains(e)}), true);
	}
	assert_eq!(
		sadd(
			"meal",
			vec![
				"durian".to_string(),
				"elderberry".to_string(),
				"fig".to_string()
			]
		),
		Ok(DataType::Integer(3))
	);
	assert!(matches!(spop("meal", "1", true), Ok(DataType::BulkString(_))));
	assert!(matches!(spop("meal", "1", false), Ok(DataType::List(_))));
	assert!(matches!(spop("meal", "2", true), Ok(DataType::List(_))));
	assert_eq!(
		del(&vec![
			"fruits".to_string(),
			"meal".to_string()
		]),
		Ok(DataType::Integer(1))
	);
	assert_eq!(
		sadd(
			"animals",
			vec![
				"ape".to_string(),
				"bird".to_string(),
				"cat".to_string(),
				"dog".to_string()
			]
		),
		Ok(DataType::Integer(4))
	);
	assert_eq!(
		srem("animals", vec!["fish".to_string()]),
		Ok(DataType::Integer(0))
	);
	assert_eq!(
		srem("animal", vec!["ape".to_string()]),
		Ok(DataType::Integer(0))
	);
	assert_eq!(
		srem("animals", vec!["ape".to_string()]),
		Ok(DataType::Integer(1))
	);
	assert_eq!(
		srem(
			"animals",
			vec![
				"bird".to_string(),
				"cat".to_string()
			]
		),
		Ok(DataType::Integer(2))
	);
	assert_eq!(
		srem(
			"animals",
			vec![
				"ape".to_string(),
				"bird".to_string(),
				"cat".to_string(),
				"dog".to_string()
			]
		),
		Ok(DataType::Integer(1))
	);
	assert_eq!(
		del(&vec!["animals".to_string()]),
		Ok(DataType::Integer(0))
	);
}

#[test]
#[serial]
fn plan9() {
	assert_eq!(
		sadd(
			"animals",
			vec![
				"aligator".to_string(),
				"bear".to_string(),
				"cat".to_string(),
				"dog".to_string(),
				"dove".to_string(),
				"eagle".to_string()
			]
		),
		Ok(DataType::Integer(6))
	);
	assert_eq!(
		sadd(
			"mammals",
			vec![
				"ape".to_string(),
				"bear".to_string(),
				"cat".to_string(),
				"dog".to_string()
			]
		),
		Ok(DataType::Integer(4))
	);
	assert_eq!(
		sadd(
			"birds",
			vec![
				"albatross".to_string(),
				"bluebird".to_string(),
				"cardinal".to_string(),
				"dove".to_string(),
				"eagle".to_string()
			]
		),
		Ok(DataType::Integer(5))
	);
	assert!(matches!(
		sunion(vec!["animals".to_string(), "mammals".to_string()]),
		Ok(DataType::List(_))
	));
	if let Ok(DataType::List(l))
		= sunion(vec!["animals".to_string(), "mammals".to_string()]) {
		let vs = vec![
			DataType::bulkStr("aligator"),
			DataType::bulkStr("ape"),
			DataType::bulkStr("bear"),
			DataType::bulkStr("cat"),
			DataType::bulkStr("dog"),
			DataType::bulkStr("dove"),
			DataType::bulkStr("eagle")
		];
		assert_eq!(l.len(), 7usize);
		assert!(vs.iter().all(|v| {l.contains(v)}));
	}
	assert!(matches!(
		sunion(vec![
			"animals".to_string(),
			"mammals".to_string(),
			"birds".to_string()
		]),
		Ok(DataType::List(_))
	));
	if let Ok(DataType::List(l)) = sunion(vec![
		"animals".to_string(),
		"mammals".to_string(),
		"birds".to_string()
	]) {
		let vs = vec![
			DataType::bulkStr("aligator"),
			DataType::bulkStr("albatross"),
			DataType::bulkStr("ape"),
			DataType::bulkStr("bear"),
			DataType::bulkStr("bluebird"),
			DataType::bulkStr("cardinal"),
			DataType::bulkStr("cat"),
			DataType::bulkStr("dog"),
			DataType::bulkStr("dove"),
			DataType::bulkStr("eagle")
		];
		assert_eq!(l.len(), 10usize);
		assert!(vs.iter().all(|v| {l.contains(v)}));
	}
	assert_eq!(
		sunionstore(
			"creatures",
			vec![
				"animals".to_string(),
				"mammals".to_string(),
				"birds".to_string()
			]
		),
		Ok(DataType::Integer(10))
	);
	if let Ok(DataType::HashSet(s)) = smembers("creatures") {
		let v = vec![
			DataType::bulkStr("aligator"),
			DataType::bulkStr("albatross"),
			DataType::bulkStr("ape"),
			DataType::bulkStr("bear"),
			DataType::bulkStr("bluebird"),
			DataType::bulkStr("cardinal"),
			DataType::bulkStr("cat"),
			DataType::bulkStr("dog"),
			DataType::bulkStr("dove"),
			DataType::bulkStr("eagle")
		];
		assert_eq!(s.len(), 10usize);
		assert_eq!(v.iter().all(|e|{s.contains(e)}), true);
	}
	assert_eq!(
		del(&vec![
			"animals".to_string(),
			"mammals".to_string(),
			"birds".to_string()
		]),
		Ok(DataType::Integer(3))
	);
}
