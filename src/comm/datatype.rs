use derivative::Derivative;
use std::collections::HashMap;
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Derivative)]
#[derivative(Hash)]
pub enum DataType {
	BigInteger(i128),
	Boolean(bool),
	BulkError(String),
	BulkString(String),
	HashMap(
		#[derivative(Hash="ignore")]
		HashMap<DataType, DataType>
	),
	Integer(i64),
	List(Vec<DataType>),
	Null,
	SimpleError(String),
	SimpleString(String)
}

#[allow(non_snake_case)]
impl DataType {
	pub fn capacity(&self) -> usize {
		match self {
			DataType::BigInteger(_) => 16usize,
			DataType::Boolean(_) => 1usize,
			DataType::BulkError(s) | DataType::BulkString(s) |
				DataType::SimpleError(s) | DataType::SimpleString(s) =>
				s.capacity(),
			DataType::HashMap(h) =>
				h.len() + h.iter().map(
					|(k, v)|{k.capacity() + v.capacity()}
				).sum::<usize>(),
			DataType::Integer(_) => 8usize,
			DataType::List(l) =>
				l.len() + l.iter().map(|e|{e.capacity()}).sum::<usize>(),
			DataType::Null => 0usize
		}
	}

	pub fn bulkErr(s: &str) -> DataType {
		DataType::BulkError(s.to_string())
	}

	pub fn bulkStr(s: &str) -> DataType {
		DataType::BulkString(s.to_string())
	}

	pub fn err(s: &str) -> DataType {
		DataType::SimpleError(s.to_string())
	}

	pub fn hset(m: &HashMap<DataType, DataType>) -> DataType {
		DataType::HashMap(m.clone())
	}

	pub fn str(s: &str) -> DataType {
		DataType::SimpleString(s.to_string())
	}
}

impl fmt::Display for DataType {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			DataType::BigInteger(n) =>
				write!(f, "({}\r\n", n),
			DataType::Boolean(b) =>
				write!(f, "#{}\r\n", if *b {"t"} else {"f"}),
			DataType::BulkError(s) =>
				write!(f, "!{}\r\n{}\n", s.capacity(), s),
			DataType::BulkString(s) =>
				write!(f, "${}\r\n{}\r\n", s.capacity(), s),
			DataType::HashMap(h) => {
				write!(f, "*{}\r\n", h.len() * 2)?;
				for (k, v) in h.iter() {
					let _ = write!(f, "{}{}", k, v);
				}
				Ok(())
			},
			DataType::Integer(i) =>
				write!(f, ":{}\r\n", i),
			DataType::List(l) => {
				write!(f, "*{}\r\n", l.len())?;
				for e in l.iter() {
					write!(f, "{}", e)?;
				}
				Ok(())
			},
			DataType::Null =>
				write!(f, "_\r\n"),
			DataType::SimpleError(s) =>
				write!(f, "-{}\r\n", s),
			DataType::SimpleString(s) =>
				write!(f, "+{}\r\n", s),
		}
	}
}
