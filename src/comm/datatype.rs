use std::fmt;

#[derive(Clone, Debug, PartialEq)]
pub enum DataType {
	BigInteger(i128),
	Boolean(bool),
	BulkError(String),
	BulkString(String),
	Double(f64),
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
			DataType::BigInteger(_) => i128::BITS as usize,
			DataType::Boolean(_) => 1 as usize,
			DataType::BulkError(s) | DataType::BulkString(s) |
				DataType::SimpleError(s) | DataType::SimpleString(s) =>
				s.capacity(),
			DataType::Double(_) => 8 as usize,
			DataType::Integer(_) => i64::BITS as usize,
			DataType::List(l) => l.len(),
			DataType::Null => 0 as usize
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

	pub fn str(s: &str) -> DataType {
		DataType::SimpleString(s.to_string())
	}
}

impl fmt::Display for DataType {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			DataType::BigInteger(n) =>
				write!(f, "({}\n", n),
			DataType::Boolean(b) =>
				write!(f, "#{}\n", if *b {"t"} else {"f"}),
			DataType::BulkError(s) =>
				write!(f, "!{}\n{}\n", s.capacity(), s),
			DataType::BulkString(s) =>
				write!(f, "${}\n{}\n", s.capacity(), s),
			DataType::Double(d) =>
				write!(f, ",{}\n", d),
			DataType::Integer(i) =>
				write!(f, ":{}\n", i),
			DataType::List(l) => {
				write!(f, "*{}\n", l.len())?;
				for e in l.iter() {
					write!(f, "{}", e)?;
				}
				Ok(())
			},
			DataType::Null =>
				write!(f, "_\n"),
			DataType::SimpleError(s) =>
				write!(f, "-{}\n", s),
			DataType::SimpleString(s) =>
				write!(f, "+{}\n", s),
		}
	}
}
