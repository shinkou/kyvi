use std::io::{BufRead, BufReader, Read};

use super::request::Request;

const EMPTY_STRING: String = String::new();

pub fn parse<R: Read>(reader: &mut BufReader<R>) -> Result<Request, &str> {
	let mut prms = get_parameters(reader)?;
	let cmd = if 0 < prms.len() {
		prms.remove(0).to_ascii_lowercase()
	} else {
		EMPTY_STRING
	};
	Ok(Request {command: cmd, parameters: prms})
}

fn get_parameters<R: Read>(reader: &mut BufReader<R>)
	-> Result<Vec<String>, &str> {
	let mut parameters: Vec<String> = Vec::new();
	let mut llen: usize = 0;
	let mut slen: usize = 0;
	let mut lcnt: usize = 0;
	let mut sln: String = String::new();
	let mut sbuf: String = String::new();
	loop {
		sln.clear();
		let line = match reader.read_line(&mut sln) {
			Ok(0) => return Err("ERR EOF reached"),
			Ok(_) => sln.trim_end(),
			Err(_) => return Err("ERR Connection error")
		};
		if 0 == line.len() && (sbuf.len() < slen || lcnt < llen) {
			return Err("ERR Protocol error");
		};
		let c = line.chars().nth(0).unwrap_or('\0');
		if 0 == llen {
			if '*' == c {
				match line[1..].parse::<usize>() {
					Ok(n) => llen = n,
					Err(_) => return Err("ERR Invalid list length")
				};
			} else {
				return Err("ERR Protocol error");
			};
		} else if 0 == slen {
			if '$' == c {
				match line[1..].parse::<usize>() {
					Ok(n) => slen = n,
					Err(_) => return Err("ERR Invalid string length")
				};
			} else {
				return Err("ERR Protocol error");
			};
		} else if 0 < slen {
			if sbuf.len() < slen {
				sbuf.push_str(&line);
			};
			if sbuf.len() == slen {
				parameters.push(sbuf.clone());
				sbuf.clear();
				slen = 0;
				lcnt += 1;
			} else if sbuf.len() > slen {
				return Err("ERR Contents unmatch string length");
			};
		} else {
			return Err("ERR Protocol error");
		};
		if 0 < llen && lcnt == llen {
			break;
		}
	};
	if slen != 0 || sbuf.len() != 0 {
		Err("ERR Contents unmatch string length")
	} else if lcnt != llen {
		Err("ERR Contents unmatch list length")
	} else {
		Ok(parameters)
	}
}
