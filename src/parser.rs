use std::io::{BufRead, BufReader, Read};

use super::request::Request;

const ERRMSG_BADLISTLEN: &str = "ERR Invalid list length";
const ERRMSG_BADSTRLEN: &str = "ERR Invalid string length";
const ERRMSG_CNXERR: &str = "ERR Connection error";
const ERRMSG_EOF: &str = "ERR EOF reached";
const ERRMSG_LISTLENDIFF: &str = "ERR Contents unmatch list length";
const ERRMSG_PROTOERR: &str = "ERR Protocol error";
const ERRMSG_STRLENDIFF: &str = "ERR Contents unmatch string length";

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
			Ok(0) => return Err(ERRMSG_EOF),
			Ok(_) => sln.trim_end(),
			Err(_) => return Err(ERRMSG_CNXERR)
		};
		if 0 == line.len() && (sbuf.len() < slen || lcnt < llen) {
			return Err(ERRMSG_PROTOERR);
		};
		let c = line.chars().nth(0).unwrap_or('\0');
		if 0 == llen {
			if '*' == c {
				match line[1..].parse::<usize>() {
					Ok(n) => llen = n,
					Err(_) => return Err(ERRMSG_BADLISTLEN)
				};
			} else {
				return Err(ERRMSG_PROTOERR);
			};
		} else if 0 == slen {
			if '$' == c {
				match line[1..].parse::<usize>() {
					Ok(n) => slen = n,
					Err(_) => return Err(ERRMSG_BADSTRLEN)
				};
			} else {
				return Err(ERRMSG_PROTOERR);
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
				return Err(ERRMSG_STRLENDIFF);
			};
		} else {
			return Err(ERRMSG_PROTOERR);
		};
		if 0 < llen && lcnt == llen {
			break;
		}
	};
	if slen != 0 || sbuf.len() != 0 {
		Err(ERRMSG_STRLENDIFF)
	} else if lcnt != llen {
		Err(ERRMSG_LISTLENDIFF)
	} else {
		Ok(parameters)
	}
}
