use std::io::{BufRead, BufReader, Read};
use thiserror::Error;

use super::request::Request;

static ERRMSG_BADLISTLEN: &str = "Invalid list length";
static ERRMSG_BADSTRLEN: &str = "Invalid string length";
static ERRMSG_LISTLENDIFF: &str = "Contents unmatch list length";
static ERRMSG_STRLENDIFF: &str = "Contents unmatch string length";

#[derive(Debug, Error)]
pub enum ParserError<'a> {
	#[error("ERR Connection error")]
	Connection,
	#[error("ERR Protocol error")]
	Protocol,
	#[error("ERR EOF reached")]
	EOF,
	#[error("ERR {0}")]
	UsizeParsing(&'a str),
	#[error("ERR {0}")]
	UnmatchedContents(&'a str)
}

const EMPTY_STRING: String = String::new();

pub fn parse<R: Read>(reader: &mut BufReader<R>) -> Result<Request, ParserError> {
	let mut prms = get_parameters(reader)?;
	let cmd = if 0 < prms.len() {
		prms.remove(0).to_ascii_lowercase()
	} else {
		EMPTY_STRING
	};
	Ok(Request {command: cmd, parameters: prms})
}

fn get_parameters<R: Read>(reader: &mut BufReader<R>)
	-> Result<Vec<String>, ParserError> {
	let mut parameters: Vec<String> = Vec::new();
	let mut llen: usize = 0;
	let mut slen: usize = 0;
	let mut lcnt: usize = 0;
	let mut sln: String = String::new();
	let mut sbuf: String = String::new();
	loop {
		sln.clear();
		let line = match reader.read_line(&mut sln) {
			Ok(0) => return Err(ParserError::EOF),
			Ok(_) => sln.trim_end(),
			Err(_) => return Err(ParserError::Connection)
		};
		if 0 == line.len() && (sbuf.len() < slen || lcnt < llen) {
			return Err(ParserError::Protocol);
		};
		let c = line.chars().nth(0).unwrap_or('\0');
		if 0 == llen {
			if '*' == c {
				match line[1..].parse::<usize>() {
					Ok(n) => llen = n,
					Err(_) => return Err(ParserError::UsizeParsing(
						ERRMSG_BADLISTLEN
					))
				};
			} else {
				return Err(ParserError::Protocol);
			};
		} else if 0 == slen {
			if '$' == c {
				match line[1..].parse::<usize>() {
					Ok(n) => slen = n,
					Err(_) => return Err(ParserError::UsizeParsing(
						ERRMSG_BADSTRLEN
					))
				};
			} else {
				return Err(ParserError::Protocol);
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
				return Err(ParserError::UnmatchedContents(
					ERRMSG_STRLENDIFF
				));
			};
		} else {
			return Err(ParserError::Protocol);
		};
		if 0 < llen && lcnt == llen {
			break;
		}
	};
	if slen != 0 || sbuf.len() != 0 {
		Err(ParserError::UnmatchedContents(ERRMSG_STRLENDIFF))
	} else if lcnt != llen {
		Err(ParserError::UnmatchedContents(ERRMSG_LISTLENDIFF))
	} else {
		Ok(parameters)
	}
}
