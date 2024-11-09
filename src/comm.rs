mod datatype;
mod kv;
mod parser;
mod request;

use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use threadpool::ThreadPool;
use phf::phf_map;

use datatype::DataType;
use request::Request;

struct Command<'a> {
	function: fn(&Request) -> DataType,
	syntax: &'a str,
	validation: fn(&Request) -> bool,
	doc: &'a str
}

static CMDS: phf::Map<&str, Command> = phf_map! {
	"del" => Command {
		function: cmd_del,
		syntax: "del KEY",
		validation: |r| {1 == r.parameters.len()},
		doc: "remove the value associated with the KEY."
	},
	"get" => Command {
		function: cmd_get,
		syntax: "get KEY",
		validation: |r| {1 == r.parameters.len()},
		doc: "obtain value associated with the KEY."
	},
	"help" => Command {
		function: cmd_help,
		syntax: "help [ COMMAND ]",
		validation: |r| {2 > r.parameters.len()},
		doc: "list commands, or show details of the given COMMAND."
	},
	"info" => Command {
		function: cmd_info,
		syntax: "info",
		validation: |r| {0 == r.parameters.len()},
		doc: "display system info."
	},
	"keys" => Command {
		function: cmd_keys,
		syntax: "keys REGEX",
		validation: |r| {1 == r.parameters.len()},
		doc: "list keys matching the REGEX pattern."
	},
	"quit" => Command {
		function: cmd_quit,
		syntax: "quit",
		validation: |r| {0 == r.parameters.len()},
		doc: "close current connection and quit."
	},
	"set" => Command {
		function: cmd_set,
		syntax: "set KEY VALUE",
		validation: |r| {2 == r.parameters.len()},
		doc: "record the given KEY VALUE pair."
	}
};

const UNITS: &'static[&'static str] = &["", "k", "M", "G", "T", "P", "E"];

pub fn listen_to(bindaddr: &str, poolsize: usize) -> std::io::Result<()> {
	let listener: TcpListener = TcpListener::bind(bindaddr)?;
	let pool = ThreadPool::new(poolsize);
	for stream in listener.incoming() {
		match stream {
			Ok(stream) => {
				pool.execute(move || {
					handle_client(stream);
				});
			}
			Err(e) => {
				eprintln!("Unhandled error: {:?}", e);
			}
		}
	}
	Ok(())
}

fn handle_client(stream: TcpStream) {
	let mut buf: String = String::new();
	let mut reader: BufReader<&TcpStream> = BufReader::new(&stream);
	let mut writer: BufWriter<&TcpStream> = BufWriter::new(&stream);
	loop {
		buf.clear();
		if let Err(_) = writer.flush() {return;}
		if let Err(_) = reader.read_line(&mut buf) {return;}
		let req = parser::parse(&buf);
		match CMDS.get(req.command.as_str()) {
			Some(cmd) => {
				if (cmd.validation)(&req) {
					if let Err(_) = write!(
							writer,
							"{}",
							(cmd.function)(&req)
						) {
						return;
					}
					if cmd.function == cmd_quit {
						let _ = writer.flush();
						return;
					}
				} else if let Err(_) = write!(
						writer,
						"{}",
						DataType::err(&format!(
							"ERR correct syntax: \"{}\"",
							cmd.syntax
						))
					) {
					return;
				}
			},
			None => if let Err(_) = write!(
					writer,
					"{}",
					DataType::err(&format!(
						"ERR unknown command \"{}\"",
						req.command
					))
				) {
					return;
				}
		}
	}
}

fn cmd_del(req: &Request) -> DataType {
	kv::del(req.parameters.iter().nth(0).unwrap().as_str());
	DataType::str("OK")
}

fn cmd_get(req: &Request) -> DataType {
	match kv::get(req.parameters.iter().nth(0).unwrap()) {
		Some(v) => v,
		None => DataType::Null
	}
}

fn cmd_help(req: &Request) -> DataType {
	if 1 == req.parameters.len() {
		let prm = req.parameters.iter().nth(0).unwrap().as_str();
		match CMDS.get(prm) {
			Some(cmd) => DataType::bulkStr(&format!(
					"Syntax:\n\t{}\n\nDescription:\n\t{}\n",
					cmd.syntax,
					cmd.doc
				)),
			None => DataType::err(&format!(
					"ERR unknown command \"{}\"",
					prm
				))
		}
	} else {
		let mut ctx = String::new();
		ctx.push_str("Available commands:\n");
		let mut cnt = 0;
		for cmd in CMDS.keys() {
			cnt += 1;
			ctx.push_str(&format!("{}) \"{}\"\n", cnt, cmd));
		}
		ctx.push_str(
			"\nUse \"help COMMAND\" for details of each COMMAND."
		);
		DataType::bulkStr(&ctx)
	}
}

fn cmd_keys(req: &Request) -> DataType {
	match kv::keys(req.parameters.iter().nth(0).unwrap().as_str()) {
		Ok(v) => v,
		Err(e) => DataType::bulkErr(&e.to_string())
	}
}

fn cmd_info(_req: &Request) -> DataType {
	let kv_memsize = kv::memsize();
	let idx = if 0 < kv_memsize {
		kv_memsize.ilog2() / 1024i64.ilog2()
	} else {
		0
	};
	let memsize: f64 = if 6 < idx {
		(kv_memsize as f64 / 1024f64.powf(6.0)) as f64
	} else if 0 < idx {
		(kv_memsize as f64 / 1024f64.powf(idx.into())) as f64
	} else {
		kv_memsize as f64
	};
	let ss = match UNITS.get(if 6 < idx {6usize} else {idx as usize}) {
		Some(u) =>
			if 0 < idx {
				format!("Data size: {:.2}{}B", memsize, u)
			} else {
				format!("Data size: {}B", memsize)
			},
		None => format!("Data size: {}B", memsize)
	};
	DataType::str(&ss)
}

fn cmd_quit(_req: &Request) -> DataType {
	DataType::str("OK")
}

fn cmd_set(req: &Request) -> DataType {
	let _oldv = kv::set(
		req.parameters.iter().nth(0).unwrap().as_str(),
		req.parameters.iter().nth(1).unwrap().as_str()
	);
	DataType::str("OK")
}
