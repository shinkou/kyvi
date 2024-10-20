mod kv;
mod parser;
mod request;

use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use threadpool::ThreadPool;
use phf::phf_map;

use request::Request;

struct Command<'a> {
	function: fn(Request, &mut BufWriter<&TcpStream>),
	syntax: &'a str,
	doc: &'a str
}

static CMDS: phf::Map<&str, Command> = phf_map! {
	"del" => Command {
		function: cmd_del,
		syntax: "del KEY",
		doc: "remove the value associated with the KEY."
	},
	"get" => Command {
		function: cmd_get,
		syntax: "get KEY",
		doc: "obtain value associated with the KEY."
	},
	"help" => Command {
		function: cmd_help,
		syntax: "help [ COMMAND ]",
		doc: "list commands, or show details of the given COMMAND."
	},
	"info" => Command {
		function: cmd_info,
		syntax: "info",
		doc: "display system info."
	},
	"keys" => Command {
		function: cmd_keys,
		syntax: "keys REGEX",
		doc: "list keys matching the REGEX pattern."
	},
	"quit" => Command {
		function: cmd_quit,
		syntax: "quit",
		doc: "close current connection and quit."
	},
	"set" => Command {
		function: cmd_set,
		syntax: "set KEY VALUE",
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
	let _ = writer.write("Welcome to kyvi!\n".as_bytes());
	let _ = writer.flush();
	loop {
		buf.clear();
		if let Err(_) = write!(writer, "kyvi> ") {return;}
		if let Err(_) = writer.flush() {return;}
		if let Err(_) = reader.read_line(&mut buf) {return;}
		let req = parser::parse(&buf);
		if let Some(cmd) = CMDS.get(req.command.as_str()) {
			(cmd.function)(req, &mut writer);
			if cmd.function == cmd_quit {return;}
		} else {
			if let Err(_) = write!(
				writer,
				"Unknown command \"{}\".\n",
				req.command
			) {
				return;
			}
		}
	}
}

fn cmd_del(req: Request, writer: &mut BufWriter<&TcpStream>) {
	let _ = if 1 > req.parameters.len() {
		write!(writer, "ERR missing 1 argument\n")
	} else {
		kv::del(req.parameters.iter().nth(0).unwrap().as_str());
		write!(writer, "OK\n")
	};
}

fn cmd_get(req: Request, writer: &mut BufWriter<&TcpStream>) {
	let _ = if 1 > req.parameters.len() {
		write!(writer, "ERR missing 1 argument\n")
	} else {
		match kv::get(req.parameters.iter().nth(0).unwrap().as_str()) {
			Some(s) => write!(writer, "{s}\n"),
			None => write!(writer, "(nil)\n")
		}
	};
}

fn cmd_help(req: Request, writer: &mut BufWriter<&TcpStream>) {
	if 1 > req.parameters.len() {
		let _ = write!(writer, "Available commands:\n");
		let mut cnt = 0;
		for cmd in CMDS.keys() {
			cnt += 1;
			let _ = write!(writer, "{cnt}) \"{cmd}\"\n");
		}
		let _ = write!(
			writer,
			"\nUse \"help COMMAND\" for details of each COMMAND.\n"
		);
	} else {
		let _ = match CMDS.get(
			req.parameters.iter().nth(0).unwrap().as_str()
		) {
			Some(cmd) => write!(
				writer,
				"Syntax:\n\t{}\n\nDescription:\n\t{}\n\n",
				cmd.syntax,
				cmd.doc
			),
			None => write!(writer, "Unknown command\n")
		};
	}
}

fn cmd_keys(req: Request, writer: &mut BufWriter<&TcpStream>) {
	if 1 > req.parameters.len() {
		let _ = writer.write("ERR missing 1 argument\n".as_bytes());
	} else {
		match kv::keys(req.parameters.iter().nth(0).unwrap().as_str()) {
			Ok(ks) => {
				if 0 < ks.len() {
					let mut cnt = 0;
					for k in ks {
						cnt += 1;
						let _ = write!(writer, "{}) \"{}\"\n", cnt, k);
					}
				} else {
					let _ = write!(writer, "(empty array)\n");
				}
			},
			Err(e) => {
				let _ = write!(writer, "ERR {}\n", e);
			}
		}
	}
}

fn cmd_info(_req: Request, writer: &mut BufWriter<&TcpStream>) {
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
	let _ = match UNITS.get(if 6 < idx {6usize} else {idx as usize}) {
		Some(u) =>
			if 0 < idx {
				write!(writer, "Data size: {:.2}{}B\n", memsize, u)
			} else {
				write!(writer, "Data size: {}B\n", memsize)
			},
		None => write!(writer, "Data size: {}B\n", memsize)
	};
}

fn cmd_quit(_req: Request, writer: &mut BufWriter<&TcpStream>) {
	let _ = writer.flush();
}

fn cmd_set(req: Request, writer: &mut BufWriter<&TcpStream>) {
	let _ = if 1 > req.parameters.len() {
		write!(writer, "ERR missing 2 arguments\n")
	} else if 2 > req.parameters.len() {
		write!(writer, "ERR missing 1 argument\n")
	} else {
		let _oldv = kv::set(
			req.parameters.iter().nth(0).unwrap().as_str(),
			req.parameters.iter().nth(1).unwrap().as_str()
		);
		write!(writer, "\"OK\"\n")
	};
}
