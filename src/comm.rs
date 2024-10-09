mod kv;
mod parser;
mod request;

use std::io::{BufRead, BufReader, BufWriter, Error, Write};
use std::net::{TcpListener, TcpStream};
use threadpool::ThreadPool;
use phf::{phf_map};

use request::Request;

struct Command<'a> {
	function: fn(Request, &mut BufWriter<&TcpStream>),
	syntax: &'a str,
	doc: &'a str
}

static CMDS: phf::Map<&str, Command> = phf_map! {
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
		let _ = writer.write("kyvi> ".as_bytes());
		let _ = writer.flush();
		let _res: Result<usize, Error> = reader.read_line(&mut buf);
		let req = parser::parse(&buf);
		if let Some(cmd) = CMDS.get(req.command.as_str()) {
			(cmd.function)(req, &mut writer);
			if cmd.function == cmd_quit {
				return;
			}
		} else {
			let _ = writer.write_fmt(
				format_args!("Unknown command \"{}\".\n", req.command)
			);
		}
	}
}

fn cmd_get(req: Request, writer: &mut BufWriter<&TcpStream>) {
	if 1 > req.parameters.len() {
		let _ = writer.write("ERR missing 1 argument\n".as_bytes());
	} else {
		match kv::get(req.parameters.iter().nth(0).unwrap().as_str()) {
			Some(s) => {
				let _ = writer.write_fmt(format_args!("\"{}\"\n", s));
			},
			None => {
				let _ = writer.write("(nil)\n".as_bytes());
			}
		};
	}
}

fn cmd_help(req: Request, writer: &mut BufWriter<&TcpStream>) {
	if 1 > req.parameters.len() {
		let _ = writer.write("Available commands:\n".as_bytes());
		let mut cnt = 0;
		for cmd in CMDS.keys() {
			cnt += 1;
			let _ = writer.write_fmt(format_args!("{cnt}) \"{cmd}\"\n"));
		}
		let _ = writer.write(
			"\nUse \"help COMMAND\" for details of each COMMAND.\n"
				.as_bytes()
		);
	} else {
		match CMDS.get(req.parameters.iter().nth(0).unwrap().as_str()) {
			Some(cmd) => {
				let _ = writer.write_fmt(format_args!(
					"Syntax:\n\t{}\n\nDescription:\n\t{}\n\n",
					cmd.syntax,
					cmd.doc
				));
			},
			None => {
				let _ = writer.write("Unknown command\n".as_bytes());
			}
		}
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
						let _ = writer.write_fmt(
							format_args!("{}) \"{}\"\n", cnt, k)
						);
					}
				} else {
					let _ = writer.write("(empty array)\n".as_bytes());
				}
			},
			Err(e) => {
				let _ = writer.write_fmt(format_args!("ERR {}\n", e));
			}
		}
	}
}

fn cmd_quit(_req: Request, writer: &mut BufWriter<&TcpStream>) {
	let _ = writer.flush();
}

fn cmd_set(req: Request, writer: &mut BufWriter<&TcpStream>) {
	if 1 > req.parameters.len() {
		let _ = writer.write("ERR missing 2 arguments\n".as_bytes());
	} else if 2 > req.parameters.len() {
		let _ = writer.write("ERR missing 1 argument\n".as_bytes());
	} else {
		let _oldv = kv::set(
			req.parameters.iter().nth(0).unwrap().as_str(),
			req.parameters.iter().nth(1).unwrap().as_str()
		);
		let _ = writer.write("\"OK\"\n".as_bytes());
	}
}
