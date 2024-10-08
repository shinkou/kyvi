mod kv;
mod parser;
mod request;

use std::collections::HashMap;
use std::io::{BufRead, BufReader, BufWriter, Error, Write};
use std::net::{TcpListener, TcpStream};
use threadpool::ThreadPool;

use request::Request;

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
		if req.command.eq("quit") {
			let _ = writer.flush();
			return;
		} else if req.command.eq("get") {
			cmd_get(req, &mut writer);
		} else if req.command.eq("help") {
			cmd_help(req, &mut writer);
		} else if req.command.eq("set") {
			cmd_set(req, &mut writer);
		} else if req.command.eq("keys") {
			cmd_keys(req, &mut writer);
		} else {
			let _ = writer.write_fmt(format_args!("Unknown command \"{}\".\n", req.command));
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
	// TODO:2024-10-08:chun:let's introduce a struct for the values
	let cmds = HashMap::from([
		("get", "get KEY\n\nobtain value associated with the KEY.\n"),
		("keys", "keys REGEX\n\nlist keys matching the REGEX pattern.\n"),
		("set", "set KEY VALUE\n\nrecord the given KEY VALUE pair.\n")
	]);

	if 1 > req.parameters.len() {
		let _ = writer.write("Available commands:\n".as_bytes());
		let mut cnt = 0;
		for cmd in cmds.keys() {
			cnt += 1;
			let _ = writer.write_fmt(format_args!("{cnt}) \"{cmd}\"\n"));
		}
		let _ = writer.write(
			"\nUse \"help COMMAND\" for details of each COMMAND.\n"
				.as_bytes()
		);
	} else {
		match cmds.get(req.parameters.iter().nth(0).unwrap().as_str()) {
			Some(help) => {
				let _ = writer.write_fmt(format_args!("{help}\n"));
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
						let _ = writer.write_fmt(format_args!("{}) \"{}\"\n", cnt, k));
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
