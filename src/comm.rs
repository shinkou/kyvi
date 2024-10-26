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
	function: fn(Request, &mut BufWriter<&TcpStream>) ->
		std::io::Result<()>,
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
	loop {
		buf.clear();
		if let Err(_) = writer.flush() {return;}
		if let Err(_) = reader.read_line(&mut buf) {return;}
		let req = parser::parse(&buf);
		if let Some(cmd) = CMDS.get(req.command.as_str()) {
			if let Err(_) = (cmd.function)(req, &mut writer) {return;}
			if cmd.function == cmd_quit {return;}
		} else {
			if let Err(_) = write!(
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

fn cmd_del(req: Request, writer: &mut BufWriter<&TcpStream>) ->
		std::io::Result<()> {
	if 1 != req.parameters.len() {
		write!(writer, "{}", DataType::err("ERR wrong number of arguments"))
	} else {
		kv::del(req.parameters.iter().nth(0).unwrap().as_str());
		write!(writer, "{}", DataType::str("OK"))
	}
}

fn cmd_get(req: Request, writer: &mut BufWriter<&TcpStream>) ->
		std::io::Result<()> {
	if 1 != req.parameters.len() {
		write!(writer, "{}", DataType::err("ERR wrong number of arguments"))
	} else {
		match kv::get(req.parameters.iter().nth(0).unwrap()) {
			Some(v) => write!(writer, "{}", v),
			None => write!(writer, "{}", DataType::Null)
		}
	}
}

fn cmd_help(req: Request, writer: &mut BufWriter<&TcpStream>) ->
		std::io::Result<()> {
	if 1 < req.parameters.len() {
		write!(writer, "{}", DataType::err("ERR wrong number of arguments"))
	} else if 1 == req.parameters.len() {
		match CMDS.get(
			req.parameters.iter().nth(0).unwrap().as_str()
		) {
			Some(cmd) => write!(
				writer,
				"{}",
				DataType::bulkStr(&format!(
					"Syntax:\n\t{}\n\nDescription:\n\t{}\n",
					cmd.syntax,
					cmd.doc
				))
			),
			None => write!(
				writer,
				"{}",
				DataType::err("ERR unknown command")
			)
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
		write!(writer, "{}", DataType::str(&ctx))
	}
}

fn cmd_keys(req: Request, writer: &mut BufWriter<&TcpStream>) ->
		std::io::Result<()> {
	if 1 != req.parameters.len() {
		write!(writer, "{}", DataType::err("ERR wrong number of arguments"))
	} else {
		match kv::keys(req.parameters.iter().nth(0).unwrap().as_str()) {
			Ok(v) =>
				write!(writer, "{}", v),
			Err(e) =>
				write!(writer, "{}", DataType::bulkErr(&e.to_string()))
		}
	}
}

fn cmd_info(req: Request, writer: &mut BufWriter<&TcpStream>) ->
		std::io::Result<()> {
	if 0 < req.parameters.len() {
		write!(writer, "{}", DataType::err("ERR wrong number of arguments"))
	} else {
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
		write!(writer, "{}", DataType::str(&ss))
	}
}

fn cmd_quit(req: Request, writer: &mut BufWriter<&TcpStream>) ->
		std::io::Result<()> {
	if 0 < req.parameters.len() {
		write!(writer, "{}", DataType::err("ERR wrong number of arguments"))
	} else if let Err(e) = write!(writer, "{}", DataType::str("OK")) {
		Err(e)
	} else {
		writer.flush()
	}
}

fn cmd_set(req: Request, writer: &mut BufWriter<&TcpStream>) ->
		std::io::Result<()> {
	let dtype = if 2 != req.parameters.len() {
		DataType::err("ERR wrong number of arguments")
	} else {
		let _oldv = kv::set(
			req.parameters.iter().nth(0).unwrap().as_str(),
			req.parameters.iter().nth(1).unwrap().as_str()
		);
		DataType::str("OK")
	};
	write!(writer, "{}", dtype)
}
