use std::io::{BufReader, BufWriter, Read, Write};
use phf::phf_map;

use super::datatype::DataType;
use super::kv;
use super::parser::parse;
use super::request::Request;

struct Command<'a> {
	function: fn(&Request) -> Result<DataType, &str>,
	syntax: &'a str,
	validation: fn(&Request) -> bool,
	doc: &'a str
}

const UNITS: &'static[&'static str] = &["", "k", "M", "G", "T", "P", "E"];

static CMDS: phf::Map<&str, Command> = phf_map! {
	"append" => Command {
		function: cmd_append,
		syntax: "append KEY VALUE",
		validation: |r| {2 == r.parameters.len()},
		doc: "append value to the string stored at the key."
	},
	"client" => Command {
		function: cmd_client,
		syntax: "client SETINFO <LIB-NAME libname | LIB-VER libver>",
		validation: |r| {3 == r.parameters.len()},
		doc: "set client library information."
	},
	"decr" => Command {
		function: cmd_decr,
		syntax: "decr KEY",
		validation: |r| {1 == r.parameters.len()},
		doc: "decrement the integer value associated with the key."
	},
	"decrby" => Command {
		function: cmd_decrby,
		syntax: "decrby KEY VALUE",
		validation: |r| {2 == r.parameters.len()},
		doc: "decrement value stored at the key by the integer provided."
	},
	"del" => Command {
		function: cmd_del,
		syntax: "del KEY [ KEY ... ]",
		validation: |r| {0 < r.parameters.len()},
		doc: "remove the value associated with the key(s)."
	},
	"get" => Command {
		function: cmd_get,
		syntax: "get KEY",
		validation: |r| {1 == r.parameters.len()},
		doc: "obtain value associated with the key."
	},
	"getdel" => Command {
		function: cmd_getdel,
		syntax: "getdel KEY",
		validation: |r| {1 == r.parameters.len()},
		doc: "obtain the value of the key and delete it."
	},
	"getset" => Command {
		function: cmd_getset,
		syntax: "getset KEY VALUE",
		validation: |r| {2 == r.parameters.len()},
		doc: "obtain the value of the key and set it to a new value"
	},
	"help" => Command {
		function: cmd_help,
		syntax: "help [ COMMAND ]",
		validation: |r| {2 > r.parameters.len()},
		doc: "list commands, or show details of the given command."
	},
	"hdel" => Command {
		function: cmd_hdel,
		syntax: "hdel KEY FIELD [ FIELD ... ]",
		validation: |r| {1 < r.parameters.len()},
		doc: "remove specified fields existed in the hash stored at key"
	},
	"hexists" => Command {
		function: cmd_hexists,
		syntax: "hexists KEY FIELD",
		validation: |r| {2 == r.parameters.len()},
		doc: "return 1 if field exists, 0 if not, in the hash stored at key"
	},
	"hget" => Command {
		function: cmd_hget,
		syntax: "hget KEY FIELD",
		validation: |r| {2 == r.parameters.len()},
		doc: "get specified field from the hash stored at key"
	},
	"hgetall" => Command {
		function: cmd_hgetall,
		syntax: "hgetall KEY",
		validation: |r| {1 == r.parameters.len()},
		doc: "get all fields and values from the hash stored at key"
	},
	"hincrby" => Command {
		function: cmd_hincrby,
		syntax: "hincrby KEY FIELD INCR",
		validation: |r| {3 == r.parameters.len()},
		doc: "increment the numerical value of the field in the hash \
			stored at key by increment"
	},
	"hkeys" => Command {
		function: cmd_hkeys,
		syntax: "hkeys KEY",
		validation: |r| {1 == r.parameters.len()},
		doc: "get all field names in the hash stored at key"
	},
	"hlen" => Command {
		function: cmd_hlen,
		syntax: "hlen KEY",
		validation: |r| {1 == r.parameters.len()},
		doc: "get the number of elements in the hash stored at key"
	},
	"hmget" => Command {
		function: cmd_hmget,
		syntax: "hmget KEY FIELD [ FIELD ... ]",
		validation: |r| {1 < r.parameters.len()},
		doc: "get values associated with the fields in the hash stored at \
			key"
	},
	"hmset" => Command {
		function: cmd_hset,
		syntax: "hmset KEY FIELD VALUE [ FIELD VALUE ... ]",
		validation: |r| {
			2 < r.parameters.len() && 1 == r.parameters.len() % 2
		},
		doc: "set specified fields to values in the hash stored at key"
	},
	"hset" => Command {
		function: cmd_hset,
		syntax: "hset KEY FIELD VALUE [ FIELD VALUE ... ]",
		validation: |r| {
			2 < r.parameters.len() && 1 == r.parameters.len() % 2
		},
		doc: "set specified fields to values in the hash stored at key"
	},
	"hsetnx" => Command {
		function: cmd_hsetnx,
		syntax: "hsetnx KEY FIELD VALUE [ FIELD VALUE ... ]",
		validation: |r| {
			2 < r.parameters.len() && 1 == r.parameters.len() % 2
		},
		doc: "set specified non-existing fields to values in the hash \
			stored at key"
	},
	"hvals" => Command {
		function: cmd_hvals,
		syntax: "hvals KEY",
		validation: |r| {1 == r.parameters.len()},
		doc: "get all field values in the hash stored at key"
	},
	"incr" => Command {
		function: cmd_incr,
		syntax: "incr KEY",
		validation: |r| {1 == r.parameters.len()},
		doc: "increment the integer value associated with the key."
	},
	"incrby" => Command {
		function: cmd_incrby,
		syntax: "incrby KEY VALUE",
		validation: |r| {2 == r.parameters.len()},
		doc: "increment value stored at the key by the integer provided."
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
	"lindex" => Command {
		function: cmd_lindex,
		syntax: "lindex KEY INDEX",
		validation: |r| {2 == r.parameters.len()},
		doc: "get element at the index from the list stored at the key."
	},
	"linsert" => Command {
		function: cmd_linsert,
		syntax: "linsert KEY <BEFORE | AFTER> PIVOT ELEMENT",
		validation: |r| {4 == r.parameters.len()},
		doc: "insert element before or after the pivot in the list stored \
			at the key"
	},
	"llen" => Command {
		function: cmd_llen,
		syntax: "llen KEY",
		validation: |r| {1 == r.parameters.len()},
		doc: "get the length of the list stored at the key"
	},
	"lpop" => Command {
		function: cmd_lpop,
		syntax: "lpop KEY [ NUMBER ]",
		validation: |r| {0 < r.parameters.len() && 3 > r.parameters.len()},
		doc: "remove and return the values from the beginning of the list \
			stored at key"
	},
	"lpush" => Command {
		function: cmd_lpush,
		syntax: "lpush KEY VALUE [ VALUE ... ]",
		validation: |r| {1 < r.parameters.len()},
		doc: "insert all the specified values at the beginning of the list \
			stored at key"
	},
	"lpushx" => Command {
		function: cmd_lpushx,
		syntax: "lpushx KEY VALUE [ VALUE ... ]",
		validation: |r| {1 < r.parameters.len()},
		doc: "insert all the specified values at the beginning of the list \
			stored at key"
	},
	"lrange" => Command {
		function: cmd_lrange,
		syntax: "lrange KEY START STOP",
		validation: |r| {3 == r.parameters.len()},
		doc: "get elements from start to stop of the list stored at key"
	},
	"lrem" => Command {
		function: cmd_lrem,
		syntax: "lrem KEY COUNT ELEMENT",
		validation: |r| {3 == r.parameters.len()},
		doc: "remove the first count occurrences of element from the list \
			stored at key"
	},
	"lset" => Command {
		function: cmd_lset,
		syntax: "lset KEY INDEX ELEMENT",
		validation: |r| {3 == r.parameters.len()},
		doc: "set element at the index in the list stored at key"
	},
	"ltrim" => Command {
		function: cmd_ltrim,
		syntax: "ltrim KEY START STOP",
		validation: |r| {3 == r.parameters.len()},
		doc: "trim the list stored at key"
	},
	"mget" => Command {
		function: cmd_mget,
		syntax: "mget KEY [ KEY ... ]",
		validation: |r| {1 < r.parameters.len()},
		doc: "get values stored at specified keys."
	},
	"mset" => Command {
		function: cmd_mset,
		syntax: "mset KEY VALUE [ KEY VALUE ... ]",
		validation: |r| {1 < r.parameters.len()},
		doc: "store values with the specified keys."
	},
	"quit" => Command {
		function: cmd_quit,
		syntax: "quit",
		validation: |r| {0 == r.parameters.len()},
		doc: "close current connection and quit."
	},
	"rpop" => Command {
		function: cmd_rpop,
		syntax: "rpop KEY [ NUMBER ]",
		validation: |r| {0 < r.parameters.len() && 3 > r.parameters.len()},
		doc: "remove and return the values from the end of the list stored \
			at key"
	},
	"rpush" => Command {
		function: cmd_rpush,
		syntax: "rpush KEY VALUE [ VALUE ... ]",
		validation: |r| {1 < r.parameters.len()},
		doc: "append all the specified values at the end of the list \
			stored at key"
	},
	"rpushx" => Command {
		function: cmd_rpushx,
		syntax: "rpushx KEY VALUE [ VALUE ... ]",
		validation: |r| {1 < r.parameters.len()},
		doc: "append all the specified values at the end of the list \
			stored at key"
	},
	"sadd" => Command {
		function: cmd_sadd,
		syntax: "sadd KEY VALUE [ VALUE ... ]",
		validation: |r| {1 < r.parameters.len()},
		doc: "add specified values to the set stored at key"
	},
	"scard" => Command {
		function: cmd_scard,
		syntax: "scard KEY",
		validation: |r| {1 == r.parameters.len()},
		doc: "get the cardinality of the set stored at key"
	},
	"sdiff" => Command {
		function: cmd_sdiff,
		syntax: "sdiff KEY [ KEY ... ]",
		validation: |r| {0 < r.parameters.len()},
		doc: "get members that only exist in the set stored at the first \
			key"
	},
	"sdiffstore" => Command {
		function: cmd_sdiffstore,
		syntax: "sdiffstore DESTINATION KEY [ KEY ... ]",
		validation: |r| {1 < r.parameters.len()},
		doc: "get members that only exist in the set stored at the first \
			key and store them in a new set stored at the destination"
	},
	"set" => Command {
		function: cmd_set,
		syntax: "set KEY VALUE",
		validation: |r| {2 == r.parameters.len()},
		doc: "record the given key value pair."
	},
	"sismember" => Command {
		function: cmd_sismember,
		syntax: "sismember KEY VALUE",
		validation: |r| {2 == r.parameters.len()},
		doc: "return if the specified value is a member of the set stored \
			at key"
	},
	"smembers" => Command {
		function: cmd_smembers,
		syntax: "smembers KEY",
		validation: |r| {1 == r.parameters.len()},
		doc: "get all values in the set stored at key"
	},
	"smismember" => Command {
		function: cmd_smismember,
		syntax: "smismember KEY VALUE [ VALUE ... ]",
		validation: |r| {1 < r.parameters.len()},
		doc: "return if the specified values are members of the set stored \
			at key"
	},
	"smove" => Command {
		function: cmd_smove,
		syntax: "smove SOURCE DESTINATION VALUE",
		validation: |r| {3 == r.parameters.len()},
		doc: "move value from the set at source to the set at destination"
	},
	"sinter" => Command {
		function: cmd_sinter,
		syntax: "sinter KEY [ KEY ... ]",
		validation: |r| {0 < r.parameters.len()},
		doc: "get values that exist in all of the sets stored at the given \
			keys"
	},
	"sinterstore" => Command {
		function: cmd_sinterstore,
		syntax: "sinter DESTINATION KEY [ KEY ... ]",
		validation: |r| {1 < r.parameters.len()},
		doc: "get values that exist in all of the sets stored at the given \
			keys and store them in a new set stored at the destination"
	},
	"spop" => Command {
		function: cmd_spop,
		syntax: "spop KEY [ COUNT ]",
		validation: |r| {0 < r.parameters.len()},
		doc: "remove and return one or more random values from the set \
			stored at key"
	},
	"srandmember" => Command {
		function: cmd_srandmember,
		syntax: "srandmember KEY [ COUNT ]",
		validation: |r| {0 < r.parameters.len()},
		doc: "get a number of random members from the set stored at key"
	},
	"srem" => Command {
		function: cmd_srem,
		syntax: "srem KEY VALUE [ VALUE ... ]",
		validation: |r| {1 < r.parameters.len()},
		doc: "remove specified values from the set stored at key"
	},
	"sunion" => Command {
		function: cmd_sunion,
		syntax: "sunion KEY [ KEY ... ]",
		validation: |r| {0 < r.parameters.len()},
		doc: "get all unique values from all sets stored by the given keys"
	},
	"sunionstore" => Command {
		function: cmd_sunionstore,
		syntax: "sunionstore DESTINATION KEY [ KEY ... ]",
		validation: |r| {1 < r.parameters.len()},
		doc: "get all unique values from all sets stored by the given \
			keys and store them in a new set at destination"
	}
};

pub fn process<R: Read + Copy, W: Write>(r: R, w: W) {
	let mut reader: BufReader<R> = BufReader::new(r);
	let mut writer: BufWriter<W> = BufWriter::new(w);
	loop {
		if let Err(_) = writer.flush() {return;}
		match parse(&mut reader) {
			Ok(req) => {
				match CMDS.get(req.command.as_str()) {
					Some(cmd) => {
						if (cmd.validation)(&req) {
							if let Err(_) = write!(
									writer,
									"{}",
									match (cmd.function)(&req) {
										Ok(dt_v) => dt_v,
										Err(e) => DataType::err(&e.to_string())
									}
								) {
								return;
							}
							if "quit" == req.command.as_str() {
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
			},
			Err(e) => {
				if let Err(_) = write!(writer, "{}", DataType::err(e)) {
					eprintln!("Error: {:?}", e);
				}
				match e {
					"ERR EOF reached" | "ERR Connection error" => {return;},
					_ => {}
				}
			}
		}
	}
}

fn cmd_append(req: &Request) -> Result<DataType, &str> {
	kv::append(
		req.parameters.iter().nth(0).unwrap().as_str(),
		req.parameters.iter().nth(1).unwrap().as_str()
	)
}

fn cmd_client(_req: &Request) -> Result<DataType, &str> {
	// TODO:shinkou:2025-03-06:Implement client command
	Ok(DataType::str("OK"))
}

fn cmd_decr(req: &Request) -> Result<DataType, &str> {
	kv::decr(req.parameters.iter().nth(0).unwrap().as_str())
}

fn cmd_decrby(req: &Request) -> Result<DataType, &str> {
	kv::decrby(
		req.parameters.iter().nth(0).unwrap().as_str(),
		req.parameters.iter().nth(1).unwrap().as_str()
	)
}

fn cmd_del(req: &Request) -> Result<DataType, &str> {
	kv::del(&req.parameters)
}

fn cmd_get(req: &Request) -> Result<DataType, &str> {
	kv::get(req.parameters.iter().nth(0).unwrap())
}

fn cmd_getdel(req: &Request) -> Result<DataType, &str> {
	kv::getdel(req.parameters.iter().nth(0).unwrap())
}

fn cmd_getset(req: &Request) -> Result<DataType, &str> {
	kv::getset(
		req.parameters.iter().nth(0).unwrap().as_str(),
		req.parameters.iter().nth(1).unwrap().as_str()
	)
}

fn cmd_hdel(req: &Request) -> Result<DataType, &str> {
	kv::hdel(&req.parameters[0], req.parameters[1..].to_vec())
}

fn cmd_help(req: &Request) -> Result<DataType, &str> {
	if 1 == req.parameters.len() {
		let prm = req.parameters.iter().nth(0).unwrap().as_str();
		match CMDS.get(prm) {
			Some(cmd) => Ok(DataType::bulkStr(&format!(
					"Syntax:\n\t{}\n\nDescription:\n\t{}\n",
					cmd.syntax,
					cmd.doc
				))),
			None => Ok(DataType::err(&format!(
					"ERR unknown command \"{}\"",
					prm
				)))
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
		Ok(DataType::bulkStr(&ctx))
	}
}

fn cmd_hexists(req: &Request) -> Result<DataType, &str> {
	kv::hexists(
		req.parameters.iter().nth(0).unwrap().as_str(),
		req.parameters.iter().nth(1).unwrap().as_str()
	)
}

fn cmd_hget(req: &Request) -> Result<DataType, &str> {
	kv::hget(
		req.parameters.iter().nth(0).unwrap().as_str(),
		req.parameters.iter().nth(1).unwrap().as_str()
	)
}

fn cmd_hgetall(req: &Request) -> Result<DataType, &str> {
	kv::hgetall(req.parameters.iter().nth(0).unwrap())
}

fn cmd_hincrby(req: &Request) -> Result<DataType, &str> {
	kv::hincrby(
		req.parameters.iter().nth(0).unwrap().as_str(),
		req.parameters.iter().nth(1).unwrap().as_str(),
		req.parameters.iter().nth(2).unwrap().as_str()
	)
}

fn cmd_hkeys(req: &Request) -> Result<DataType, &str> {
	kv::hkeys(req.parameters.iter().nth(0).unwrap().as_str())
}

fn cmd_hlen(req: &Request) -> Result<DataType, &str> {
	kv::hlen(req.parameters.iter().nth(0).unwrap().as_str())
}

fn cmd_hmget(req: &Request) -> Result<DataType, &str> {
	kv::hmget(&req.parameters[0], req.parameters[1..].to_vec())
}

fn cmd_hset(req: &Request) -> Result<DataType, &str> {
	kv::hset(&req.parameters[0], req.parameters[1..].to_vec(), &false)
}

fn cmd_hsetnx(req: &Request) -> Result<DataType, &str> {
	kv::hset(&req.parameters[0], req.parameters[1..].to_vec(), &true)
}

fn cmd_hvals(req: &Request) -> Result<DataType, &str> {
	kv::hvals(req.parameters.iter().nth(0).unwrap().as_str())
}

fn cmd_incr(req: &Request) -> Result<DataType, &str> {
	kv::incr(req.parameters.iter().nth(0).unwrap().as_str())
}

fn cmd_incrby(req: &Request) -> Result<DataType, &str> {
	kv::incrby(
		req.parameters.iter().nth(0).unwrap().as_str(),
		req.parameters.iter().nth(1).unwrap().as_str()
	)
}

fn cmd_info(_req: &Request) -> Result<DataType, &str> {
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
	Ok(DataType::str(&ss))
}

fn cmd_keys(req: &Request) -> Result<DataType, &str> {
	kv::keys(req.parameters.iter().nth(0).unwrap().as_str())
}

fn cmd_lindex(req: &Request) -> Result<DataType, &str> {
	kv::lindex(
		req.parameters.iter().nth(0).unwrap(),
		req.parameters.iter().nth(1).unwrap()
	)
}

fn cmd_linsert(req: &Request) -> Result<DataType, &str> {
	kv::linsert(
		req.parameters.iter().nth(0).unwrap(),
		req.parameters.iter().nth(1).unwrap(),
		req.parameters.iter().nth(2).unwrap(),
		req.parameters.iter().nth(3).unwrap()
	)
}

fn cmd_llen(req: &Request) -> Result<DataType, &str> {
	kv::llen(req.parameters.iter().nth(0).unwrap().as_str())
}

fn cmd_lpop(req: &Request) -> Result<DataType, &str> {
	kv::lpop(
		req.parameters.iter().nth(0).unwrap().as_str(),
		if 1 < req.parameters.len() {
			req.parameters.iter().nth(1).unwrap().as_str()
		} else {
			"1"
		}
	)
}

fn cmd_lpush(req: &Request) -> Result<DataType, &str> {
	kv::lpush(&req.parameters[0], req.parameters[1..].to_vec(), false)
}

fn cmd_lpushx(req: &Request) -> Result<DataType, &str> {
	kv::lpush(&req.parameters[0], req.parameters[1..].to_vec(), true)
}

fn cmd_lrange(req: &Request) -> Result<DataType, &str> {
	kv::lrange(
		req.parameters.iter().nth(0).unwrap().as_str(),
		req.parameters.iter().nth(1).unwrap().as_str(),
		req.parameters.iter().nth(2).unwrap().as_str()
	)
}

fn cmd_lrem(req: &Request) -> Result<DataType, &str> {
	kv::lrem(
		req.parameters.iter().nth(0).unwrap().as_str(),
		req.parameters.iter().nth(1).unwrap().as_str(),
		req.parameters.iter().nth(2).unwrap().as_str()
	)
}

fn cmd_lset(req: &Request) -> Result<DataType, &str> {
	kv::lset(
		req.parameters.iter().nth(0).unwrap().as_str(),
		req.parameters.iter().nth(1).unwrap().as_str(),
		req.parameters.iter().nth(2).unwrap().as_str()
	)
}

fn cmd_ltrim(req: &Request) -> Result<DataType, &str> {
	kv::ltrim(
		req.parameters.iter().nth(0).unwrap().as_str(),
		req.parameters.iter().nth(1).unwrap().as_str(),
		req.parameters.iter().nth(2).unwrap().as_str()
	)
}

fn cmd_mget(req: &Request) -> Result<DataType, &str> {
	kv::mget(&req.parameters)
}

fn cmd_mset(req: &Request) -> Result<DataType, &str> {
	kv::mset(&req.parameters)
}

fn cmd_quit(_req: &Request) -> Result<DataType, &str> {
	Ok(DataType::str("OK"))
}

fn cmd_rpop(req: &Request) -> Result<DataType, &str> {
	kv::rpop(
		req.parameters.iter().nth(0).unwrap().as_str(),
		if 1 < req.parameters.len() {
			req.parameters.iter().nth(1).unwrap().as_str()
		} else {
			"1"
		}
	)
}

fn cmd_rpush(req: &Request) -> Result<DataType, &str> {
	kv::rpush(&req.parameters[0], req.parameters[1..].to_vec(), &false)
}

fn cmd_rpushx(req: &Request) -> Result<DataType, &str> {
	kv::rpush(&req.parameters[0], req.parameters[1..].to_vec(), &true)
}

fn cmd_sadd(req: &Request) -> Result<DataType, &str> {
	kv::sadd(&req.parameters[0], req.parameters[1..].to_vec())
}

fn cmd_scard(req: &Request) -> Result<DataType, &str> {
	kv::scard(req.parameters.iter().nth(0).unwrap().as_str())
}

fn cmd_sdiff(req: &Request) -> Result<DataType, &str> {
	kv::sdiff(&req.parameters[0], req.parameters[1..].to_vec())
}

fn cmd_sdiffstore(req: &Request) -> Result<DataType, &str> {
	kv::sdiffstore(
		req.parameters.iter().nth(0).unwrap().as_str(),
		req.parameters.iter().nth(1).unwrap().as_str(),
		req.parameters[2..].to_vec()
	)
}

fn cmd_set(req: &Request) -> Result<DataType, &str> {
	kv::set(
		req.parameters.iter().nth(0).unwrap().as_str(),
		req.parameters.iter().nth(1).unwrap().as_str()
	)
}

fn cmd_sismember(req: &Request) -> Result<DataType, &str> {
	kv::sismember(
		req.parameters.iter().nth(0).unwrap().as_str(),
		req.parameters.iter().nth(1).unwrap().as_str()
	)
}

fn cmd_smembers(req: &Request) -> Result<DataType, &str> {
	kv::smembers(req.parameters.iter().nth(0).unwrap().as_str())
}

fn cmd_smismember(req: &Request) -> Result<DataType, &str> {
	kv::smismember(&req.parameters[0], req.parameters[1..].to_vec())
}

fn cmd_smove(req: &Request) -> Result<DataType, &str> {
	kv::smove(
		req.parameters.iter().nth(0).unwrap().as_str(),
		req.parameters.iter().nth(1).unwrap().as_str(),
		req.parameters.iter().nth(2).unwrap().as_str()
	)
}

fn cmd_sinter(req: &Request) -> Result<DataType, &str> {
	kv::sinter(&req.parameters[0], req.parameters[1..].to_vec())
}

fn cmd_sinterstore(req: &Request) -> Result<DataType, &str> {
	kv::sinterstore(
		req.parameters.iter().nth(0).unwrap().as_str(),
		req.parameters.iter().nth(1).unwrap().as_str(),
		req.parameters[2..].to_vec()
	)
}

fn cmd_spop(req: &Request) -> Result<DataType, &str> {
	kv::spop(
		req.parameters.iter().nth(0).unwrap().as_str(),
		if 1 < req.parameters.len() {
			req.parameters.iter().nth(1).unwrap().as_str()
		} else {
			"1"
		},
		1 == req.parameters.len()
	)
}

fn cmd_srandmember(req: &Request) -> Result<DataType, &str> {
	kv::srandmember(
		req.parameters.iter().nth(0).unwrap().as_str(),
		if 1 < req.parameters.len() {
			req.parameters.iter().nth(1).unwrap().as_str()
		} else {
			"1"
		}
	)
}

fn cmd_srem(req: &Request) -> Result<DataType, &str> {
	kv::srem(&req.parameters[0], req.parameters[1..].to_vec())
}

fn cmd_sunion(req: &Request) -> Result<DataType, &str> {
	kv::sunion(req.parameters.clone())
}

fn cmd_sunionstore(req: &Request) -> Result<DataType, &str> {
	kv::sunionstore(&req.parameters[0], req.parameters[1..].to_vec())
}
