extern crate getopts;

use getopts::Options;
use std::env;
use std::error::Error;

pub fn do_args() -> Result<(bool, String, usize), Box<dyn Error>> {
	let mut to_quit = false;
	let mut bindaddr = String::from("0.0.0.0:6379");
	let mut thpoolsize: usize = 64;
	let args: Vec<String> = env::args().collect();
	let progname = args[0].clone();

	let mut opts = Options::new();
	opts.optopt(
		"b", "bind",
		"bind address for inbound connections\n(default: \"0.0.0.0:6379\")",
		"ADDR"
	);
	opts.optopt("t", "thpool", "threadpool size (default: 64)", "SIZE");
	opts.optflag("h", "help", "print this help menu");

	match opts.parse(&args[1..]) {
		Ok(m) => {
			if m.opt_present("h") {
				print_usage(&progname, opts);
				to_quit = true;
			}

			if let Some(s) = m.opt_str("b") {
				bindaddr = s.to_string();
			}

			if let Some(s) = m.opt_str("t") {
				thpoolsize = usize::from_str_radix(&s, 10).unwrap();
			}

			Ok((to_quit, bindaddr, thpoolsize))
		},
		Err(e) => Err(Box::new(e))
	}
}

fn print_usage(progname: &str, opts: Options) {
	let msg = format!("Usage: {progname} [options]");
	print!("{}", opts.usage(&msg));
}
