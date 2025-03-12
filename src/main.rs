mod cli;
mod comm;
mod datatype;
mod kv;
mod request;
mod parser;

use std::fs::File;
use std::io::stdout;

fn main() {
	match cli::do_args() {
		Ok((to_quit, datafilepath, bindaddr, thpoolsize)) => {
			if !to_quit {
				if 0 < datafilepath.len() {
					match File::open(datafilepath) {
						Ok(f) => parser::process(&f, &stdout()),
						Err(e) => {
							eprintln!("{:?}", e.to_string());
							return;
						}
					}
				}
				println!("Listening on \"{bindaddr}\"...");
				if let Err(e) = comm::listen_to(&bindaddr, thpoolsize) {
					eprintln!("{:?}", e.to_string());
				}
			}
		},
		Err(e) => eprintln!("{e}")
	}
}
