mod cli;
mod comm;
mod datatype;
mod kv;
mod request;
mod parser;

use std::fs::File;

fn main() {
	match cli::do_args() {
		Ok((to_quit, datafilepath, bindaddr, thpoolsize)) => {
			if !to_quit {
				if 0 < datafilepath.len() {
					// this Vec is used to discard output from the parser
					let mut buf: Vec<u8> = Vec::new();
					match File::open(datafilepath) {
						Ok(f) => parser::process(&f, &mut buf),
						Err(e) => {
							eprintln!("{:?}", e.to_string());
							return;
						}
					}
					// uncomment the 3 next lines for debugging as needed:
					// String::from_utf8(buf).unwrap().lines()
					// 	.filter(|s| {s.starts_with("-")})
					// 	.for_each(|s| {println!("{}", s);});
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
