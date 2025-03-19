mod cli;
mod comm;
mod command;
mod datatype;
mod kv;
mod request;
mod parser;

use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::flag::register;
use std::fs::File;
use std::process::exit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use threadpool::ThreadPool;

fn main() {
	match cli::do_args() {
		Ok((to_quit, datafilepath, bindaddr, thpoolsize)) => {
			let is_stopped = Arc::new(AtomicBool::new(false));
			for sig in vec![SIGINT, SIGTERM] {
				if let Err(e) = register(sig, Arc::clone(&is_stopped)) {
					eprintln!("{:?}", e.to_string());
					return;
				}
			}
			let pool = ThreadPool::new(thpoolsize);
			if !to_quit {
				if 0 < datafilepath.len() {
					// this Vec is used to discard output from the parser
					let mut buf: Vec<u8> = Vec::new();
					match File::open(datafilepath.clone()) {
						Ok(f) => command::process(&f, &mut buf),
						Err(e) => eprintln!("{:?}", e.to_string())
					}
					// uncomment the 3 next lines for debugging as needed:
					// String::from_utf8(buf).unwrap().lines()
					// 	.filter(|s| {s.starts_with("-")})
					// 	.for_each(|s| {println!("{}", s);});
				}

				thread::spawn(move || {
					while !is_stopped.load(Ordering::Relaxed) {
						thread::sleep(Duration::from_secs(2));
					}
					exit(
						if 0 < datafilepath.len() {
							match File::options().create(true).write(true)
								.open(datafilepath.clone()) {
								Ok(mut f) => {
									if let Err(e) = kv::write_data(&mut f) {
										eprintln!("{:?}", e);
									};
									0i32
								},
								Err(e) => {
									eprintln!("{:?}", e.to_string());
									1i32
								}
							}
						} else {
							0i32
						}
					);
				});
				println!("Listening on \"{bindaddr}\"...");
				if let Err(e) = comm::listen_to(&pool, &bindaddr) {
					eprintln!("{}", e.to_string());
				}
			}
		},
		Err(e) => eprintln!("{e}")
	}
}
