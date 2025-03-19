use std::net::{TcpListener, TcpStream};
use threadpool::ThreadPool;

use super::command::process;

pub fn listen_to(pool: &ThreadPool, bindaddr: &str) -> std::io::Result<()> {
	let listener: TcpListener = TcpListener::bind(bindaddr)?;
	for stream in listener.incoming() {
		match stream {
			Ok(stream) => pool.execute(move || {handle_client(stream);}),
			Err(e) => eprintln!("Unhandled error: {:?}", e)
		}
	}
	Ok(())
}

fn handle_client(stream: TcpStream) {
	println!("Accepted connection from: {}", stream.peer_addr().unwrap());
	process(&stream, &stream);
}
