mod cli;
mod comm;

fn main() {
	match cli::do_args() {
		Ok((to_quit, bindaddr, thpoolsize)) => {
			if !to_quit {
				println!("Listening on \"{bindaddr}\"...");
				let _ = comm::listen_to(&bindaddr, thpoolsize);
			}
		},
		Err(e) => eprintln!("{e}")
	}
}
