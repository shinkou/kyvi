mod cli;
mod comm;

fn main() {
	match cli::do_args() {
		Ok((to_quit, bindaddr, thpoolsize)) => {
			if !to_quit {
				println!("Listening on \"{bindaddr}\"...");
				if let Err(e) = comm::listen_to(&bindaddr, thpoolsize) {
					eprintln!("{:?}", e.to_string());
				}
			}
		},
		Err(e) => eprintln!("{e}")
	}
}
