mod comm;

fn main() {
	println!("Starting listener...");
	let _ = comm::listen_to("0.0.0.0:6379", 64);
}
