use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Request {
	pub command: String,
	pub parameters: Vec<String>
}
