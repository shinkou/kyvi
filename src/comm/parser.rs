use super::Request;

const EMPTY_STRING: String = String::new();

pub fn parse(request: &str) -> Request {
	let mut prms = tokenize(request);
	let cmd = if 0 < prms.len() {prms.remove(0)} else {EMPTY_STRING};
	Request {command: cmd, parameters: prms}
}

fn tokenize(s: &str) -> Vec<String> {
	let mut tokens: Vec<String> = Vec::new();
	let mut quotechar: char = '\0';
	let mut is_escaped: bool = false;
	let mut token = String::new();
	for c in s.chars() {
		if c == '\\' {
			if is_escaped {
				token.push('\\');
			}
			is_escaped = !is_escaped;
		} else {
			if c == ' ' || c == '\n' || c == '\r' || c == '\t' {
				if is_escaped {
					token.push(c);
				} else if quotechar == '"' || quotechar == '\'' {
					token.push(c);
				} else {
					if 0 < token.len() {
						tokens.push(token);
						token = String::new();
					}
				}
			} else if c == '"' || c == '\'' {
				if is_escaped {
					token.push(c);
				} else if quotechar == '\0' {
					quotechar = c;
				} else if quotechar == c {
					quotechar = '\0';
				} else {
					token.push(c);
				}
			} else if is_escaped {
				if c == 'n' {
					token.push('\n');
				} else if c == 'r' {
					token.push('\r');
				} else if c == 't' {
					token.push('\t');
				} else {
					token.push('\\');
					token.push(c);
				}
			} else {
				token.push(c);
			}
			is_escaped = false;
		}
	}
	if 0 < token.len() {
		tokens.push(token);
	}
	tokens
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn backslashes() {
		let res = tokenize("  One\\ Two\\ Three  Four Five\\ \\ Six ");
		assert_eq!(res.len(), 3);
		assert_eq!(res[0], "One Two Three");
		assert_eq!(res[1], "Four");
		assert_eq!(res[2], "Five  Six");
	}

	#[test]
	fn doublequotes() {
		let res = tokenize(" \" Thomas' birthday \" is \"October 27\".");
		assert_eq!(res.len(), 3);
		assert_eq!(res[0], " Thomas' birthday ");
		assert_eq!(res[1], "is");
		assert_eq!(res[2], "October 27.");
	}

	#[test]
	fn singlequotes() {
		let res = tokenize(
			" 'Unlimited Refill' during weekdays '(from 10 a.m. to 2 p.m.)' only"
		);
		assert_eq!(res.len(), 5);
		assert_eq!(res[0], "Unlimited Refill");
		assert_eq!(res[1], "during");
		assert_eq!(res[2], "weekdays");
		assert_eq!(res[3], "(from 10 a.m. to 2 p.m.)");
		assert_eq!(res[4], "only");
	}

	#[test]
	fn doublequote() {
		let res = tokenize(
			" Some \"experts say, 'You've forgotten something.'"
		);
		assert_eq!(res.len(), 2);
		assert_eq!(res[0], "Some");
		assert_eq!(res[1], "experts say, 'You've forgotten something.'");
	}

	#[test]
	fn singlequote() {
		let res = tokenize(
			" They say Dennis' birthday is \"once in a bluemoon\"."
		);
		assert_eq!(res.len(), 3);
		assert_eq!(res[0], "They");
		assert_eq!(res[1], "say");
		assert_eq!(res[2], "Dennis birthday is \"once in a bluemoon\".");
	}

	#[test]
	fn escapables() {
		let res = tokenize(
			"col1\\tcol2\\tcol3\\nr1c1\\tr1c2\\tr1c3\\nr2c1\\tr2c2\\tr2c3\\nr3c1\\tr3c2\\tr3c3\\r"
		);
		assert_eq!(res.len(), 1);
		assert_eq!(
			res[0],
			"col1\tcol2\tcol3\nr1c1\tr1c2\tr1c3\nr2c1\tr2c2\tr2c3\nr3c1\tr3c2\tr3c3\r"
		);
	}

	#[test]
	fn non_escapables() {
		let res = tokenize(
			"\\e\\s\\1"
		);
		assert_eq!(res.len(), 1);
		assert_eq!(res[0], "\\e\\s\\1");
	}
}
