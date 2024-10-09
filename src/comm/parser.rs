use super::Request;

pub fn parse(request: &str) -> Request {
	if let Some((cmd, prms)) = request.trim().split_once(" ") {
		let command = String::from(cmd);
		let parameters = tokenize(prms);
		Request {command, parameters}
	} else {
		let command = request.trim().to_string();
		let parameters = Vec::new();
		Request {command, parameters}
	}
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
		} else if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			if is_escaped {
				token.push(c);
				is_escaped = false;
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
				is_escaped = false;
			} else if quotechar == '\0' {
				quotechar = c;
			} else if quotechar == c {
				quotechar = '\0';
			} else {
				token.push(c);
			}
		} else {
			token.push(c);
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
}
