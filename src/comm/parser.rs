use super::Request;

const EMPTY_STRING: String = String::new();

pub fn parse(request: &str) -> Result<Request, &str> {
    let mut prms = get_parameters(request)?;
    let cmd = if 0 < prms.len() {
        prms.remove(0).to_ascii_lowercase()
    } else {
        EMPTY_STRING
    };
    Ok(Request {command: cmd, parameters: prms})
}

fn get_parameters(s: &str) -> Result<Vec<String>, &str> {
    let mut parameters: Vec<String> = Vec::new();
    let mut llen: usize = 0;
    let mut slen: usize = 0;
    let mut lcnt: usize = 0;
    let mut sbuf: String = String::new();
    for line in s.lines() {
        if 0 < slen {
            if sbuf.len() < slen {
                sbuf.push_str(line);
            } else if sbuf.len() == slen {
                parameters.push(sbuf.clone());
                sbuf.clear();
                slen = 0;
                lcnt += 1;
            } else {
                return Err("ERR Contents unmatch string length");
            };
        };
        if 0 == llen && '*' == line.chars().nth(0).unwrap_or('\0') {
            match line[1..].parse::<usize>() {
                Ok(n) => llen = n,
                Err(_) => return Err("ERR Invalid list length")
            }
        };
        if 0 == slen && '$' == line.chars().nth(0).unwrap_or('\0') {
            match line[1..].parse::<usize>() {
                Ok(n) => slen = n,
                Err(_) => return Err("ERR Invalid string length")
            }
        };
    };
    if slen != 0 || sbuf.len() != 0 {
        Err("ERR Contents unmatch string length")
    } else if lcnt != llen {
        Err("ERR Contents unmatch list length")
    } else {
        Ok(parameters)
    }
}