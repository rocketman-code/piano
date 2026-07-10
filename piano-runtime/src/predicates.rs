//! Predicate oracles for carve-generated invariants.

pub fn is_dual_of_header(arg0: &crate::SerializedLine) -> bool {
    if !is_self_contained(arg0) {
        return false;
    }
    let body = match arg0.value().strip_suffix('\n') {
        Some(body) => body,
        None => return false,
    };

    body.starts_with("{\"type\":\"trailer\",\"bias_ns\":")
        && body.contains(",\"cpu_bias_ns\":")
        && body.contains(",\"names\":{")
        && body.contains("},\"qualified\":{")
        && body.ends_with("}}")
        && !body.contains("\"run_id\"")
        && !body.contains("\"timestamp_ms\"")
}

pub fn is_le_inclusive(inclusive: &crate::WallNs, value: &crate::WallNs) -> bool {
    value.raw() <= inclusive.raw()
}

pub fn is_nonnegative_cpu_ns(arg0: &crate::CpuNs) -> bool {
    i128::from(arg0.raw()) >= 0
}

pub fn is_nonnegative_delta(arg0: &crate::AllocDelta) -> bool {
    i128::from(arg0.alloc_count()) >= 0
        && i128::from(arg0.alloc_bytes()) >= 0
        && i128::from(arg0.free_count()) >= 0
        && i128::from(arg0.free_bytes()) >= 0
}

pub fn is_nonnegative_wall_ns(arg0: &crate::WallNs) -> bool {
    i128::from(arg0.raw()) >= 0
}

pub fn is_self_contained(arg0: &crate::SerializedLine) -> bool {
    let body = match arg0.value().strip_suffix('\n') {
        Some(body) => body,
        None => return false,
    };
    if body.is_empty() || body.bytes().any(|b| b == b'\n' || b == b'\r') {
        return false;
    }

    let mut parser = JsonParser::new(body.as_bytes());
    parser.skip_ws();
    if parser.peek() != Some(b'{') {
        return false;
    }
    parser.parse_value() && {
        parser.skip_ws();
        parser.is_eof()
    }
}

struct JsonParser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> JsonParser<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn is_eof(&self) -> bool {
        self.pos == self.bytes.len()
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn next(&mut self) -> Option<u8> {
        let byte = self.peek()?;
        self.pos += 1;
        Some(byte)
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t')) {
            self.pos += 1;
        }
    }

    fn consume(&mut self, expected: u8) -> bool {
        if self.peek() == Some(expected) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn parse_value(&mut self) -> bool {
        self.skip_ws();
        match self.peek() {
            Some(b'{') => self.parse_object(),
            Some(b'[') => self.parse_array(),
            Some(b'"') => self.parse_string(),
            Some(b'-' | b'0'..=b'9') => self.parse_number(),
            Some(b't') => self.consume_bytes(b"true"),
            Some(b'f') => self.consume_bytes(b"false"),
            Some(b'n') => self.consume_bytes(b"null"),
            _ => false,
        }
    }

    fn parse_object(&mut self) -> bool {
        if !self.consume(b'{') {
            return false;
        }
        self.skip_ws();
        if self.consume(b'}') {
            return true;
        }

        loop {
            self.skip_ws();
            if !self.parse_string() {
                return false;
            }
            self.skip_ws();
            if !self.consume(b':') {
                return false;
            }
            if !self.parse_value() {
                return false;
            }
            self.skip_ws();
            if self.consume(b'}') {
                return true;
            }
            if !self.consume(b',') {
                return false;
            }
        }
    }

    fn parse_array(&mut self) -> bool {
        if !self.consume(b'[') {
            return false;
        }
        self.skip_ws();
        if self.consume(b']') {
            return true;
        }

        loop {
            if !self.parse_value() {
                return false;
            }
            self.skip_ws();
            if self.consume(b']') {
                return true;
            }
            if !self.consume(b',') {
                return false;
            }
        }
    }

    fn parse_string(&mut self) -> bool {
        if !self.consume(b'"') {
            return false;
        }
        while let Some(byte) = self.next() {
            match byte {
                b'"' => return true,
                b'\\' => {
                    if !self.parse_escape() {
                        return false;
                    }
                }
                0x00..=0x1f => return false,
                _ => {}
            }
        }
        false
    }

    fn parse_escape(&mut self) -> bool {
        match self.next() {
            Some(b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't') => true,
            Some(b'u') => {
                for _ in 0..4 {
                    if !matches!(self.next(), Some(b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F')) {
                        return false;
                    }
                }
                true
            }
            _ => false,
        }
    }

    fn parse_number(&mut self) -> bool {
        let start = self.pos;
        let _ = self.consume(b'-');

        match self.peek() {
            Some(b'0') => {
                self.pos += 1;
            }
            Some(b'1'..=b'9') => {
                self.pos += 1;
                while matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.pos += 1;
                }
            }
            _ => return false,
        }

        if self.consume(b'.') {
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return false;
            }
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
        }

        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.pos += 1;
            let _ = self.consume(b'+') || self.consume(b'-');
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return false;
            }
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
        }

        self.pos > start
    }

    fn consume_bytes(&mut self, expected: &[u8]) -> bool {
        if self.bytes[self.pos..].starts_with(expected) {
            self.pos += expected.len();
            true
        } else {
            false
        }
    }
}
