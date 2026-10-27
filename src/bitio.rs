use crate::error::{Error, Result};

// msb-first bit packing, same convention as the Gorilla paper's figures
pub struct BitWriter {
    buf: Vec<u8>,
    cur: u8,
    used: u32,
}

impl BitWriter {
    pub fn with_capacity(bytes: usize) -> Self {
        BitWriter {
            buf: Vec::with_capacity(bytes),
            cur: 0,
            used: 0,
        }
    }

    pub fn write_bit(&mut self, b: bool) {
        self.cur |= (b as u8) << (7 - self.used);
        self.used += 1;
        if self.used == 8 {
            self.buf.push(self.cur);
            self.cur = 0;
            self.used = 0;
        }
    }

    pub fn write_bits(&mut self, v: u64, mut n: u32) {
        debug_assert!(n <= 64);
        while n > 0 {
            let free = 8 - self.used;
            let take = free.min(n);
            let part = ((v >> (n - take)) & ((1u64 << take) - 1)) as u8;
            self.cur |= part << (free - take);
            self.used += take;
            n -= take;
            if self.used == 8 {
                self.buf.push(self.cur);
                self.cur = 0;
                self.used = 0;
            }
        }
    }

    pub fn finish(mut self) -> Vec<u8> {
        if self.used > 0 {
            self.buf.push(self.cur);
        }
        self.buf
    }
}

pub struct BitReader<'a> {
    data: &'a [u8],
    pos: usize,
}

fn truncated() -> Error {
    Error::corrupt("bitstream truncated")
}

impl<'a> BitReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        BitReader { data, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.data.len() * 8 - self.pos
    }

    pub fn read_bit(&mut self) -> Result<bool> {
        let byte = *self.data.get(self.pos >> 3).ok_or_else(truncated)?;
        let bit = (byte >> (7 - (self.pos & 7))) & 1;
        self.pos += 1;
        Ok(bit == 1)
    }

    pub fn read_bits(&mut self, mut n: u32) -> Result<u64> {
        if n as usize > self.remaining() {
            return Err(truncated());
        }
        let mut out = 0u64;
        while n > 0 {
            let byte = self.data[self.pos >> 3];
            let avail = 8 - (self.pos & 7) as u32;
            let take = avail.min(n);
            let part = ((byte >> (avail - take)) as u64) & ((1u64 << take) - 1);
            out = (out << take) | part;
            self.pos += take as usize;
            n -= take;
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_mixed_widths() {
        let mut w = BitWriter::with_capacity(16);
        w.write_bit(true);
        w.write_bits(0b101, 3);
        w.write_bits(u64::MAX, 64);
        w.write_bits(0x1234, 13);
        w.write_bit(false);
        let bytes = w.finish();

        let mut r = BitReader::new(&bytes);
        assert!(r.read_bit().unwrap());
        assert_eq!(r.read_bits(3).unwrap(), 0b101);
        assert_eq!(r.read_bits(64).unwrap(), u64::MAX);
        assert_eq!(r.read_bits(13).unwrap(), 0x1234);
        assert!(!r.read_bit().unwrap());
    }

    #[test]
    fn read_past_end_fails() {
        let mut r = BitReader::new(&[0xFF]);
        assert!(r.read_bits(9).is_err());
        assert_eq!(r.read_bits(8).unwrap(), 0xFF);
        assert!(r.read_bit().is_err());
    }
}
