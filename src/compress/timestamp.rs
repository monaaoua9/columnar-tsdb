//! Delta-of-delta timestamp codec (Gorilla, section 4.1.1).
//!
//! Block layout: `count:u32 | first_ts:64 bits | dod*` where each delta-of-delta
//! is prefix coded:
//!
//! ```text
//! 0                 dod == 0
//! 10   + 7 bits     dod in [-63, 64]
//! 110  + 9 bits     dod in [-255, 256]
//! 1110 + 12 bits    dod in [-2047, 2048]
//! 1111 + 64 bits    anything else (raw)
//! ```
//!
//! Payloads are stored with a bias so they're unsigned. Regular scrape
//! intervals collapse to a single 0 bit per point. The paper uses 32 bits for
//! the escape case; we use 64 so any i64 sequence round-trips (arithmetic is
//! wrapping on purpose).

use crate::bitio::{BitReader, BitWriter};
use crate::error::{Error, Result};

pub fn encode(ts: &[i64]) -> Vec<u8> {
    let mut w = BitWriter::with_capacity(ts.len() / 4 + 16);
    w.write_bits(ts.len() as u64, 32);
    let Some(&first) = ts.first() else {
        return w.finish();
    };
    w.write_bits(first as u64, 64);

    let mut prev = first;
    let mut prev_delta = 0i64;
    for &t in &ts[1..] {
        let delta = t.wrapping_sub(prev);
        write_dod(&mut w, delta.wrapping_sub(prev_delta));
        prev = t;
        prev_delta = delta;
    }
    w.finish()
}

fn write_dod(w: &mut BitWriter, dod: i64) {
    match dod {
        0 => w.write_bit(false),
        -63..=64 => {
            w.write_bits(0b10, 2);
            w.write_bits((dod + 63) as u64, 7);
        }
        -255..=256 => {
            w.write_bits(0b110, 3);
            w.write_bits((dod + 255) as u64, 9);
        }
        -2047..=2048 => {
            w.write_bits(0b1110, 4);
            w.write_bits((dod + 2047) as u64, 12);
        }
        _ => {
            w.write_bits(0b1111, 4);
            w.write_bits(dod as u64, 64);
        }
    }
}

// limit lets range queries stop early; pass usize::MAX for everything
pub fn decode(data: &[u8], limit: usize) -> Result<Vec<i64>> {
    let mut r = BitReader::new(data);
    let count = r.read_bits(32)? as usize;
    // every point costs at least one bit, so this also bounds the allocation
    if count > r.remaining() + 1 {
        return Err(Error::corrupt("timestamp count exceeds payload"));
    }
    let n = count.min(limit);
    let mut out = Vec::with_capacity(n);
    if n == 0 {
        return Ok(out);
    }

    let mut prev = r.read_bits(64)? as i64;
    out.push(prev);
    let mut prev_delta = 0i64;
    while out.len() < n {
        let dod = read_dod(&mut r)?;
        let delta = prev_delta.wrapping_add(dod);
        prev = prev.wrapping_add(delta);
        prev_delta = delta;
        out.push(prev);
    }
    Ok(out)
}

fn read_dod(r: &mut BitReader) -> Result<i64> {
    if !r.read_bit()? {
        return Ok(0);
    }
    if !r.read_bit()? {
        return Ok(r.read_bits(7)? as i64 - 63);
    }
    if !r.read_bit()? {
        return Ok(r.read_bits(9)? as i64 - 255);
    }
    if !r.read_bit()? {
        return Ok(r.read_bits(12)? as i64 - 2047);
    }
    Ok(r.read_bits(64)? as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::Rng;

    fn rt(ts: &[i64]) {
        let enc = encode(ts);
        assert_eq!(decode(&enc, usize::MAX).unwrap(), ts);
    }

    #[test]
    fn edge_shapes() {
        rt(&[]);
        rt(&[42]);
        rt(&[i64::MIN, i64::MAX]);
        rt(&[0, 1, 1_000_000_000_000, -5]);
    }

    #[test]
    fn regular_interval_is_about_one_bit_per_point() {
        let ts: Vec<i64> = (0..10_000)
            .map(|i| 1_700_000_000_000 + i * 10_000)
            .collect();
        let enc = encode(&ts);
        assert!(enc.len() < 10_000 / 8 + 32, "got {} bytes", enc.len());
        assert_eq!(decode(&enc, usize::MAX).unwrap(), ts);
    }

    #[test]
    fn jittery_and_random() {
        let mut rng = Rng::new(7);
        let mut t = 1_600_000_000_000i64;
        let mut ts = vec![];
        for _ in 0..5_000 {
            t += 1000 + rng.below(40) as i64 - 20;
            if rng.below(50) == 0 {
                t += rng.below(10_000_000) as i64;
            }
            ts.push(t);
        }
        rt(&ts);

        let wild: Vec<i64> = (0..2_000).map(|_| rng.next() as i64).collect();
        rt(&wild);
    }

    #[test]
    fn limit_and_truncation() {
        let ts: Vec<i64> = (0..100).map(|i| i * 7).collect();
        let enc = encode(&ts);
        assert_eq!(decode(&enc, 10).unwrap(), &ts[..10]);
        assert!(decode(&enc[..enc.len() / 2], usize::MAX).is_err());
    }
}
