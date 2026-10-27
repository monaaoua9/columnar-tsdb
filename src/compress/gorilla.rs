//! XOR float codec (Gorilla, section 4.1.2).
//!
//! Block layout: `count:u32 | first_value:64 bits | xor-coded values*`.
//!
//! Each value is XORed with its predecessor:
//!
//! ```text
//! 0                          identical to previous value
//! 1 0 + meaningful bits      xor fits in the previous (leading, trailing) window
//! 1 1 + lead:5 + len:6 + bits  new window; len==0 means 64
//! ```
//!
//! Slowly changing gauges and counters share most high bits (sign, exponent,
//! top mantissa) so the xor has long zero runs on both sides. Leading zeros are
//! capped at 31 because they only get 5 bits.

use crate::bitio::{BitReader, BitWriter};
use crate::error::{Error, Result};

pub fn encode(vals: &[f64]) -> Vec<u8> {
    let mut w = BitWriter::with_capacity(vals.len() * 2 + 16);
    w.write_bits(vals.len() as u64, 32);
    let Some(first) = vals.first() else {
        return w.finish();
    };
    let mut prev = first.to_bits();
    w.write_bits(prev, 64);

    // no window yet; first nonzero xor always opens one
    let mut lead = u32::MAX;
    let mut trail = 0u32;

    for v in &vals[1..] {
        let bits = v.to_bits();
        let x = bits ^ prev;
        prev = bits;

        if x == 0 {
            w.write_bit(false);
            continue;
        }
        w.write_bit(true);

        let l = x.leading_zeros().min(31);
        let t = x.trailing_zeros();
        if lead != u32::MAX && l >= lead && t >= trail {
            w.write_bit(false);
            w.write_bits(x >> trail, 64 - lead - trail);
        } else {
            let len = 64 - l - t;
            w.write_bit(true);
            w.write_bits(l as u64, 5);
            w.write_bits((len & 63) as u64, 6);
            w.write_bits(x >> t, len);
            lead = l;
            trail = t;
        }
    }
    w.finish()
}

pub fn decode(data: &[u8], limit: usize) -> Result<Vec<f64>> {
    let mut r = BitReader::new(data);
    let count = r.read_bits(32)? as usize;
    if count > r.remaining() + 1 {
        return Err(Error::corrupt("value count exceeds payload"));
    }
    let n = count.min(limit);
    let mut out = Vec::with_capacity(n);
    if n == 0 {
        return Ok(out);
    }

    let mut prev = r.read_bits(64)?;
    out.push(f64::from_bits(prev));
    let mut lead = 0u32;
    let mut trail = 0u32;
    let mut have_window = false;

    while out.len() < n {
        if r.read_bit()? {
            if r.read_bit()? {
                lead = r.read_bits(5)? as u32;
                let mut len = r.read_bits(6)? as u32;
                if len == 0 {
                    len = 64;
                }
                if lead + len > 64 {
                    return Err(Error::corrupt("xor window out of range"));
                }
                trail = 64 - lead - len;
                have_window = true;
            } else if !have_window {
                return Err(Error::corrupt("xor window reused before it was set"));
            }
            let len = 64 - lead - trail;
            prev ^= r.read_bits(len)? << trail;
        }
        out.push(f64::from_bits(prev));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::Rng;

    fn rt(vals: &[f64]) {
        let enc = encode(vals);
        let dec = decode(&enc, usize::MAX).unwrap();
        assert_eq!(dec.len(), vals.len());
        // compare bits so -0.0 / NaN payloads survive too
        for (a, b) in vals.iter().zip(&dec) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn edge_shapes() {
        rt(&[]);
        rt(&[2.75]);
        rt(&[
            0.0,
            -0.0,
            f64::MAX,
            f64::MIN_POSITIVE,
            f64::INFINITY,
            f64::NAN,
            1.0,
        ]);
        rt(&[1.0; 500]);
    }

    #[test]
    fn constant_series_is_one_bit_per_point() {
        let enc = encode(&vec![12.5; 8_000]);
        assert!(enc.len() < 8_000 / 8 + 16);
    }

    #[test]
    fn random_walks_and_noise() {
        let mut rng = Rng::new(99);
        let mut v = 100.0f64;
        let walk: Vec<f64> = (0..5_000)
            .map(|_| {
                v += (rng.below(2001) as f64 - 1000.0) / 100.0;
                v
            })
            .collect();
        rt(&walk);

        let noise: Vec<f64> = (0..3_000).map(|_| f64::from_bits(rng.next())).collect();
        rt(&noise);

        let ints: Vec<f64> = (0..3_000).map(|i| (i / 3) as f64).collect();
        rt(&ints);
    }

    #[test]
    fn limit_and_garbage() {
        let vals: Vec<f64> = (0..64).map(|i| i as f64 * 0.5).collect();
        let enc = encode(&vals);
        assert_eq!(decode(&enc, 5).unwrap(), &vals[..5]);
        assert!(decode(&enc[..enc.len() - 8], usize::MAX).is_err());
        assert!(decode(&[0xFF; 3], usize::MAX).is_err());
    }
}
