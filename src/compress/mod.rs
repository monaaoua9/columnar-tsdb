pub mod gorilla;
pub mod timestamp;

#[cfg(test)]
mod tests {
    use super::{gorilla, timestamp};
    use crate::testutil::Rng;

    // corrupt blocks must come back as Err (or garbage Ok), never a panic or huge alloc
    #[test]
    fn decoders_survive_garbage() {
        let mut rng = Rng::new(2024);
        for round in 0..20_000 {
            let len = rng.below(64) as usize;
            let mut buf: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
            // half the time start from a plausible header so we get past the count check
            if round % 2 == 0 && buf.len() >= 4 {
                buf[..4].copy_from_slice(&(rng.below(40) as u32).to_be_bytes());
            }
            let _ = timestamp::decode(&buf, usize::MAX);
            let _ = gorilla::decode(&buf, usize::MAX);
        }
    }

    #[test]
    fn bit_flips_in_valid_blocks() {
        let ts: Vec<i64> = (0..200).map(|i| i * 1000 + (i % 7)).collect();
        let vs: Vec<f64> = (0..200).map(|i| (i as f64).sin()).collect();
        let (te, ve) = (timestamp::encode(&ts), gorilla::encode(&vs));
        for i in 0..te.len() * 8 {
            let mut b = te.clone();
            b[i / 8] ^= 1 << (i % 8);
            let _ = timestamp::decode(&b, usize::MAX);
        }
        for i in 0..ve.len() * 8 {
            let mut b = ve.clone();
            b[i / 8] ^= 1 << (i % 8);
            let _ = gorilla::decode(&b, usize::MAX);
        }
    }
}
