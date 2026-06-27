pub struct Bloom {
    bits: Vec<u8>,
    k: u32,
}

impl Bloom {
    pub fn new(num_keys: usize, bits_per_key: usize) -> Self {
        let m_bits = (num_keys * bits_per_key).max(64);
        let bytes = m_bits.div_ceil(8);
        let k = ((bits_per_key * 69 / 100).max(1)) as u32;
        Bloom {
            bits: vec![0u8; bytes],
            k,
        }
    }

    pub fn add(&mut self, key: &[u8]) {
        let (h1, h2) = hashes(key);
        let m = (self.bits.len() * 8) as u64;
        for i in 0..self.k as u64 {
            let bit = (h1.wrapping_add(i.wrapping_mul(h2)) % m) as usize;
            self.bits[bit / 8] |= 1 << (bit % 8);
        }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        let (h1, h2) = hashes(key);
        let m = (self.bits.len() * 8) as u64;
        for i in 0..self.k as u64 {
            let bit = (h1.wrapping_add(i.wrapping_mul(h2)) % m) as usize;
            if self.bits[bit / 8] & (1 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.bits.len() + 8);
        buf.extend_from_slice(&self.k.to_le_bytes());
        buf.extend_from_slice(&(self.bits.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.bits);
        buf
    }

    pub fn decode(bytes: &[u8]) -> Bloom {
        let k = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
        let bits = bytes[8..8 + len].to_vec();
        Bloom { bits, k }
    }
}

fn hashes(key: &[u8]) -> (u64, u64) {
    let h = fnv1a(key);
    (h, h.rotate_left(32) | 1)
}

fn fnv1a(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in data {
        h ^= byte as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}
