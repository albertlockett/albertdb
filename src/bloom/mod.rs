use fasthash;

pub struct BloomFilter {
    num_hashes: u8,
    size: u32,
    seed: u32,
    segments: Vec<u128>,
}

impl BloomFilter {
    pub fn new(size: u32, seed: u32, num_hashes: u8) -> Self {
        if size % 128 != 0 {
            panic!("size must be integer mutliple of 128")
        }

        return BloomFilter {
            num_hashes,
            seed,
            size,
            segments: vec![0; (size / 128) as usize],
        };
    }

    pub fn contains(&mut self, key: &[u8]) -> bool {
        let indices = self.indices(key);

        for i in indices {
            let (segment_num, segment_i) = self.segments_inices(i);
            let val = 1 << segment_i;
            if self.segments[segment_num] & val == 0 {
                return false
            }
        }
        return true;
    }

    pub fn insert(&mut self, key: &[u8]) {
        let indices = self.indices(key);

        for i in indices {
            let (segment_num, segment_i) = self.segments_inices(i);
            let val = 1 << segment_i;
            if self.segments[segment_num] & val == 0 {
                self.segments[segment_num] += val;
            }
        }
    }

    fn segments_inices(&self, i: usize) -> (usize, usize) {
        let segment_num = i / 128;
        let segment_i = i % 128;
        return (segment_num, segment_i);
    }

    fn indices(&self, key: &[u8]) -> Vec<usize> {
        let mut indices = vec![0; self.num_hashes as usize];
        for i in 0..self.num_hashes {
            let hash_murmur3 = fasthash::murmur3::hash32_with_seed(key, self.seed) as u64;
            let hash_city = fasthash::city::hash32_with_seed(key, self.seed) as u64;
            let hash_num = i as u64;

            // double hash to avoid collisions
            let hash = hash_murmur3 + hash_num * hash_city + hash_num * hash_num;
            indices[i as usize] = (hash % self.size as u64) as usize;
        }

        return indices;
    }
}

#[cfg(test)]
mod mod_tests {
    use super::*;

    #[test]
    #[should_panic]
    fn throws_if_size_not_divisible_by_128() {
        BloomFilter::new(127, 3, 3);
    }

    #[test]
    fn indices_smoke_test() {
        let bloom_filter = BloomFilter::new(256, 0, 3);
        let indices = bloom_filter.indices("abc".as_bytes());
        assert_eq!(3, indices.len());
        assert_ne!(indices[0], indices[1]);
        assert_ne!(indices[0], indices[2]);
        assert_ne!(indices[1], indices[2]);
    }

    #[test]
    fn insert_test() {
        let mut bloom_filter = BloomFilter::new(256, 0, 3);
        let indices = bloom_filter.indices("a".as_bytes());
        assert_eq!(3, indices.len());
        assert_eq!(178, indices[0]);
        assert_eq!(0, indices[1]);
        assert_eq!(80, indices[2]);
        bloom_filter.insert("a".as_bytes());

        assert_eq!((1 << 80) + (1 << 0), bloom_filter.segments[0]);
        assert_eq!(1 << (178 % 128), bloom_filter.segments[1]);
    }

    #[test]
    fn contains_test() {
        let mut bloom_filter = BloomFilter::new(256, 0, 3);
        bloom_filter.insert("a".as_bytes());
        // double check we won't get a false positive on "b" ...
        assert_ne!(bloom_filter.indices("a".as_bytes()), bloom_filter.indices("b".as_bytes()));
        assert_eq!(true, bloom_filter.contains("a".as_bytes()));
        assert_eq!(false, bloom_filter.contains("b".as_bytes()));
    }

}
