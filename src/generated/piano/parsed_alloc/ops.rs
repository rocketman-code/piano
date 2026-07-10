#[allow(unused_imports)]
use crate::*;

pub fn take_alloc(rec: &crate::NdjsonAggregate) -> crate::ParsedAlloc {
    super::ParsedAlloc::new(
        rec.alloc_count(),
        rec.alloc_bytes(),
        rec.free_count(),
        rec.free_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn take_alloc_extracts_all_allocation_counters() {
        let agg = crate::NdjsonAggregate::new(
            0,
            1,
            2,
            crate::ParsedWall::new(30),
            crate::ParsedWall::new(50),
            crate::ParsedCpu::new(12),
            3,
            96,
            1,
            32,
            false,
        );

        let alloc = take_alloc(&agg);

        assert_eq!(alloc.alloc_count(), 3);
        assert_eq!(alloc.alloc_bytes(), 96);
        assert_eq!(alloc.free_count(), 1);
        assert_eq!(alloc.free_bytes(), 32);
    }
}
