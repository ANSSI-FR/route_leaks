
// Guillaume Valadon <guillaume.valadon@ssi.gouv.fr>
//
// deroleru - Detect Route Leaks in Rust - deroleru::pre_computations() unit tests
//


extern crate deroleru;


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pre_computations_0() {

        let values = vec![0, 1, 2, 0, 1];

        let (local_maxes, variations, absolute_max) = deroleru::pre_computations(&values);
        println!("{:?}", local_maxes);
        assert!(local_maxes == [2]);
        println!("{:?}", variations);
        assert!(variations == vec![1, 1, 2, 1]);
        println!("{}", absolute_max);
        assert!(absolute_max == 2);
        assert!(local_maxes.is_empty() == false);
    }
}
