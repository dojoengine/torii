use std::{
    cmp::Ordering,
    ops::{Add, AddAssign, Sub, SubAssign},
};

use starknet_core::types::U256;

#[derive(Debug, Clone, Copy)]
pub struct I256 {
    pub value: U256,
    pub is_negative: bool,
}

impl Default for I256 {
    fn default() -> Self {
        Self {
            value: U256::from(0u8),
            is_negative: false,
        }
    }
}

impl From<U256> for I256 {
    fn from(value: U256) -> Self {
        Self {
            value,
            is_negative: false,
        }
    }
}

impl From<u8> for I256 {
    fn from(value: u8) -> Self {
        Self {
            value: U256::from(value),
            is_negative: false,
        }
    }
}

impl Add for I256 {
    type Output = I256;

    fn add(self, other: I256) -> I256 {
        // Special case: if both are negative zero, return positive zero
        if self.value == U256::from(0u8)
            && other.value == U256::from(0u8)
            && self.is_negative
            && other.is_negative
        {
            return I256 {
                value: U256::from(0u8),
                is_negative: false,
            };
        }

        if self.is_negative == other.is_negative {
            // Same sign: add the values and keep the sign
            I256 {
                value: self.value + other.value,
                is_negative: self.is_negative,
            }
        } else {
            // Different signs: subtract the smaller value from the larger one
            match self.value.cmp(&other.value) {
                Ordering::Greater => I256 {
                    value: self.value - other.value,
                    is_negative: self.is_negative,
                },
                Ordering::Less => I256 {
                    value: other.value - self.value,
                    is_negative: other.is_negative,
                },
                // If both values are equal, the result is zero and not negative
                Ordering::Equal => I256 {
                    value: U256::from(0u8),
                    is_negative: false,
                },
            }
        }
    }
}

impl Sub for I256 {
    type Output = I256;

    fn sub(self, other: I256) -> I256 {
        let new_sign = if other.value == U256::from(0u8) {
            false
        } else {
            !other.is_negative
        };
        let negated_other = I256 {
            value: other.value,
            is_negative: new_sign,
        };
        self.add(negated_other)
    }
}

impl AddAssign for I256 {
    fn add_assign(&mut self, other: I256) {
        *self = *self + other;
    }
}

impl SubAssign for I256 {
    fn sub_assign(&mut self, other: I256) {
        *self = *self - other;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_zero_false_and_zero_false() {
        // 0,false + 0,false == 0,false
        let a = I256::default();
        let b = I256::default();
        let result = a + b;
        assert_eq!(result.value, U256::from(0u8));
        assert!(!result.is_negative);
    }

    #[test]
    fn test_add_zero_true_and_zero_false() {
        // 0,true + 0,false == 0,false
        let a = I256 {
            value: U256::from(0u8),
            is_negative: true,
        };
        let b = I256::default();
        let result = a + b;
        assert_eq!(result.value, U256::from(0u8));
        assert!(!result.is_negative);
    }

    #[test]
    fn test_sub_zero_false_and_zero_false() {
        // 0,false - 0,false == 0,false
        let a = I256::default();
        let b = I256::default();
        let result = a - b;
        assert_eq!(result.value, U256::from(0u8));
        assert!(!result.is_negative);
    }

    #[test]
    fn test_sub_zero_true_and_zero_false() {
        // 0,true - 0,false == 0,false
        let a = I256 {
            value: U256::from(0u8),
            is_negative: true,
        };
        let b = I256::default();
        let result = a - b;
        assert_eq!(result.value, U256::from(0u8));
        assert!(!result.is_negative);
    }

    #[test]
    fn test_add_positive_and_negative_equal_values() {
        // 5,false + 5,true == 0,false
        let a = I256::from(U256::from(5u8));
        let b = I256 {
            value: U256::from(5u8),
            is_negative: true,
        };
        let result = a + b;
        assert_eq!(result.value, U256::from(0u8));
        assert!(!result.is_negative);
    }

    #[test]
    fn test_sub_positive_and_negative() {
        // 10,false - 5,true == 15,false
        let a = I256::from(U256::from(10u8));
        let b = I256 {
            value: U256::from(5u8),
            is_negative: true,
        };
        let result = a - b;
        assert_eq!(result.value, U256::from(15u8));
        assert!(!result.is_negative);
    }

    #[test]
    fn test_sub_larger_from_smaller() {
        // 5,false - 10,true == 15,true
        let a = I256::from(U256::from(5u8));
        let b = I256 {
            value: U256::from(10u8),
            is_negative: true,
        };
        let result = a - b;
        assert_eq!(result.value, U256::from(15u8));
        assert!(!result.is_negative);
    }

    #[test]
    fn test_add_mixed_signs() {
        // 15,false + 10,true == 5,false
        let a = I256::from(U256::from(15u8));
        let b = I256 {
            value: U256::from(10u8),
            is_negative: true,
        };
        let result = a + b;
        assert_eq!(result.value, U256::from(5u8));
        assert!(!result.is_negative);
    }

    #[test]
    fn test_sub_mixed_signs() {
        // 5,false - 10,true == 15,false
        let a = I256::from(U256::from(5u8));
        let b = I256 {
            value: U256::from(10u8),
            is_negative: true,
        };
        let result = a - b;
        assert_eq!(result.value, U256::from(15u8));
        assert!(!result.is_negative);
    }

    #[test]
    fn test_add_negative_and_negative() {
        // -5,true + -3,true == -8,true
        let a = I256 {
            value: U256::from(5u8),
            is_negative: true,
        };
        let b = I256 {
            value: U256::from(3u8),
            is_negative: true,
        };
        let result = a + b;
        assert_eq!(result.value, U256::from(8u8));
        assert!(result.is_negative);
    }

    #[test]
    fn test_sub_negative_and_negative() {
        // -5,true - -3,true == -2,true
        let a = I256 {
            value: U256::from(5u8),
            is_negative: true,
        };
        let b = I256 {
            value: U256::from(3u8),
            is_negative: true,
        };
        let result = a - b;
        assert_eq!(result.value, U256::from(2u8));
        assert!(result.is_negative);
    }

    #[test]
    fn test_subtraction_resulting_zero() {
        // 5,false - 5,false == 0,false
        let a = I256::from(U256::from(5u8));
        let b = I256::from(U256::from(5u8));
        let result = a - b;
        assert_eq!(result.value, U256::from(0u8));
        assert!(!result.is_negative);
    }

    #[test]
    fn test_subtraction_resulting_zero_negative() {
        // 5,true - 5,true == 0,false
        let a = I256 {
            value: U256::from(5u8),
            is_negative: true,
        };
        let b = I256 {
            value: U256::from(5u8),
            is_negative: true,
        };
        let result = a - b;
        assert_eq!(result.value, U256::from(0u8));
        assert!(!result.is_negative);
    }

    #[test]
    fn test_add_negative_and_positive_result_positive() {
        // -10,true + 15,false == 5,false
        let a = I256 {
            value: U256::from(10u8),
            is_negative: true,
        };
        let b = I256::from(U256::from(15u8));
        let result = a + b;
        assert_eq!(result.value, U256::from(5u8));
        assert!(!result.is_negative);
    }

    #[test]
    fn test_add_negative_and_positive_result_negative() {
        // -15,true + 5,false == -10,true
        let a = I256 {
            value: U256::from(15u8),
            is_negative: true,
        };
        let b = I256::from(U256::from(5u8));
        let result = a + b;
        assert_eq!(result.value, U256::from(10u8));
        assert!(result.is_negative);
    }

    #[test]
    fn test_add_zero_true_and_fifteen_true() {
        // 0,true + 15,true == 15,true
        let a = I256 {
            value: U256::from(0u8),
            is_negative: true,
        };
        let b = I256 {
            value: U256::from(15u8),
            is_negative: true,
        };
        let result = a + b;
        assert_eq!(result.value, U256::from(15u8));
        assert!(result.is_negative);
    }

    #[test]
    fn test_sub_zero_true_and_fifteen_true() {
        // 0,true - 15,true == 15,false
        let a = I256 {
            value: U256::from(0u8),
            is_negative: true,
        };
        let b = I256 {
            value: U256::from(15u8),
            is_negative: true,
        };
        let result = a - b;
        assert_eq!(result.value, U256::from(15u8));
        assert!(!result.is_negative);
    }

    #[test]
    fn test_add_fifteen_true_and_zero_true() {
        // 15,true + 0,true == 15,true
        let a = I256 {
            value: U256::from(15u8),
            is_negative: true,
        };
        let b = I256 {
            value: U256::from(0u8),
            is_negative: true,
        };
        let result = a + b;
        assert_eq!(result.value, U256::from(15u8));
        assert!(result.is_negative);
    }

    #[test]
    fn test_sub_fifteen_true_and_zero_true() {
        // 15,true - 0,true == 15,true
        let a = I256 {
            value: U256::from(15u8),
            is_negative: true,
        };
        let b = I256 {
            value: U256::from(0u8),
            is_negative: true,
        };
        let result = a - b;
        assert_eq!(result.value, U256::from(15u8));
        assert!(result.is_negative);
    }

    #[test]
    fn test_negative_zero() {
        // 0,true + 0,true == 0,false
        let a = I256 {
            value: U256::from(0u8),
            is_negative: true,
        };
        let b = I256 {
            value: U256::from(0u8),
            is_negative: true,
        };
        let result = a + b;
        assert_eq!(result.value, U256::from(0u8));
        assert!(!result.is_negative);
    }

    #[test]
    fn test_sub_positive_and_negative_zero() {
        // 15,false - 0,true == 15,false
        let a = I256::from(U256::from(15u8));
        let b = I256 {
            value: U256::from(0u8),
            is_negative: true,
        };
        let result = a - b;
        assert_eq!(result.value, U256::from(15u8));
        assert!(!result.is_negative);
    }

    #[test]
    fn test_add_positive_and_negative_zero() {
        // 15,false + 0,true == 15,false
        let a = I256::from(U256::from(15u8));
        let b = I256 {
            value: U256::from(0u8),
            is_negative: true,
        };
        let result = a + b;
        assert_eq!(result.value, U256::from(15u8));
        assert!(!result.is_negative);
    }
}
