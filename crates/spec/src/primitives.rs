use std::{
    fmt,
    ops::{Add, Sub},
};

// ---------------------------------------------------------------------------
// Byte-array newtypes
// ---------------------------------------------------------------------------

macro_rules! byte_newtype {
    ($name:ident, $size:expr) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
        pub struct $name(pub [u8; $size]);

        impl $name {
            #[must_use]
            pub const fn new(inner: [u8; $size]) -> Self {
                Self(inner)
            }

            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; $size] {
                &self.0
            }
        }

        impl From<[u8; $size]> for $name {
            fn from(v: [u8; $size]) -> Self {
                Self(v)
            }
        }

        impl From<$name> for [u8; $size] {
            fn from(v: $name) -> Self {
                v.0
            }
        }

        impl AsRef<[u8]> for $name {
            fn as_ref(&self) -> &[u8] {
                &self.0
            }
        }
    };
}

byte_newtype!(EthAddress, 20);
byte_newtype!(TxHash, 32);
byte_newtype!(SuperblockHash, 32);
byte_newtype!(BlockHash, 32);
byte_newtype!(StateRoot, 32);
byte_newtype!(InstanceId, 32);

impl fmt::Display for InstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Display for EthAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("0x")?;
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Numeric newtypes
// ---------------------------------------------------------------------------

macro_rules! numeric_newtype {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
        pub struct $name(pub u64);

        impl $name {
            #[must_use]
            pub const fn new(v: u64) -> Self {
                Self(v)
            }

            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl From<u64> for $name {
            fn from(v: u64) -> Self {
                Self(v)
            }
        }

        impl From<$name> for u64 {
            fn from(v: $name) -> Self {
                v.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl Add<u64> for $name {
            type Output = Self;
            fn add(self, rhs: u64) -> Self {
                Self(self.0 + rhs)
            }
        }

        impl Sub<u64> for $name {
            type Output = Self;
            fn sub(self, rhs: u64) -> Self {
                Self(self.0 - rhs)
            }
        }

        impl Add for $name {
            type Output = Self;
            fn add(self, rhs: Self) -> Self {
                Self(self.0 + rhs.0)
            }
        }

        impl Sub for $name {
            type Output = Self;
            fn sub(self, rhs: Self) -> Self {
                Self(self.0 - rhs.0)
            }
        }
    };
}

numeric_newtype!(ChainId);
numeric_newtype!(SessionId);
numeric_newtype!(PeriodId);
numeric_newtype!(SequenceNumber);
numeric_newtype!(SuperblockNumber);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_id_display() {
        let id = InstanceId([
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c,
            0x1d, 0x1e, 0x1f, 0x20,
        ]);
        assert_eq!(
            id.to_string(),
            "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
        );
    }

    #[test]
    fn eth_address_display() {
        let addr = EthAddress([1; 20]);
        assert!(addr.to_string().starts_with("0x"));
        assert_eq!(addr.to_string().len(), 42);
    }

    #[test]
    fn numeric_newtype_arithmetic() {
        let a = SuperblockNumber(10);
        let b = SuperblockNumber(3);
        assert_eq!(a + b, SuperblockNumber(13));
        assert_eq!(a - b, SuperblockNumber(7));
        assert_eq!(a + 5, SuperblockNumber(15));
        assert_eq!(a - 2, SuperblockNumber(8));
    }
}
