use serde::{Deserialize, Serialize};
use std::fmt;
use std::num::NonZeroU64;
use std::ops::{Add, Sub};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GuildId(pub NonZeroU64);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChannelId(pub NonZeroU64);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(pub NonZeroU64);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TxnId(pub NonZeroU64);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RowId(pub NonZeroU64);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Lsn(pub NonZeroU64);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId(pub NonZeroU64);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SegmentId(pub NonZeroU64);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SchemaEpoch(pub NonZeroU64);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PageId(pub NonZeroU64);

macro_rules! impl_id {
    ($($ty:ident),*) => {
        $(
            impl $ty {
                pub fn new(n: u64) -> Option<Self> {
                    NonZeroU64::new(n).map($ty)
                }

                pub const fn new_unchecked(n: u64) -> Self {
                    unsafe { $ty(NonZeroU64::new_unchecked(n)) }
                }

                pub fn get(self) -> u64 {
                    self.0.get()
                }

                pub fn increment(self) -> Self {
                    $ty(NonZeroU64::new(self.0.get() + 1).unwrap())
                }

                pub fn max_value() -> Self {
                    $ty(NonZeroU64::new(u64::MAX).unwrap())
                }

                pub fn min_value() -> Self {
                    $ty(NonZeroU64::new(1).unwrap())
                }
            }

            impl Default for $ty {
                fn default() -> Self {
                    $ty(NonZeroU64::new(1).unwrap())
                }
            }

            impl fmt::Debug for $ty {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    write!(f, "{}({})", stringify!($ty), self.0.get())
                }
            }

            impl fmt::Display for $ty {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    write!(f, "{}", self.0.get())
                }
            }

            impl PartialOrd for $ty {
                fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                    Some(self.0.get().cmp(&other.0.get()))
                }
            }

            impl Ord for $ty {
                fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                    self.0.get().cmp(&other.0.get())
                }
            }

            impl Add<u64> for $ty {
                type Output = Self;
                fn add(self, rhs: u64) -> Self::Output {
                    $ty(NonZeroU64::new(self.0.get().checked_add(rhs).unwrap()).unwrap())
                }
            }

            impl Sub<u64> for $ty {
                type Output = Self;
                fn sub(self, rhs: u64) -> Self::Output {
                    $ty(NonZeroU64::new(self.0.get().checked_sub(rhs).unwrap()).unwrap())
                }
            }

            impl Sub<$ty> for $ty {
                type Output = u64;
                fn sub(self, rhs: $ty) -> Self::Output {
                    self.0.get() - rhs.0.get()
                }
            }
        )*
    };
}

impl_id!(
    GuildId,
    ChannelId,
    MessageId,
    TxnId,
    RowId,
    Lsn,
    TableId,
    SegmentId,
    SchemaEpoch,
    PageId
);

pub trait Id: Copy + Eq + std::hash::Hash + Ord + Default + fmt::Debug + fmt::Display {
    fn new(n: u64) -> Option<Self>;
    fn get(self) -> u64;
}

impl Id for GuildId {
    fn new(n: u64) -> Option<Self> {
        GuildId::new(n)
    }
    fn get(self) -> u64 {
        self.0.get()
    }
}

impl Id for ChannelId {
    fn new(n: u64) -> Option<Self> {
        ChannelId::new(n)
    }
    fn get(self) -> u64 {
        self.0.get()
    }
}

impl Id for MessageId {
    fn new(n: u64) -> Option<Self> {
        MessageId::new(n)
    }
    fn get(self) -> u64 {
        self.0.get()
    }
}

impl Id for TxnId {
    fn new(n: u64) -> Option<Self> {
        TxnId::new(n)
    }
    fn get(self) -> u64 {
        self.0.get()
    }
}

impl Id for RowId {
    fn new(n: u64) -> Option<Self> {
        RowId::new(n)
    }
    fn get(self) -> u64 {
        self.0.get()
    }
}

impl Id for Lsn {
    fn new(n: u64) -> Option<Self> {
        Lsn::new(n)
    }
    fn get(self) -> u64 {
        self.0.get()
    }
}

impl Id for TableId {
    fn new(n: u64) -> Option<Self> {
        TableId::new(n)
    }
    fn get(self) -> u64 {
        self.0.get()
    }
}

impl Id for SegmentId {
    fn new(n: u64) -> Option<Self> {
        SegmentId::new(n)
    }
    fn get(self) -> u64 {
        self.0.get()
    }
}

impl Id for SchemaEpoch {
    fn new(n: u64) -> Option<Self> {
        SchemaEpoch::new(n)
    }
    fn get(self) -> u64 {
        self.0.get()
    }
}

impl Id for PageId {
    fn new(n: u64) -> Option<Self> {
        PageId::new(n)
    }
    fn get(self) -> u64 {
        self.0.get()
    }
}
