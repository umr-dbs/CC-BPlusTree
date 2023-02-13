use parking_lot::lock_api::{MutexGuard, RwLockReadGuard, RwLockWriteGuard};
use parking_lot::{Mutex, RawMutex, RawRwLock, RwLock};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::mem;
use std::ops::{Deref, DerefMut};
use crate::utils::safe_cell::SafeCell;

/// Basic structure for five access types:
/// 1. Write via RwLock write lock.
/// 2. Read via RwLock read lock.
/// 3. Write Exclusive via Mutex lock.
/// 4. Lock-free read via unsafe borrow.
/// 5. Lock-free write via unsafe borrow mut.
/// Fields:
/// 1. data - holds the actual data in an UnsafeCell i.e. SafeCell.
/// 2. Holds Nothing in a RwLock.
/// 3. Holds Nothing in a Mutex.
pub struct CCCell<E: Default> {
    pub data: SafeCell<E>,
    pub rwlock: RwLock<()>,
    pub mutex: Mutex<()>,
}

/// Implements default initializations for the default variant of E.
impl<E: Default> Default for CCCell<E> {
    fn default() -> Self {
        CCCell::new(Default::default())
    }
}

/// The locking types present when a locking method is invoked.
/// LockFree - indicates an unsafe borrow, read or mutable.
/// Reader - indicates a RwLock read lock.
/// Writer - indicates a RwLock write lock.
/// Exclusive - indicates a Mutex exclusive lock.
/// The Guard is only for logic purposes, while the reference represents the actual data,
/// which is allowed to be accessed in the present of the guard.
/// Additionally, lifetime 'a must never be exceeded for data access, i.e. as declared for data's guard.
// #[repr(u8)]
pub enum CCCellGuard<'a, E: 'a + Default> {
    LockFree(&'a mut E),
    Reader(RwLockReadGuard<'a, RawRwLock, ()>, &'a E),
    Writer(RwLockWriteGuard<'a, RawRwLock, ()>, &'a mut E),
    Exclusive(MutexGuard<'a, RawMutex, ()>, &'a mut E),
}

/// Follow completeness impl.
/// Caution: This variant is UB, if accessed - but is allocated on the stack, without drop constraints.
impl<'a, E: 'a + Default> Default for CCCellGuard<'a, E> {
    fn default() -> Self {
        Self::LockFree(unsafe { mem::transmute(0_usize) })
    }
}

/// Implements lock checking functions.
impl<'a, E: 'a + Default> CCCellGuard<'a, E> {
    /// Returns true, if the CCCellGuard is locked via mutex or rwlock write lock.
    /// Returns false, otherwise.
    #[inline(always)]
    pub const fn is_write_lock(&self) -> bool {
        match self {
            Self::Reader(..) => false,
            Self::LockFree(..) => false,
            _ => true,
        }
    }

    /// Returns true, if the CCCellGuard is locked via rwlock read lock.
    /// Returns false, otherwise.
    #[inline(always)]
    pub const fn is_reader_lock(&self) -> bool {
        match self {
            Self::Reader(..) => true,
            _ => false,
        }
    }

    /// Returns true, if the CCCellGuard is locked via mutex exclusive lock.
    /// Returns false, otherwise.
    #[inline(always)]
    pub const fn is_exclusive_lock(&self) -> bool {
        match self {
            Self::Exclusive(..) => true,
            _ => false,
        }
    }

    /// Returns true, if the CCCellGuard is locked no lock.
    /// Returns false, otherwise.
    #[inline(always)]
    pub const fn is_lock_free_lock(&self) -> bool {
        match self {
            Self::LockFree(..) => true,
            _ => false,
        }
    }
}

/// Implements sugar for the CCCellGuard, i.e. auto deref for inner data references.
impl<'a, E: Default> Deref for CCCellGuard<'a, E> {
    type Target = E;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        match self {
            CCCellGuard::Reader(_, r) => r,
            CCCellGuard::Writer(_, w) => w,
            CCCellGuard::Exclusive(_, ex) => ex,
            CCCellGuard::LockFree(e) => e,
        }
    }
}

/// Implements sugar for the CCCellGuard, i.e. auto deref mut for inner data references.
impl<'a, E: Default> DerefMut for CCCellGuard<'a, E> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            CCCellGuard::LockFree(e) => e,
            CCCellGuard::Writer(_, w) => w,
            CCCellGuard::Exclusive(_, ex) => ex,
            CCCellGuard::Reader(..) => {
                unreachable!("Sleepy joe hit me -> deref mut a reader guard!")
            }
        }
    }
}

/// Safely implements serde::Serialize for internal type.
impl<E: Serialize + Default> Serialize for CCCell<E> {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        self.unsafe_borrow().serialize(serializer)
    }
}

/// Safely implements serde::Deserialize for internal type.
impl<'de, E: Deserialize<'de> + Default> Deserialize<'de> for CCCell<E> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self::new(E::deserialize(deserializer)?))
    }
}

/// Implements functional methods for access types.
impl<E: Default> CCCell<E> {
    /// Standard constructor.
    #[inline(always)]
    pub const fn new(e: E) -> Self {
        Self {
            data: SafeCell::new(e),
            rwlock: RwLock::new(()),
            mutex: Mutex::new(()),
        }
    }

    /// Read access via static life-time.
    #[inline(always)]
    pub fn borrow_read_static(&self) -> CCCellGuard<'static, E> {
        unsafe { mem::transmute(self.borrow_read()) }
    }

    /// Read access.
    #[inline(always)]
    pub fn borrow_read(&self) -> CCCellGuard<'_, E> {
        CCCellGuard::Reader(self.rwlock.read(), self.unsafe_borrow())
    }

    /// Lock-less access.
    #[inline(always)]
    pub fn unsafe_borrow(&self) -> &E {
        self.data.as_ref()
    }

    /// Lock-less read access via static life-time.
    #[inline(always)]
    pub fn unsafe_borrow_static(&self) -> &'static E {
        unsafe { mem::transmute(self.unsafe_borrow()) }
    }

    /// Lock-less write access via static life-time.
    #[inline(always)]
    pub fn unsafe_borrow_mut(&self) -> &mut E {
        self.data.get_mut()
    }

    /// Lock-less write access via static life-time.
    #[inline(always)]
    pub fn unsafe_borrow_mut_static(&self) -> &'static mut E {
        unsafe { mem::transmute(self.unsafe_borrow_mut()) }
    }

    /// Write access via static life-time.
    #[inline(always)]
    pub fn borrow_mut_static(&self) -> CCCellGuard<'static, E> {
        unsafe { mem::transmute(self.borrow_mut()) }
    }

    /// Write access.
    #[inline(always)]
    pub fn borrow_mut(&self) -> CCCellGuard<'_, E> {
        CCCellGuard::Writer(self.rwlock.write(), self.unsafe_borrow_mut())
    }

    /// Exclusive write access.
    #[inline(always)]
    pub fn borrow_mut_exclusive(&self) -> CCCellGuard<'_, E> {
        CCCellGuard::Exclusive(self.mutex.lock(), self.unsafe_borrow_mut())
    }

    /// Exclusive write access.
    #[inline(always)]
    pub fn borrow_mut_exclusive_static(&self) -> CCCellGuard<'static, E> {
        unsafe { mem::transmute(self.borrow_mut_exclusive()) }
    }

    /// LockFree access.
    #[inline(always)]
    pub fn borrow_free(&self) -> CCCellGuard<'_, E> {
        CCCellGuard::LockFree(self.unsafe_borrow_mut())
    }

    /// LockFree access.
    #[inline(always)]
    pub fn borrow_free_static(&self) -> CCCellGuard<'static, E> {
        unsafe { mem::transmute(self.borrow_free()) }
    }

    /// Structure flatten method.
    /// Retrieves underlying object.
    #[inline(always)]
    pub fn into_inner(self) -> E {
        mem::drop(self.rwlock);
        mem::drop(self.mutex);
        self.data.into_inner()
    }
}

/// Explicitly allow Send for all E via CCCellRRW.
unsafe impl<E: Default> Send for CCCell<E> {}

/// Explicitly allow Sync for all E via CCCellRRW.
unsafe impl<E: Default> Sync for CCCell<E> {}

/// Sugar implementation, allowing auto lock-less readers.
impl<E: Default> Deref for CCCell<E> {
    type Target = E;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.unsafe_borrow()
    }
}
