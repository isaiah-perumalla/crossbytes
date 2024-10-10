use memmap;
use std::alloc;
use std::alloc::Layout;
use std::ops::Deref;
use std::ptr::NonNull;

use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, AtomicU8};
use memmap::MmapMut;

enum MemType {
    Heap(NonNull<u8>),
    Mapped(MmapMut)
}
impl MemType {
    //converting *const u8 to *mut u8
    unsafe fn  as_ptr(&self) -> *mut u8 {
        match  &self{
            MemType::Heap(ptr) => ptr.as_ptr(),
            MemType::Mapped(memmap) => memmap.as_ptr() as *mut u8
        }
    }
}
pub struct Bytes {
    bytes: MemType,
    cap: usize,
}

impl Bytes {
    pub fn heap_allocate(size: usize) -> Bytes {
        let ptr = heap_allocate(size);
        Bytes { bytes:  MemType::Heap(ptr), cap: size }
    }

    pub fn memap(memap : MmapMut) -> Bytes {
        let cap = memap.len();
        Bytes {bytes: MemType::Mapped(memap), cap}
    }

    pub fn capacity(&self) -> usize {
        self.cap
    }
}
impl Drop for Bytes {
    fn drop(&mut self) {
        match &self.bytes {
            MemType::Heap(ptr) => {
                if self.cap > 0 {
                    let layout = Layout::array::<u8>(self.cap).unwrap();
                    unsafe {
                        alloc::dealloc(ptr.as_ptr(), layout);
                    }
                }
            },
            MemType::Mapped(_) => {
                //auto dropped
                //MapMut is owned and will be do it own clean up
            }
        }
        }
}

pub struct AtomicBuffer<'a> {
    offset: usize,
    length: usize,
    bytes: &'a Bytes,
}

impl<'a> AtomicBuffer<'a> {
    pub fn from_bytes(offset: usize, length: usize, bytes: &'a Bytes) -> AtomicBuffer<'a> {
        assert!((offset + length) < bytes.capacity(), "bounds error");
        AtomicBuffer {
            offset,
            length,
            bytes,
        }
    }

    unsafe fn data_ptr(&self) -> *mut u8 {
        self.bytes.bytes.as_ptr().add(self.offset)
    }
}

impl<'a> Deref for AtomicBuffer<'a> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data_ptr(), self.length) }
    }
}

fn heap_allocate(size: usize) -> NonNull<u8> {
    let new_layout = Layout::array::<u8>(size).unwrap();
    // Ensure that the new allocation doesn't exceed `isize::MAX` bytes.
    assert!(
        new_layout.size() <= isize::MAX as usize,
        "Allocation too large"
    );
    let allocated_ptr = unsafe { alloc::alloc(new_layout) };
    // If allocation fails, `new_ptr` will be null, in which case we abort.
    let ptr = match NonNull::new(allocated_ptr) {
        Some(p) => p,
        None => alloc::handle_alloc_error(new_layout),
    };
    ptr
}

pub struct AtomicRef<'a, T> {
    buffer: &'a AtomicBuffer<'a>,
    offset: usize,
    atomic: &'a T,
}
impl<'a, T> AsRef<T> for AtomicRef<'a, T> {
    fn as_ref(&self) -> &'a T {
        self.atomic
    }
}
unsafe impl<T> Send for AtomicRef<'_, T> {}
unsafe impl<T> Sync for AtomicRef<'_, T> {}

impl<'a, T> AtomicRef<'a, T> {
    /// Borrow the inner value bounded by lifetime 'a
    pub fn get(&self) -> &'a T {
        self.atomic
    }

    pub fn map<U, F>(&self, f: F) -> U
    where
        F: FnOnce(&'a T) -> U,
    {
        f(self.atomic)
    }
}

pub trait AtomicRefCell<T> {
    fn borrow_ref(&self, index: usize) -> AtomicRef<'_, T>;
}

macro_rules! atomic_ref_impl {
    ($type: ty, $atomic_ty: ty) => {
        impl AtomicRefCell<$atomic_ty> for AtomicBuffer<'_> {
            fn borrow_ref(&self, offset: usize) -> AtomicRef<'_, $atomic_ty> {
                debug_assert!(offset < self.length, "bounds error");
                let atomic = unsafe {
                    let ptr = self.data_ptr().add(offset) as *mut $type;
                    <$atomic_ty>::from_ptr(ptr)
                };
                AtomicRef {
                    atomic,
                    offset,
                    buffer: &self,
                }
            }
        }
    };
}

atomic_ref_impl!(bool, AtomicBool);
atomic_ref_impl!(u8, AtomicU8);
atomic_ref_impl!(u16, AtomicU16);
atomic_ref_impl!(u32, AtomicU32);
atomic_ref_impl!(u64, AtomicU64);

#[cfg(test)]
mod tests {
    use crate::{AtomicBuffer, AtomicRef, AtomicRefCell, Bytes};
    use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, Ordering};

    #[test]
    fn test_atomic_ref() {
        let bytes = Bytes::heap_allocate(32);
        let mut buffer: AtomicBuffer = AtomicBuffer::from_bytes(0, 16, &bytes);
        let atomic_ref: AtomicRef<AtomicU64> = buffer.borrow_ref(0);
        atomic_ref.map(|r| r.store(0xFF00FFu64, Ordering::Relaxed));
        let atomic = atomic_ref.get();

        assert_eq!(atomic.load(Ordering::Relaxed), 0xFF00FFu64);
        assert_eq!(*(&buffer[0]), 0xFFu8);
        assert_eq!(*(&buffer[1]), 0u8);
        assert_eq!(*(&buffer[2]), 0xFFu8);
    }

    #[test]
    fn test_atomic_u() {
        let bytes = Bytes::heap_allocate(32);
        let mut buffer: AtomicBuffer = AtomicBuffer::from_bytes(0, 16, &bytes);
        let atomic64: AtomicRef<AtomicU64> = buffer.borrow_ref(0);
        atomic64.get().store(0xF000FFu64, Ordering::Relaxed);

        let atomic32: AtomicRef<AtomicU32> = buffer.borrow_ref(8);
        atomic32.get().store(0xF000FFu32, Ordering::Relaxed);

        let atomic16: AtomicRef<AtomicU16> = buffer.borrow_ref(12);
        atomic16.get().store(0xF0FFu16, Ordering::Relaxed);

        assert_eq!(*(&buffer[0]), 0xFFu8);
        assert_eq!(*(&buffer[2]), 0xF0u8);

        assert_eq!(*(&buffer[8]), 0xFFu8);
        assert_eq!(*(&buffer[10]), 0xF0u8);

        assert_eq!(*(&buffer[12]), 0xFFu8);
        assert_eq!(*(&buffer[13]), 0xF0u8);
    }
}
