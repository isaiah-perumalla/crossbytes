use memmap::MmapMut;
use std::alloc;
use std::alloc::Layout;
use std::fs::File;
use std::ops::{Deref, DerefMut, RangeFrom};
use std::path::Path;
use std::ptr::NonNull;
use std::sync::atomic::{
    AtomicBool, AtomicI16, AtomicI32, AtomicI64, AtomicI8, AtomicU16, AtomicU32, AtomicU64,
    AtomicU8, Ordering,
};

enum MemType {
    Heap(NonNull<u8>),
    Mapped(MmapMut),
}
impl MemType {
    //converting *const u8 to *mut u8
    unsafe fn as_ptr(&self) -> *mut u8 {
        match &self {
            MemType::Heap(ptr) => ptr.as_ptr(),
            MemType::Mapped(memmap) => memmap.as_ptr() as *mut u8,
        }
    }
}
pub struct Bytes {
    bytes: MemType,
    cap: usize,
}

impl Bytes {
    pub fn from_file_backed<P: AsRef<Path>>(file: P, size: u64) -> Self {
        let file = File::create_new(file).expect("failed to open the file");
        file.set_len(size).expect("filed to set size");
        let mmap = unsafe { MmapMut::map_mut(&file) };
        Self::memap(mmap.unwrap())
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

impl Bytes {
    pub fn heap_allocate(size: usize) -> Bytes {
        let ptr = heap_allocate(size);
        Bytes {
            bytes: MemType::Heap(ptr),
            cap: size,
        }
    }
    pub fn memap(memap: MmapMut) -> Bytes {
        let cap = memap.len();
        Bytes {
            bytes: MemType::Mapped(memap),
            cap,
        }
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
            }
            MemType::Mapped(_) => {
                //auto dropped
                //MapMut is owned and will be do it own clean up
            }
        }
    }
}

pub struct BytesAtomicView<'a> {
    offset: usize,
    length: usize,
    bytes: &'a Bytes,
}

impl<'a> BytesAtomicView<'a> {
    pub fn from_bytes(offset: usize, length: usize, bytes: &'a Bytes) -> BytesAtomicView<'a> {
        assert!((offset + length) <= bytes.capacity(), "bounds error");
        let alignment = align_of::<usize>();

        let ptr = unsafe { bytes.bytes.as_ptr().add(offset) };
        assert_eq!(ptr.align_offset(alignment), 0, "invalid alignment");

        BytesAtomicView {
            offset,
            length,
            bytes,
        }
    }

    pub fn sub_slice(&self, range_from: RangeFrom<u32>) -> BytesAtomicView<'a> {
        let start = range_from.start as usize;
        assert!(start < self.length);
        let new_len = self.length - start;
        let buffer = BytesAtomicView {
            offset: self.offset + start,
            length: new_len,
            bytes: self.bytes
        };
        buffer
    }
    pub fn sub_view(&self, start: u32, length: u32) -> BytesAtomicView<'a> {
        assert!((start + length ) as usize <= self.length );
        let buffer = BytesAtomicView {
            offset: self.offset + start as usize,
            length: length as usize,
            bytes: self.bytes,
        };
        buffer
    }
    unsafe fn data_ptr(&self) -> *mut u8 {
        self.bytes.bytes.as_ptr().add(self.offset)
    }
}

impl<'a> Deref for BytesAtomicView<'a> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data_ptr(), self.length) }
    }
}

impl<'a> DerefMut for BytesAtomicView<'a> {
    fn deref_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data_ptr(), self.length) }
    }
}

impl<'a> Clone for BytesAtomicView<'a> {
    fn clone(&self) -> Self {
        let buffer = BytesAtomicView {
            offset: self.offset,
            length: self.length,
            bytes: self.bytes,
        };
        buffer
    }
}


pub trait AtomicRefCell<'a, T> {
    /// return a reference to an atomic view of type T
    /// index must be align_of<T>
    fn get_atomic(&'a self, index: usize) -> &'a T;
}

pub trait LoadStore<T> {
    fn load_at(&self, offset: usize, ordering: Ordering) -> T;
    fn store_at(&mut self, offset: usize, val: T, ordering: Ordering);
}


macro_rules! load_store_impl {
    ($type: ty, $atomic_ty: ty) => {
        impl<'a> LoadStore<$type> for BytesAtomicView<'a> {
            fn load_at(&self, offset: usize, ordering: Ordering) -> $type {
                let atomic: &$atomic_ty = self.get_atomic(offset);
                atomic.load(ordering)
            }

            fn store_at(&mut self, offset: usize, val: $type, ordering: Ordering) {
                let atomic: &$atomic_ty = self.get_atomic(offset);
                atomic.store(val, ordering)
            }
        }
    };
}
macro_rules! atomic_ref_impl {
    ($type: ty, $atomic_ty: ty) => {
        impl<'a> AtomicRefCell<'a, $atomic_ty> for BytesAtomicView<'a> {
            fn get_atomic(&'a self, offset: usize) -> &'a $atomic_ty {
                debug_assert!(offset < self.length, "bounds error");
                let atomic = unsafe {
                    let ptr = self.data_ptr().add(offset) as *mut $type;
                    let expected_alignment = align_of::<$atomic_ty>();
                    let is_aligned = ptr.align_offset(expected_alignment) == 0;
                    if !is_aligned {
                        let err = format!("invalid alignment offset={}, Atomic type", offset);
                        debug_assert!(is_aligned, "{}", err);
                    }
                    <$atomic_ty>::from_ptr(ptr)
                };
                atomic
            }
        }
    };
}

atomic_ref_impl!(bool, AtomicBool);
atomic_ref_impl!(u8, AtomicU8);
atomic_ref_impl!(u16, AtomicU16);
atomic_ref_impl!(u32, AtomicU32);
atomic_ref_impl!(u64, AtomicU64);

atomic_ref_impl!(i8, AtomicI8);
atomic_ref_impl!(i16, AtomicI16);
atomic_ref_impl!(i32, AtomicI32);
atomic_ref_impl!(i64, AtomicI64);

load_store_impl!(u64, AtomicU64);
load_store_impl!(u32, AtomicU32);
load_store_impl!(u16, AtomicU16);
load_store_impl!(u8, AtomicU8);

load_store_impl!(i64, AtomicI64);
load_store_impl!(i32, AtomicI32);
load_store_impl!(i16, AtomicI16);
load_store_impl!(i8, AtomicI8);


#[cfg(test)]
mod tests {

    use crate::bytes::{AtomicRefCell, Bytes, BytesAtomicView, LoadStore};
    use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, Ordering};
    use std::sync::atomic::Ordering::Relaxed;

    #[test]
    fn test_atomic_ref() {
        let bytes = Bytes::heap_allocate(32);
        let buffer: BytesAtomicView = BytesAtomicView::from_bytes(0, 16, &bytes);
        let atomic_ref: &AtomicU64 = buffer.get_atomic(0);
        atomic_ref.store(0xFF00FFu64, Ordering::Relaxed);
        assert_eq!(atomic_ref.load(Ordering::Relaxed), 0xFF00FFu64);
        assert_eq!(*(&buffer[0]), 0xFFu8);
        assert_eq!(*(&buffer[1]), 0u8);
        assert_eq!(*(&buffer[2]), 0xFFu8);
    }

    #[test]
    fn test_atomic_u() {
        let bytes = Bytes::heap_allocate(32);
        let buffer: BytesAtomicView = BytesAtomicView::from_bytes(0, 16, &bytes);
        let atomic64: &AtomicU64 = buffer.get_atomic(0);
        atomic64.store(0xF000FFu64, Ordering::Relaxed);

        let atomic32: &AtomicU32 = buffer.get_atomic(8);
        atomic32.store(0xF000FFu32, Ordering::Relaxed);

        let atomic16: &AtomicU16 = buffer.get_atomic(12);
        atomic16.store(0xF0FFu16, Ordering::Relaxed);

        assert_eq!(*(&buffer[0]), 0xFFu8);
        assert_eq!(*(&buffer[2]), 0xF0u8);

        assert_eq!(*(&buffer[8]), 0xFFu8);
        assert_eq!(*(&buffer[10]), 0xF0u8);

        assert_eq!(*(&buffer[12]), 0xFFu8);
        assert_eq!(*(&buffer[13]), 0xF0u8);
    }

    #[test]
    fn test_load_store_convinence_methods() {
        let bytes = Bytes::heap_allocate(32);
        let mut buffer: BytesAtomicView = BytesAtomicView::from_bytes(0, 16, &bytes);
        buffer.store_at(0, 75u64, Ordering::Relaxed);
        assert_eq!(75u64, buffer.load_at(0, Ordering::Relaxed));
    }

    
    #[test]
    fn test_sub_slice() {
        let bytes = Bytes::heap_allocate(32);
        let mut buffer: BytesAtomicView = BytesAtomicView::from_bytes(0, 16, &bytes);
        buffer.store_at(8, 8u64, Relaxed);
        let mut sub_slice = buffer.sub_slice(8..);
        let val : u64 = buffer.load_at(8, Relaxed);
        assert_eq!(val, sub_slice.load_at(0, Relaxed))
    }
    #[test]
    #[should_panic(expected = "invalid alignment offset=2, Atomic type")]
    fn test_disallow_unaligned_access() {
        let bytes = Bytes::heap_allocate(32);
        let buffer: BytesAtomicView = BytesAtomicView::from_bytes(0, 16, &bytes);
        let _: &AtomicU64 = buffer.get_atomic(2);
    }

    #[test]
    #[should_panic(expected = "invalid alignment offset=3, Atomic type")]
    fn test_disallow_unaligned_u16_access() {
        let bytes = Bytes::heap_allocate(32);
        let buffer: BytesAtomicView = BytesAtomicView::from_bytes(0, 16, &bytes);
        let _: &AtomicU16 = buffer.get_atomic(3);
    }
}
