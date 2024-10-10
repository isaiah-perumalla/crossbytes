use std::fs::File;
use atomic_bytebuffer::{AtomicBuffer, AtomicRef, AtomicRefCell, Bytes};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::atomic::Ordering::Acquire;
use std::{io, thread};
use std::io::Read;
use memmap::MmapMut;

fn main() {
    thread_check();
    let file = File::create_new("/dev/shm/test.mmap.bin").expect("failed to open the file");
    file.set_len(32);
    let mut buff = [0u8; 1];
    let memmap = unsafe { MmapMut::map_mut(&file) };
    {
        let bytes = Bytes::memap(memmap.unwrap());
        io::stdin().read_exact(&mut buff);
    }

    // drop(memmap);
    println!("dropped memap");
    io::stdin().read_exact(&mut buff);


    // let x = test_borrow();
}

// fn test_leak_ref<'a>() -> &'a AtomicU64 {
//     let bytes = Bytes::heap_allocate(32);
//     let mut buffer: AtomicBuffer = AtomicBuffer::from_bytes(0, 16, &bytes);
//     let atomic_ref:AtomicRef<AtomicU64> = buffer.borrow_ref(0);
//     let atomic = atomic_ref.get();
//     atomic
// }

// fn test_borrow<'a>() -> &'a AtomicBuffer<'a> {
//     let bytes = Bytes::heap_allocate(32);
//     let mut buffer: AtomicBuffer = AtomicBuffer::from_bytes(0, 16, &bytes);
//     let buf = &buffer;
//     buf
//
// }

fn thread_check() {
    let bytes = Bytes::heap_allocate(32);
    let mut buffer: AtomicBuffer = AtomicBuffer::from_bytes(0, 16, &bytes);
    let buf = &buffer;

    let atomic : AtomicRef<AtomicU64> = buf.borrow_ref(0);
    let flag : AtomicRef<AtomicBool> = buf.borrow_ref(10);
    thread::scope(|s| {
        s.spawn(|| {
            atomic.get().store(0xFFFF, Ordering::Relaxed);
            flag.get().store(true, Ordering::Release);
        });
    });

    {
        let flag: AtomicRef<AtomicBool> = buf.borrow_ref(10);

        let atomic: AtomicRef<AtomicU64> = buf.borrow_ref(0);
        if flag.get().load(Acquire) {
            assert_eq!(0xFFFF, atomic.get().load(Acquire));
        }
        println!("at offset 0 {}", atomic.get().load(Ordering::Acquire));
    }
    // let x_8: u32 = buf.load(8, Ordering::Acquire);
    // println!("at offset 8 {}", x_8);
}
