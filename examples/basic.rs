use crossbytes::bytes::{AtomicBuffer, AtomicRefCell, Bytes};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{fs, thread};

fn main() {
    let file = "/dev/shm/test.mmap.bin";
    let _ = fs::remove_file(file).unwrap();

    let bytes = Bytes::from_file_backed(file, 32);
    let buffer = AtomicBuffer::from_bytes(0, 16, &bytes);
    let counter: &AtomicU64 = buffer.get_atomic(8);
    let max_iters = 100000000;
    thread::scope(|s| {
        s.spawn(|| {
            for i in 0..max_iters {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });

        s.spawn(|| {
            for _ in 0..max_iters {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });
    });

    println!("counter value={}, expected={}", counter.load(Ordering::Acquire), max_iters * 2);

    // let x = test_borrow();
}

// fn test_leak_ref<'a>() -> &'a AtomicU64 {
//     let bytes = Bytes::heap_allocate(32);
//     let mut buffer: AtomicBuffer = AtomicBuffer::from_bytes(0, 16, &bytes);
//     let atomic_ref: &AtomicU64 = buffer.get_atomic(0);
//     atomic_ref
// }


