use std::fs::OpenOptions;
use crossbytes::broadcast::BroadcastRx;
use crossbytes::bytes::{Bytes, BytesAtomicView, LoadStore};
use std::sync::atomic::Ordering::Relaxed;


fn main() {
    let path = "/dev/shm/broadcast-test.dat";
    let EXPECTED_MSG_SIZE= 25;
    let size = 1024 + crossbytes::broadcast::TRAILER_SIZE;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("failed to open the file");
    file.set_len(size as u64).expect("filed to set size");

    let bytes = Bytes::from_file_backed(file);
    let buffer = BytesAtomicView::from_bytes(0, bytes.capacity(), &bytes);
    let mut rx = BroadcastRx::new(buffer.clone());
    let mut count = 0u64;
    let mut loop_count = 0u64;
    let mut last_msg_id = 0u32;
    loop {
        let mut current_id = 0;
        let mut last_val = 0u64;
        let mut va0 = 0u64;
        let mut va1 = 0u64;
        let result = rx.receive_next(|msg_id, buffer| {
            let size = buffer.len();
            assert_eq!(size, EXPECTED_MSG_SIZE);
            current_id = msg_id.inner();
            va0 = buffer.load_at(0, Relaxed);
            va1 = buffer.load_at(8, Relaxed);
        });
        if let Ok(_) = result {
            assert_ne!(last_msg_id, current_id);
            assert!(va0 > last_val);
            assert_eq!(va1, 1 + va0 );
            count += 1;
            last_msg_id = current_id;
            last_val = va0;
        }

        if loop_count == 1000_000_000 {
            break;
        }
        loop_count += 1;
    }
    println!("read {} messages, lapped count={}", count, rx.lapped_count());
}