use crossbytes::agrona::broadcast::{BroadcastTx, MsgTypeId, TAIL_COUNTER_OFFSET, TRAILER_SIZE};
use crossbytes::bytes::{Bytes, BytesAtomicView, LoadStore};
use std::fs::OpenOptions;
use std::ops::BitAnd;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::time;                                                                                                                              

fn main() {
    let path = "/dev/shm/broadcast-test.dat";
    let EXPECTED_MSG_SIZE = 25;
    let buffer_size = 1024;
    let size = buffer_size + crossbytes::agrona::broadcast::TRAILER_SIZE;
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)
        .expect("failed to open the file");
    file.set_len(size as u64).expect("filed to set size");

    let bytes = Bytes::from_file_backed(file);

    let buffer = BytesAtomicView::from_bytes(0, bytes.capacity(), &bytes);
    let counters = BytesAtomicView::from_bytes(buffer_size, TRAILER_SIZE, &bytes);
    let mut transmit_value: u64 = counters.load_at(TAIL_COUNTER_OFFSET as usize, Acquire);
    let mut tx = BroadcastTx::new(buffer.clone());

    let duration = time::Duration::from_secs_f32(60.0);
    let time_now = time::Instant::now();
    let mut count = 0u64;
    while time_now.elapsed() < duration {
        let id = count.bitand(0x000000007FFFFFFF) as u32;
        assert_ne!(-1, id as i32);
        let msg_id = MsgTypeId::from(id);
        let result = tx.transmit(EXPECTED_MSG_SIZE, msg_id, |mut buffer| {
            buffer.store_at(0, transmit_value, Relaxed);
            buffer.store_at(8, transmit_value + 1, Relaxed);
            EXPECTED_MSG_SIZE as usize
        });
        count += 1;
        transmit_value += 1;
        assert!(result.is_ok());
    }
    println!("published {} messages,  duration={:?}", count, duration);
}
