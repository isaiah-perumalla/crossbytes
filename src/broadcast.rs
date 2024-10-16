use crate::broadcast::RxErr::Overwritten;
use crate::bytes::{AtomicRefCell, BytesAtomicView, LoadStore};
use std::ops::{Add, BitAnd};
use std::sync::atomic;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicU64, Ordering};

const TRAILER_SIZE: usize = 128;
const TAIL_INTENT_COUNTER_OFFSET: u32 = 0;
const TAIL_COUNTER_OFFSET: u32 = TAIL_INTENT_COUNTER_OFFSET + (size_of::<u64>() as u32);
const LAST_COUNTER_OFFSET: u32 = TAIL_COUNTER_OFFSET + (size_of::<u64>() as u32);
const HEADER_SIZE: u32 = 8;
const RECORD_ALIGNMENT: u32 = 8;
const PADDING_MSD_ID: MsgTypeId = MsgTypeId(0);

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct MsgTypeId(u32);

impl MsgTypeId {
    pub fn inner(&self) -> u32 {
        self.0
    }
}

struct CountersInner<'a> {
    buff: BytesAtomicView<'a>,
}

impl<'a> CountersInner<'a> {
    #[inline]
    fn latest_record_counter(&'a self) -> &'a AtomicU64 {
        self.buff.get_atomic(LAST_COUNTER_OFFSET as usize)
    }

    #[inline]
    fn tail_counter(&'a self) -> &'a AtomicU64 {
        self.buff.get_atomic(TAIL_COUNTER_OFFSET as usize)
    }

    #[inline]
    fn tail_intent_counter(&'a self) -> &'a AtomicU64 {
        self.buff.get_atomic(TAIL_INTENT_COUNTER_OFFSET as usize)
    }

    #[cfg(debug_assertions)]
    fn check_invariants(&self) {
        debug_assert!(is_aligned8(self.latest_record_counter().load(Relaxed)));
        debug_assert!(is_aligned8(self.tail_counter().load(Relaxed)));
        debug_assert!(is_aligned8(self.tail_intent_counter().load(Relaxed)));
        debug_assert!(
            self.latest_record_counter().load(Ordering::Acquire)
                <= self.tail_counter().load(Ordering::Relaxed)
        );
        debug_assert!(
            self.tail_counter().load(Ordering::Relaxed)
                <= self.tail_intent_counter().load(Ordering::Relaxed)
        );
    }

    fn commit_record(&mut self, latest_value_counter: u64, tail_counter: u64) {
        debug_assert!(latest_value_counter <= tail_counter);
        debug_assert!(
            self.tail_intent_counter().load(Relaxed) == tail_counter,
            "tail-intent_counter must match tail_counter on commit"
        );
        self.latest_record_counter()
            .store(latest_value_counter, Release);
        self.tail_counter().store(tail_counter, Release);
        self.check_invariants();
    }
    fn new(buffer: BytesAtomicView<'a>) -> CountersInner<'a> {
        let length = buffer.len();
        assert_eq!(buffer.len(), TRAILER_SIZE);

        CountersInner { buff: buffer }
    }
}

#[cfg(target_has_atomic = "64")]
pub struct BroadcastTx<'a> {
    counters_inner: CountersInner<'a>,
    buffer: BytesAtomicView<'a>,
}

impl<'a> BroadcastTx<'a> {
    pub fn new(buffer: BytesAtomicView<'a>) -> BroadcastTx<'a> {
        let length = buffer.len();
        let capacity = (length - TRAILER_SIZE) as u32;
        assert!(
            (capacity).is_power_of_two(),
            "invalid buffer size, not pow of 2 + TrailerLength"
        );
        let index_inner = CountersInner::new(buffer.sub_slice(capacity..));
        BroadcastTx {
            counters_inner: index_inner,
            buffer: buffer.sub_view(0, capacity),
        }
    }

    fn max_msg_size(&self) -> u32 {
        (self.buffer.len() / 8) as u32
    }

    pub fn transmit<F>(&mut self, msg_size: u32, id: MsgTypeId, f: F) -> Result<u32, TxErr>
    where
        F: Fn(BytesAtomicView) -> usize,
    {
        if id == PADDING_MSD_ID {
            return Err(TxErr::InvalidMsgType);
        }
        if msg_size > self.max_msg_size() {
            return Err(TxErr::MsgTooLarge(msg_size));
        }
        let capacity = self.buffer.len() as u32;
        let tail_counter = self.counters_inner.tail_counter();
        let tail_intent_counter = self.counters_inner.tail_intent_counter();
        //relaxed load is sufficient as only this thread can mutate this value
        let current_tail: u64 = tail_counter.load(Relaxed);
        let record_offset = current_tail.bitand(capacity as u64 - 1) as u32;
        let record_len = msg_size + HEADER_SIZE;
        let aligned_record_len = align(record_len, RECORD_ALIGNMENT);
        let new_tail = current_tail + aligned_record_len as u64;

        if capacity < (record_offset + aligned_record_len) {
            //record cannot fit in given capacity, to avoid wrapping
            //insert padding
            let padding_size = capacity - record_offset;
            //we are adding padding + data for new tail
            let tail_intent = new_tail + padding_size as u64;
            tail_intent_counter.store(tail_intent, Release);
            // //ensure all writes above this fence happen before all write below the fence
            atomic::fence(Release);
            let mut padding_buf = self
                .buffer
                .sub_view(record_offset, padding_size + HEADER_SIZE);
            Self::write_header(PADDING_MSD_ID, &mut padding_buf, padding_size);

            //record_offset wraps for actual data
            let mut buffer = self.buffer.sub_view(0, msg_size + HEADER_SIZE);
            Self::write_header(id, &mut buffer, record_len);
            let data_buffer = self.buffer.sub_view(HEADER_SIZE, msg_size);
            f(data_buffer);
            let latest_record_counter = current_tail;
            let new_tail_counter = tail_intent;
            self.counters_inner
                .commit_record(latest_record_counter, new_tail_counter);
            Ok(aligned_record_len + padding_size)
        } else {
            tail_intent_counter.store(new_tail, Release);
            //ensure all writes above this fence happen before all write below the fence
            atomic::fence(Release);
            let mut buffer = self.buffer.sub_view(record_offset, msg_size + HEADER_SIZE);

            Self::write_header(id, &mut buffer, record_len);

            let data_slot = buffer.sub_view(HEADER_SIZE, msg_size);
            f(data_slot);
            self.counters_inner.commit_record(current_tail, new_tail);
            Ok(aligned_record_len)
        }
    }

    fn write_header(id: MsgTypeId, buffer: &mut BytesAtomicView, record_len: u32) {
        buffer.store_at(0, record_len, Relaxed);
        buffer.store_at(4, id.0, Relaxed);
    }
}

#[inline]
fn is_aligned8(val: u64) -> bool {
    0 == val.bitand(7) // check aligned to 8
}
#[inline]
fn align(val: u32, alignment: u32) -> u32 {
    let val = val as i32;
    let alignment = alignment as i32;
    //avoid branches
    let res = (-alignment).bitand(val + (alignment - 1));
    res as u32
}

pub enum TxErr {
    InvalidMsgType,
    MsgTooLarge(u32),
}
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum RxErr {
    // No new messages available for consumption
    NoElement,
    // receiver is not consuming messages fast enough to keep up with the transmitter,
    // resulting in messages being overwritten thus making them no longer valid.
    Overwritten,

    InvalidMsgType,
}
#[cfg(target_has_atomic = "64")]
pub struct BroadcastRx<'a> {
    counters: CountersInner<'a>,
    buffer: BytesAtomicView<'a>,
    cursor: u64,
    lapped_count: u64,
}

impl<'a> BroadcastRx<'a> {
    pub fn new(buffer: BytesAtomicView<'a>) -> BroadcastRx<'a> {
        let capacity: u32 = (buffer.len() - TRAILER_SIZE) as u32;
        assert!(capacity.is_power_of_two(), "capacity must be pow 2");

        let inner = CountersInner::new(buffer.sub_slice(capacity..));
        let start_position = inner.latest_record_counter().load(Acquire);
        BroadcastRx {
            counters: inner,
            buffer: buffer.sub_view(0, capacity),
            cursor: start_position,
            lapped_count: 0,
        }
    }

    pub fn lapped_count(&self) -> u64 {
        self.lapped_count
    }
    pub fn receive_next<F>(&mut self, mut read_callback: F) -> Result<u32, RxErr>
    where
        F: FnMut(MsgTypeId, BytesAtomicView),
    {
        let buffer = &self.buffer;
        let capacity = self.buffer.len();
        debug_assert!(capacity.is_power_of_two(), "capacity must be pow 2");
        let tail_counter = self.counters.tail_counter();
        let tail_intent_counter = self.counters.tail_intent_counter();
        let latest_counter = self.counters.latest_record_counter();
        let tail = tail_counter.load(Acquire);

        if tail == self.cursor {
            return Err(RxErr::NoElement);
        }
        assert!(tail > self.cursor, "invalid state cursor cannot be > tail");

        let tail_intent_position = tail_intent_counter.load(Acquire);
        let is_valid = (self.cursor + capacity as u64) > tail_intent_position;
        if !is_valid {
            self.lapped_count += 1;
            self.cursor = latest_counter.load(Acquire);
            return Err(RxErr::Overwritten);
        }
        let record_offset = self.cursor.bitand(capacity as u64 - 1);
        let record_size: u32 = buffer.load_at(record_offset as usize, Relaxed);
        let aligned_record_size: u32 = align(record_size, RECORD_ALIGNMENT);
        let msg_id: u32 = buffer.load_at(record_offset as usize + 4, Relaxed);
        let next_record_position = record_offset + aligned_record_size as u64;
        if PADDING_MSD_ID.inner() == msg_id {
            let record_offset = next_record_position;
            let record_size: u32 = buffer.load_at(record_offset as usize, Relaxed);
            let aligned_record_size: u32 = align(record_size, RECORD_ALIGNMENT);
            let msg_id: u32 = buffer.load_at(record_offset as usize + 4, Relaxed);
            assert_ne!(
                msg_id,
                PADDING_MSD_ID.inner(),
                "cannot have two consecutive paddings"
            );
            let next_record_position = record_offset + aligned_record_size as u64;
            let start = (record_offset + HEADER_SIZE as u64) as u32;

            let data_buffer = buffer.sub_view(start, record_size - HEADER_SIZE);

            read_callback(MsgTypeId(msg_id), data_buffer);

            let res = self.commit_read(record_size, next_record_position);
            res
        } else {
            let start = (record_offset + HEADER_SIZE as u64) as u32;
            let data_buffer = buffer.sub_view(start, record_size - HEADER_SIZE);
            read_callback(MsgTypeId(msg_id), data_buffer);
            let res = self.commit_read(record_size, next_record_position);
            res
        }
    }

    // check read was valid , ie data was not overwritten while reading
    // update internal counter to reflect the outcome
    fn commit_read(&mut self, record_size: u32, next_record_position: u64) -> Result<u32, RxErr> {
        //need to ensure reads / writes above this fence happen before any subsequent reads
        atomic::fence(Acquire);
        let tail_intent_counter = self.counters.tail_intent_counter();
        let tail_intent_position = tail_intent_counter.load(Acquire);
        let read_ok = (self.cursor + self.buffer.len() as u64) > tail_intent_position;

        if read_ok {
            self.cursor = next_record_position;
            Ok(record_size)
        } else {
            let latest_record = self.counters.latest_record_counter().load(Acquire);
            self.cursor = latest_record;
            self.lapped_count += 1;
            Err(Overwritten)
        }
    }
    
}

#[cfg(test)]
mod tests {
    use crate::broadcast::{
        align, BroadcastRx, BroadcastTx, MsgTypeId, RxErr, HEADER_SIZE, TRAILER_SIZE,
    };
    use crate::bytes::{Bytes, BytesAtomicView};

    #[test]
    fn test_align() {
        assert_eq!(16, align(12, 8));
        assert_eq!(16, align(16, 8));
        assert_eq!(8, align(6, 8));
    }
    #[test]
    fn test_send_receive() {
        let bytes = Bytes::heap_allocate(32 + TRAILER_SIZE);
        let buffer = BytesAtomicView::from_bytes(0, bytes.capacity(), &bytes);
        let mut tx = BroadcastTx::new(buffer.clone());
        let mut rx = BroadcastRx::new(buffer.clone());
        let msg_id = MsgTypeId(1);
        let res = tx.transmit(4u32, msg_id, |mut bytes| {
            *(&mut bytes[0]) = 0xFFu8;
            *(&mut bytes[1]) = 0xF0u8;
            2
        });

        let mut one = 0;
        let mut two = 0;
        let res = rx.receive_next(|id, slice| {
            assert_eq!(1, id.0);
            one = slice[0];
            two = slice[1];
            assert_eq!(4, slice.len());
        });

        assert_eq!(one, 0xFFu8);
        assert_eq!(two, 0xF0u8);
    }

    #[test]
    fn test_slow_receiver() {
        let bytes = Bytes::heap_allocate(32 + TRAILER_SIZE);
        let buffer = BytesAtomicView::from_bytes(0, bytes.capacity(), &bytes);
        let mut tx = BroadcastTx::new(buffer.clone());
        let mut rx = BroadcastRx::new(buffer.clone());
        for i in 1..5 {
            let msg_id = MsgTypeId(i);
            let res = tx.transmit(4u32, msg_id, |mut bytes| {
                *(&mut bytes[0]) = i as u8;
                *(&mut bytes[1]) = i as u8;
                2
            });
            assert!(res.is_ok());
        }

        let res = rx.receive_next(|id, slice| {});
        assert_eq!(Err(RxErr::Overwritten), res);

        //next receive should receive the latest message
        let res = rx.receive_next(|id, slice| {
            assert_eq!(4, id.0);
            assert_eq!(4, slice[0]);
            assert_eq!(4, slice[1]);
        });
        assert_eq!(Ok(4 + HEADER_SIZE), res);
    }

    #[test]
    fn test_overwrite_during_read() {
        let bytes = Bytes::heap_allocate(32 + TRAILER_SIZE);
        let buffer = BytesAtomicView::from_bytes(0, bytes.capacity(), &bytes);
        let mut tx = BroadcastTx::new(buffer.clone());
        let mut rx = BroadcastRx::new(buffer.clone());
        let msg_id = MsgTypeId(1);
        let res = tx.transmit(4u32, msg_id, |mut bytes| {
            *(&mut bytes[0]) = 1 as u8;
            *(&mut bytes[1]) = 1 as u8;
            2
        });
        assert!(res.is_ok());

        //scenario while reading, publisher updates
        let res = rx.receive_next(|id, slice| {
            let tx = &mut tx;
            for i in 1..5 {
                tx.transmit(4u32, MsgTypeId(i), |mut bytes| {
                    *(&mut bytes[0]) = i as u8;
                    *(&mut bytes[1]) = i as u8;
                    2
                });
            }
        });
        assert_eq!(Err(RxErr::Overwritten), res);
        assert_eq!(1, rx.lapped_count());

        let res = rx.receive_next(|id, slice| {
            assert_eq!(id, MsgTypeId(4));
            assert_eq!(4, slice[0]);
            assert_eq!(4, slice[1]);
        });
        assert!(res.is_ok());
    }
    #[test]
    fn test_late_joiner_read_latest() {
        let bytes = Bytes::heap_allocate(32 + TRAILER_SIZE);
        let buffer = BytesAtomicView::from_bytes(0, bytes.capacity(), &bytes);
        let mut tx = BroadcastTx::new(buffer.clone());
        for i in 1..5 {
            let msg_id = MsgTypeId(i);
            let res = tx.transmit(4u32, msg_id, |mut bytes| {
                *(&mut bytes[0]) = i as u8;
                *(&mut bytes[1]) = i as u8;
                2
            });
            assert!(res.is_ok());
        }

        let mut rx = BroadcastRx::new(buffer.clone());

        //next receive should receive the latest message
        let res = rx.receive_next(|id, slice| {
            assert_eq!(4, id.0);
            assert_eq!(4, slice[0]);
            assert_eq!(4, slice[1]);
        });
        assert_eq!(Ok(4 + HEADER_SIZE), res);
    }
}
