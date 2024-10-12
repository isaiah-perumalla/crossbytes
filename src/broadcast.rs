use crate::bytes::AtomicBuffer;

const TRAILER_SIZE : usize = 128;
const TAIL_INTENT_COUNTER_OFFSET : usize = 0usize;
const TAIL_COUNTER_OFFSET : usize = TAIL_INTENT_COUNTER_OFFSET + size_of::<u64>();
const LAST_COUNTER_OFFSET : usize = TAIL_COUNTER_OFFSET + size_of::<u64>();

pub enum RxErr {
    // No new messages available for consumption
    NoElement,
    // receiver is not consuming messages fast enough to keep up with the transmitter,
    // resulting in messages being overwritten thus making them no longer valid.
    Overwritten
    
} 
pub struct BroadcastRx<'a> {
    buffer: AtomicBuffer<'a>,
    tail_intent_counter_index: usize,
    tail_counter_index: usize,
    latest_counter_index: usize
}

impl<'a> BroadcastRx<'a> {
    pub fn new(buffer: AtomicBuffer<'a>) -> BroadcastRx<'a> {
        let length = buffer.len();
        let capacity = length - TRAILER_SIZE;
        assert!((capacity).is_power_of_two(), "invalid buffer size, not pow of 2 + TrailerLength");
        let tail_intent_counter_index = capacity + TAIL_INTENT_COUNTER_OFFSET;
        let tail_counter_index = capacity + TAIL_COUNTER_OFFSET;
        let latest_counter_index = capacity + LAST_COUNTER_OFFSET;
        BroadcastRx {buffer, tail_counter_index, tail_intent_counter_index, latest_counter_index}
    }
    
    pub fn receive_next<F>(&self, f:F) -> Result<usize, RxErr> 
    where F: Fn(&[u8]) -> usize {
        Err(RxErr::NoElement)
    }
}