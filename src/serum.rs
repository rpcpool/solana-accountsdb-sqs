use {
    enumflags2::BitFlags,
    safe_transmute::transmute_one_pedantic,
    serum_dex::state::{
        Event, EventQueueHeader, QueueHeader, ACCOUNT_HEAD_PADDING, ACCOUNT_TAIL_PADDING,
    },
    std::{collections::HashSet, mem::size_of, str::FromStr},
};

// https://github.com/project-serum/serum-dex/blob/v0.5.6/dex/src/state.rs#L1138-L1146
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, BitFlags)]
#[repr(u8)]
pub enum EventFlag {
    Fill = 0x1,
    Out = 0x2,
    Bid = 0x4,
    Maker = 0x8,
    ReleaseFunds = 0x10,
}

impl FromStr for EventFlag {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Fill" => Ok(EventFlag::Fill),
            "Out" => Ok(EventFlag::Out),
            "Bid" => Ok(EventFlag::Bid),
            "Maker" => Ok(EventFlag::Maker),
            "ReleaseFunds" => Ok(EventFlag::ReleaseFunds),
            event => Err(format!("Unknown event: {}", event)),
        }
    }
}

impl EventFlag {
    pub fn as_str(self) -> &'static str {
        match self {
            EventFlag::Fill => "Fill",
            EventFlag::Out => "Out",
            EventFlag::Bid => "Bid",
            EventFlag::Maker => "Maker",
            EventFlag::ReleaseFunds => "ReleaseFunds",
        }
    }
}

pub fn match_events(
    data: &[u8],
    flags_names: &[(&Vec<BitFlags<EventFlag>>, &HashSet<String>)],
) -> Vec<String> {
    // Remove DEX padding
    let data = match dex_remove_padding(data) {
        Some(data) => data,
        None => return vec![],
    };

    // Get `head` and `count` from header
    let (header, events) = data.split_at(size_of::<EventQueueHeader>());
    let (head, count) = match transmute_one_pedantic::<EventQueueHeader>(header) {
        Ok(header) => (header.head() as usize, header.count() as usize),
        Err(_) => return vec![],
    };

    // Remove extra bytes from events
    let events = &events[0..(events.len() - events.len() % size_of::<Event>())];

    // Calculate number of events at head and tail
    let (tail_seg, head_seg) = events.split_at(head * size_of::<Event>());
    let head_len = count.min(head_seg.len() / size_of::<Event>());
    let tail_len = count - head_len;

    // Match flags
    let mut set = HashSet::new();
    for i in 0..head_len {
        for (flags, names) in flags_names {
            for flag in flags.iter() {
                if flag.bits() == head_seg[i * size_of::<Event>()] {
                    for name in names.iter() {
                        set.insert(name);
                    }
                    break;
                }
            }
        }
    }
    for i in 0..tail_len {
        for (flags, names) in flags_names {
            for flag in flags.iter() {
                if flag.bits() == tail_seg[i * size_of::<Event>()] {
                    for name in names.iter() {
                        set.insert(name);
                    }
                    break;
                }
            }
        }
    }
    set.into_iter().cloned().collect()
}

fn dex_remove_padding(data: &[u8]) -> Option<&[u8]> {
    if data.len() < ACCOUNT_HEAD_PADDING.len() + ACCOUNT_TAIL_PADDING.len() {
        return None;
    }
    let head = &data[..ACCOUNT_HEAD_PADDING.len()];
    if head != ACCOUNT_HEAD_PADDING {
        return None;
    }
    let tail = &data[data.len() - ACCOUNT_TAIL_PADDING.len()..];
    if tail != ACCOUNT_TAIL_PADDING {
        return None;
    }
    Some(&data[ACCOUNT_HEAD_PADDING.len()..(data.len() - ACCOUNT_TAIL_PADDING.len())])
}
