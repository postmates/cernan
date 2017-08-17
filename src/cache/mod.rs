//! A module for caches
//!
//! There are many structure in cernan which are copied repeatedly in memory if
//! we take the straightforward approach of allocating everything we need as we
//! need it. The problem with this is we end up fragmenting memory, especially
//! if the allocator keeps back memory from the operating system for lolspeed
//! reasons.

pub mod string;
