#![deny(clippy::clone_on_ref_ptr)]
#![deny(clippy::missing_const_for_fn)]
#![deny(clippy::trivially_copy_pass_by_ref)]

pub mod admin;
pub mod aws;
pub mod config;
pub mod filters;
pub mod plugin;
pub mod prom;
pub mod serum;
pub mod sqs;
pub mod version;
