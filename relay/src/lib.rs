//! # moqt-relay: MOQT relay server
//!
//! A server that relays media data between publishers and subscribers.
//! When a subscriber requests a subscription to a namespace registered by a publisher,
//! the relay forwards the SUBSCRIBE and relays data streams.

pub(crate) mod cache;
mod control;
mod data;
mod fetch_handler;
pub mod relay;
mod state;
mod subscriber_relay;
