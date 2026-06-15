# moq-mini

A minimal [MOQT (Media over QUIC Transport)](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/) implementation in Rust, built from scratch to *understand* the protocol.

## Approach

- Use Luke's [moq.dev](https://moq.dev) as a model of clean design, and draft-18 as the reference for wire format and terminology.
- Rename things to whatever makes sense to me.

## Roadmap

- [ ] rawQUIC and WebTransport
- [ ] Relay clustering
- [ ] Fetch support (MoQT/HTTP, Standalone/Joining)
- [ ] Graceful Shutdown (GOAWAY for session migration)
- [ ] Muxer / Demuxer (WebCodecs)
- [ ] MoQ Player (jitter buffer, ABR, SVC)
- [ ] JWT-based access control for relay
- [ ] TOML based configuration for relay
- [ ] Fallback (rawQUIC over TCP, WebTransport over HTTP/2)
- [ ] 0-RTT Setup
- [ ] QUIC-LB
