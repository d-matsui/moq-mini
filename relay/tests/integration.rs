use std::net::SocketAddr;
use std::sync::{Arc, Once};
use std::time::Instant;

use cli_lib::client::{self, TlsConfig};
use moqt::quic_config;
use moqt::session::{MoqtSession, SessionEvent};
use moqt::stream::read_varint;
use moqt::wire::parameter::{MessageParameter, SubscriptionFilter};
use moqt::wire::track_namespace::TrackNamespace;

static INIT: Once = Once::new();

fn init_crypto() {
    INIT.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("Failed to install rustls crypto provider");
    });
}

/// Helper: generate self-signed cert and return (cert_der, key_der)
fn gen_cert() -> (
    rustls_pki_types::CertificateDer<'static>,
    rustls_pki_types::PrivateKeyDer<'static>,
) {
    let rcgen::CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = rustls_pki_types::CertificateDer::from(cert);
    let key_der = rustls_pki_types::PrivateKeyDer::Pkcs8(
        rustls_pki_types::PrivatePkcs8KeyDer::from(key_pair.serialize_der()),
    );
    (cert_der, key_der)
}

/// Helper: start relay on a random port and return the endpoint + address.
async fn start_relay() -> (
    quinn::Endpoint,
    SocketAddr,
    rustls_pki_types::CertificateDer<'static>,
) {
    let (cert_der, key_der) = gen_cert();
    let server_config = quic_config::make_server_config(cert_der.clone(), key_der).unwrap();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let endpoint = quinn::Endpoint::server(server_config, addr).unwrap();
    let local_addr = endpoint.local_addr().unwrap();
    (endpoint, local_addr, cert_der)
}

/// Helper: connect a client to the relay and do SETUP exchange.
async fn connect_client(
    relay_addr: SocketAddr,
    cert_der: rustls_pki_types::CertificateDer<'static>,
) -> MoqtSession {
    client::connect(relay_addr, "localhost", TlsConfig::TrustCert(cert_der))
        .await
        .unwrap()
}

/// Helper: send PUBLISH_NAMESPACE and receive REQUEST_OK
async fn publish_namespace(session: &MoqtSession, namespace: TrackNamespace) {
    session.publish_namespace(namespace).await.unwrap();
}

// ============================================================
// Tests
// ============================================================

/// 3.1 + 3.2: QUIC connection + SETUP exchange
#[tokio::test]
async fn session_setup() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    // Spawn relay accept loop
    let ep = endpoint.clone();
    tokio::spawn(async move {
        if let Some(incoming) = ep.accept().await {
            let conn = incoming.await.unwrap();
            let wt_session = web_transport_quinn::Session::raw(
                conn,
                url::Url::parse("https://localhost").unwrap(),
                web_transport_quinn::http::StatusCode::OK,
            );
            let _session = MoqtSession::accept(wt_session).await.unwrap();
        }
    });

    let _session = Arc::new(connect_client(addr, cert_der).await);
    // If we get here, SETUP exchange succeeded
}

/// 4.1: PUBLISH_NAMESPACE -> REQUEST_OK
#[tokio::test]
async fn publish_namespace_registration() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    // Spawn relay
    let relay = relay::relay::Relay::new(endpoint);
    tokio::spawn(async move {
        relay.run().await.unwrap();
    });

    // Wait for relay to start
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let pub_session = Arc::new(connect_client(addr, cert_der).await);

    // Send PUBLISH_NAMESPACE
    publish_namespace(
        &pub_session,
        TrackNamespace::from(["daiki", "example"].as_slice()),
    )
    .await;
    // If we get here, registration succeeded
}

/// 4.2: SUBSCRIBE -> SUBSCRIBE_OK (via Relay)
#[tokio::test]
async fn subscribe_via_relay() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    // Spawn relay
    let relay = relay::relay::Relay::new(endpoint);
    tokio::spawn(async move {
        relay.run().await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Publisher connects and registers namespace
    let pub_session = Arc::new(connect_client(addr, cert_der.clone()).await);
    publish_namespace(
        &pub_session,
        TrackNamespace::from(["daiki", "example"].as_slice()),
    )
    .await;

    // Keep publisher connection alive for the duration of the test
    let _pub_session_keepalive = pub_session.clone();

    // Publisher: spawn task to accept SUBSCRIBE and respond with SUBSCRIBE_OK
    tokio::spawn(async move {
        let event = pub_session.next_event().await.unwrap();
        match event {
            SessionEvent::Subscribe(mut req) => {
                req.accept(1).await.unwrap();
            }
            _ => panic!("expected Subscribe event"),
        }
    });

    // Subscriber connects and sends SUBSCRIBE
    let sub_session = Arc::new(connect_client(addr, cert_der).await);
    let subscription = sub_session
        .subscribe(
            TrackNamespace::from(["daiki", "example"].as_slice()),
            "video",
            vec![MessageParameter::SubscriptionFilter(
                SubscriptionFilter::NextGroupStart,
            )],
        )
        .await
        .unwrap();

    assert_eq!(subscription.track_alias(), 1);
}

/// 5.1: Object data forwarding through Relay
#[tokio::test]
async fn object_forwarding() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    let relay = relay::relay::Relay::new(endpoint);
    tokio::spawn(async move {
        relay.run().await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Publisher setup
    let pub_session = Arc::new(connect_client(addr, cert_der.clone()).await);
    publish_namespace(
        &pub_session,
        TrackNamespace::from(["daiki", "example"].as_slice()),
    )
    .await;

    // Keep connection alive after the spawned task completes.
    let _pub_session_keepalive = pub_session.clone();
    let pub_handle = tokio::spawn(async move {
        let event = pub_session.next_event().await.unwrap();
        match event {
            SessionEvent::Subscribe(mut req) => {
                req.accept(1).await.unwrap();

                // Send a subgroup with 2 objects
                let mut group = pub_session.open_subgroup(1, 0, 0).await.unwrap();
                group.write_object(b"hello").await.unwrap();
                group.write_object(b"world").await.unwrap();
            }
            _ => panic!("expected Subscribe event"),
        }
    });

    // Subscriber setup
    let sub_session = Arc::new(connect_client(addr, cert_der).await);
    let _subscription = sub_session
        .subscribe(
            TrackNamespace::from(["daiki", "example"].as_slice()),
            "video",
            vec![],
        )
        .await
        .unwrap();

    // Wait for publisher to send objects
    pub_handle.await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Read forwarded objects on subscriber
    let event = sub_session.next_event().await.unwrap();
    match event {
        SessionEvent::DataStream(mut group) => {
            assert_eq!(group.track_alias(), 1);
            assert_eq!(group.group_id(), 0);

            let payload0 = group.read_object().await.unwrap().unwrap();
            assert_eq!(payload0, b"hello");

            let payload1 = group.read_object().await.unwrap().unwrap();
            assert_eq!(payload1, b"world");
        }
        _ => panic!("expected DataStream event"),
    }
}

/// 6.2: PUBLISH_DONE forwarding through Relay
#[tokio::test]
async fn publish_done_forwarding() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    let relay = relay::relay::Relay::new(endpoint);
    tokio::spawn(async move {
        relay.run().await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Publisher setup
    let pub_session = Arc::new(connect_client(addr, cert_der.clone()).await);
    publish_namespace(
        &pub_session,
        TrackNamespace::from(["daiki", "example"].as_slice()),
    )
    .await;

    // Publisher: accept SUBSCRIBE, respond, send object, then PUBLISH_DONE
    let _pub_session_keepalive = pub_session.clone();
    tokio::spawn(async move {
        let event = pub_session.next_event().await.unwrap();
        match event {
            SessionEvent::Subscribe(mut req) => {
                req.accept(1).await.unwrap();

                // Send one object
                let mut group = pub_session.open_subgroup(1, 0, 0).await.unwrap();
                group.write_object(b"done").await.unwrap();
                group.finish().unwrap();

                // Send PUBLISH_DONE on the bidi stream
                req.send_publish_done(1).await.unwrap();
            }
            _ => panic!("expected Subscribe event"),
        }
    });

    // Subscriber setup
    let sub_session = Arc::new(connect_client(addr, cert_der).await);
    let mut subscription = sub_session
        .subscribe(
            TrackNamespace::from(["daiki", "example"].as_slice()),
            "video",
            vec![],
        )
        .await
        .unwrap();

    // Read PUBLISH_DONE (forwarded by relay)
    let publish_done = subscription.recv_publish_done().await.unwrap().unwrap();
    assert_eq!(publish_done.status_code, 0x2); // TRACK_ENDED
    assert_eq!(publish_done.stream_count, 1);
}

/// 5.3: Multiple Groups forwarded through Relay
#[tokio::test]
async fn multiple_groups() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    let relay = relay::relay::Relay::new(endpoint);
    tokio::spawn(async move { relay.run().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let pub_session = Arc::new(connect_client(addr, cert_der.clone()).await);
    publish_namespace(
        &pub_session,
        TrackNamespace::from(["daiki", "example"].as_slice()),
    )
    .await;

    // Publisher: accept SUBSCRIBE, send 3 groups with 2 objects each
    let _pub_session_keepalive = pub_session.clone();
    let pub_handle = tokio::spawn(async move {
        let event = pub_session.next_event().await.unwrap();
        match event {
            SessionEvent::Subscribe(mut req) => {
                req.accept(1).await.unwrap();

                for group_id in 0u64..3 {
                    let mut group = pub_session.open_subgroup(1, group_id, 0).await.unwrap();
                    for obj_id in 0u64..2 {
                        let payload = format!("g{group_id}o{obj_id}");
                        group.write_object(payload.as_bytes()).await.unwrap();
                    }
                }

                // PUBLISH_DONE
                req.send_publish_done(3).await.unwrap();
            }
            _ => panic!("expected Subscribe event"),
        }
    });

    // Subscriber
    let sub_session = Arc::new(connect_client(addr, cert_der).await);
    let mut subscription = sub_session
        .subscribe(
            TrackNamespace::from(["daiki", "example"].as_slice()),
            "video",
            vec![],
        )
        .await
        .unwrap();

    pub_handle.await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Receive 3 groups
    let mut received_groups: Vec<(u64, Vec<String>)> = Vec::new();
    for _ in 0..3 {
        let event = sub_session.next_event().await.unwrap();
        match event {
            SessionEvent::DataStream(mut group) => {
                let group_id = group.group_id();
                let mut payloads = Vec::new();
                while let Some(payload) = group.read_object().await.unwrap() {
                    payloads.push(String::from_utf8(payload).unwrap());
                }
                received_groups.push((group_id, payloads));
            }
            _ => panic!("expected DataStream event"),
        }
    }

    // Sort by group_id (streams may arrive out of order)
    received_groups.sort_by_key(|(gid, _)| *gid);

    assert_eq!(received_groups.len(), 3);
    assert_eq!(
        received_groups[0],
        (0, vec!["g0o0".to_string(), "g0o1".to_string()])
    );
    assert_eq!(
        received_groups[1],
        (1, vec!["g1o0".to_string(), "g1o1".to_string()])
    );
    assert_eq!(
        received_groups[2],
        (2, vec!["g2o0".to_string(), "g2o1".to_string()])
    );

    // Verify PUBLISH_DONE
    let publish_done = subscription.recv_publish_done().await.unwrap().unwrap();
    assert_eq!(publish_done.stream_count, 3);
}

/// 7.2: Late join -- Subscriber connects while Publisher is already sending
#[tokio::test]
async fn late_join() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    let relay = relay::relay::Relay::new(endpoint);
    tokio::spawn(async move { relay.run().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Publisher connects first and registers
    let pub_session = Arc::new(connect_client(addr, cert_der.clone()).await);
    publish_namespace(
        &pub_session,
        TrackNamespace::from(["daiki", "example"].as_slice()),
    )
    .await;

    // Publisher: accept SUBSCRIBE (will come later), respond, send objects
    let _pub_session_keepalive = pub_session.clone();
    let pub_handle = tokio::spawn(async move {
        let event = pub_session.next_event().await.unwrap();
        match event {
            SessionEvent::Subscribe(mut req) => {
                req.accept(1).await.unwrap();

                // Send 2 groups after subscriber joins
                for group_id in 0u64..2 {
                    let mut group = pub_session.open_subgroup(1, group_id, 0).await.unwrap();
                    let payload = format!("late-g{group_id}");
                    group.write_object(payload.as_bytes()).await.unwrap();
                }

                req.send_publish_done(2).await.unwrap();
            }
            _ => panic!("expected Subscribe event"),
        }
    });

    // Subscriber connects AFTER publisher is ready (simulating late join)
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let sub_session = Arc::new(connect_client(addr, cert_der).await);
    let _subscription = sub_session
        .subscribe(
            TrackNamespace::from(["daiki", "example"].as_slice()),
            "video",
            vec![MessageParameter::SubscriptionFilter(
                SubscriptionFilter::NextGroupStart,
            )],
        )
        .await
        .unwrap();

    pub_handle.await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Should receive objects sent after subscription
    let event = sub_session.next_event().await.unwrap();
    match event {
        SessionEvent::DataStream(mut group) => {
            assert_eq!(group.track_alias(), 1);

            let payload = group.read_object().await.unwrap().unwrap();
            let payload_str = std::str::from_utf8(&payload).unwrap();
            assert!(payload_str.starts_with("late-g"));
        }
        _ => panic!("expected DataStream event"),
    }
}

/// 3.1: ALPN mismatch -- connection should fail
#[tokio::test]
async fn alpn_mismatch() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    // Spawn relay
    let relay = relay::relay::Relay::new(endpoint);
    tokio::spawn(async move { relay.run().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Create client with wrong ALPN
    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(cert_der).unwrap();
    let mut client_crypto = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![b"wrong-alpn".to_vec()];

    let quic_client_config =
        quinn::crypto::rustls::QuicClientConfig::try_from(client_crypto).unwrap();
    let client_config = quinn::ClientConfig::new(std::sync::Arc::new(quic_client_config));

    let mut client_endpoint = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
    client_endpoint.set_default_client_config(client_config);

    let result = client_endpoint.connect(addr, "localhost").unwrap().await;

    assert!(result.is_err(), "connection with wrong ALPN should fail");
}

/// 4.3: SUBSCRIBE to unknown namespace -> REQUEST_ERROR
#[tokio::test]
async fn subscribe_unknown_namespace() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    let relay = relay::relay::Relay::new(endpoint);
    tokio::spawn(async move { relay.run().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Subscriber connects (no publisher registered)
    let sub_session = Arc::new(connect_client(addr, cert_der).await);
    let result = sub_session
        .subscribe(
            TrackNamespace::from(["nonexistent"].as_slice()),
            "video",
            vec![],
        )
        .await;

    // session.subscribe() returns an error when REQUEST_ERROR is received
    let err = result
        .err()
        .expect("subscribe should fail for unknown namespace");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("rejected"),
        "error should indicate rejection: {err_msg}"
    );
}

/// 6.1: Multiple subscribers receive the same objects
#[tokio::test]
async fn multiple_subscribers() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    let relay = relay::relay::Relay::new(endpoint);
    tokio::spawn(async move { relay.run().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Publisher setup
    let pub_session = Arc::new(connect_client(addr, cert_der.clone()).await);
    publish_namespace(
        &pub_session,
        TrackNamespace::from(["daiki", "example"].as_slice()),
    )
    .await;

    // Publisher: accept 1 SUBSCRIBE (aggregation means only one arrives),
    // send objects, then PUBLISH_DONE.
    let _pub_session_keepalive = pub_session.clone();
    let pub_handle = tokio::spawn(async move {
        let event = pub_session.next_event().await.unwrap();
        let mut req = match event {
            SessionEvent::Subscribe(req) => req,
            _ => panic!("expected Subscribe event"),
        };
        req.accept(1).await.unwrap();

        // Wait for both subscribers to be registered via aggregation
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Send 1 group with 1 object
        let mut group = pub_session.open_subgroup(1, 0, 0).await.unwrap();
        group.write_object(b"shared").await.unwrap();
        group.finish().unwrap();

        req.send_publish_done(1).await.unwrap();
    });

    // Helper to subscribe and receive objects
    async fn subscribe_and_receive(
        addr: SocketAddr,
        cert_der: rustls_pki_types::CertificateDer<'static>,
    ) -> Vec<u8> {
        let session = Arc::new(connect_client(addr, cert_der).await);
        let _subscription = session
            .subscribe(
                TrackNamespace::from(["daiki", "example"].as_slice()),
                "video",
                vec![],
            )
            .await
            .unwrap();

        // Wait for objects
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let event = session.next_event().await.unwrap();
        match event {
            SessionEvent::DataStream(mut group) => group.read_object().await.unwrap().unwrap(),
            _ => panic!("expected DataStream event"),
        }
    }

    // Two subscribers connect concurrently.
    // Per-track lock ensures only one SUBSCRIBE reaches the publisher.
    let sub1 = tokio::spawn(subscribe_and_receive(addr, cert_der.clone()));
    let sub2 = tokio::spawn(subscribe_and_receive(addr, cert_der));

    pub_handle.await.unwrap();

    let payload1 = sub1.await.unwrap();
    let payload2 = sub2.await.unwrap();

    assert_eq!(payload1, b"shared");
    assert_eq!(payload2, b"shared");
}

/// 5.4: Multiple tracks -- video and audio simultaneously
#[tokio::test]
async fn multiple_tracks() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    let relay = relay::relay::Relay::new(endpoint);
    tokio::spawn(async move { relay.run().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Publisher
    let pub_session = Arc::new(connect_client(addr, cert_der.clone()).await);
    publish_namespace(
        &pub_session,
        TrackNamespace::from(["daiki", "example"].as_slice()),
    )
    .await;

    // Publisher: accept 2 SUBSCRIBEs (video + audio), send objects on each
    let _pub_session_keepalive = pub_session.clone();
    let pub_handle = tokio::spawn(async move {
        // Accept SUBSCRIBE for video (alias=1)
        let event_v = pub_session.next_event().await.unwrap();
        let mut req_v = match event_v {
            SessionEvent::Subscribe(req) => req,
            _ => panic!("expected Subscribe event"),
        };
        req_v.accept(1).await.unwrap();

        // Accept SUBSCRIBE for audio (alias=2)
        let event_a = pub_session.next_event().await.unwrap();
        let mut req_a = match event_a {
            SessionEvent::Subscribe(req) => req,
            _ => panic!("expected Subscribe event"),
        };
        req_a.accept(2).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Send video object
        let mut group_v = pub_session.open_subgroup(1, 0, 0).await.unwrap();
        group_v.write_object(b"video").await.unwrap();

        // Send audio object
        let mut group_a = pub_session.open_subgroup(2, 0, 0).await.unwrap();
        group_a.write_object(b"audio").await.unwrap();

        // PUBLISH_DONE on both
        req_v.send_publish_done(1).await.unwrap();
        req_a.send_publish_done(1).await.unwrap();
    });

    // Subscriber: subscribe to both tracks
    let sub_session = Arc::new(connect_client(addr, cert_der).await);

    // Subscribe to video
    let _sub_v = sub_session
        .subscribe(
            TrackNamespace::from(["daiki", "example"].as_slice()),
            "video",
            vec![],
        )
        .await
        .unwrap();

    // Subscribe to audio
    let _sub_a = sub_session
        .subscribe(
            TrackNamespace::from(["daiki", "example"].as_slice()),
            "audio",
            vec![],
        )
        .await
        .unwrap();

    pub_handle.await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Receive 2 uni streams (video + audio, order may vary)
    let mut payloads: Vec<String> = Vec::new();
    for _ in 0..2 {
        let event = sub_session.next_event().await.unwrap();
        match event {
            SessionEvent::DataStream(mut group) => {
                let payload = group.read_object().await.unwrap().unwrap();
                payloads.push(String::from_utf8(payload).unwrap());
            }
            _ => panic!("expected DataStream event"),
        }
    }

    payloads.sort();
    assert_eq!(payloads, vec!["audio", "video"]);
}

/// Subscription aggregation: second subscriber reuses the upstream subscription.
/// The publisher should receive only ONE SUBSCRIBE, and both subscribers
/// should receive the forwarded data.
#[tokio::test]
async fn subscription_aggregation() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    let relay = relay::relay::Relay::new(endpoint);
    tokio::spawn(async move { relay.run().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Publisher
    let pub_session = Arc::new(connect_client(addr, cert_der.clone()).await);
    publish_namespace(
        &pub_session,
        TrackNamespace::from(["daiki", "example"].as_slice()),
    )
    .await;

    // Publisher: accept exactly 1 SUBSCRIBE, send data, then PUBLISH_DONE
    let pub_handle = tokio::spawn(async move {
        let event = pub_session.next_event().await.unwrap();
        let mut req = match event {
            SessionEvent::Subscribe(req) => req,
            _ => panic!("expected Subscribe event"),
        };
        req.accept(1).await.unwrap();

        // Wait for both subscribers to be registered
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Send 1 group with 1 object
        let mut group = pub_session.open_subgroup(1, 0, 0).await.unwrap();
        group.write_object(b"aggr").await.unwrap();
        group.finish().unwrap();

        req.send_publish_done(1).await.unwrap();

        // Verify no second SUBSCRIBE arrives (would timeout)
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            pub_session.next_event(),
        )
        .await;
        assert!(
            result.is_err(),
            "publisher should NOT receive a second SUBSCRIBE"
        );
    });

    // Subscriber 1: subscribe and wait for SUBSCRIBE_OK
    let sub1_session = Arc::new(connect_client(addr, cert_der.clone()).await);
    let _sub1 = sub1_session
        .subscribe(
            TrackNamespace::from(["daiki", "example"].as_slice()),
            "video",
            vec![],
        )
        .await
        .unwrap();

    // Subscriber 2: subscribe AFTER sub1 is established (triggers aggregation)
    let sub2_session = Arc::new(connect_client(addr, cert_der).await);
    let _sub2 = sub2_session
        .subscribe(
            TrackNamespace::from(["daiki", "example"].as_slice()),
            "video",
            vec![],
        )
        .await
        .unwrap();

    // Both subscribers receive data
    let recv1 = tokio::spawn(async move {
        let event = sub1_session.next_event().await.unwrap();
        match event {
            SessionEvent::DataStream(mut group) => group.read_object().await.unwrap().unwrap(),
            _ => panic!("expected DataStream event"),
        }
    });

    let recv2 = tokio::spawn(async move {
        let event = sub2_session.next_event().await.unwrap();
        match event {
            SessionEvent::DataStream(mut group) => group.read_object().await.unwrap().unwrap(),
            _ => panic!("expected DataStream event"),
        }
    });

    pub_handle.await.unwrap();

    let payload1 = recv1.await.unwrap();
    let payload2 = recv2.await.unwrap();

    assert_eq!(payload1, b"aggr");
    assert_eq!(payload2, b"aggr");
}

/// 6.3: Subscriber disconnect -- relay cleans up, publisher continues
#[tokio::test]
async fn subscriber_disconnect() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    let relay = relay::relay::Relay::new(endpoint);
    tokio::spawn(async move { relay.run().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let pub_session = Arc::new(connect_client(addr, cert_der.clone()).await);
    publish_namespace(
        &pub_session,
        TrackNamespace::from(["daiki", "example"].as_slice()),
    )
    .await;

    // Publisher: accept SUBSCRIBE, respond, send objects continuously
    let _pub_session_keepalive = pub_session.clone();
    let pub_handle = tokio::spawn(async move {
        let event = pub_session.next_event().await.unwrap();
        match event {
            SessionEvent::Subscribe(mut req) => {
                req.accept(1).await.unwrap();

                // Send a few groups
                for group_id in 0u64..3 {
                    let mut group = pub_session.open_subgroup(1, group_id, 0).await.unwrap();
                    group.write_object(b"data").await.unwrap();
                    group.finish().unwrap();
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }

                // Publisher session should still be usable after subscriber disconnects.
                // (Previously verified via connection().close_reason(), now
                // implicitly validated by the successful group writes above.)
            }
            _ => panic!("expected Subscribe event"),
        }
    });

    // Subscriber connects, subscribes, receives 1 group, then disconnects
    {
        let sub_session = Arc::new(connect_client(addr, cert_der).await);
        let _subscription = sub_session
            .subscribe(
                TrackNamespace::from(["daiki", "example"].as_slice()),
                "video",
                vec![],
            )
            .await
            .unwrap();

        // Receive at least 1 object
        let event = sub_session.next_event().await.unwrap();
        match event {
            SessionEvent::DataStream(mut group) => {
                let _ = group.read_object().await.unwrap();
            }
            _ => panic!("expected DataStream event"),
        }

        // Disconnect subscriber
        sub_session.close();
    }

    // Publisher should complete without error
    pub_handle.await.unwrap();
}

// ============================================================
// WebTransport tests
// ============================================================

/// Helper: connect a WebTransport client to the relay and do SETUP exchange.
async fn connect_webtransport_client(
    relay_addr: SocketAddr,
    cert_der: rustls_pki_types::CertificateDer<'static>,
) -> MoqtSession {
    // Build a quinn client with h3 ALPN that trusts the self-signed cert
    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(cert_der).unwrap();
    let mut client_crypto = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![web_transport_quinn::ALPN.as_bytes().to_vec()];

    let quic_client_config =
        quinn::crypto::rustls::QuicClientConfig::try_from(client_crypto).unwrap();
    let client_config = quinn::ClientConfig::new(Arc::new(quic_client_config));

    let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(client_config);

    // QUIC connect, then WebTransport (HTTP/3 CONNECT) handshake
    let connection = endpoint
        .connect(relay_addr, "localhost")
        .unwrap()
        .await
        .expect("QUIC connect failed");
    let url = url::Url::parse(&format!("https://localhost:{}", relay_addr.port())).unwrap();
    let wt_session = web_transport_quinn::Session::connect(connection, url)
        .await
        .expect("WebTransport handshake failed");
    MoqtSession::connect(wt_session)
        .await
        .expect("MOQT SETUP failed over WebTransport")
}

/// WebTransport: SETUP exchange succeeds
#[tokio::test]
async fn webtransport_session_setup() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    let relay = relay::relay::Relay::new(endpoint);
    tokio::spawn(async move { relay.run().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let _session = connect_webtransport_client(addr, cert_der).await;
    // If we get here, WebTransport SETUP exchange succeeded
}

/// WebTransport: end-to-end object forwarding (WT publisher -> relay -> WT subscriber)
#[tokio::test]
async fn webtransport_object_forwarding() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    let relay = relay::relay::Relay::new(endpoint);
    tokio::spawn(async move { relay.run().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Publisher (WebTransport)
    let pub_session = Arc::new(connect_webtransport_client(addr, cert_der.clone()).await);
    publish_namespace(
        &pub_session,
        TrackNamespace::from(["daiki", "example"].as_slice()),
    )
    .await;

    let _pub_keepalive = pub_session.clone();
    let pub_handle = tokio::spawn(async move {
        let event = pub_session.next_event().await.unwrap();
        match event {
            SessionEvent::Subscribe(mut req) => {
                req.accept(1).await.unwrap();
                let mut group = pub_session.open_subgroup(1, 0, 0).await.unwrap();
                group.write_object(b"hello-wt").await.unwrap();
                group.write_object(b"world-wt").await.unwrap();
            }
            _ => panic!("expected Subscribe event"),
        }
    });

    // Subscriber (WebTransport)
    let sub_session = Arc::new(connect_webtransport_client(addr, cert_der).await);
    let _subscription = sub_session
        .subscribe(
            TrackNamespace::from(["daiki", "example"].as_slice()),
            "video",
            vec![],
        )
        .await
        .unwrap();

    match sub_session.next_event().await.unwrap() {
        SessionEvent::DataStream(mut group) => {
            let obj1 = group.read_object().await.unwrap().unwrap();
            assert_eq!(obj1, b"hello-wt");
            let obj2 = group.read_object().await.unwrap().unwrap();
            assert_eq!(obj2, b"world-wt");
        }
        _ => panic!("expected DataStream event"),
    }

    pub_handle.await.unwrap();
}

/// Cross-transport: raw QUIC publisher -> relay -> WebTransport subscriber
#[tokio::test]
async fn cross_transport_quic_to_webtransport() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    let relay = relay::relay::Relay::new(endpoint);
    tokio::spawn(async move { relay.run().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Publisher (raw QUIC)
    let pub_session = Arc::new(connect_client(addr, cert_der.clone()).await);
    publish_namespace(
        &pub_session,
        TrackNamespace::from(["daiki", "example"].as_slice()),
    )
    .await;

    let _pub_keepalive = pub_session.clone();
    let pub_handle = tokio::spawn(async move {
        let event = pub_session.next_event().await.unwrap();
        match event {
            SessionEvent::Subscribe(mut req) => {
                req.accept(1).await.unwrap();
                let mut group = pub_session.open_subgroup(1, 0, 0).await.unwrap();
                group.write_object(b"cross-transport").await.unwrap();
            }
            _ => panic!("expected Subscribe event"),
        }
    });

    // Subscriber (WebTransport)
    let sub_session = Arc::new(connect_webtransport_client(addr, cert_der).await);
    let _subscription = sub_session
        .subscribe(
            TrackNamespace::from(["daiki", "example"].as_slice()),
            "video",
            vec![],
        )
        .await
        .unwrap();

    match sub_session.next_event().await.unwrap() {
        SessionEvent::DataStream(mut group) => {
            let obj = group.read_object().await.unwrap().unwrap();
            assert_eq!(obj, b"cross-transport");
        }
        _ => panic!("expected DataStream event"),
    }

    pub_handle.await.unwrap();
}

// ============================================================
// LargestObject filter tests
// ============================================================

/// LargestObject filter on empty track: SUBSCRIBE_OK should have no LARGEST_OBJECT param.
#[tokio::test]
async fn largest_object_empty_track() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    let relay = relay::relay::Relay::new(endpoint);
    tokio::spawn(async move { relay.run().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Publisher
    let pub_session = Arc::new(connect_client(addr, cert_der.clone()).await);
    publish_namespace(
        &pub_session,
        TrackNamespace::from(["daiki", "example"].as_slice()),
    )
    .await;

    // Publisher: accept SUBSCRIBE and respond
    let _pub_keepalive = pub_session.clone();
    tokio::spawn(async move {
        let event = pub_session.next_event().await.unwrap();
        match event {
            SessionEvent::Subscribe(mut req) => {
                req.accept(1).await.unwrap();
            }
            _ => panic!("expected Subscribe event"),
        }
    });

    // Subscriber: SUBSCRIBE with LargestObject filter
    let sub_session = Arc::new(connect_client(addr, cert_der).await);
    let subscription = sub_session
        .subscribe(
            TrackNamespace::from(["daiki", "example"].as_slice()),
            "video",
            vec![MessageParameter::SubscriptionFilter(
                SubscriptionFilter::LargestObject,
            )],
        )
        .await
        .unwrap();

    // SUBSCRIBE_OK should not have LARGEST_OBJECT (no objects published yet)
    let has_largest = subscription
        .subscribe_ok
        .parameters
        .iter()
        .any(|p| matches!(p, MessageParameter::LargestObject { .. }));
    assert!(!has_largest, "empty track should not have LARGEST_OBJECT");
}

/// LargestObject: subscriber receives SUBSCRIBE_OK with correct position
/// and gets subsequent objects.
#[tokio::test]
async fn largest_object_returns_position_and_receives_data() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    let relay = relay::relay::Relay::new(endpoint);
    tokio::spawn(async move { relay.run().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Synchronization: publisher waits for late subscriber to join before sending group 2
    let sub2_ready = Arc::new(tokio::sync::Notify::new());

    // Publisher
    let pub_session = Arc::new(connect_client(addr, cert_der.clone()).await);
    publish_namespace(
        &pub_session,
        TrackNamespace::from(["daiki", "example"].as_slice()),
    )
    .await;

    // Publisher: accept SUBSCRIBE, send 2 groups, wait for signal, then send 1 more
    let _pub_keepalive = pub_session.clone();
    let sub2_ready_pub = sub2_ready.clone();
    let pub_handle = tokio::spawn(async move {
        let event = pub_session.next_event().await.unwrap();
        match event {
            SessionEvent::Subscribe(mut req) => {
                req.accept(1).await.unwrap();

                // Send 2 groups before the late subscriber connects
                for group_id in 0u64..2 {
                    let mut group = pub_session.open_subgroup(1, group_id, 0).await.unwrap();
                    for obj_id in 0u64..3 {
                        let payload = format!("g{group_id}o{obj_id}");
                        group.write_object(payload.as_bytes()).await.unwrap();
                    }
                }

                // Wait for late subscriber to be ready
                sub2_ready_pub.notified().await;

                // Send 1 more group (after late subscriber joins)
                let mut group = pub_session.open_subgroup(1, 2, 0).await.unwrap();
                group.write_object(b"g2o0").await.unwrap();
                group.finish().unwrap();

                req.send_publish_done(3).await.unwrap();
            }
            _ => panic!("expected Subscribe event"),
        }
    });

    // First subscriber (NextGroupStart) to establish the upstream subscription
    let sub1_session = Arc::new(connect_client(addr, cert_der.clone()).await);
    let _sub1 = sub1_session
        .subscribe(
            TrackNamespace::from(["daiki", "example"].as_slice()),
            "video",
            vec![MessageParameter::SubscriptionFilter(
                SubscriptionFilter::NextGroupStart,
            )],
        )
        .await
        .unwrap();

    // Wait for publisher to send 2 groups (they get cached)
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Late subscriber with LargestObject filter
    let sub2_session = Arc::new(connect_client(addr, cert_der).await);
    let sub2 = sub2_session
        .subscribe(
            TrackNamespace::from(["daiki", "example"].as_slice()),
            "video",
            vec![MessageParameter::SubscriptionFilter(
                SubscriptionFilter::LargestObject,
            )],
        )
        .await
        .unwrap();

    // Check SUBSCRIBE_OK has LARGEST_OBJECT parameter
    let largest = sub2.subscribe_ok.parameters.iter().find_map(|p| match p {
        MessageParameter::LargestObject { group, object } => Some((*group, *object)),
        _ => None,
    });
    assert!(
        largest.is_some(),
        "SUBSCRIBE_OK should contain LARGEST_OBJECT"
    );
    let (lg, lo) = largest.unwrap();
    // Publisher sent groups 0,1 with 3 objects each (0,1,2)
    // Largest should be (1, 2)
    assert_eq!(lg, 1, "largest group should be 1");
    assert_eq!(lo, 2, "largest object should be 2");

    // Signal publisher to send group 2
    sub2_ready.notify_one();

    // Late subscriber should receive group 2 (sent after join)
    let event = sub2_session.next_event().await.unwrap();
    match event {
        SessionEvent::DataStream(mut group) => {
            assert_eq!(group.group_id(), 2);
            let payload = group.read_object().await.unwrap().unwrap();
            assert_eq!(payload, b"g2o0");
        }
        _ => panic!("expected DataStream event"),
    }

    pub_handle.await.unwrap();
}

/// Cross-transport: WebTransport publisher -> relay -> raw QUIC subscriber
#[tokio::test]
async fn cross_transport_webtransport_to_quic() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    let relay = relay::relay::Relay::new(endpoint);
    tokio::spawn(async move { relay.run().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Publisher (WebTransport)
    let pub_session = Arc::new(connect_webtransport_client(addr, cert_der.clone()).await);
    publish_namespace(
        &pub_session,
        TrackNamespace::from(["daiki", "example"].as_slice()),
    )
    .await;

    let _pub_keepalive = pub_session.clone();
    let pub_handle = tokio::spawn(async move {
        let event = pub_session.next_event().await.unwrap();
        match event {
            SessionEvent::Subscribe(mut req) => {
                req.accept(1).await.unwrap();
                let mut group = pub_session.open_subgroup(1, 0, 0).await.unwrap();
                group.write_object(b"wt-to-quic").await.unwrap();
            }
            _ => panic!("expected Subscribe event"),
        }
    });

    // Subscriber (raw QUIC)
    let sub_session = Arc::new(connect_client(addr, cert_der).await);
    let _subscription = sub_session
        .subscribe(
            TrackNamespace::from(["daiki", "example"].as_slice()),
            "video",
            vec![],
        )
        .await
        .unwrap();

    match sub_session.next_event().await.unwrap() {
        SessionEvent::DataStream(mut group) => {
            let obj = group.read_object().await.unwrap().unwrap();
            assert_eq!(obj, b"wt-to-quic");
        }
        _ => panic!("expected DataStream event"),
    }

    pub_handle.await.unwrap();
}

// ============================================================
// FETCH tests
// ============================================================

/// Helper: read objects from a FETCH_HEADER uni stream.
async fn read_fetch_objects(
    stream: &mut web_transport_quinn::RecvStream,
) -> Vec<(u64, u64, Vec<u8>)> {
    let mut objects = Vec::new();
    let mut prev_group: Option<u64> = None;

    loop {
        let flags = match moqt::stream::try_read_varint(stream).await.unwrap() {
            Some((v, _)) => v,
            None => break,
        };

        let has_object_id = flags & 0x04 != 0;
        let has_group_id = flags & 0x08 != 0;
        let has_priority = flags & 0x10 != 0;
        let subgroup_mode = flags & 0x03;

        let group_id = if has_group_id {
            let (v, _) = read_varint(stream).await.unwrap();
            v
        } else {
            prev_group.unwrap_or(0)
        };

        if subgroup_mode == 0x03 {
            let _ = read_varint(stream).await.unwrap();
        }

        let object_id = if has_object_id {
            let (v, _) = read_varint(stream).await.unwrap();
            v
        } else {
            0
        };

        if has_priority {
            let mut buf = [0u8; 1];
            stream.read_exact(&mut buf).await.unwrap();
        }

        let (payload_len, _) = read_varint(stream).await.unwrap();
        let mut payload = vec![0u8; payload_len as usize];
        if payload_len > 0 {
            stream.read_exact(&mut payload).await.unwrap();
        }

        prev_group = Some(group_id);
        objects.push((group_id, object_id, payload));
    }

    objects
}

/// Basic Joining Fetch: SUBSCRIBE(LargestObject) then FETCH retrieves cached objects.
#[tokio::test]
async fn fetch_joining_basic() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    let relay = relay::relay::Relay::new(endpoint);
    tokio::spawn(async move { relay.run().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let sub2_ready = Arc::new(tokio::sync::Notify::new());

    let pub_session = Arc::new(connect_client(addr, cert_der.clone()).await);
    publish_namespace(
        &pub_session,
        TrackNamespace::from(["daiki", "example"].as_slice()),
    )
    .await;

    let _pub_keepalive = pub_session.clone();
    let sub2_ready_pub = sub2_ready.clone();
    let pub_handle = tokio::spawn(async move {
        let event = pub_session.next_event().await.unwrap();
        match event {
            SessionEvent::Subscribe(mut req) => {
                req.accept(1).await.unwrap();

                for group_id in 0u64..3 {
                    let mut group = pub_session.open_subgroup(1, group_id, 0).await.unwrap();
                    for obj_id in 0u64..2 {
                        let payload = format!("g{group_id}o{obj_id}");
                        group.write_object(payload.as_bytes()).await.unwrap();
                    }
                }

                sub2_ready_pub.notified().await;
                req.send_publish_done(3).await.unwrap();
            }
            _ => panic!("expected Subscribe event"),
        }
    });

    let sub1_session = Arc::new(connect_client(addr, cert_der.clone()).await);
    let _sub1 = sub1_session
        .subscribe(
            TrackNamespace::from(["daiki", "example"].as_slice()),
            "video",
            vec![MessageParameter::SubscriptionFilter(
                SubscriptionFilter::NextGroupStart,
            )],
        )
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let sub2_session = Arc::new(connect_client(addr, cert_der).await);
    let sub2 = sub2_session
        .subscribe(
            TrackNamespace::from(["daiki", "example"].as_slice()),
            "video",
            vec![MessageParameter::SubscriptionFilter(
                SubscriptionFilter::LargestObject,
            )],
        )
        .await
        .unwrap();

    let largest = sub2.subscribe_ok.parameters.iter().find_map(|p| match p {
        MessageParameter::LargestObject { group, object } => Some((*group, *object)),
        _ => None,
    });
    assert_eq!(largest, Some((2, 1)));

    let fetch_ok = sub2_session.fetch(2, 0, 2).await.unwrap();
    assert!(!fetch_ok.end_of_track);

    let mut fetch_stream = sub2_session.accept_uni_stream().await.unwrap();
    let (header_type, _) = read_varint(&mut fetch_stream).await.unwrap();
    assert_eq!(header_type, 0x05);
    let (header_req_id, _) = read_varint(&mut fetch_stream).await.unwrap();
    assert_eq!(header_req_id, 2);

    let objects = read_fetch_objects(&mut fetch_stream).await;
    assert_eq!(objects.len(), 6);
    assert_eq!(objects[0], (0, 0, b"g0o0".to_vec()));
    assert_eq!(objects[1], (0, 1, b"g0o1".to_vec()));
    assert_eq!(objects[2], (1, 0, b"g1o0".to_vec()));
    assert_eq!(objects[3], (1, 1, b"g1o1".to_vec()));
    assert_eq!(objects[4], (2, 0, b"g2o0".to_vec()));
    assert_eq!(objects[5], (2, 1, b"g2o1".to_vec()));

    sub2_ready.notify_one();
    pub_handle.await.unwrap();
}

/// FETCH with invalid joining request ID returns REQUEST_ERROR.
#[tokio::test]
async fn fetch_invalid_joining_request_id() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    let relay = relay::relay::Relay::new(endpoint);
    tokio::spawn(async move { relay.run().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let pub_session = Arc::new(connect_client(addr, cert_der.clone()).await);
    publish_namespace(
        &pub_session,
        TrackNamespace::from(["daiki", "example"].as_slice()),
    )
    .await;

    let _pub_keepalive = pub_session.clone();
    tokio::spawn(async move {
        let event = pub_session.next_event().await.unwrap();
        match event {
            SessionEvent::Subscribe(mut req) => {
                req.accept(1).await.unwrap();
            }
            _ => panic!("expected Subscribe event"),
        }
    });

    let sub_session = Arc::new(connect_client(addr, cert_der).await);
    let _sub = sub_session
        .subscribe(
            TrackNamespace::from(["daiki", "example"].as_slice()),
            "video",
            vec![MessageParameter::SubscriptionFilter(
                SubscriptionFilter::LargestObject,
            )],
        )
        .await
        .unwrap();

    let result = sub_session.fetch(2, 99, 1).await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("rejected"), "should be rejected: {err}");
}

/// Compare time-to-first-object between NextGroupStart (no FETCH) and
/// LargestObject + Joining FETCH.
///
/// Publisher sends groups at 300ms intervals. Two subscribers join
/// simultaneously after several groups are cached:
///   - Sub A (NextGroupStart): must wait for the next group boundary
///   - Sub B (LargestObject + FETCH): receives cached objects immediately
///
/// The test asserts that Sub B's time-to-first-object is significantly
/// shorter than Sub A's.
#[tokio::test]
async fn fetch_reduces_time_to_first_object() {
    init_crypto();
    let (endpoint, addr, cert_der) = start_relay().await;

    let relay = relay::relay::Relay::new(endpoint);
    tokio::spawn(async move { relay.run().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let group_interval = std::time::Duration::from_millis(300);

    // Publisher: send groups continuously at fixed intervals
    let pub_session = Arc::new(connect_client(addr, cert_der.clone()).await);
    publish_namespace(
        &pub_session,
        TrackNamespace::from(["daiki", "latency"].as_slice()),
    )
    .await;

    let _pub_keepalive = pub_session.clone();
    let pub_handle = tokio::spawn(async move {
        let event = pub_session.next_event().await.unwrap();
        match event {
            SessionEvent::Subscribe(mut req) => {
                req.accept(1).await.unwrap();

                // Send 10 groups at 300ms intervals
                for group_id in 0u64..10 {
                    let mut group = pub_session.open_subgroup(1, group_id, 0).await.unwrap();
                    let payload = format!("g{group_id}");
                    group.write_object(payload.as_bytes()).await.unwrap();
                    group.finish().unwrap();
                    tokio::time::sleep(group_interval).await;
                }

                req.send_publish_done(10).await.unwrap();
            }
            _ => panic!("expected Subscribe event"),
        }
    });

    // Initial subscriber to establish upstream subscription and start caching
    let init_session = Arc::new(connect_client(addr, cert_der.clone()).await);
    let _init_sub = init_session
        .subscribe(
            TrackNamespace::from(["daiki", "latency"].as_slice()),
            "video",
            vec![MessageParameter::SubscriptionFilter(
                SubscriptionFilter::NextGroupStart,
            )],
        )
        .await
        .unwrap();

    // Wait for several groups to be cached (wait ~1.5s for ~5 groups)
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    // === Sub A: NextGroupStart (no FETCH) ===
    let sub_a_session = Arc::new(connect_client(addr, cert_der.clone()).await);
    let time_a_start = Instant::now();
    let _sub_a = sub_a_session
        .subscribe(
            TrackNamespace::from(["daiki", "latency"].as_slice()),
            "video",
            vec![MessageParameter::SubscriptionFilter(
                SubscriptionFilter::NextGroupStart,
            )],
        )
        .await
        .unwrap();

    // Measure time to first object via SUBSCRIBE data stream
    let sub_a_handle = {
        let sub_a_session = sub_a_session.clone();
        tokio::spawn(async move {
            let event = sub_a_session.next_event().await.unwrap();
            match event {
                SessionEvent::DataStream(mut group) => {
                    let _payload = group.read_object().await.unwrap().unwrap();
                    time_a_start.elapsed()
                }
                _ => panic!("expected DataStream event"),
            }
        })
    };

    // === Sub B: LargestObject + Joining FETCH ===
    let sub_b_session = Arc::new(connect_client(addr, cert_der).await);
    let time_b_start = Instant::now();
    let _sub_b = sub_b_session
        .subscribe(
            TrackNamespace::from(["daiki", "latency"].as_slice()),
            "video",
            vec![MessageParameter::SubscriptionFilter(
                SubscriptionFilter::LargestObject,
            )],
        )
        .await
        .unwrap();

    // Send FETCH to get cached objects
    let subscribe_request_id: u64 = 0;
    let _fetch_ok = sub_b_session
        .fetch(2, subscribe_request_id, 2)
        .await
        .unwrap();

    // Read FETCH_HEADER uni stream — first object comes from cache
    let mut fetch_stream = sub_b_session.accept_uni_stream().await.unwrap();
    let _ = read_varint(&mut fetch_stream).await.unwrap(); // type
    let _ = read_varint(&mut fetch_stream).await.unwrap(); // request_id

    // Read first fetched object
    let flags = match moqt::stream::try_read_varint(&mut fetch_stream)
        .await
        .unwrap()
    {
        Some((v, _)) => v,
        None => panic!("expected at least one object in FETCH response"),
    };
    // Skip fields based on flags to get to payload
    if flags & 0x08 != 0 {
        let _ = read_varint(&mut fetch_stream).await.unwrap(); // group_id
    }
    if flags & 0x03 == 0x03 {
        let _ = read_varint(&mut fetch_stream).await.unwrap(); // subgroup_id
    }
    if flags & 0x04 != 0 {
        let _ = read_varint(&mut fetch_stream).await.unwrap(); // object_id
    }
    if flags & 0x10 != 0 {
        let mut buf = [0u8; 1];
        fetch_stream.read_exact(&mut buf).await.unwrap(); // priority
    }
    let (payload_len, _) = read_varint(&mut fetch_stream).await.unwrap();
    let mut payload = vec![0u8; payload_len as usize];
    if payload_len > 0 {
        fetch_stream.read_exact(&mut payload).await.unwrap();
    }
    let time_b_elapsed = time_b_start.elapsed();

    // Wait for Sub A to receive its first object
    let time_a_elapsed = tokio::time::timeout(std::time::Duration::from_secs(3), sub_a_handle)
        .await
        .expect("Sub A should receive an object within 3 seconds")
        .unwrap();

    // Print results for visibility
    eprintln!("=== Time-to-first-object comparison ===");
    eprintln!(
        "  NextGroupStart (no FETCH): {:>6.1}ms",
        time_a_elapsed.as_secs_f64() * 1000.0
    );
    eprintln!(
        "  LargestObject + FETCH:     {:>6.1}ms",
        time_b_elapsed.as_secs_f64() * 1000.0
    );
    eprintln!(
        "  Speedup:                   {:>6.1}x faster",
        time_a_elapsed.as_secs_f64() / time_b_elapsed.as_secs_f64()
    );

    // Sub B (FETCH) should be significantly faster than Sub A (NextGroupStart).
    // NextGroupStart must wait for the next group boundary (~0-300ms),
    // while FETCH gets cached objects immediately (~single-digit ms).
    assert!(
        time_b_elapsed < time_a_elapsed,
        "FETCH should be faster: fetch={:?}, next_group={:?}",
        time_b_elapsed,
        time_a_elapsed
    );

    // FETCH should complete in well under the group interval
    assert!(
        time_b_elapsed.as_millis() < 100,
        "FETCH time-to-first-object should be <100ms, was {:?}",
        time_b_elapsed
    );

    pub_handle.await.unwrap();
}
