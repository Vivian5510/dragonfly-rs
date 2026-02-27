#[cfg(target_os = "linux")]
use super::IoUringBackend;
use super::{
    ConnectionLifecycle, ReactorConnection, ServerReactor, ServerReactorConfig,
    ThreadedServerReactor,
};
use crate::app::{AppExecutor, ParsedCommandExecution, ServerApp};
use dfly_common::config::RuntimeConfig;
use dfly_core::command::CommandFrame;
use dfly_facade::net_proactor::{select_multiplex_backend, MultiplexApi, MultiplexSelection};
use dfly_facade::protocol::{ClientProtocol, ParsedCommand};
use googletest::prelude::*;
use mio::{Poll, Token};
use rstest::rstest;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::{atomic::AtomicUsize, Arc};
use std::time::{Duration, Instant};

#[rstest]
fn reactor_executes_resp_ping_roundtrip() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut reactor = ServerReactor::bind(bind_addr, ServerReactorConfig::default())
        .expect("reactor bind should succeed");
    let listen_addr = reactor
        .local_addr()
        .expect("local addr should be available");

    let mut client = TcpStream::connect(listen_addr).expect("connect should succeed");
    client
        .set_nonblocking(true)
        .expect("nonblocking client should be configurable");
    client
        .write_all(b"*1\r\n$4\r\nPING\r\n")
        .expect("write ping should succeed");

    let deadline = Instant::now() + Duration::from_millis(600);
    let mut response = Vec::new();
    while Instant::now() < deadline {
        let _ = reactor
            .poll_once(&mut app, Some(Duration::from_millis(5)))
            .expect("reactor poll should succeed");

        let mut chunk = [0_u8; 64];
        match client.read(&mut chunk) {
            Ok(0) => break,
            Ok(read_len) => {
                response.extend_from_slice(&chunk[..read_len]);
                if response.ends_with(b"+PONG\r\n") {
                    break;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("read from client failed: {error}"),
        }
    }

    assert_that!(&response, eq(&b"+PONG\r\n".to_vec()));
}

#[rstest]
fn reactor_executes_memcache_roundtrip_on_optional_listener() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let resp_bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let memcache_bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut reactor = ServerReactor::bind_with_memcache(
        resp_bind_addr,
        Some(memcache_bind_addr),
        ServerReactorConfig::default(),
    )
    .expect("reactor bind with memcache should succeed");
    let memcache_addr = reactor
        .memcache_local_addr()
        .expect("memcache local addr should be available");

    let mut client = TcpStream::connect(memcache_addr).expect("connect should succeed");
    client
        .set_nonblocking(true)
        .expect("nonblocking client should be configurable");
    client
        .write_all(b"set user:42 0 0 5\r\nalice\r\nget user:42\r\n")
        .expect("write memcache commands should succeed");

    let deadline = Instant::now() + Duration::from_millis(600);
    let mut response = Vec::new();
    while Instant::now() < deadline {
        let _ = reactor
            .poll_once(&mut app, Some(Duration::from_millis(5)))
            .expect("reactor poll should succeed");

        let mut chunk = [0_u8; 128];
        match client.read(&mut chunk) {
            Ok(0) => break,
            Ok(read_len) => {
                response.extend_from_slice(&chunk[..read_len]);
                if response.ends_with(b"END\r\n") {
                    break;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("read from memcache client failed: {error}"),
        }
    }

    assert_that!(
        &response,
        eq(&b"STORED\r\nVALUE user:42 0 5\r\nalice\r\nEND\r\n".to_vec())
    );
}

#[rstest]
fn reactor_memcache_parse_error_uses_client_error_and_keeps_connection_open() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let mut reactor = ServerReactor::bind_with_memcache(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        Some(SocketAddr::from(([127, 0, 0, 1], 0))),
        ServerReactorConfig::default(),
    )
    .expect("reactor bind with memcache should succeed");
    let memcache_addr = reactor
        .memcache_local_addr()
        .expect("memcache local addr should be available");

    let mut client = TcpStream::connect(memcache_addr).expect("connect should succeed");
    client
        .set_nonblocking(true)
        .expect("nonblocking client should be configurable");

    client
        .write_all(b"set user:42 0 0 5\r\nalice\r\n")
        .expect("write memcache set should succeed");

    let store_deadline = Instant::now() + Duration::from_millis(600);
    let mut store_reply = Vec::new();
    while Instant::now() < store_deadline {
        let _ = reactor
            .poll_once(&mut app, Some(Duration::from_millis(5)))
            .expect("reactor poll should succeed");

        let mut chunk = [0_u8; 64];
        match client.read(&mut chunk) {
            Ok(0) => {}
            Ok(read_len) => {
                store_reply.extend_from_slice(&chunk[..read_len]);
                if store_reply.ends_with(b"STORED\r\n") {
                    break;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("read from memcache client failed: {error}"),
        }
    }
    assert_that!(&store_reply, eq(&b"STORED\r\n".to_vec()));

    client
        .write_all(b"set broken 0 0 -1\r\nget user:42\r\n")
        .expect("write invalid+valid memcache pipeline should succeed");

    let deadline = Instant::now() + Duration::from_millis(800);
    let mut response = Vec::new();
    while Instant::now() < deadline {
        let _ = reactor
            .poll_once(&mut app, Some(Duration::from_millis(5)))
            .expect("reactor poll should succeed");

        let mut chunk = [0_u8; 256];
        match client.read(&mut chunk) {
            Ok(0) => {}
            Ok(read_len) => {
                response.extend_from_slice(&chunk[..read_len]);
                if response.ends_with(b"END\r\n") {
                    break;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("read from memcache client failed: {error}"),
        }
    }

    let text = String::from_utf8_lossy(&response);
    assert_that!(text.contains("CLIENT_ERROR"), eq(true));
    assert_that!(
        text.contains("VALUE user:42 0 5\r\nalice\r\nEND\r\n"),
        eq(true)
    );
}

#[rstest]
fn reactor_drops_connection_state_after_peer_close() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut reactor = ServerReactor::bind(bind_addr, ServerReactorConfig::default())
        .expect("reactor bind should succeed");
    let listen_addr = reactor
        .local_addr()
        .expect("local addr should be available");

    let client = TcpStream::connect(listen_addr).expect("connect should succeed");
    drop(client);

    let deadline = Instant::now() + Duration::from_millis(600);
    while Instant::now() < deadline {
        let _ = reactor
            .poll_once(&mut app, Some(Duration::from_millis(5)))
            .expect("reactor poll should succeed");
        if reactor.connection_count() == 0 {
            break;
        }
    }

    assert_that!(reactor.connection_count(), eq(0_usize));
}

#[rstest]
fn reactor_read_stops_same_cycle_after_backpressure_arms() {
    let mut app = ServerApp::new(RuntimeConfig::default());
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .expect("listener bind should succeed");
    let listen_addr = listener
        .local_addr()
        .expect("listener must expose local addr");

    let mut client = TcpStream::connect(listen_addr).expect("connect should succeed");
    let (server_stream, _) = listener.accept().expect("accept should succeed");
    server_stream
        .set_nonblocking(true)
        .expect("accepted socket should be nonblocking");

    let mut connection = ReactorConnection::new(
        mio::net::TcpStream::from_std(server_stream),
        ClientProtocol::Resp,
    );
    let one_ping = b"*1\r\n$4\r\nPING\r\n";
    let mut payload = Vec::with_capacity(one_ping.len() * 1024);
    for _ in 0..1024 {
        payload.extend_from_slice(one_ping);
    }
    client
        .write_all(&payload)
        .expect("client payload write should succeed");
    client
        .shutdown(Shutdown::Write)
        .expect("client write-half shutdown should succeed");

    let deadline = Instant::now() + Duration::from_millis(300);
    while Instant::now() < deadline {
        ServerReactor::read_connection_bytes(&mut app, &mut connection, 64, 32);
        if connection.read_paused_by_backpressure {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }

    assert_that!(connection.read_paused_by_backpressure, eq(true));
    assert_that!(connection.lifecycle, eq(ConnectionLifecycle::Active));
    assert_that!(connection.write_buffer.is_empty(), eq(false));
}

#[rstest]
fn reactor_config_accepts_small_custom_backpressure_values() {
    let config = ServerReactorConfig {
        max_events: 64,
        write_high_watermark_bytes: 512,
        write_low_watermark_bytes: 128,
        io_worker_count: 1,
        max_pending_requests_per_connection: 32,
        max_pending_requests_per_worker: 128,
        ..ServerReactorConfig::default()
    };
    let watermarks = config
        .normalized_backpressure_watermarks()
        .expect("custom backpressure values should be accepted");

    assert_that!(&watermarks, eq(&(512_usize, 128_usize)));
}

#[rstest]
fn reactor_config_rejects_non_increasing_backpressure_values() {
    let config = ServerReactorConfig {
        max_events: 64,
        write_high_watermark_bytes: 256,
        write_low_watermark_bytes: 256,
        io_worker_count: 1,
        max_pending_requests_per_connection: 32,
        max_pending_requests_per_worker: 128,
        ..ServerReactorConfig::default()
    };
    let error = config
        .normalized_backpressure_watermarks()
        .expect_err("low >= high should be rejected");

    assert_that!(
        format!("{error}").contains("low watermark must be smaller than high watermark"),
        eq(true)
    );
}

#[rstest]
fn backend_fallback_selection() {
    let selection = select_multiplex_backend(&RuntimeConfig {
        force_epoll: true,
        ..RuntimeConfig::default()
    });
    assert_that!(selection.api_label(), eq("mio"));
    assert_that!(selection.fallback_reason.is_some(), eq(true));
}

#[cfg(not(target_os = "linux"))]
#[rstest]
fn net_engine_rejects_io_uring_backend_on_non_linux() {
    let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
    let selection = MultiplexSelection {
        api: MultiplexApi::IoUring,
        fallback_reason: None,
    };
    let bind_result = super::NetEngine::bind_with_memcache(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        None,
        ServerReactorConfig::default(),
        selection,
        &app_executor,
    );
    let error_text = match bind_result {
        Ok(_) => panic!("io_uring backend should not bind on non-linux"),
        Err(error) => format!("{error}"),
    };
    assert_that!(error_text.contains("Linux-only"), eq(true));
}

#[cfg(target_os = "linux")]
#[rstest]
fn net_engine_binds_io_uring_backend_on_linux() {
    let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
    let selection = MultiplexSelection {
        api: MultiplexApi::IoUring,
        fallback_reason: None,
    };
    let bind_result = super::NetEngine::bind_with_memcache(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        None,
        ServerReactorConfig::default(),
        selection,
        &app_executor,
    );
    assert_that!(bind_result.is_ok(), eq(true));
}

#[cfg(target_os = "linux")]
fn io_uring_listener_addr(backend: &IoUringBackend, protocol: ClientProtocol) -> SocketAddr {
    backend
        .listeners
        .iter()
        .find_map(|listener| {
            if listener.protocol == protocol {
                listener.socket.local_addr().ok()
            } else {
                None
            }
        })
        .expect("listener address should be available")
}

#[cfg(target_os = "linux")]
fn io_uring_wait_connection_id(backend: &mut IoUringBackend) -> u64 {
    let deadline = Instant::now() + Duration::from_millis(1000);
    while Instant::now() < deadline {
        let _ = backend
            .poll_once(Some(Duration::from_millis(5)))
            .expect("io_uring backend poll should succeed");
        if let Some(connection_id) = backend.connection_owner_by_id.keys().next().copied() {
            return connection_id;
        }
    }
    panic!("expected accepted io_uring connection id");
}

#[cfg(target_os = "linux")]
#[rstest]
fn io_uring_resp_roundtrip() {
    let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
    let mut backend = IoUringBackend::bind_with_memcache(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        None,
        ServerReactorConfig {
            io_worker_count: 2,
            ..ServerReactorConfig::default()
        },
        &app_executor,
    )
    .expect("io_uring backend bind should succeed");
    let resp_addr = io_uring_listener_addr(&backend, ClientProtocol::Resp);
    let mut client = TcpStream::connect(resp_addr).expect("connect should succeed");
    client
        .set_nonblocking(true)
        .expect("client should be nonblocking");
    client
        .write_all(b"*1\r\n$4\r\nPING\r\n")
        .expect("ping write should succeed");

    let deadline = Instant::now() + Duration::from_millis(1200);
    let mut reply = Vec::new();
    while Instant::now() < deadline {
        let _ = backend
            .poll_once(Some(Duration::from_millis(5)))
            .expect("io_uring backend poll should succeed");
        let mut chunk = [0_u8; 64];
        match client.read(&mut chunk) {
            Ok(0) => {}
            Ok(read_len) => {
                reply.extend_from_slice(&chunk[..read_len]);
                if reply.ends_with(b"+PONG\r\n") {
                    break;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("resp read failed: {error}"),
        }
    }
    assert_that!(&reply, eq(&b"+PONG\r\n".to_vec()));
}

#[cfg(target_os = "linux")]
#[rstest]
fn io_uring_memcache_roundtrip() {
    let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
    let mut backend = IoUringBackend::bind_with_memcache(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        Some(SocketAddr::from(([127, 0, 0, 1], 0))),
        ServerReactorConfig {
            io_worker_count: 2,
            ..ServerReactorConfig::default()
        },
        &app_executor,
    )
    .expect("io_uring backend bind should succeed");
    let memcache_addr = io_uring_listener_addr(&backend, ClientProtocol::Memcache);
    let mut client = TcpStream::connect(memcache_addr).expect("connect should succeed");
    client
        .set_nonblocking(true)
        .expect("client should be nonblocking");
    client
        .write_all(b"set mk 0 0 2\r\nok\r\nget mk\r\n")
        .expect("memcache roundtrip write should succeed");

    let deadline = Instant::now() + Duration::from_millis(1400);
    let mut reply = Vec::new();
    while Instant::now() < deadline {
        let _ = backend
            .poll_once(Some(Duration::from_millis(5)))
            .expect("io_uring backend poll should succeed");
        let mut chunk = [0_u8; 256];
        match client.read(&mut chunk) {
            Ok(0) => {}
            Ok(read_len) => {
                reply.extend_from_slice(&chunk[..read_len]);
                if reply.ends_with(b"END\r\n") {
                    break;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("memcache read failed: {error}"),
        }
    }
    assert_that!(
        &reply,
        eq(&b"STORED\r\nVALUE mk 0 2\r\nok\r\nEND\r\n".to_vec())
    );
}

#[cfg(target_os = "linux")]
#[rstest]
fn io_uring_migration_preserves_order() {
    const IN_FLIGHT_GETS: usize = 1024;

    let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
    let mut backend = IoUringBackend::bind_with_memcache(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        None,
        ServerReactorConfig {
            io_worker_count: 2,
            io_worker_dispatch_count: 2,
            enable_connection_migration: true,
            max_pending_requests_per_connection: 64,
            max_pending_requests_per_worker: 256,
            ..ServerReactorConfig::default()
        },
        &app_executor,
    )
    .expect("io_uring backend bind should succeed");
    let resp_addr = io_uring_listener_addr(&backend, ClientProtocol::Resp);
    let mut client = TcpStream::connect(resp_addr).expect("connect should succeed");
    client
        .set_nonblocking(true)
        .expect("client should be nonblocking");

    let connection_id = io_uring_wait_connection_id(&mut backend);
    let owner = backend
        .connection_owner_by_id
        .get(&connection_id)
        .copied()
        .expect("connection owner should exist");
    let target_worker = (owner + 1) % 2;

    let mut deferred_request = Vec::new();
    for _ in 0..IN_FLIGHT_GETS {
        deferred_request.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
    }
    client
        .write_all(&deferred_request)
        .expect("deferred pipeline write should succeed");

    let mut first_reply = Vec::new();
    let first_observe_deadline = Instant::now() + Duration::from_millis(1200);
    while Instant::now() < first_observe_deadline {
        let _ = backend
            .poll_once(Some(Duration::from_millis(5)))
            .expect("io_uring backend poll should succeed");
        let mut chunk = [0_u8; 1024];
        match client.read(&mut chunk) {
            Ok(0) => {}
            Ok(read_len) => {
                first_reply.extend_from_slice(&chunk[..read_len]);
                if !first_reply.is_empty() {
                    break;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("deferred reply read failed: {error}"),
        }
    }
    assert_that!(first_reply.is_empty(), eq(false));

    let mut migrated = false;
    match backend.migrate_connection(connection_id, target_worker) {
        Ok(()) => {
            // Runtime can occasionally drain the whole deferred wave before this first probe.
            // Treat this as already-migrated and continue with order validation below.
            migrated = true;
        }
        Err(migration_error) => {
            assert_that!(
                format!("{migration_error}").contains("state does not allow migration"),
                eq(true)
            );
            assert_that!(
                backend.connection_owner_by_id.get(&connection_id).copied(),
                eq(Some(owner))
            );
        }
    }

    if !migrated {
        let first_deadline = Instant::now() + Duration::from_millis(3000);
        while Instant::now() < first_deadline {
            let _ = backend
                .poll_once(Some(Duration::from_millis(5)))
                .expect("io_uring backend poll should succeed");
            let mut chunk = [0_u8; 2048];
            match client.read(&mut chunk) {
                Ok(0) => {}
                Ok(read_len) => {
                    first_reply.extend_from_slice(&chunk[..read_len]);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("deferred reply read failed: {error}"),
            }
            if backend
                .migrate_connection(connection_id, target_worker)
                .is_ok()
            {
                migrated = true;
                break;
            }
        }
        assert_that!(migrated, eq(true));
        assert_that!(first_reply.is_empty(), eq(false));
    }

    let second_migration = backend.migrate_connection(connection_id, owner);
    assert_that!(second_migration.is_err(), eq(true));

    let mut request = Vec::new();
    let mut expected = Vec::new();
    for index in 0..24 {
        let key = format!("mk{index}");
        let value = format!("mv{index}");
        request.extend_from_slice(
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                value.len(),
                value
            )
            .as_bytes(),
        );
        request.extend_from_slice(
            format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key).as_bytes(),
        );
        expected.extend_from_slice(b"+OK\r\n");
        expected.extend_from_slice(format!("${}\r\n{}\r\n", value.len(), value).as_bytes());
    }
    client
        .write_all(&request)
        .expect("ordered pipeline write should succeed");

    let mut response = Vec::new();
    let deadline = Instant::now() + Duration::from_millis(3000);
    while Instant::now() < deadline {
        let _ = backend
            .poll_once(Some(Duration::from_millis(5)))
            .expect("io_uring backend poll should succeed");
        let mut chunk = [0_u8; 4096];
        match client.read(&mut chunk) {
            Ok(0) => {}
            Ok(read_len) => response.extend_from_slice(&chunk[..read_len]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("ordered pipeline read failed: {error}"),
        }
        if response.ends_with(&expected) {
            break;
        }
    }
    assert_that!(response.ends_with(&expected), eq(true));
}

#[cfg(target_os = "linux")]
#[rstest]
fn io_uring_backpressure_pauses_read() {
    const DEFERRED_GETS: usize = 512;

    let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
    let mut backend = IoUringBackend::bind_with_memcache(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        None,
        ServerReactorConfig {
            io_worker_count: 1,
            io_worker_dispatch_count: 1,
            max_pending_requests_per_connection: 1,
            max_pending_requests_per_worker: 1,
            ..ServerReactorConfig::default()
        },
        &app_executor,
    )
    .expect("io_uring backend bind should succeed");
    let resp_addr = io_uring_listener_addr(&backend, ClientProtocol::Resp);
    let mut client = TcpStream::connect(resp_addr).expect("connect should succeed");
    client
        .set_nonblocking(true)
        .expect("client should be nonblocking");
    let _ = io_uring_wait_connection_id(&mut backend);

    let mut payload = Vec::new();
    for _ in 0..DEFERRED_GETS {
        payload.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
    }
    payload.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
    client
        .write_all(&payload)
        .expect("pipeline write should succeed");

    let mut early_reply = Vec::new();
    for _ in 0..6 {
        let _ = backend
            .poll_once(Some(Duration::from_millis(5)))
            .expect("io_uring backend poll should succeed");
        let mut chunk = [0_u8; 1024];
        match client.read(&mut chunk) {
            Ok(0) => {}
            Ok(read_len) => early_reply.extend_from_slice(&chunk[..read_len]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("early read failed: {error}"),
        }
    }
    assert_that!(
        String::from_utf8_lossy(&early_reply).contains("+PONG"),
        eq(false)
    );

    let mut full_reply = early_reply;
    let expected_len = DEFERRED_GETS
        .saturating_mul(b"$-1\r\n".len())
        .saturating_add(b"+PONG\r\n".len());
    let deadline = Instant::now() + Duration::from_millis(3500);
    while Instant::now() < deadline {
        let _ = backend
            .poll_once(Some(Duration::from_millis(5)))
            .expect("io_uring backend poll should succeed");
        let mut chunk = [0_u8; 2048];
        match client.read(&mut chunk) {
            Ok(0) => {}
            Ok(read_len) => full_reply.extend_from_slice(&chunk[..read_len]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("full read failed: {error}"),
        }
        if full_reply.len() >= expected_len {
            break;
        }
    }
    assert_that!(full_reply.ends_with(b"+PONG\r\n"), eq(true));
    assert_that!(full_reply.len() >= expected_len, eq(true));
}

#[cfg(target_os = "linux")]
#[rstest]
fn io_uring_peer_close_reap_state() {
    let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
    let mut backend = IoUringBackend::bind_with_memcache(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        None,
        ServerReactorConfig {
            io_worker_count: 2,
            io_worker_dispatch_count: 2,
            ..ServerReactorConfig::default()
        },
        &app_executor,
    )
    .expect("io_uring backend bind should succeed");
    let resp_addr = io_uring_listener_addr(&backend, ClientProtocol::Resp);
    let client = TcpStream::connect(resp_addr).expect("connect should succeed");
    client
        .set_nonblocking(true)
        .expect("client should be nonblocking");
    let connection_id = io_uring_wait_connection_id(&mut backend);

    let _ = client.shutdown(Shutdown::Both);
    drop(client);

    let deadline = Instant::now() + Duration::from_millis(1500);
    while Instant::now() < deadline {
        let _ = backend
            .poll_once(Some(Duration::from_millis(5)))
            .expect("io_uring backend poll should succeed");
        if !backend.connection_owner_by_id.contains_key(&connection_id) {
            break;
        }
    }
    assert_that!(
        backend.connection_owner_by_id.contains_key(&connection_id),
        eq(false)
    );
    assert_that!(
        backend.migrated_connections.contains(&connection_id),
        eq(false)
    );
}

#[cfg(target_os = "linux")]
#[rstest]
fn io_uring_add_connection_ack_avoids_dirty_owner_on_worker_failure() {
    let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
    let mut backend = IoUringBackend::bind_with_memcache(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        None,
        ServerReactorConfig {
            io_worker_count: 1,
            io_worker_dispatch_count: 1,
            ..ServerReactorConfig::default()
        },
        &app_executor,
    )
    .expect("io_uring backend bind should succeed");
    let resp_addr = io_uring_listener_addr(&backend, ClientProtocol::Resp);

    backend.workers[0]
        .sender
        .send(super::IoUringWorkerCommand::Shutdown)
        .expect("shutdown command should send");
    if let Some(join) = backend.workers[0].join.take() {
        let _ = join.join();
    }

    let _client = TcpStream::connect(resp_addr).expect("connect should succeed");
    let poll_result = backend.poll_once(Some(Duration::from_millis(5)));
    assert_that!(poll_result.is_err(), eq(true));
    assert_that!(backend.connection_owner_by_id.is_empty(), eq(true));
}

#[rstest]
fn listener_affinity_distribution() {
    let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
    let config = ServerReactorConfig {
        io_worker_count: 4,
        io_worker_dispatch_start: 1,
        io_worker_dispatch_count: 2,
        ..ServerReactorConfig::default()
    };
    let mut reactor =
        ThreadedServerReactor::bind(SocketAddr::from(([127, 0, 0, 1], 0)), config, &app_executor)
            .expect("threaded reactor bind should succeed");
    let listen_addr = reactor
        .local_addr()
        .expect("local address should be available");

    let mut clients = Vec::new();
    for _ in 0..8 {
        let client = TcpStream::connect(listen_addr).expect("connect should succeed");
        clients.push(client);
    }

    let deadline = Instant::now() + Duration::from_millis(800);
    while Instant::now() < deadline {
        let _ = reactor
            .poll_once(Some(Duration::from_millis(5)))
            .expect("threaded reactor poll should succeed");
        let counts = reactor.worker_connection_counts();
        if counts[1] > 0 && counts[2] > 0 {
            assert_that!(counts[0], eq(0_usize));
            assert_that!(counts[3], eq(0_usize));
            return;
        }
    }
    panic!("expected connection distribution to stay within configured dispatch window");
}

#[rstest]
fn connection_migration_preserves_order() {
    let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
    let config = ServerReactorConfig {
        io_worker_count: 2,
        io_worker_dispatch_count: 2,
        enable_connection_migration: true,
        ..ServerReactorConfig::default()
    };
    let mut reactor =
        ThreadedServerReactor::bind(SocketAddr::from(([127, 0, 0, 1], 0)), config, &app_executor)
            .expect("threaded reactor bind should succeed");
    let listen_addr = reactor
        .local_addr()
        .expect("local address should be available");
    let mut client = TcpStream::connect(listen_addr).expect("connect should succeed");
    client
        .set_nonblocking(true)
        .expect("client should be nonblocking");

    let connection_id = {
        let deadline = Instant::now() + Duration::from_millis(800);
        let mut connection_id = None;
        while Instant::now() < deadline {
            let _ = reactor
                .poll_once(Some(Duration::from_millis(5)))
                .expect("threaded reactor poll should succeed");
            if let Some(id) = reactor.active_connection_ids().first().copied() {
                connection_id = Some(id);
                break;
            }
        }
        connection_id.expect("accepted connection should be tracked")
    };

    let owner = reactor
        .connection_owner(connection_id)
        .expect("connection owner should be known");
    let target_worker = (owner + 1) % 2;
    reactor
        .migrate_connection(connection_id, target_worker)
        .expect("migration should succeed");

    let second_migration = reactor.migrate_connection(connection_id, owner);
    assert_that!(second_migration.is_err(), eq(true));

    let mut request = Vec::new();
    let mut expected = Vec::new();
    for index in 0..24 {
        let key = format!("mk{index}");
        let value = format!("mv{index}");
        request.extend_from_slice(
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                value.len(),
                value
            )
            .as_bytes(),
        );
        request.extend_from_slice(
            format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key).as_bytes(),
        );
        expected.extend_from_slice(b"+OK\r\n");
        expected.extend_from_slice(format!("${}\r\n{}\r\n", value.len(), value).as_bytes());
    }
    client
        .write_all(&request)
        .expect("pipeline write should succeed");

    let mut response = Vec::new();
    let deadline = Instant::now() + Duration::from_millis(2500);
    while Instant::now() < deadline {
        let _ = reactor
            .poll_once(Some(Duration::from_millis(5)))
            .expect("threaded reactor poll should succeed");

        let mut chunk = [0_u8; 4096];
        match client.read(&mut chunk) {
            Ok(0) => {}
            Ok(read_len) => response.extend_from_slice(&chunk[..read_len]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("pipeline read failed: {error}"),
        }
        if response.len() >= expected.len() {
            break;
        }
    }

    assert_that!(&response, eq(&expected));
}

#[rstest]
fn connection_migration_failure_rolls_back_to_source_owner() {
    let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
    let config = ServerReactorConfig {
        io_worker_count: 2,
        io_worker_dispatch_count: 2,
        enable_connection_migration: true,
        ..ServerReactorConfig::default()
    };
    let mut reactor =
        ThreadedServerReactor::bind(SocketAddr::from(([127, 0, 0, 1], 0)), config, &app_executor)
            .expect("threaded reactor bind should succeed");
    let listen_addr = reactor
        .local_addr()
        .expect("local address should be available");
    let mut client = TcpStream::connect(listen_addr).expect("connect should succeed");
    client
        .set_nonblocking(true)
        .expect("client should be nonblocking");

    let connection_id = {
        let deadline = Instant::now() + Duration::from_millis(800);
        let mut connection_id = None;
        while Instant::now() < deadline {
            let _ = reactor
                .poll_once(Some(Duration::from_millis(5)))
                .expect("threaded reactor poll should succeed");
            if let Some(id) = reactor.active_connection_ids().first().copied() {
                connection_id = Some(id);
                break;
            }
        }
        connection_id.expect("accepted connection should be tracked")
    };

    let owner = reactor
        .connection_owner(connection_id)
        .expect("connection owner should be known");
    let target_worker = (owner + 1) % 2;
    reactor.workers[target_worker]
        .sender
        .send(super::WorkerCommand::Shutdown)
        .expect("target worker shutdown signal should send");
    if let Some(join) = reactor.workers[target_worker].join.take() {
        let _ = join.join();
    }

    let migration = reactor.migrate_connection(connection_id, target_worker);
    assert_that!(migration.is_err(), eq(true));
    assert_that!(reactor.connection_owner(connection_id), eq(Some(owner)));

    client
        .write_all(b"*1\r\n$4\r\nPING\r\n")
        .expect("ping write should succeed after rollback");
    let deadline = Instant::now() + Duration::from_millis(1200);
    let mut response = Vec::new();
    while Instant::now() < deadline {
        let _ = reactor
            .poll_once(Some(Duration::from_millis(5)))
            .expect("threaded reactor poll should succeed");
        let mut chunk = [0_u8; 64];
        match client.read(&mut chunk) {
            Ok(0) => {}
            Ok(read_len) => {
                response.extend_from_slice(&chunk[..read_len]);
                if response.ends_with(b"+PONG\r\n") {
                    break;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("ping read failed after rollback: {error}"),
        }
    }

    assert_that!(&response, eq(&b"+PONG\r\n".to_vec()));
}

#[rstest]
fn connection_close_cleans_migrated_marker() {
    let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
    let config = ServerReactorConfig {
        io_worker_count: 2,
        io_worker_dispatch_count: 2,
        enable_connection_migration: true,
        ..ServerReactorConfig::default()
    };
    let mut reactor =
        ThreadedServerReactor::bind(SocketAddr::from(([127, 0, 0, 1], 0)), config, &app_executor)
            .expect("threaded reactor bind should succeed");
    let listen_addr = reactor
        .local_addr()
        .expect("local address should be available");
    let client = TcpStream::connect(listen_addr).expect("connect should succeed");
    client
        .set_nonblocking(true)
        .expect("client should be nonblocking");

    let connection_id = {
        let deadline = Instant::now() + Duration::from_millis(800);
        let mut connection_id = None;
        while Instant::now() < deadline {
            let _ = reactor
                .poll_once(Some(Duration::from_millis(5)))
                .expect("threaded reactor poll should succeed");
            if let Some(id) = reactor.active_connection_ids().first().copied() {
                connection_id = Some(id);
                break;
            }
        }
        connection_id.expect("accepted connection should be tracked")
    };

    let owner = reactor
        .connection_owner(connection_id)
        .expect("connection owner should be known");
    let target_worker = (owner + 1) % 2;
    reactor
        .migrate_connection(connection_id, target_worker)
        .expect("migration should succeed");
    assert_that!(
        reactor.migrated_connections.contains(&connection_id),
        eq(true)
    );

    let _ = client.shutdown(Shutdown::Both);
    drop(client);

    let deadline = Instant::now() + Duration::from_millis(1200);
    while Instant::now() < deadline {
        let _ = reactor
            .poll_once(Some(Duration::from_millis(5)))
            .expect("threaded reactor poll should succeed");
        if reactor.connection_owner(connection_id).is_none() {
            break;
        }
    }

    assert_that!(reactor.connection_owner(connection_id).is_none(), eq(true));
    assert_that!(
        reactor.migrated_connections.contains(&connection_id),
        eq(false)
    );
}

#[rstest]
fn threaded_reactor_distributes_connections_across_workers() {
    let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
    let config = ServerReactorConfig {
        io_worker_count: 2,
        ..ServerReactorConfig::default()
    };
    let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut reactor = ThreadedServerReactor::bind(bind_addr, config, &app_executor)
        .expect("threaded reactor bind should succeed");
    let listen_addr = reactor
        .local_addr()
        .expect("local addr should be available");

    let _client_a = TcpStream::connect(listen_addr).expect("first connect should succeed");
    let _client_b = TcpStream::connect(listen_addr).expect("second connect should succeed");

    let deadline = Instant::now() + Duration::from_millis(800);
    let mut counts = Vec::new();
    while Instant::now() < deadline {
        let _ = reactor
            .poll_once(Some(Duration::from_millis(5)))
            .expect("threaded reactor poll should succeed");
        counts = reactor.worker_connection_counts();
        if counts.iter().filter(|count| **count > 0).count() == 2 {
            break;
        }
    }

    assert_that!(counts.len(), eq(2_usize));
    assert_that!(
        counts.iter().filter(|count| **count > 0).count(),
        eq(2_usize)
    );
}

#[rstest]
fn threaded_reactor_workers_progress_two_pipelines_concurrently() {
    const PIPELINE_GETS: usize = 16;

    let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
    let config = ServerReactorConfig {
        io_worker_count: 2,
        ..ServerReactorConfig::default()
    };
    let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut reactor = ThreadedServerReactor::bind(bind_addr, config, &app_executor)
        .expect("threaded reactor bind should succeed");
    let listen_addr = reactor
        .local_addr()
        .expect("local addr should be available");

    let mut client_a = TcpStream::connect(listen_addr).expect("first connect should succeed");
    let mut client_b = TcpStream::connect(listen_addr).expect("second connect should succeed");
    client_a
        .set_nonblocking(true)
        .expect("first client should be nonblocking");
    client_b
        .set_nonblocking(true)
        .expect("second client should be nonblocking");

    let distribution_deadline = Instant::now() + Duration::from_millis(800);
    let mut counts = Vec::new();
    while Instant::now() < distribution_deadline {
        let _ = reactor
            .poll_once(Some(Duration::from_millis(5)))
            .expect("threaded reactor poll should succeed");
        counts = reactor.worker_connection_counts();
        if counts.iter().filter(|count| **count > 0).count() == 2 {
            break;
        }
    }
    assert_that!(counts.len(), eq(2_usize));
    assert_that!(
        counts.iter().filter(|count| **count > 0).count(),
        eq(2_usize)
    );

    client_a
        .write_all(b"*3\r\n$3\r\nSET\r\n$2\r\nka\r\n$1\r\n1\r\n")
        .expect("first SET should write");
    client_b
        .write_all(b"*3\r\n$3\r\nSET\r\n$2\r\nkb\r\n$1\r\n2\r\n")
        .expect("second SET should write");

    let mut set_reply_a = Vec::new();
    let mut set_reply_b = Vec::new();
    let set_deadline = Instant::now() + Duration::from_millis(900);
    while Instant::now() < set_deadline {
        let _ = reactor
            .poll_once(Some(Duration::from_millis(5)))
            .expect("threaded reactor poll should succeed");

        let mut chunk_a = [0_u8; 64];
        match client_a.read(&mut chunk_a) {
            Ok(0) => {}
            Ok(read_len) => set_reply_a.extend_from_slice(&chunk_a[..read_len]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("first set reply read failed: {error}"),
        }

        let mut chunk_b = [0_u8; 64];
        match client_b.read(&mut chunk_b) {
            Ok(0) => {}
            Ok(read_len) => set_reply_b.extend_from_slice(&chunk_b[..read_len]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("second set reply read failed: {error}"),
        }

        if set_reply_a.ends_with(b"+OK\r\n") && set_reply_b.ends_with(b"+OK\r\n") {
            break;
        }
    }
    assert_that!(&set_reply_a, eq(&b"+OK\r\n".to_vec()));
    assert_that!(&set_reply_b, eq(&b"+OK\r\n".to_vec()));

    let mut pipeline_a = Vec::new();
    let mut pipeline_b = Vec::new();
    let one_get_a = b"*2\r\n$3\r\nGET\r\n$2\r\nka\r\n";
    let one_get_b = b"*2\r\n$3\r\nGET\r\n$2\r\nkb\r\n";
    for _ in 0..PIPELINE_GETS {
        pipeline_a.extend_from_slice(one_get_a);
        pipeline_b.extend_from_slice(one_get_b);
    }

    client_a
        .write_all(&pipeline_a)
        .expect("first pipeline write should succeed");
    client_b
        .write_all(&pipeline_b)
        .expect("second pipeline write should succeed");

    let mut expected_a = Vec::new();
    let mut expected_b = Vec::new();
    for _ in 0..PIPELINE_GETS {
        expected_a.extend_from_slice(b"$1\r\n1\r\n");
        expected_b.extend_from_slice(b"$1\r\n2\r\n");
    }

    let mut pipeline_reply_a = Vec::new();
    let mut pipeline_reply_b = Vec::new();
    let pipeline_deadline = Instant::now() + Duration::from_millis(2000);
    while Instant::now() < pipeline_deadline {
        let _ = reactor
            .poll_once(Some(Duration::from_millis(5)))
            .expect("threaded reactor poll should succeed");

        let mut chunk_a = [0_u8; 512];
        match client_a.read(&mut chunk_a) {
            Ok(0) => {}
            Ok(read_len) => pipeline_reply_a.extend_from_slice(&chunk_a[..read_len]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("first pipeline reply read failed: {error}"),
        }

        let mut chunk_b = [0_u8; 512];
        match client_b.read(&mut chunk_b) {
            Ok(0) => {}
            Ok(read_len) => pipeline_reply_b.extend_from_slice(&chunk_b[..read_len]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("second pipeline reply read failed: {error}"),
        }

        if pipeline_reply_a.len() >= expected_a.len() && pipeline_reply_b.len() >= expected_b.len()
        {
            break;
        }
    }

    assert_that!(&pipeline_reply_a, eq(&expected_a));
    assert_that!(&pipeline_reply_b, eq(&expected_b));
}

#[rstest]
fn many_idle_connections_do_not_slow_deferred_progress() {
    const PIPELINE_GETS: usize = 256;
    const PING_COUNT: usize = 12;
    const IDLE_CLIENTS: usize = 32;

    let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
    let config = ServerReactorConfig {
        io_worker_count: 2,
        ..ServerReactorConfig::default()
    };
    let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut reactor = ThreadedServerReactor::bind(bind_addr, config, &app_executor)
        .expect("threaded reactor bind should succeed");
    let listen_addr = reactor
        .local_addr()
        .expect("local addr should be available");

    let mut client_a = TcpStream::connect(listen_addr).expect("first connect should succeed");
    let mut client_b = TcpStream::connect(listen_addr).expect("second connect should succeed");
    client_a
        .set_nonblocking(true)
        .expect("first client should be nonblocking");
    client_b
        .set_nonblocking(true)
        .expect("second client should be nonblocking");
    let mut idle_clients = Vec::with_capacity(IDLE_CLIENTS);
    for _ in 0..IDLE_CLIENTS {
        let mut attempts = 0_u32;
        let idle = loop {
            attempts = attempts.saturating_add(1);
            assert_that!(attempts <= 200, eq(true));
            match TcpStream::connect(listen_addr) {
                Ok(stream) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                    let _ = reactor
                        .poll_once(Some(Duration::from_millis(1)))
                        .expect("threaded reactor poll should succeed");
                }
                Err(error) => panic!("idle connect should succeed: {error}"),
            }
        };
        idle.set_nonblocking(true)
            .expect("idle client should be nonblocking");
        idle_clients.push(idle);
    }
    assert_that!(idle_clients.len(), eq(IDLE_CLIENTS));

    let distribution_deadline = Instant::now() + Duration::from_millis(800);
    let mut counts = Vec::new();
    while Instant::now() < distribution_deadline {
        let _ = reactor
            .poll_once(Some(Duration::from_millis(5)))
            .expect("threaded reactor poll should succeed");
        counts = reactor.worker_connection_counts();
        if counts.iter().filter(|count| **count > 0).count() == 2 {
            break;
        }
    }
    assert_that!(
        counts.iter().filter(|count| **count > 0).count(),
        eq(2_usize)
    );

    client_a
        .write_all(b"*3\r\n$3\r\nSET\r\n$2\r\nka\r\n$1\r\n1\r\n")
        .expect("seed set should write");
    let mut set_reply = Vec::new();
    let set_deadline = Instant::now() + Duration::from_millis(900);
    while Instant::now() < set_deadline {
        let _ = reactor
            .poll_once(Some(Duration::from_millis(5)))
            .expect("threaded reactor poll should succeed");
        let mut chunk = [0_u8; 64];
        match client_a.read(&mut chunk) {
            Ok(0) => {}
            Ok(read_len) => set_reply.extend_from_slice(&chunk[..read_len]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("seed set read failed: {error}"),
        }
        if set_reply.ends_with(b"+OK\r\n") {
            break;
        }
    }
    assert_that!(&set_reply, eq(&b"+OK\r\n".to_vec()));

    let mut deferred_pipeline = Vec::new();
    for _ in 0..PIPELINE_GETS {
        deferred_pipeline.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$2\r\nka\r\n");
    }
    client_a
        .write_all(&deferred_pipeline)
        .expect("deferred pipeline write should succeed");

    let mut ping_pipeline = Vec::new();
    for _ in 0..PING_COUNT {
        ping_pipeline.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
    }
    client_b
        .write_all(&ping_pipeline)
        .expect("ping pipeline write should succeed");

    let expected_ping = b"+PONG\r\n".repeat(PING_COUNT);
    let mut ping_reply = Vec::new();
    let ping_deadline = Instant::now() + Duration::from_millis(1200);
    while Instant::now() < ping_deadline {
        let _ = reactor
            .poll_once(Some(Duration::from_millis(5)))
            .expect("threaded reactor poll should succeed");

        let mut chunk_b = [0_u8; 512];
        match client_b.read(&mut chunk_b) {
            Ok(0) => {}
            Ok(read_len) => ping_reply.extend_from_slice(&chunk_b[..read_len]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("ping reply read failed: {error}"),
        }
        if ping_reply.len() >= expected_ping.len() {
            break;
        }
    }

    assert_that!(&ping_reply, eq(&expected_ping));
}

#[rstest]
fn threaded_reactor_mixed_read_write_pipelines_progress_without_hol_blocking() {
    const PAIRS: usize = 16;

    let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
    let config = ServerReactorConfig {
        io_worker_count: 2,
        ..ServerReactorConfig::default()
    };
    let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut reactor = ThreadedServerReactor::bind(bind_addr, config, &app_executor)
        .expect("threaded reactor bind should succeed");
    let listen_addr = reactor
        .local_addr()
        .expect("local addr should be available");

    let mut client_a = TcpStream::connect(listen_addr).expect("first connect should succeed");
    let mut client_b = TcpStream::connect(listen_addr).expect("second connect should succeed");
    client_a
        .set_nonblocking(true)
        .expect("first client should be nonblocking");
    client_b
        .set_nonblocking(true)
        .expect("second client should be nonblocking");

    let distribution_deadline = Instant::now() + Duration::from_millis(800);
    let mut counts = Vec::new();
    while Instant::now() < distribution_deadline {
        let _ = reactor
            .poll_once(Some(Duration::from_millis(5)))
            .expect("threaded reactor poll should succeed");
        counts = reactor.worker_connection_counts();
        if counts.iter().filter(|count| **count > 0).count() == 2 {
            break;
        }
    }
    assert_that!(
        counts.iter().filter(|count| **count > 0).count(),
        eq(2_usize)
    );

    let mut pipeline_a = Vec::new();
    let mut pipeline_b = Vec::new();
    let mut expected_a = Vec::new();
    let mut expected_b = Vec::new();
    for index in 0..PAIRS {
        let key_a = format!("ka{index}");
        let value_a = format!("va{index}");
        pipeline_a.extend_from_slice(
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key_a.len(),
                key_a,
                value_a.len(),
                value_a
            )
            .as_bytes(),
        );
        pipeline_a.extend_from_slice(
            format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key_a.len(), key_a).as_bytes(),
        );
        expected_a.extend_from_slice(b"+OK\r\n");
        expected_a.extend_from_slice(format!("${}\r\n{}\r\n", value_a.len(), value_a).as_bytes());

        let key_b = format!("kb{index}");
        let value_b = format!("vb{index}");
        pipeline_b.extend_from_slice(
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key_b.len(),
                key_b,
                value_b.len(),
                value_b
            )
            .as_bytes(),
        );
        pipeline_b.extend_from_slice(
            format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key_b.len(), key_b).as_bytes(),
        );
        expected_b.extend_from_slice(b"+OK\r\n");
        expected_b.extend_from_slice(format!("${}\r\n{}\r\n", value_b.len(), value_b).as_bytes());
    }

    client_a
        .write_all(&pipeline_a)
        .expect("first pipeline write should succeed");
    client_b
        .write_all(&pipeline_b)
        .expect("second pipeline write should succeed");

    let mut reply_a = Vec::new();
    let mut reply_b = Vec::new();
    let deadline = Instant::now() + Duration::from_millis(2500);
    while Instant::now() < deadline {
        let _ = reactor
            .poll_once(Some(Duration::from_millis(5)))
            .expect("threaded reactor poll should succeed");

        let mut chunk_a = [0_u8; 4096];
        match client_a.read(&mut chunk_a) {
            Ok(0) => {}
            Ok(read_len) => reply_a.extend_from_slice(&chunk_a[..read_len]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("first mixed pipeline read failed: {error}"),
        }

        let mut chunk_b = [0_u8; 4096];
        match client_b.read(&mut chunk_b) {
            Ok(0) => {}
            Ok(read_len) => reply_b.extend_from_slice(&chunk_b[..read_len]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("second mixed pipeline read failed: {error}"),
        }

        if reply_a.len() >= expected_a.len() && reply_b.len() >= expected_b.len() {
            break;
        }
    }

    assert_that!(&reply_a, eq(&expected_a));
    assert_that!(&reply_b, eq(&expected_b));
}

#[rstest]
fn threaded_reactor_executes_resp_and_memcache_roundtrip() {
    let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
    let config = ServerReactorConfig {
        io_worker_count: 2,
        ..ServerReactorConfig::default()
    };
    let mut reactor = ThreadedServerReactor::bind_with_memcache(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        Some(SocketAddr::from(([127, 0, 0, 1], 0))),
        config,
        &app_executor,
    )
    .expect("threaded reactor bind should succeed");
    let resp_addr = reactor
        .local_addr()
        .expect("resp local addr should be available");
    let memcache_addr = reactor
        .memcache_local_addr()
        .expect("memcache local addr should be available");

    let mut resp_client = TcpStream::connect(resp_addr).expect("resp connect should succeed");
    let mut mem_client =
        TcpStream::connect(memcache_addr).expect("memcache connect should succeed");
    resp_client
        .set_nonblocking(true)
        .expect("resp client should be nonblocking");
    mem_client
        .set_nonblocking(true)
        .expect("memcache client should be nonblocking");

    resp_client
        .write_all(b"*1\r\n$4\r\nPING\r\n")
        .expect("RESP ping write should succeed");
    mem_client
        .write_all(b"set mk 0 0 2\r\nok\r\nget mk\r\n")
        .expect("Memcache roundtrip write should succeed");

    let deadline = Instant::now() + Duration::from_millis(1000);
    let mut resp_reply = Vec::new();
    let mut mem_reply = Vec::new();
    while Instant::now() < deadline {
        let _ = reactor
            .poll_once(Some(Duration::from_millis(5)))
            .expect("threaded reactor poll should succeed");

        let mut resp_chunk = [0_u8; 64];
        match resp_client.read(&mut resp_chunk) {
            Ok(0) => {}
            Ok(read_len) => resp_reply.extend_from_slice(&resp_chunk[..read_len]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("read from resp client failed: {error}"),
        }

        let mut mem_chunk = [0_u8; 256];
        match mem_client.read(&mut mem_chunk) {
            Ok(0) => {}
            Ok(read_len) => mem_reply.extend_from_slice(&mem_chunk[..read_len]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("read from memcache client failed: {error}"),
        }

        if resp_reply.ends_with(b"+PONG\r\n") && mem_reply.ends_with(b"END\r\n") {
            break;
        }
    }

    assert_that!(&resp_reply, eq(&b"+PONG\r\n".to_vec()));
    assert_that!(
        &mem_reply,
        eq(&b"STORED\r\nVALUE mk 0 2\r\nok\r\nEND\r\n".to_vec())
    );
}

#[rstest]
fn deferred_ready_queue_preserves_reply_order() {
    let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
    let config = ServerReactorConfig {
        io_worker_count: 2,
        ..ServerReactorConfig::default()
    };
    let mut reactor =
        ThreadedServerReactor::bind(SocketAddr::from(([127, 0, 0, 1], 0)), config, &app_executor)
            .expect("threaded reactor bind should succeed");
    let listen_addr = reactor
        .local_addr()
        .expect("resp local addr should be available");

    let mut client = TcpStream::connect(listen_addr).expect("connect should succeed");
    client
        .set_nonblocking(true)
        .expect("client should be nonblocking");

    client
        .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
        .expect("SET write should succeed");

    let mut set_reply = Vec::new();
    let set_deadline = Instant::now() + Duration::from_millis(800);
    while Instant::now() < set_deadline {
        let _ = reactor
            .poll_once(Some(Duration::from_millis(5)))
            .expect("threaded reactor poll should succeed");
        let mut chunk = [0_u8; 64];
        match client.read(&mut chunk) {
            Ok(0) => {}
            Ok(read_len) => {
                set_reply.extend_from_slice(&chunk[..read_len]);
                if set_reply.ends_with(b"+OK\r\n") {
                    break;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("read set reply failed: {error}"),
        }
    }
    assert_that!(&set_reply, eq(&b"+OK\r\n".to_vec()));

    client
        .write_all(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n*1\r\n$4\r\nPING\r\n")
        .expect("pipeline write should succeed");

    let mut pipeline_reply = Vec::new();
    let pipeline_deadline = Instant::now() + Duration::from_millis(1000);
    while Instant::now() < pipeline_deadline {
        let _ = reactor
            .poll_once(Some(Duration::from_millis(5)))
            .expect("threaded reactor poll should succeed");
        let mut chunk = [0_u8; 128];
        match client.read(&mut chunk) {
            Ok(0) => {}
            Ok(read_len) => {
                pipeline_reply.extend_from_slice(&chunk[..read_len]);
                if pipeline_reply.ends_with(b"+PONG\r\n") {
                    break;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("read pipeline reply failed: {error}"),
        }
    }

    assert_that!(&pipeline_reply, eq(&b"$1\r\nv\r\n+PONG\r\n".to_vec()));
}

#[rstest]
fn threaded_reactor_memcache_parse_error_uses_client_error_and_keeps_connection_open() {
    let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
    let mut reactor = ThreadedServerReactor::bind_with_memcache(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        Some(SocketAddr::from(([127, 0, 0, 1], 0))),
        ServerReactorConfig {
            io_worker_count: 2,
            ..ServerReactorConfig::default()
        },
        &app_executor,
    )
    .expect("threaded reactor bind should succeed");
    let memcache_addr = reactor
        .memcache_local_addr()
        .expect("memcache local addr should be available");

    let mut client = TcpStream::connect(memcache_addr).expect("connect should succeed");
    client
        .set_nonblocking(true)
        .expect("client should be nonblocking");

    client
        .write_all(b"set user:42 0 0 5\r\nalice\r\n")
        .expect("write set should succeed");
    let store_deadline = Instant::now() + Duration::from_millis(600);
    let mut store_reply = Vec::new();
    while Instant::now() < store_deadline {
        let _ = reactor
            .poll_once(Some(Duration::from_millis(5)))
            .expect("threaded reactor poll should succeed");
        let mut chunk = [0_u8; 64];
        match client.read(&mut chunk) {
            Ok(0) => {}
            Ok(read_len) => {
                store_reply.extend_from_slice(&chunk[..read_len]);
                if store_reply.ends_with(b"STORED\r\n") {
                    break;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("read store reply failed: {error}"),
        }
    }
    assert_that!(&store_reply, eq(&b"STORED\r\n".to_vec()));

    client
        .write_all(b"set broken 0 0 -1\r\nget user:42\r\n")
        .expect("write invalid+valid pipeline should succeed");
    let deadline = Instant::now() + Duration::from_millis(1000);
    let mut response = Vec::new();
    while Instant::now() < deadline {
        let _ = reactor
            .poll_once(Some(Duration::from_millis(5)))
            .expect("threaded reactor poll should succeed");
        let mut chunk = [0_u8; 256];
        match client.read(&mut chunk) {
            Ok(0) => {}
            Ok(read_len) => {
                response.extend_from_slice(&chunk[..read_len]);
                if response.ends_with(b"END\r\n") {
                    break;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("read memcache reply failed: {error}"),
        }
    }
    let text = String::from_utf8_lossy(&response);
    assert_that!(text.contains("CLIENT_ERROR"), eq(true));
    assert_that!(
        text.contains("VALUE user:42 0 5\r\nalice\r\nEND\r\n"),
        eq(true)
    );
}

#[rstest]
fn worker_read_pauses_when_command_queue_limit_is_reached() {
    let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .expect("listener bind should succeed");
    let listen_addr = listener
        .local_addr()
        .expect("listener must expose local addr");
    let mut client = TcpStream::connect(listen_addr).expect("connect should succeed");
    let (server_stream, _) = listener.accept().expect("accept should succeed");
    server_stream
        .set_nonblocking(true)
        .expect("accepted socket should be nonblocking");
    client
        .set_nonblocking(false)
        .expect("client blocking mode should be configurable");

    let mut connection = super::WorkerConnection::new(
        1,
        mio::net::TcpStream::from_std(server_stream),
        ClientProtocol::Resp,
    );
    let mut payload = Vec::new();
    for _ in 0..8 {
        payload.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
    }
    client
        .write_all(&payload)
        .expect("pipeline write should succeed");
    client
        .shutdown(Shutdown::Write)
        .expect("write-half shutdown should succeed");

    let mut io_state = super::WorkerIoState::new(Arc::new(AtomicUsize::new(0)));
    let deadline = Instant::now() + Duration::from_millis(300);
    while Instant::now() < deadline {
        super::read_worker_connection_bytes(
            Token(2),
            &mut connection,
            &app_executor,
            &mut io_state.loop_state,
            super::BackpressureThresholds {
                high: usize::MAX,
                low: usize::MAX / 2,
                max_pending_per_connection: 2,
                max_pending_per_worker: 2,
            },
        );
        if connection.read_paused_by_command_backpressure {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }

    assert_that!(connection.read_paused_by_command_backpressure, eq(true));
    assert_that!(connection.pending_request_count, eq(2_usize));
    assert_that!(io_state.loop_state.pending_requests_for_worker, eq(2_usize));
}

#[rstest]
fn disconnected_worker_connection_reaps_detached_deferred_replies() {
    let app_executor = AppExecutor::new(ServerApp::new(RuntimeConfig::default()));
    let frame = CommandFrame::new("GET", vec![b"leak-check".to_vec()]);
    let mut command_connection = ServerApp::new_connection(ClientProtocol::Resp);
    let execution = app_executor.execute_parsed_command_deferred(
        &mut command_connection,
        ParsedCommand {
            name: frame.name.clone(),
            args: frame.args.clone(),
        },
    );
    let ParsedCommandExecution::Deferred(ticket) = execution else {
        panic!("GET should dispatch via deferred runtime path");
    };
    let _shard = ticket
        .barriers
        .first()
        .map(|barrier| barrier.shard)
        .expect("deferred ticket should have one shard barrier");
    let mut probe_ticket = ticket.clone();

    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .expect("listener bind should succeed");
    let listen_addr = listener
        .local_addr()
        .expect("listener must expose local addr");
    let _client = TcpStream::connect(listen_addr).expect("connect should succeed");
    let (server_stream, _) = listener.accept().expect("accept should succeed");
    server_stream
        .set_nonblocking(true)
        .expect("accepted socket should be nonblocking");

    let mut connection = super::WorkerConnection::new(
        1,
        mio::net::TcpStream::from_std(server_stream),
        ClientProtocol::Resp,
    );
    connection.pending_request_count = 1;
    connection
        .pending_runtime_requests
        .push(super::PendingRuntimeRequest {
            request_id: 1,
            ticket,
        });

    let poll = Poll::new().expect("poll creation should succeed");
    let mut io_state = super::WorkerIoState::new(Arc::new(AtomicUsize::new(1)));
    let (event_sender, _event_receiver) = std::sync::mpsc::channel::<super::WorkerEvent>();
    super::close_worker_connection(
        &poll,
        Token(2),
        connection,
        &mut io_state.loop_state,
        &app_executor,
        &event_sender,
    );
    assert_that!(
        io_state.loop_state.detached_runtime_tickets.len(),
        eq(1_usize)
    );
    let deadline = std::time::Instant::now() + Duration::from_millis(800);
    let mut reply_ready = false;
    while std::time::Instant::now() < deadline {
        if app_executor
            .runtime_reply_ticket_ready(&mut probe_ticket)
            .expect("runtime readiness probe should succeed")
        {
            reply_ready = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_that!(reply_ready, eq(true));

    super::reap_detached_runtime_replies(&app_executor, &mut io_state);
    assert_that!(
        io_state.loop_state.detached_runtime_tickets.is_empty(),
        eq(true)
    );
    let missing_after_reap = app_executor.take_runtime_reply_ticket_ready(&mut probe_ticket);
    assert_that!(missing_after_reap.is_err(), eq(true));
}
