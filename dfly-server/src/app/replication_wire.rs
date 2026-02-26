use super::{
    FlowLsnParseError, ReplicaEndpointIdentity, ServerApp, ServerConnection,
    parse_flow_lsn_vector_for_flow, sync_session_error_reply, wrong_arity_message,
};
use dfly_common::config::ClusterMode;
use dfly_core::command::{CommandFrame, CommandReply};
use dfly_replication::state::FlowSyncType;
use std::sync::atomic::Ordering;
use std::time::Duration;

impl ServerApp {
    pub(super) fn execute_info(&self, frame: &CommandFrame) -> CommandReply {
        use std::fmt::Write as _;

        let mut include_replication = frame.args.is_empty();
        let mut include_persistence = frame.args.is_empty();
        for raw in &frame.args {
            let Ok(section_raw) = std::str::from_utf8(raw) else {
                return CommandReply::Error("INFO section must be valid UTF-8".to_owned());
            };

            for token in section_raw.split(',') {
                let section = token.trim().to_ascii_uppercase();
                match section.as_str() {
                    "ALL" | "DEFAULT" => {
                        include_replication = true;
                        include_persistence = true;
                    }
                    "REPLICATION" => include_replication = true,
                    "PERSISTENCE" => include_persistence = true,
                    _ => {}
                }
            }
        }

        let mut info = String::new();
        if include_replication {
            let (
                connected_replicas,
                master_replid,
                replication_offset,
                last_acked_lsn,
                replica_rows,
            ) = {
                let replication = self.replication_guard_with_latest_journal();
                (
                    replication.connected_replicas(),
                    replication.master_replid().to_owned(),
                    replication.replication_offset(),
                    replication.last_acked_lsn(),
                    replication.replica_info_rows(),
                )
            };
            write!(
                info,
                "# Replication\r\nrole:master\r\nconnected_slaves:{connected_replicas}\r\nmaster_replid:{master_replid}\r\nmaster_repl_offset:{replication_offset}\r\nlast_ack_lsn:{last_acked_lsn}\r\n",
            )
            .expect("writing to String should not fail");
            for (index, (address, port, state, lag)) in replica_rows.into_iter().enumerate() {
                write!(
                    info,
                    "slave{index}:ip={address},port={port},state={state},lag={lag}\r\n"
                )
                .expect("writing to String should not fail");
            }
        }
        if include_persistence {
            // Snapshot internals are intentionally simple in this learning path, but INFO shape
            // keeps Dragonfly-compatible section naming so section routing remains faithful.
            info.push_str("# Persistence\r\nloading:0\r\n");
        }
        CommandReply::BulkString(info.into_bytes())
    }

    pub(super) fn execute_time(frame: &CommandFrame) -> CommandReply {
        if !frame.args.is_empty() {
            return CommandReply::Error("wrong number of arguments for 'TIME' command".to_owned());
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or((0_u64, 0_u32), |duration| {
                (duration.as_secs(), duration.subsec_micros())
            });
        CommandReply::Array(vec![
            CommandReply::BulkString(now.0.to_string().into_bytes()),
            CommandReply::BulkString(now.1.to_string().into_bytes()),
        ])
    }

    pub(super) fn execute_readonly(frame: &CommandFrame) -> CommandReply {
        if !frame.args.is_empty() {
            return CommandReply::Error(
                "wrong number of arguments for 'READONLY' command".to_owned(),
            );
        }
        CommandReply::SimpleString("OK".to_owned())
    }

    pub(super) fn execute_readwrite(&self, frame: &CommandFrame) -> CommandReply {
        if !frame.args.is_empty() {
            return CommandReply::Error(
                "wrong number of arguments for 'READWRITE' command".to_owned(),
            );
        }
        if self.cluster_read_guard().mode != ClusterMode::Emulated {
            return CommandReply::Error(
                "Cluster is disabled. Use --cluster_mode=yes to enable.".to_owned(),
            );
        }
        CommandReply::SimpleString("OK".to_owned())
    }

    pub(super) fn execute_flushdb(&self, db: u16, frame: &CommandFrame) -> CommandReply {
        if !frame.args.is_empty() {
            return CommandReply::Error(
                "wrong number of arguments for 'FLUSHDB' command".to_owned(),
            );
        }
        let _ = self.core.flush_db(db);
        CommandReply::SimpleString("OK".to_owned())
    }

    pub(super) fn execute_dbsize(&self, db: u16, frame: &CommandFrame) -> CommandReply {
        if !frame.args.is_empty() {
            return CommandReply::Error(
                "wrong number of arguments for 'DBSIZE' command".to_owned(),
            );
        }

        CommandReply::Integer(i64::try_from(self.core.db_size(db)).unwrap_or(i64::MAX))
    }

    pub(super) fn execute_randomkey(&self, db: u16, frame: &CommandFrame) -> CommandReply {
        if !frame.args.is_empty() {
            return CommandReply::Error(
                "wrong number of arguments for 'RANDOMKEY' command".to_owned(),
            );
        }

        self.core
            .random_key(db)
            .map_or(CommandReply::Null, CommandReply::BulkString)
    }

    pub(super) fn execute_keys(&self, db: u16, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 1 {
            return CommandReply::Error("wrong number of arguments for 'KEYS' command".to_owned());
        }

        let keys = self.core.keys_matching(db, &frame.args[0]);
        let replies = keys
            .into_iter()
            .map(CommandReply::BulkString)
            .collect::<Vec<_>>();
        CommandReply::Array(replies)
    }

    pub(super) fn execute_flushall(&self, frame: &CommandFrame) -> CommandReply {
        if !frame.args.is_empty() {
            return CommandReply::Error(
                "wrong number of arguments for 'FLUSHALL' command".to_owned(),
            );
        }
        let _ = self.core.flush_all();
        CommandReply::SimpleString("OK".to_owned())
    }

    pub(super) fn execute_del(&self, db: u16, frame: &CommandFrame) -> CommandReply {
        self.execute_counting_key_command(db, frame, "DEL")
    }

    pub(super) fn execute_unlink(&self, db: u16, frame: &CommandFrame) -> CommandReply {
        self.execute_counting_key_command(db, frame, "UNLINK")
    }

    pub(super) fn execute_exists(&self, db: u16, frame: &CommandFrame) -> CommandReply {
        self.execute_counting_key_command(db, frame, "EXISTS")
    }

    pub(super) fn execute_touch(&self, db: u16, frame: &CommandFrame) -> CommandReply {
        self.execute_counting_key_command(db, frame, "TOUCH")
    }

    pub(super) fn execute_counting_key_command(
        &self,
        db: u16,
        frame: &CommandFrame,
        command: &str,
    ) -> CommandReply {
        if frame.args.is_empty() {
            return CommandReply::Error(wrong_arity_message(command));
        }

        let mut total = 0_i64;
        for key in &frame.args {
            if let Some(moved) = self.cluster_moved_reply_for_key(key) {
                return moved;
            }

            let single_key_frame = CommandFrame::new(command, vec![key.clone()]);
            let reply = self.core.execute_in_db(db, &single_key_frame);
            let CommandReply::Integer(delta) = reply else {
                return CommandReply::Error(format!(
                    "internal error: {command} did not return integer reply"
                ));
            };
            total = total.saturating_add(delta.max(0));
        }

        CommandReply::Integer(total)
    }

    pub(super) fn execute_role(&self, frame: &CommandFrame) -> CommandReply {
        if !frame.args.is_empty() {
            return CommandReply::Error("wrong number of arguments for 'ROLE' command".to_owned());
        }

        let replicas = self
            .replication_guard()
            .replica_role_rows()
            .into_iter()
            .map(|(address, listening_port, state)| {
                CommandReply::Array(vec![
                    CommandReply::BulkString(address.into_bytes()),
                    CommandReply::BulkString(listening_port.to_string().into_bytes()),
                    CommandReply::BulkString(state.as_bytes().to_vec()),
                ])
            })
            .collect::<Vec<_>>();
        // Dragonfly reports master mode as:
        // ["master", [[replica_ip, replica_port, replica_state], ...]]
        CommandReply::Array(vec![
            CommandReply::BulkString(b"master".to_vec()),
            CommandReply::Array(replicas),
        ])
    }

    pub(super) fn execute_replconf(
        &self,
        connection: &mut ServerConnection,
        frame: &CommandFrame,
    ) -> CommandReply {
        if frame.args.is_empty() || !frame.args.len().is_multiple_of(2) {
            return CommandReply::Error("syntax error".to_owned());
        }

        if frame.args.len() == 2
            && frame.args[0].eq_ignore_ascii_case(b"CAPA")
            && frame.args[1].eq_ignore_ascii_case(b"dragonfly")
        {
            let (master_replid, sync_id) = {
                let mut replication = self.replication_guard();
                (
                    replication.master_replid().to_owned(),
                    replication.create_sync_session(usize::from(self.config.shard_count.get())),
                )
            };
            return CommandReply::Array(vec![
                CommandReply::BulkString(master_replid.into_bytes()),
                CommandReply::BulkString(sync_id.into_bytes()),
                CommandReply::Integer(i64::from(self.config.shard_count.get())),
                CommandReply::Integer(1),
            ]);
        }

        let mut announced_port: Option<u16> = None;
        let mut announced_ip: Option<String> = None;

        for pair in frame.args.chunks_exact(2) {
            let option = pair[0].as_slice();
            let value = pair[1].as_slice();
            if option.eq_ignore_ascii_case(b"ACK") {
                if frame.args.len() != 2 {
                    return CommandReply::Error("syntax error".to_owned());
                }
                if let Err(error_reply) = self.handle_replconf_ack(connection, value) {
                    return error_reply;
                }
                continue;
            }
            if option.eq_ignore_ascii_case(b"CLIENT-ID") {
                if frame.args.len() != 2 {
                    return CommandReply::Error("syntax error".to_owned());
                }
                let Ok(text) = std::str::from_utf8(value) else {
                    return CommandReply::Error(
                        "REPLCONF CLIENT-ID must be valid UTF-8".to_owned(),
                    );
                };
                connection.replica_client_id = Some(text.to_owned());
                continue;
            }
            if option.eq_ignore_ascii_case(b"CLIENT-VERSION") {
                if frame.args.len() != 2 {
                    return CommandReply::Error("syntax error".to_owned());
                }
                let Ok(text) = std::str::from_utf8(value) else {
                    return CommandReply::Error(
                        "REPLCONF CLIENT-VERSION must be valid UTF-8".to_owned(),
                    );
                };
                let Ok(version) = text.parse::<u64>() else {
                    return CommandReply::Error(
                        "value is not an integer or out of range".to_owned(),
                    );
                };
                connection.replica_client_version = Some(version);
                continue;
            }
            if option.eq_ignore_ascii_case(b"LISTENING-PORT") {
                let Ok(text) = std::str::from_utf8(value) else {
                    return CommandReply::Error(
                        "REPLCONF LISTENING-PORT must be valid UTF-8".to_owned(),
                    );
                };
                let Ok(port) = text.parse::<u16>() else {
                    return CommandReply::Error(
                        "value is not an integer or out of range".to_owned(),
                    );
                };
                announced_port = Some(port);
                continue;
            }
            if option.eq_ignore_ascii_case(b"IP-ADDRESS") {
                let Ok(text) = std::str::from_utf8(value) else {
                    return CommandReply::Error(
                        "REPLCONF IP-ADDRESS must be valid UTF-8".to_owned(),
                    );
                };
                announced_ip = Some(text.to_owned());
                continue;
            }
            if option.eq_ignore_ascii_case(b"CAPA") {
                continue;
            }
            return CommandReply::Error("syntax error".to_owned());
        }

        self.apply_replconf_endpoint_update(connection, announced_port, announced_ip);

        CommandReply::SimpleString("OK".to_owned())
    }

    pub(super) fn handle_replconf_ack(
        &self,
        connection: &mut ServerConnection,
        value: &[u8],
    ) -> Result<(), CommandReply> {
        let Ok(text) = std::str::from_utf8(value) else {
            return Err(CommandReply::Error(
                "REPLCONF ACK offset must be valid UTF-8".to_owned(),
            ));
        };
        let Ok(ack_lsn) = text.parse::<u64>() else {
            return Err(CommandReply::Error(
                "value is not an integer or out of range".to_owned(),
            ));
        };
        self.ensure_replica_endpoint_for_ack(connection);
        if let Some(endpoint) = connection.replica_endpoint.as_ref() {
            let _ = self
                .replication_guard_with_latest_journal()
                .record_replica_ack_for_endpoint(
                    &endpoint.address,
                    endpoint.listening_port,
                    ack_lsn,
                );
        }
        Ok(())
    }

    pub(super) fn ensure_replica_endpoint_for_ack(&self, connection: &mut ServerConnection) {
        if connection.replica_endpoint.is_some() {
            return;
        }

        // Some replicas may ACK before announcing LISTENING-PORT/IP metadata.
        // Register a stable synthetic endpoint so WAIT/INFO can account for that replica.
        let implicit_id = self.next_implicit_replica_id.fetch_add(1, Ordering::AcqRel);
        let endpoint = ReplicaEndpointIdentity {
            address: format!("implicit-replica-{implicit_id}"),
            listening_port: 0,
        };
        self.register_connection_replica_endpoint(connection, endpoint);
    }

    pub(super) fn register_connection_replica_endpoint(
        &self,
        connection: &mut ServerConnection,
        endpoint: ReplicaEndpointIdentity,
    ) {
        let mut replication = self.replication_guard();
        if let Some(existing) = connection.replica_endpoint.clone()
            && existing != endpoint
        {
            let _ = replication.remove_replica_endpoint(&existing.address, existing.listening_port);
        }

        replication.register_replica_endpoint(endpoint.address.clone(), endpoint.listening_port);
        connection.replica_endpoint = Some(endpoint);
    }

    pub(super) fn apply_replconf_endpoint_update(
        &self,
        connection: &mut ServerConnection,
        announced_port: Option<u16>,
        announced_ip: Option<String>,
    ) {
        if let Some(port) = announced_port {
            let address = announced_ip
                .or_else(|| {
                    connection
                        .replica_endpoint
                        .as_ref()
                        .map(|endpoint| endpoint.address.clone())
                })
                .unwrap_or_else(|| "127.0.0.1".to_owned());
            self.register_connection_replica_endpoint(
                connection,
                ReplicaEndpointIdentity {
                    address,
                    listening_port: port,
                },
            );
            return;
        }

        if let Some(address) = announced_ip
            && let Some(existing_port) = connection
                .replica_endpoint
                .as_ref()
                .map(|endpoint| endpoint.listening_port)
        {
            self.register_connection_replica_endpoint(
                connection,
                ReplicaEndpointIdentity {
                    address,
                    listening_port: existing_port,
                },
            );
        }
    }

    pub(super) fn execute_psync(&self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 2 {
            return CommandReply::Error("wrong number of arguments for 'PSYNC' command".to_owned());
        }

        let Ok(requested_replid) = std::str::from_utf8(&frame.args[0]) else {
            return CommandReply::Error("PSYNC replid must be valid UTF-8".to_owned());
        };
        let Ok(requested_offset_text) = std::str::from_utf8(&frame.args[1]) else {
            return CommandReply::Error("PSYNC offset must be valid UTF-8".to_owned());
        };
        let Ok(requested_offset) = requested_offset_text.parse::<i64>() else {
            return CommandReply::Error("PSYNC offset must be a valid integer".to_owned());
        };

        let requested_offset_u64 = u64::try_from(requested_offset).ok();
        let mut replication = self.replication_guard_with_latest_journal();
        let master_replid = replication.master_replid().to_owned();
        let master_offset = replication.replication_offset();
        if requested_replid == master_replid
            && requested_offset_u64
                .is_some_and(|offset| replication.can_partial_sync_from_offset(offset))
        {
            replication.mark_replicas_stable_sync();
            return CommandReply::SimpleString("CONTINUE".to_owned());
        }

        replication.mark_replicas_full_sync();
        CommandReply::SimpleString(format!("FULLRESYNC {master_replid} {master_offset}"))
    }

    pub(super) fn execute_wait(&self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 2 {
            return CommandReply::Error("wrong number of arguments for 'WAIT' command".to_owned());
        }

        let Ok(required_text) = std::str::from_utf8(&frame.args[0]) else {
            return CommandReply::Error("WAIT numreplicas must be valid UTF-8".to_owned());
        };
        let Ok(required) = required_text.parse::<u64>() else {
            return CommandReply::Error("value is not an integer or out of range".to_owned());
        };
        let Ok(timeout_text) = std::str::from_utf8(&frame.args[1]) else {
            return CommandReply::Error("WAIT timeout must be valid UTF-8".to_owned());
        };
        let Ok(timeout_millis) = timeout_text.parse::<u64>() else {
            return CommandReply::Error("value is not an integer or out of range".to_owned());
        };

        self.flush_journal_append_lane();
        // WAIT returns the number of replicas that acknowledged the master offset captured at
        // WAIT entry time. We block on ACK progress with a cloneable watcher so the blocking wait
        // does not hold the outer replication mutex and starve ACK updates.
        let (target_offset, ack_watcher) = {
            let replication = self.replication_guard();
            (
                replication.replication_offset(),
                replication.ack_progress_watcher(),
            )
        };
        let timeout = Duration::from_millis(timeout_millis);
        let wait_started_at = std::time::Instant::now();
        let current_replica_count = || -> u64 {
            let replication = self.replication_guard();
            u64::try_from(replication.acked_replica_count_at_or_above(target_offset))
                .unwrap_or(u64::MAX)
        };
        let integer_reply =
            |replicated: u64| CommandReply::Integer(i64::try_from(replicated).unwrap_or(i64::MAX));

        loop {
            let observed_progress = ack_watcher.token();
            let replicated = current_replica_count();
            if replicated >= required || timeout.is_zero() {
                return integer_reply(replicated);
            }

            let elapsed = wait_started_at.elapsed();
            if elapsed >= timeout {
                return integer_reply(replicated);
            }

            let remaining = timeout.saturating_sub(elapsed);
            let progress = ack_watcher.wait_for_progress_since(observed_progress, remaining);
            if !progress {
                return integer_reply(current_replica_count());
            }
        }
    }

    pub(super) fn execute_dfly_flow(&self, frame: &CommandFrame) -> CommandReply {
        if !(frame.args.len() == 4 || frame.args.len() == 5 || frame.args.len() == 6) {
            return CommandReply::Error(
                "wrong number of arguments for 'DFLY FLOW' command".to_owned(),
            );
        }

        let Ok(master_id) = std::str::from_utf8(&frame.args[1]) else {
            return CommandReply::Error("DFLY FLOW master id must be valid UTF-8".to_owned());
        };

        let Ok(sync_id) = std::str::from_utf8(&frame.args[2]) else {
            return CommandReply::Error("DFLY FLOW sync id must be valid UTF-8".to_owned());
        };

        let Ok(flow_id_text) = std::str::from_utf8(&frame.args[3]) else {
            return CommandReply::Error("DFLY FLOW flow id must be valid UTF-8".to_owned());
        };
        let Ok(flow_id) = flow_id_text.parse::<usize>() else {
            return CommandReply::Error("value is not an integer or out of range".to_owned());
        };
        if flow_id >= usize::from(self.config.shard_count.get()) {
            return CommandReply::Error("value is not an integer or out of range".to_owned());
        }

        let requested_offset = if frame.args.len() == 6 {
            if !frame.args[4].eq_ignore_ascii_case(b"LASTMASTER") {
                return CommandReply::Error("syntax error".to_owned());
            }
            let Ok(lsn_vec_text) = std::str::from_utf8(&frame.args[5]) else {
                return CommandReply::Error("DFLY FLOW lsn vector must be valid UTF-8".to_owned());
            };
            match parse_flow_lsn_vector_for_flow(
                lsn_vec_text,
                flow_id,
                usize::from(self.config.shard_count.get()),
            ) {
                Ok(lsn) => Some(lsn),
                Err(FlowLsnParseError::Syntax) => {
                    return CommandReply::Error("syntax error".to_owned());
                }
                Err(FlowLsnParseError::InvalidInt) => {
                    return CommandReply::Error(
                        "value is not an integer or out of range".to_owned(),
                    );
                }
            }
        } else if let Some(lsn_arg) = frame.args.get(4) {
            let Ok(lsn_text) = std::str::from_utf8(lsn_arg) else {
                return CommandReply::Error("DFLY FLOW lsn must be valid UTF-8".to_owned());
            };
            let Ok(lsn) = lsn_text.parse::<u64>() else {
                return CommandReply::Error("value is not an integer or out of range".to_owned());
            };
            Some(lsn)
        } else {
            None
        };

        let mut replication = self.replication_guard_with_latest_journal();
        if master_id != replication.master_replid() {
            return CommandReply::Error("bad master id".to_owned());
        }
        if !replication.is_known_sync_session(sync_id) {
            return CommandReply::Error("syncid not found".to_owned());
        }

        let (sync_type, flow_start_offset) = requested_offset
            .filter(|offset| replication.can_partial_sync_from_offset(*offset))
            .map_or((FlowSyncType::Full, None), |offset| {
                (FlowSyncType::Partial, Some(offset))
            });

        let eof_token = replication.allocate_flow_eof_token();
        if let Err(error) = replication.register_sync_flow(
            sync_id,
            flow_id,
            sync_type,
            flow_start_offset,
            eof_token.clone(),
        ) {
            return sync_session_error_reply(error);
        }

        CommandReply::Array(vec![
            CommandReply::SimpleString(match sync_type {
                FlowSyncType::Full => "FULL".to_owned(),
                FlowSyncType::Partial => "PARTIAL".to_owned(),
            }),
            CommandReply::SimpleString(eof_token),
        ])
    }

    pub(super) fn execute_dfly_sync(&self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 2 {
            return CommandReply::Error(
                "wrong number of arguments for 'DFLY SYNC' command".to_owned(),
            );
        }
        let Ok(sync_id) = std::str::from_utf8(&frame.args[1]) else {
            return CommandReply::Error("DFLY SYNC sync id must be valid UTF-8".to_owned());
        };
        let mut replication = self.replication_guard();
        if let Err(error) = replication.mark_sync_session_full_sync(sync_id) {
            return sync_session_error_reply(error);
        }

        replication.mark_replicas_full_sync();
        CommandReply::SimpleString("OK".to_owned())
    }

    pub(super) fn execute_dfly_startstable(&self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 2 {
            return CommandReply::Error(
                "wrong number of arguments for 'DFLY STARTSTABLE' command".to_owned(),
            );
        }
        let Ok(sync_id) = std::str::from_utf8(&frame.args[1]) else {
            return CommandReply::Error("DFLY STARTSTABLE sync id must be valid UTF-8".to_owned());
        };
        let mut replication = self.replication_guard();
        if let Err(error) = replication.mark_sync_session_stable_sync(sync_id) {
            return sync_session_error_reply(error);
        }

        replication.mark_replicas_stable_sync();
        CommandReply::SimpleString("OK".to_owned())
    }

    pub(super) fn execute_dfly_admin(&self, frame: &CommandFrame) -> CommandReply {
        let Some(subcommand_raw) = frame.args.first() else {
            return CommandReply::Error("wrong number of arguments for 'DFLY' command".to_owned());
        };
        let Ok(subcommand) = std::str::from_utf8(subcommand_raw) else {
            return CommandReply::Error("DFLY subcommand must be valid UTF-8".to_owned());
        };

        match subcommand.to_ascii_uppercase().as_str() {
            "FLOW" => self.execute_dfly_flow(frame),
            "REPLICAOFFSET" => self.execute_dfly_replicaoffset(frame),
            "SAVE" => self.execute_dfly_save(frame),
            "SYNC" => self.execute_dfly_sync(frame),
            "STARTSTABLE" => self.execute_dfly_startstable(frame),
            "LOAD" => self.execute_dfly_load(frame),
            _ => CommandReply::Error(format!("unknown DFLY subcommand '{subcommand}'")),
        }
    }

    pub(super) fn execute_dfly_replicaoffset(&self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 1 {
            return CommandReply::Error(
                "wrong number of arguments for 'DFLY REPLICAOFFSET' command".to_owned(),
            );
        }

        let offset = self
            .replication_guard_with_latest_journal()
            .replication_offset();
        let offsets = (0..usize::from(self.config.shard_count.get()))
            .map(|_| CommandReply::Integer(i64::try_from(offset).unwrap_or(i64::MAX)))
            .collect::<Vec<_>>();
        CommandReply::Array(offsets)
    }

    pub(super) fn execute_dfly_save(&self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 2 {
            return CommandReply::Error(
                "wrong number of arguments for 'DFLY SAVE' command".to_owned(),
            );
        }
        let Ok(path) = std::str::from_utf8(&frame.args[1]) else {
            return CommandReply::Error("DFLY SAVE path must be valid UTF-8".to_owned());
        };

        let snapshot = self.core.export_snapshot();
        match self.storage.write_snapshot_file(path, &snapshot) {
            Ok(()) => CommandReply::SimpleString("OK".to_owned()),
            Err(error) => CommandReply::Error(format!("DFLY SAVE failed: {error}")),
        }
    }

    pub(super) fn execute_dfly_load(&self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 2 {
            return CommandReply::Error(
                "wrong number of arguments for 'DFLY LOAD' command".to_owned(),
            );
        }
        let Ok(path) = std::str::from_utf8(&frame.args[1]) else {
            return CommandReply::Error("DFLY LOAD path must be valid UTF-8".to_owned());
        };

        let snapshot = match self.storage.read_snapshot_file(path) {
            Ok(snapshot) => snapshot,
            Err(error) => return CommandReply::Error(format!("DFLY LOAD failed: {error}")),
        };
        if let Err(error) = self.install_snapshot(&snapshot) {
            return CommandReply::Error(format!("DFLY LOAD failed: {error}"));
        }
        CommandReply::SimpleString("OK".to_owned())
    }
}
