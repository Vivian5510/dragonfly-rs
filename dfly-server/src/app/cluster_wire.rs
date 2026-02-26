use super::ServerApp;
use crate::app::protocol_reply::split_endpoint_host_port;
use dfly_common::config::ClusterMode;
use dfly_cluster::slot::key_slot;
use dfly_core::command::{CommandFrame, CommandReply};

impl ServerApp {
    pub(super) fn execute_cluster(&self, db: u16, frame: &CommandFrame) -> CommandReply {
        if self.cluster_read_guard().mode == ClusterMode::Disabled {
            return CommandReply::Error(
                "Cluster is disabled. Use --cluster_mode=yes to enable.".to_owned(),
            );
        }

        let Some(subcommand_raw) = frame.args.first() else {
            return CommandReply::Error(
                "wrong number of arguments for 'CLUSTER' command".to_owned(),
            );
        };
        let Ok(subcommand) = std::str::from_utf8(subcommand_raw) else {
            return CommandReply::Error("CLUSTER subcommand must be valid UTF-8".to_owned());
        };

        match subcommand.to_ascii_uppercase().as_str() {
            "HELP" => Self::execute_cluster_help(frame),
            "COUNTKEYSINSLOT" => self.execute_cluster_countkeysinslot(db, frame),
            "GETKEYSINSLOT" => self.execute_cluster_getkeysinslot(db, frame),
            "INFO" => self.execute_cluster_info(frame),
            "KEYSLOT" => Self::execute_cluster_keyslot(frame),
            "MYID" => self.execute_cluster_myid(frame),
            "NODES" => self.execute_cluster_nodes(frame),
            "SHARDS" => self.execute_cluster_shards(frame),
            "SLOTS" => self.execute_cluster_slots(frame),
            _ => CommandReply::Error(format!("unknown CLUSTER subcommand '{subcommand}'")),
        }
    }

    fn execute_cluster_keyslot(frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 2 {
            return CommandReply::Error(
                "wrong number of arguments for 'CLUSTER KEYSLOT' command".to_owned(),
            );
        }
        CommandReply::Integer(i64::from(key_slot(&frame.args[1])))
    }

    fn execute_cluster_getkeysinslot(&self, db: u16, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 3 {
            return CommandReply::Error(
                "wrong number of arguments for 'CLUSTER GETKEYSINSLOT' command".to_owned(),
            );
        }

        let Ok(slot_text) = std::str::from_utf8(&frame.args[1]) else {
            return CommandReply::Error(
                "CLUSTER GETKEYSINSLOT slot must be valid UTF-8".to_owned(),
            );
        };
        let Ok(slot) = slot_text.parse::<u16>() else {
            return CommandReply::Error("value is not an integer or out of range".to_owned());
        };
        if slot > 16_383 {
            return CommandReply::Error("slot is out of range".to_owned());
        }

        let Ok(count_text) = std::str::from_utf8(&frame.args[2]) else {
            return CommandReply::Error(
                "CLUSTER GETKEYSINSLOT count must be valid UTF-8".to_owned(),
            );
        };
        let Ok(limit) = count_text.parse::<usize>() else {
            return CommandReply::Error("value is not an integer or out of range".to_owned());
        };

        let mut keys = self.core.keys_in_slot(db, slot);
        if keys.len() > limit {
            keys.truncate(limit);
        }
        CommandReply::Array(
            keys.into_iter()
                .map(CommandReply::BulkString)
                .collect::<Vec<_>>(),
        )
    }

    fn execute_cluster_countkeysinslot(&self, db: u16, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 2 {
            return CommandReply::Error(
                "wrong number of arguments for 'CLUSTER COUNTKEYSINSLOT' command".to_owned(),
            );
        }

        let Ok(slot_text) = std::str::from_utf8(&frame.args[1]) else {
            return CommandReply::Error(
                "CLUSTER COUNTKEYSINSLOT slot must be valid UTF-8".to_owned(),
            );
        };
        let Ok(slot) = slot_text.parse::<u16>() else {
            return CommandReply::Error("value is not an integer or out of range".to_owned());
        };
        if slot > 16_383 {
            return CommandReply::Error("slot is out of range".to_owned());
        }

        let count = self.core.count_keys_in_slot(db, slot);
        CommandReply::Integer(i64::try_from(count).unwrap_or(i64::MAX))
    }

    fn execute_cluster_help(frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 1 {
            return CommandReply::Error(
                "wrong number of arguments for 'CLUSTER HELP' command".to_owned(),
            );
        }
        let lines = [
            "CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "SLOTS",
            "   Return information about slots range mappings. Each range is made of:",
            "   start, end, master and replicas IP addresses, ports and ids.",
            "COUNTKEYSINSLOT <slot>",
            "   Return the number of keys in one hash slot.",
            "GETKEYSINSLOT <slot> <count>",
            "   Return up to <count> keys for one hash slot.",
            "NODES",
            "   Return cluster configuration seen by node. Output format:",
            "   <id> <ip:port> <flags> <master> <pings> <pongs> <epoch> <link> <slot> ...",
            "INFO",
            "  Return information about the cluster",
            "HELP",
            "    Prints this help.",
        ];
        CommandReply::Array(
            lines
                .into_iter()
                .map(|line| CommandReply::BulkString(line.as_bytes().to_vec()))
                .collect(),
        )
    }

    fn execute_cluster_slots(&self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 1 {
            return CommandReply::Error(
                "wrong number of arguments for 'CLUSTER SLOTS' command".to_owned(),
            );
        }

        let cluster = self.cluster_read_guard();
        let (host, port) = split_endpoint_host_port(&cluster.redirect_endpoint);
        let slots = cluster
            .owned_ranges()
            .iter()
            .map(|range| {
                CommandReply::Array(vec![
                    CommandReply::Integer(i64::from(range.start)),
                    CommandReply::Integer(i64::from(range.end)),
                    CommandReply::Array(vec![
                        CommandReply::BulkString(host.as_bytes().to_vec()),
                        CommandReply::Integer(port),
                    ]),
                ])
            })
            .collect::<Vec<_>>();
        CommandReply::Array(slots)
    }

    fn execute_cluster_myid(&self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 1 {
            return CommandReply::Error(
                "wrong number of arguments for 'CLUSTER MYID' command".to_owned(),
            );
        }
        let cluster = self.cluster_read_guard();
        CommandReply::BulkString(cluster.node_id.as_bytes().to_vec())
    }

    fn execute_cluster_shards(&self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 1 {
            return CommandReply::Error(
                "wrong number of arguments for 'CLUSTER SHARDS' command".to_owned(),
            );
        }

        let cluster = self.cluster_read_guard();
        let (host, port) = split_endpoint_host_port(&cluster.redirect_endpoint);
        let slots = cluster
            .owned_ranges()
            .iter()
            .flat_map(|range| {
                [
                    CommandReply::Integer(i64::from(range.start)),
                    CommandReply::Integer(i64::from(range.end)),
                ]
            })
            .collect::<Vec<_>>();
        let nodes = vec![CommandReply::Array(vec![
            CommandReply::BulkString(b"id".to_vec()),
            CommandReply::BulkString(cluster.node_id.as_bytes().to_vec()),
            CommandReply::BulkString(b"endpoint".to_vec()),
            CommandReply::BulkString(host.as_bytes().to_vec()),
            CommandReply::BulkString(b"ip".to_vec()),
            CommandReply::BulkString(host.as_bytes().to_vec()),
            CommandReply::BulkString(b"port".to_vec()),
            CommandReply::Integer(port),
            CommandReply::BulkString(b"role".to_vec()),
            CommandReply::BulkString(b"master".to_vec()),
            CommandReply::BulkString(b"replication-offset".to_vec()),
            CommandReply::Integer(0),
            CommandReply::BulkString(b"health".to_vec()),
            CommandReply::BulkString(b"online".to_vec()),
        ])];

        CommandReply::Array(vec![CommandReply::Array(vec![
            CommandReply::BulkString(b"slots".to_vec()),
            CommandReply::Array(slots),
            CommandReply::BulkString(b"nodes".to_vec()),
            CommandReply::Array(nodes),
        ])])
    }

    fn execute_cluster_info(&self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 1 {
            return CommandReply::Error(
                "wrong number of arguments for 'CLUSTER INFO' command".to_owned(),
            );
        }

        let cluster = self.cluster_read_guard();
        let assigned = cluster.assigned_slots();
        let state = if assigned > 0 { "ok" } else { "fail" };
        let body = format!(
            "cluster_state:{state}\r\ncluster_slots_assigned:{assigned}\r\ncluster_slots_ok:{assigned}\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:1\r\ncluster_size:1\r\ncluster_current_epoch:{}\r\ncluster_my_epoch:{}\r\n",
            cluster.config_epoch, cluster.config_epoch
        );
        CommandReply::BulkString(body.into_bytes())
    }

    fn execute_cluster_nodes(&self, frame: &CommandFrame) -> CommandReply {
        if frame.args.len() != 1 {
            return CommandReply::Error(
                "wrong number of arguments for 'CLUSTER NODES' command".to_owned(),
            );
        }

        let cluster = self.cluster_read_guard();
        let (host, port) = split_endpoint_host_port(&cluster.redirect_endpoint);
        let bus_port = port.saturating_add(10_000);
        let ranges = cluster
            .owned_ranges()
            .iter()
            .map(|range| format!("{}-{}", range.start, range.end))
            .collect::<Vec<_>>()
            .join(" ");

        let mut line = format!(
            "{} {}:{}@{} myself,master - 0 0 {} connected",
            cluster.node_id, host, port, bus_port, cluster.config_epoch
        );
        if !ranges.is_empty() {
            line.push(' ');
            line.push_str(&ranges);
        }
        line.push('\n');
        CommandReply::BulkString(line.into_bytes())
    }

    pub(super) fn cluster_moved_reply_for_key(&self, key: &[u8]) -> Option<CommandReply> {
        let cluster = self.cluster_read_guard();
        cluster
            .moved_slot_for_key(key)
            .map(|slot| CommandReply::Moved {
                slot,
                endpoint: cluster.redirect_endpoint.clone(),
            })
    }
}
