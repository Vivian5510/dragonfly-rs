use super::{CommandArity, CommandSpec, DispatchState};
use super::handlers_counter::{handle_decr, handle_decrby, handle_incr, handle_incrby};
use super::handlers_expiry::{
    handle_expire, handle_expireat, handle_expiretime, handle_pexpire, handle_pexpireat,
    handle_pexpiretime, handle_persist, handle_psetex, handle_pttl, handle_setex, handle_ttl,
};
use super::handlers_keyspace::{
    handle_copy, handle_del, handle_exists, handle_move, handle_rename, handle_renamenx,
};
use super::handlers_string::{
    handle_append, handle_echo, handle_get, handle_getdel, handle_getrange, handle_getset,
    handle_mget, handle_mset, handle_msetnx, handle_ping, handle_set, handle_setnx,
    handle_setrange, handle_strlen, handle_type,
};
use crate::command::{CommandFrame, CommandReply};
use crate::containers::HotMap as HashMap;
use dfly_common::ids::DbIndex;
/// Runtime command registry.
#[derive(Debug, Clone, Default)]
pub struct CommandRegistry {
    entries: HashMap<String, CommandSpec>,
}

impl CommandRegistry {
    /// Builds an empty command registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Builds a registry preloaded with core learning-path commands.
    #[must_use]
    pub fn with_builtin_commands() -> Self {
        let mut registry = Self::new();
        registry.register_connection_commands();
        registry.register_string_commands();
        registry.register_expiry_commands();
        registry.register_counter_commands();
        registry
    }

    fn register_connection_commands(&mut self) {
        self.register(CommandSpec {
            name: "PING",
            arity: CommandArity::AtLeast(0),
            handler: handle_ping,
        });
        self.register(CommandSpec {
            name: "ECHO",
            arity: CommandArity::Exact(1),
            handler: handle_echo,
        });
    }

    fn register_string_commands(&mut self) {
        self.register(CommandSpec {
            name: "SET",
            arity: CommandArity::AtLeast(2),
            handler: handle_set,
        });
        self.register(CommandSpec {
            name: "SETNX",
            arity: CommandArity::Exact(2),
            handler: handle_setnx,
        });
        self.register_multi_key_string_commands();
        self.register(CommandSpec {
            name: "GET",
            arity: CommandArity::Exact(1),
            handler: handle_get,
        });
        self.register(CommandSpec {
            name: "TYPE",
            arity: CommandArity::Exact(1),
            handler: handle_type,
        });
        self.register(CommandSpec {
            name: "GETSET",
            arity: CommandArity::Exact(2),
            handler: handle_getset,
        });
        self.register(CommandSpec {
            name: "GETDEL",
            arity: CommandArity::Exact(1),
            handler: handle_getdel,
        });
        self.register(CommandSpec {
            name: "APPEND",
            arity: CommandArity::Exact(2),
            handler: handle_append,
        });
        self.register(CommandSpec {
            name: "STRLEN",
            arity: CommandArity::Exact(1),
            handler: handle_strlen,
        });
        self.register(CommandSpec {
            name: "GETRANGE",
            arity: CommandArity::Exact(3),
            handler: handle_getrange,
        });
        self.register(CommandSpec {
            name: "SETRANGE",
            arity: CommandArity::Exact(3),
            handler: handle_setrange,
        });
        self.register(CommandSpec {
            name: "DEL",
            arity: CommandArity::AtLeast(1),
            handler: handle_del,
        });
        self.register(CommandSpec {
            name: "UNLINK",
            arity: CommandArity::AtLeast(1),
            handler: handle_del,
        });
        self.register(CommandSpec {
            name: "EXISTS",
            arity: CommandArity::AtLeast(1),
            handler: handle_exists,
        });
        self.register(CommandSpec {
            name: "TOUCH",
            arity: CommandArity::AtLeast(1),
            handler: handle_exists,
        });
        self.register(CommandSpec {
            name: "MOVE",
            arity: CommandArity::Exact(2),
            handler: handle_move,
        });
        self.register(CommandSpec {
            name: "COPY",
            arity: CommandArity::AtLeast(2),
            handler: handle_copy,
        });
        self.register(CommandSpec {
            name: "RENAME",
            arity: CommandArity::Exact(2),
            handler: handle_rename,
        });
        self.register(CommandSpec {
            name: "RENAMENX",
            arity: CommandArity::Exact(2),
            handler: handle_renamenx,
        });
    }

    fn register_multi_key_string_commands(&mut self) {
        self.register(CommandSpec {
            name: "MSET",
            arity: CommandArity::AtLeast(2),
            handler: handle_mset,
        });
        self.register(CommandSpec {
            name: "MSETNX",
            arity: CommandArity::AtLeast(2),
            handler: handle_msetnx,
        });
        self.register(CommandSpec {
            name: "MGET",
            arity: CommandArity::AtLeast(1),
            handler: handle_mget,
        });
    }

    fn register_expiry_commands(&mut self) {
        self.register(CommandSpec {
            name: "PSETEX",
            arity: CommandArity::Exact(3),
            handler: handle_psetex,
        });
        self.register(CommandSpec {
            name: "SETEX",
            arity: CommandArity::Exact(3),
            handler: handle_setex,
        });
        self.register(CommandSpec {
            name: "EXPIRE",
            arity: CommandArity::AtLeast(2),
            handler: handle_expire,
        });
        self.register(CommandSpec {
            name: "PEXPIRE",
            arity: CommandArity::AtLeast(2),
            handler: handle_pexpire,
        });
        self.register(CommandSpec {
            name: "PEXPIREAT",
            arity: CommandArity::AtLeast(2),
            handler: handle_pexpireat,
        });
        self.register(CommandSpec {
            name: "EXPIREAT",
            arity: CommandArity::AtLeast(2),
            handler: handle_expireat,
        });
        self.register(CommandSpec {
            name: "TTL",
            arity: CommandArity::Exact(1),
            handler: handle_ttl,
        });
        self.register(CommandSpec {
            name: "PTTL",
            arity: CommandArity::Exact(1),
            handler: handle_pttl,
        });
        self.register(CommandSpec {
            name: "EXPIRETIME",
            arity: CommandArity::Exact(1),
            handler: handle_expiretime,
        });
        self.register(CommandSpec {
            name: "PEXPIRETIME",
            arity: CommandArity::Exact(1),
            handler: handle_pexpiretime,
        });
        self.register(CommandSpec {
            name: "PERSIST",
            arity: CommandArity::Exact(1),
            handler: handle_persist,
        });
    }

    fn register_counter_commands(&mut self) {
        self.register(CommandSpec {
            name: "INCR",
            arity: CommandArity::Exact(1),
            handler: handle_incr,
        });
        self.register(CommandSpec {
            name: "DECR",
            arity: CommandArity::Exact(1),
            handler: handle_decr,
        });
        self.register(CommandSpec {
            name: "INCRBY",
            arity: CommandArity::Exact(2),
            handler: handle_incrby,
        });
        self.register(CommandSpec {
            name: "DECRBY",
            arity: CommandArity::Exact(2),
            handler: handle_decrby,
        });
    }

    /// Registers or replaces one command in the table.
    pub fn register(&mut self, spec: CommandSpec) {
        self.entries.insert(spec.name.to_owned(), spec);
    }

    /// Validates command existence and arity without executing handler logic.
    ///
    /// # Errors
    ///
    /// Returns user-facing error text for unknown command names or invalid argument count.
    pub fn validate_frame(&self, frame: &CommandFrame) -> Result<(), String> {
        let command_name = frame.name.to_ascii_uppercase();
        let Some(spec) = self.entries.get(&command_name) else {
            return Err(format!("unknown command '{command_name}'"));
        };

        match spec.arity {
            CommandArity::Exact(expected) if frame.args.len() != expected => Err(format!(
                "wrong number of arguments for '{}' command",
                spec.name
            )),
            CommandArity::AtLeast(minimum) if frame.args.len() < minimum => Err(format!(
                "wrong number of arguments for '{}' command",
                spec.name
            )),
            _ => Ok(()),
        }
    }

    /// Dispatches one canonical command frame to its registered handler.
    #[must_use]
    pub fn dispatch(
        &self,
        db: DbIndex,
        frame: &CommandFrame,
        state: &mut DispatchState,
    ) -> CommandReply {
        if let Err(message) = self.validate_frame(frame) {
            return CommandReply::Error(message);
        }

        let command_name = frame.name.to_ascii_uppercase();
        let Some(spec) = self.entries.get(&command_name) else {
            return CommandReply::Error(format!("unknown command '{command_name}'"));
        };
        (spec.handler)(db, frame, state)
    }
}

