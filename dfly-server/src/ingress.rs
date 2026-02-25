//! Shared connection ingress for runtime I/O loop and integration-style unit tests.

use crate::app::{ParsedCommandExecution, RuntimeReplyTicket, ServerApp, ServerConnection};
use dfly_common::error::DflyResult;
use dfly_core::command::CommandReply;
use dfly_facade::protocol::ClientProtocol;

/// Feeds raw protocol bytes into one logical connection and executes every complete command.
///
/// The returned vector contains one encoded reply buffer per command that became complete after
/// appending `bytes` to this connection parser state.
///
/// # Errors
///
/// Returns protocol errors when payload bytes violate active protocol framing.
pub(crate) fn ingress_connection_bytes(
    app: &mut ServerApp,
    connection: &mut ServerConnection,
    bytes: &[u8],
) -> DflyResult<Vec<Vec<u8>>> {
    connection.parser.feed_bytes(bytes);
    let mut responses = Vec::new();
    loop {
        match connection.parser.try_pop_command() {
            Ok(Some(parsed)) => match app.execute_parsed_command_deferred(connection, parsed) {
                ParsedCommandExecution::Immediate(encoded) => {
                    if let Some(encoded) = encoded {
                        responses.push(encoded);
                    }
                }
                ParsedCommandExecution::Deferred(ticket) => {
                    responses.push(resolve_deferred_ingress_reply(app, &ticket));
                }
            },
            Ok(None) => break,
            Err(error) => {
                if connection.context.protocol != ClientProtocol::Memcache {
                    return Err(error);
                }
                responses.push(format!("CLIENT_ERROR {error}\r\n").as_bytes().to_vec());
                if !connection.parser.recover_after_protocol_error() {
                    break;
                }
            }
        }
    }
    Ok(responses)
}

fn resolve_deferred_ingress_reply(app: &ServerApp, ticket: &RuntimeReplyTicket) -> Vec<u8> {
    match app
        .runtime
        .wait_until_processed_sequence(ticket.shard, ticket.sequence)
    {
        Ok(()) => match app.take_runtime_reply_ticket(ticket) {
            Ok(encoded) => encoded,
            Err(error) => encode_runtime_dispatch_error(
                ticket.protocol,
                format!("runtime dispatch failed: {error}"),
            ),
        },
        Err(error) => encode_runtime_dispatch_error(
            ticket.protocol,
            format!("runtime dispatch failed: {error}"),
        ),
    }
}

fn encode_runtime_dispatch_error(protocol: ClientProtocol, message: String) -> Vec<u8> {
    match protocol {
        ClientProtocol::Resp => CommandReply::Error(message).to_resp_bytes(),
        ClientProtocol::Memcache => format!("SERVER_ERROR {message}\r\n").into_bytes(),
    }
}
