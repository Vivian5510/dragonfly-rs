//! Shared connection ingress for runtime I/O loop and integration-style unit tests.

use crate::app::{ServerApp, ServerConnection};
use dfly_common::error::DflyResult;

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
        let Some(parsed) = connection.parser.try_pop_command()? else {
            break;
        };
        if let Some(encoded) = app.execute_parsed_command(connection, parsed) {
            responses.push(encoded);
        }
    }
    Ok(responses)
}
