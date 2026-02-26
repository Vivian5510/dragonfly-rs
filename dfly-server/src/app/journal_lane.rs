use dfly_replication::ReplicationModule;
use dfly_replication::journal::JournalEntry;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const JOURNAL_APPEND_BATCH_SIZE: usize = 256;

#[derive(Debug)]
struct JournalAppendEnvelope {
    enqueue_order: u64,
    entry: JournalEntry,
}

#[derive(Debug)]
enum JournalAppendLaneCommand {
    Entry(JournalAppendEnvelope),
    Flush(mpsc::Sender<()>),
    Shutdown,
}

pub(super) struct JournalAppendLane {
    sender: mpsc::Sender<JournalAppendLaneCommand>,
    pending_entries: Arc<AtomicUsize>,
    next_enqueue_order: AtomicU64,
    worker: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for JournalAppendLane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalAppendLane")
            .field(
                "pending_entries",
                &self.pending_entries.load(Ordering::Acquire),
            )
            .field(
                "next_enqueue_order",
                &self.next_enqueue_order.load(Ordering::Acquire),
            )
            .field("has_worker", &self.worker.is_some())
            .finish_non_exhaustive()
    }
}

impl JournalAppendLane {
    pub(super) fn spawn(replication: Arc<Mutex<ReplicationModule>>) -> Self {
        let (sender, receiver) = mpsc::channel::<JournalAppendLaneCommand>();
        let pending_entries = Arc::new(AtomicUsize::new(0));
        let pending_for_worker = Arc::clone(&pending_entries);
        let worker = thread::Builder::new()
            .name("dfly-journal-append-lane".to_owned())
            .spawn(move || {
                journal_append_lane_main(&receiver, &replication, &pending_for_worker);
            })
            .ok();
        Self {
            sender,
            pending_entries,
            next_enqueue_order: AtomicU64::new(1),
            worker,
        }
    }

    pub(super) fn enqueue(&self, entry: JournalEntry) -> Result<(), JournalEntry> {
        let enqueue_order = self.next_enqueue_order.fetch_add(1, Ordering::AcqRel);
        self.pending_entries.fetch_add(1, Ordering::AcqRel);
        match self
            .sender
            .send(JournalAppendLaneCommand::Entry(JournalAppendEnvelope {
                enqueue_order,
                entry,
            })) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.pending_entries.fetch_sub(1, Ordering::AcqRel);
                let JournalAppendLaneCommand::Entry(envelope) = error.0 else {
                    unreachable!("journal enqueue send failure must carry entry payload")
                };
                Err(envelope.entry)
            }
        }
    }

    pub(super) fn flush(&self) {
        if self.pending_entries.load(Ordering::Acquire) == 0 {
            return;
        }
        let (ack_sender, ack_receiver) = mpsc::channel::<()>();
        if self
            .sender
            .send(JournalAppendLaneCommand::Flush(ack_sender))
            .is_err()
        {
            return;
        }
        let _ = ack_receiver.recv_timeout(Duration::from_secs(1));
    }

    pub(super) fn shutdown(&mut self) {
        self.flush();
        let _ = self.sender.send(JournalAppendLaneCommand::Shutdown);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn journal_append_lane_main(
    receiver: &mpsc::Receiver<JournalAppendLaneCommand>,
    replication: &Arc<Mutex<ReplicationModule>>,
    pending_entries: &Arc<AtomicUsize>,
) {
    let mut envelopes = Vec::<JournalAppendEnvelope>::with_capacity(JOURNAL_APPEND_BATCH_SIZE);
    loop {
        let Ok(command) = receiver.recv() else {
            break;
        };
        let mut flush_waiters = Vec::<mpsc::Sender<()>>::new();
        let mut should_shutdown = false;
        match command {
            JournalAppendLaneCommand::Entry(envelope) => envelopes.push(envelope),
            JournalAppendLaneCommand::Flush(waiter) => flush_waiters.push(waiter),
            JournalAppendLaneCommand::Shutdown => should_shutdown = true,
        }

        while envelopes.len() < JOURNAL_APPEND_BATCH_SIZE {
            match receiver.try_recv() {
                Ok(JournalAppendLaneCommand::Entry(envelope)) => envelopes.push(envelope),
                Ok(JournalAppendLaneCommand::Flush(waiter)) => {
                    flush_waiters.push(waiter);
                    break;
                }
                Ok(JournalAppendLaneCommand::Shutdown) | Err(mpsc::TryRecvError::Disconnected) => {
                    should_shutdown = true;
                    break;
                }
                Err(mpsc::TryRecvError::Empty) => break,
            }
        }

        if !envelopes.is_empty() {
            envelopes.sort_unstable_by(|left, right| {
                left.entry
                    .txid
                    .cmp(&right.entry.txid)
                    .then(left.enqueue_order.cmp(&right.enqueue_order))
            });
            let mut replication_guard = replication
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for envelope in envelopes.drain(..) {
                replication_guard.append_journal(envelope.entry);
                pending_entries.fetch_sub(1, Ordering::AcqRel);
            }
        }

        for waiter in flush_waiters {
            let _ = waiter.send(());
        }
        if should_shutdown {
            break;
        }
    }
}
