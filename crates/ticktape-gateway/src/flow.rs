//! Per-session admission: dedup, gap detection, and windowed flow control,
//! as a pure state machine.

/// The verdict on one incoming client command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admit {
    /// In order and within the window: sequence it.
    Accept,
    /// Already admitted (a retry after a lost ack): drop, the effect
    /// already happened exactly once.
    Duplicate,
    /// The client skipped ahead — a protocol violation (its own prior
    /// message can never arrive now); refuse and let it resynchronize.
    Gap { expected: u64 },
    /// The window of unacknowledged commands is full. The client seq was
    /// NOT consumed; the same command is admissible once acks drain.
    Throttled,
}

/// One session's admission state. `client_seq` starts at 1.
#[derive(Debug, Clone)]
pub struct SessionFlow {
    next: u64,
    window: u32,
    outstanding: u32,
}

impl SessionFlow {
    /// A fresh session expecting `client_seq = 1`, allowing up to `window`
    /// unacknowledged commands (1 = the classic single-outstanding
    /// discipline).
    pub fn new(window: u32) -> Self {
        Self::resume(0, window)
    }

    /// Resume a session whose last admitted client seq was `last_admitted`
    /// (recovered from the journaled envelopes after a gateway restart).
    pub fn resume(last_admitted: u64, window: u32) -> Self {
        SessionFlow {
            next: last_admitted + 1,
            window: window.max(1),
            outstanding: 0,
        }
    }

    pub fn admit(&mut self, client_seq: u64) -> Admit {
        if client_seq < self.next {
            return Admit::Duplicate;
        }
        if client_seq > self.next {
            return Admit::Gap {
                expected: self.next,
            };
        }
        if self.outstanding >= self.window {
            return Admit::Throttled;
        }
        self.next += 1;
        self.outstanding += 1;
        Admit::Accept
    }

    /// An admitted command was sequenced and acked; the window frees a slot.
    pub fn on_acked(&mut self) {
        self.outstanding = self.outstanding.saturating_sub(1);
    }

    /// The next client seq this session must send.
    pub fn expected(&self) -> u64 {
        self.next
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exactly_once_despite_retries() {
        let mut flow = SessionFlow::new(8);
        assert_eq!(flow.admit(1), Admit::Accept);
        flow.on_acked();
        // The ack was lost; the client retries 1, then proceeds.
        assert_eq!(flow.admit(1), Admit::Duplicate);
        assert_eq!(flow.admit(2), Admit::Accept);
        assert_eq!(flow.admit(2), Admit::Duplicate);
    }

    #[test]
    fn gaps_are_protocol_errors() {
        let mut flow = SessionFlow::new(8);
        assert_eq!(flow.admit(1), Admit::Accept);
        assert_eq!(flow.admit(3), Admit::Gap { expected: 2 });
        // The refusal consumed nothing; 2 then 3 proceed normally.
        assert_eq!(flow.admit(2), Admit::Accept);
        assert_eq!(flow.admit(3), Admit::Accept);
    }

    #[test]
    fn single_outstanding_discipline() {
        let mut flow = SessionFlow::new(1);
        assert_eq!(flow.admit(1), Admit::Accept);
        // Nothing acked yet: the next command must wait...
        assert_eq!(flow.admit(2), Admit::Throttled);
        flow.on_acked();
        // ...and the SAME seq is admissible after the ack (Throttled did
        // not consume it).
        assert_eq!(flow.admit(2), Admit::Accept);
    }

    #[test]
    fn resume_after_gateway_restart() {
        let mut flow = SessionFlow::resume(41, 4);
        assert_eq!(flow.admit(41), Admit::Duplicate, "already journaled");
        assert_eq!(flow.admit(42), Admit::Accept);
    }
}
