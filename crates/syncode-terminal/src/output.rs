//! Output buffering — buffered terminal output with ack protocol
//!
//! Terminal output is buffered in a ring buffer and served to clients
//! with an acknowledgment protocol to ensure no output is lost.

use std::collections::VecDeque;

/// Terminal output event
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OutputChunk {
    /// Sequence number for ack tracking
    pub seq: u64,
    /// Raw terminal output data (UTF-8)
    pub data: String,
    /// Timestamp when this chunk was captured
    pub timestamp: String,
}

/// Buffered output store with ack-based delivery
pub struct OutputBuffer {
    /// Ring buffer of output chunks
    chunks: VecDeque<OutputChunk>,
    /// Maximum chunks to retain
    max_chunks: usize,
    /// Maximum bytes per chunk before flushing
    max_chunk_size: usize,
    /// Current sequence counter
    next_seq: u64,
    /// Current pending chunk being built
    pending: String,
    /// Highest acknowledged sequence (None = nothing acked)
    ack_seq: Option<u64>,
}

impl OutputBuffer {
    /// Create a new output buffer
    pub fn new(max_chunks: usize, max_chunk_size: usize) -> Self {
        Self {
            chunks: VecDeque::new(),
            max_chunks,
            max_chunk_size,
            next_seq: 0,
            pending: String::new(),
            ack_seq: None,
        }
    }

    /// Write raw output data to the buffer
    pub fn write(&mut self, data: &str) -> Vec<OutputChunk> {
        self.pending.push_str(data);
        let mut flushed = Vec::new();

        while self.pending.len() >= self.max_chunk_size {
            let chunk_data = self.pending[..self.max_chunk_size].to_string();
            self.pending = self.pending[self.max_chunk_size..].to_string();
            flushed.push(self.flush_chunk(chunk_data));
        }

        flushed
    }

    /// Flush remaining pending data into a chunk
    pub fn flush(&mut self) -> Option<OutputChunk> {
        if self.pending.is_empty() {
            return None;
        }
        let chunk_data = std::mem::take(&mut self.pending);
        Some(self.flush_chunk(chunk_data))
    }

    fn flush_chunk(&mut self, data: String) -> OutputChunk {
        let chunk = OutputChunk {
            seq: self.next_seq,
            data,
            timestamp: chrono::Utc::now().to_rfc3339(),
        };
        self.next_seq += 1;

        self.chunks.push_back(chunk.clone());

        // Trim old chunks beyond max capacity
        while self.chunks.len() > self.max_chunks {
            self.chunks.pop_front();
        }

        chunk
    }

    /// Acknowledge chunks up to and including a sequence number.
    /// After ack(N), chunks with seq <= N are considered acknowledged.
    pub fn ack(&mut self, seq: u64) {
        match self.ack_seq {
            None => self.ack_seq = Some(seq),
            Some(current) if seq > current => self.ack_seq = Some(seq),
            _ => {}
        }
    }

    /// Get chunks that haven't been acknowledged yet
    pub fn unacked_chunks(&self) -> Vec<&OutputChunk> {
        match self.ack_seq {
            None => self.chunks.iter().collect(),
            Some(acked) => self.chunks.iter().filter(|c| c.seq > acked).collect(),
        }
    }

    /// Get all chunks from a given sequence number
    pub fn chunks_from(&self, seq: u64) -> Vec<&OutputChunk> {
        self.chunks.iter().filter(|c| c.seq >= seq).collect()
    }

    /// Get the current sequence number (next to be assigned)
    pub fn current_seq(&self) -> u64 {
        self.next_seq
    }

    /// Get total byte count of buffered data
    pub fn buffered_bytes(&self) -> usize {
        self.chunks.iter().map(|c| c.data.len()).sum::<usize>() + self.pending.len()
    }

    /// Clear all buffered data
    pub fn clear(&mut self) {
        self.chunks.clear();
        self.pending.clear();
        self.next_seq = 0;
        self.ack_seq = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_buffer_basic() {
        let mut buf = OutputBuffer::new(100, 1024);
        let chunks = buf.write("hello");
        assert!(chunks.is_empty()); // Below chunk size

        let flushed = buf.flush().unwrap();
        assert_eq!(flushed.data, "hello");
        assert_eq!(flushed.seq, 0);
    }

    #[test]
    fn output_buffer_auto_flush() {
        let mut buf = OutputBuffer::new(100, 5);
        let chunks = buf.write("abcdefghij");
        assert_eq!(chunks.len(), 2); // "abcde" + "fghij"
        assert_eq!(chunks[0].data, "abcde");
        assert_eq!(chunks[1].data, "fghij");
        assert_eq!(chunks[0].seq, 0);
        assert_eq!(chunks[1].seq, 1);
    }

    #[test]
    fn output_buffer_ack() {
        let mut buf = OutputBuffer::new(100, 5);
        buf.write("abcdefghij"); // 2 chunks (seq 0, 1)
        buf.flush(); // Nothing pending

        let unacked = buf.unacked_chunks();
        assert_eq!(unacked.len(), 2); // Nothing acked yet

        buf.ack(1); // Ack chunks 0 and 1
        let unacked = buf.unacked_chunks();
        assert_eq!(unacked.len(), 0);
    }

    #[test]
    fn output_buffer_from_seq() {
        let mut buf = OutputBuffer::new(100, 3);
        buf.write("abcdef"); // 2 chunks: seq 0="abc", seq 1="def"

        let from_1 = buf.chunks_from(1);
        assert_eq!(from_1.len(), 1);
        assert_eq!(from_1[0].data, "def");
    }

    #[test]
    fn output_buffer_trim() {
        let mut buf = OutputBuffer::new(3, 2);
        buf.write("aaa"); // auto-flushes "aa" as chunk 0, pending "a"
        buf.flush(); // flushes "a" as chunk 1
        buf.write("bbb"); // auto-flushes "bb" as chunk 2, pending "b"
        buf.flush(); // flushes "b" as chunk 3 — trims chunk 0

        assert_eq!(buf.chunks.len(), 3);
        assert_eq!(buf.chunks[0].data, "a");
        assert_eq!(buf.chunks[1].data, "bb");
        assert_eq!(buf.chunks[2].data, "b");
    }

    #[test]
    fn output_buffer_clear() {
        let mut buf = OutputBuffer::new(100, 1024);
        buf.write("hello");
        buf.flush();
        buf.clear();
        assert_eq!(buf.current_seq(), 0);
        assert_eq!(buf.buffered_bytes(), 0);
    }

    #[test]
    fn output_chunk_serialization() {
        let chunk = OutputChunk {
            seq: 42,
            data: "hello world".to_string(),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
        };
        let json = serde_json::to_string(&chunk).unwrap();
        assert!(json.contains("42"));
        let back: OutputChunk = serde_json::from_str(&json).unwrap();
        assert_eq!(back.data, "hello world");
    }
}
