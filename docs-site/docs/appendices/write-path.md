---
title: "Appendix: WRITE_PATH.md"
---

This appendix summarizes the write pipeline from `WRITE_PATH.md`:

1. API mutation ingestion (`put/delete/write_batch/transaction`).
2. WAL append with transactional boundaries.
3. In-memory/index updates.
4. Segment persistence and periodic compaction.
5. Recovery and replay behavior after restart.

Canonical source file: `WRITE_PATH.md` at repository root.
