---
title: Advanced Features
---

## Encryption at rest

Set `FireLiteConfig.encryption_key` to encrypt WAL and segment payloads.

## Background compaction scheduling

`FireLite::open` launches maintenance loop that periodically triggers storage background maintenance (`run_background_maintenance`) and can fold segments.

## Conflict-aware serializable transactions

`begin_serializable_transaction` captures read versions (`get`) and validates at commit:

- if current version differs from expected read version, commit fails with conflict.
- if valid, mutations are atomically applied via batch path.

This provides serializable semantics for local concurrent writers without a remote lock service.
