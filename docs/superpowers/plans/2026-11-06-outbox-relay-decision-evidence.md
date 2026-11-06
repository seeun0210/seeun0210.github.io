# Outbox Relay decision evidence implementation plan

> **For agentic workers:** Use superpowers:executing-plans to implement this approved scope inline, then superpowers:requesting-code-review for independent review.

**Goal:** Complete the article's Relay choice and failure-recovery argument with measured evidence, then publish at its existing URL.

**Architecture:** Extend the isolated Java/MySQL/Kafka model that commits PaymentResult+Outbox, publishes through Kafka, and commits order application+inbox. Compare batch future collection, bounded get workers and callback completion with the same limits. Fault tests use real child JVM termination, DB connection interruption and Kafka offset replay; no production endpoint is contacted.

**Tech Stack:** Java21, MySQL8.4, Kafka client/broker4.1.2, local Docker, Python analysis, existing static GitHub Pages.

**Spec:** User-approved chat description: compare three Relay designs on the same order-application boundary; verify restart/failure duplicate handling; narrow the main article and republish. Existing article remains canonical at posts/transactional-outbox-relay/article.md.

## Global constraints

- App tracked source remains unchanged. Lab source/results remain local artifacts under the existing .archify relay-lab directory; this plan is committed with the article, not the application.
- No new public raw-data/repository links. Preserve eight uploaded diagrams, Korean polite style and existing article URL.
- Clearly distinguish local model measurements, operating recollection and application design contracts.
- Keep poll100ms, fetch100, total unfinished16 and result DB pool4 equal across comparison modes. Slots last until result commit. Kafka waits hold no DB transaction.
- Initial160 +300 new outcomes targeted100/s; repeat normal, one result-TX sleep1000ms, and proxy downstream40ms conditions three times in rotated mode order. The delay affects result recording or broker responses as specified, not a fabricated PG failure.
- Measurement boundary is payment/outbox commit-return to order/inbox commit-return in one JVM monotonic clock. No seller UI/acceptance or actual PG latency claim.

## Review focus

- Same pending rows must be excluded while in flight; final broker count detects accidental re-sends.
- Result executor/worker tasks must drain before connections close; record commits and slots are verified at end.
- Consumer offset restart must start from committed position, not seekToEnd; crash before offset commit must replay without a second order mutation.
- ACK before outbox completion must leave PENDING and recover via the same eventId; broker duplicates are expected.
- DB failure after ACK must retry the result write and preserve the slot instead of blindly sending again.

### Task 1: Comparable Relay model and analysis

**Files:** local relay-lab/src/main/java/RelayDecisionLab.java; relay-decision-results-20261106 raw CSV/JSONL and analysis.

- [x] Define row-count, final state, apply_count and broker-ID assertions before measurement implementation; fail if any invariant differs.
- [x] Implement equal slot/poll/connection limits, distinct completion mechanisms, real DB result sleep and proxy latency condition.
- [x] Compile and smoke all modes, then run 27 trials with new output paths and isolated compose project.
- [x] Independently recompute quantiles and delivery/publish timing from raw rows; report 3-run medians/ranges and transaction cost.

### Task 2: Order recovery across process, DB and offset boundaries

**Files:** local relay-lab/src/main/java/OrderRecoveryLab.java; decision-results fault JSONL.

- [x] Assert one final order mutation and one durable apply identity before implementing each recovery branch.
- [x] Child JVM halts after Kafka ACK before PUBLISHED: verify PENDING and one broker copy, resend same ID after restart, then verify two copies but one order mutation.
- [x] Kill only the result-record connection after ACK: retry DB write without a new send; verify one copy, PUBLISHED and one order mutation.
- [x] Consumer child commits inbox+order then halts before offset commit: restart same group from committed offsets, verify replay and one order mutation.
- [x] Keep unprotected order-apply counter as a negative control proving that duplicate delivery could have changed it twice.

### Task 3: Evidence-led article, review and publication

**Files:** posts/transactional-outbox-relay/article.md and index.html.

- [x] Rewrite Relay selection around observed behavior/cost; retain alternatives and limits even if callback loses.
- [x] Add compact failure matrix with concrete source/broker/order states, separating tested prototype from richer refund/ordering contracts.
- [x] Collapse supporting CDC/partial-refund detail and trim repetition; preserve core story and all images.
- [x] Independent technical/evidence review and fix Important issues.
- [ ] Render GFM, verify tables/anchors/images/numeric ranges, commit and push within prior authorization.
- [ ] Wait for Pages success and verify public bytes of article, Markdown, CSS and all images. Sync local recovery copies without changing Velog.

### Task 4: User-requested thread memory and CPU measurements

- [x] Separate resource experiment, active get16 worker pool OR callback4 result pool only; other mode pools remain unstarted.
- [x] Same warmup, Java21 flags and -Xss512k; collect native Thread stack reserved/committed via NMT, JVM RSS via ps, and process CPU delta over the delivery workload. Report virtual reservation separately from RSS and exclude NMT attach from timed CPU interval.
- [x] Repeat normal and broker-response40ms conditions for workers/callback three times, rotated order. Keep load460, poll100ms and unfinished16/pool4 unchanged.
- [x] Independently aggregate run medians/ranges. CPU includes the same-JVM source/consumer and is not per-Relay or host/DB CPU; use equal-work CPU time rather than raw percent from differing durations.
- [x] Replace unmeasured-resource wording with scoped results and replace user-facing 나이 with 대기 시간.

Execution note: comparison27runs/12420events, recovery5freshchecks, resources12measuredruns/5520events all passed independent review. Initial resource warmup output-path failure was corrected and excluded. Public rendering validates7sections/8images/9details and all key numerical values. Deployment and live readback are recorded in the local publication verification artifact after push.

### Task 5: Approved narrative compression

- [x] Reframe the visible article as six steps: observed mismatch, durable intent, polling delay, callback choice, apply-once recovery, observability.
- [x] Reduce visible prose from19147 to9887characters without changing measured values or claiming an incident cause that was not established.
- [x] Preserve all8figures once and move full measurements, CPU/NMT scope, retry, CDC, ordering and partial refund contracts to collapsed supplements.
- [ ] Independently review technical/evidence boundaries and rendered nested details, then republish at the same URL and verify public bytes.

Compression review: no technical/evidence regression; fixed particle corruption 대기 시간는 and provider-name corruption caused by earlier broad replacement. Structural render and public byte readback are the publication gates.
