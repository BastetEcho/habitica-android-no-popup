# Offline support restart

## Baseline

This branch starts at `v4.10.0-no-yesterdailies-rc.3` (`68474bbe3`), the last
release before the custom offline task queue. It retains the positive,
default-on Yesterday's Dailies Popup setting and the startup fixes. Prior
branches and commits remain available for investigation; their caching
implementation is not the basis for the replacement.

RC8 bypassed the existing adapter filters by converting queryable Realm task
collections into ordinary lists. User screenshots showed non-due Dailies with
Due selected and completed Todos among incomplete Todos. Future changes must
preserve the original Due, Grey, active/completed, search, tag, and sorting
behavior. Personal screenshot contents are not reproduced here.

## Rollback compatibility

An unmodified RC3 APK can discard a newer local database because RC3 uses schema
1 with `deleteRealmIfMigrationNeeded`, whereas RC8 stores its pending operations
in schema 5. A rollback update must leave the old database and its pending
operations intact. Pending work must not be blindly replayed by old scheduled
workers. Tasks already synced to Habitica can be downloaded into a fresh cache;
unsynced work needs a separate, verified recovery path before replay.

This baseline selects a separate `baseline-rc3.realm` cache and does not open,
migrate, or delete the previous default Realm. The historical offline worker
class remains as a no-op so already scheduled work can finish without replaying
the retired queue. Existing authentication is retained. The first launch needs
internet access to download synced tasks; previously unsynced tasks remain in
the preserved database and are not yet shown or replayed by this baseline.

Do not uninstall the app or clear its storage while pending work needs recovery.
The new offline implementation must provide an explicitly tested import of that
preserved work, not silently assume every old task exists on the server.

## Baseline verification

The focused production-debug unit run passes all ten tests: nine repository
tests and one legacy-worker test. The repository tests include an identity
assertion that queryable Realm task results reach the list adapter unchanged.
Realm's optional RxJava types are included only in the unit-test runtime so
MockK can instrument database classes; this adds no APK runtime dependency.
On-device database preservation and a signed release APK remain separate checks.

## Replacement acceptance criteria

Implement personal Todos first. Keep network operations in a dedicated durable
outbox, separate from list queries and filtering. Each queued operation must
retain its originating account and server. Persist it before reporting local
success, and order edits, scores, position changes, and deletes behind creation
of the same task. Map temporary IDs to server IDs for all dependent operations.

Verify offline creation, editing, deletion, reordering, checklist changes, and
completion across refresh, process restart, reconnect, and retry. Reconcile lost
responses before retrying operations that could duplicate tasks or rewards.
Completion must eventually submit the full server scoring request so quests and
other game effects are processed. Play the immediate reward sound once; replay
must produce no reward or punishment sound. Ordinary queued Todo actions must
not show no-internet warnings.

Before publishing, verify unchanged filters for Dailies, Habits, and Todos with
mixed due/not-due and completed/incomplete fixtures, plus tags, search, and date
ordering. Expand offline support beyond personal Todos only after this behavior
has been confirmed.

## Replacement design

The replacement stores personal Todo requests and their latest local presentation
in an app-private SQLite database, independently of Realm. Enqueueing atomically
saves both records before the UI reports success. A monotonically increasing
sequence preserves operation order within each account and API server. Request
bodies never contain authentication credentials; the authenticated transport
supplies current credentials only when replaying that same account's work.

The UI still receives the original queryable Realm collection. Pending task
snapshots are materialized into Realm and merged before refresh reconciliation;
they are not added by wrapping or filtering the list returned to the adapter.
Personal Todo creates, edits, completion/undo, checklist toggles, moves, and deletes
use this outbox. Habits, Dailies, and group-task mutations retain their existing
implementation.

Connected-only WorkManager jobs drain the queue serially. A create uses a stable
client-generated UUID. Successful responses and any server ID mapping are
checkpointed before applying them locally, so dependent operations use the
canonical identity. A retry reconciles uncertain creates and desired completion
or checklist state before repeating a mutating request. Completion uses the full
server score endpoint, followed by an authoritative account read; local scoring
does not synthesize quest progress, drops, or rewards. Replay has no sound or
notification presentation callbacks.

These safeguards do not constitute a server-side exactly-once guarantee. The
Habitica API does not provide an atomic idempotency receipt covering task scoring
and all game effects. Concurrent changes from another client or a partial server
write can remain ambiguous. Unresolved operations stay persisted rather than
being discarded or blindly converted into a new task.

New-outbox test results and release validation must be recorded after the focused
tests and signed build finish. The separate legacy database remains preserved;
records from releases without reliable server provenance must not be silently
imported or rescored.
