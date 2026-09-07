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
