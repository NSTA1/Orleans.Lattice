# Change History

## What it shows

Every write to a lattice key becomes a revision in that key's timeline.
`ILattice.ScanEntryHistoryAsync` reads that timeline back, oldest first, so you
can answer "how did this value get here?" for any key. This sample writes five
successive values to a single key and then prints the full revision timeline -
each revision's hybrid-logical-clock stamp, kind, value length, and content
hash - alongside a plain `GetAsync` that only ever sees the latest value. It uses
the zero-setup **WAL-window fallback** (no durable history view configured), so
`Source` is reported as `WalWindow`.

## Run it

```
dotnet run --project samples/ChangeHistory
```

## Expected output

```
Silo starting... ready.

Writing 5 successive revisions to key 'order-42':
  set 'order-42' = 'placed'
  set 'order-42' = 'paid'
  set 'order-42' = 'packed'
  set 'order-42' = 'shipped'
  set 'order-42' = 'delivered'

Plain GetAsync('order-42') -> 'delivered' (latest only)

ScanEntryHistoryAsync('order-42') - the revision timeline:
  #1 hlc=HLC(639186130401564317:0) kind=Set valueLen=6 valueHash=0x9c47e7c2cd8e44a
  #2 hlc=HLC(639186130401883202:0) kind=Set valueLen=4 valueHash=0x9cf71314683dc6c2
  #3 hlc=HLC(639186130401922192:0) kind=Set valueLen=6 valueHash=0xaf49d4d5d84095
  #4 hlc=HLC(639186130401939858:0) kind=Set valueLen=7 valueHash=0xeb8bd0214fed79e0
  #5 hlc=HLC(639186130401948855:0) kind=Set valueLen=9 valueHash=0x89bb07d314f252a7

Source=WalWindow (WalWindow = best-effort fallback, no history view enabled)
Truncated=False (true would mean older revisions were already trimmed by WAL GC)
Total revisions read: 5
```

(Clock values differ per run.)

## When to use

- Ad-hoc inspection of how a key evolved, with no configuration or extra storage.
- Debugging or audit spot-checks where the recent WAL window is enough.

## When not to use

- When you need a **complete, retention-bounded** audit trail. The WAL fallback
  is bounded by WAL garbage collection, so older revisions may already be
  trimmed (`page.Truncated == true`). Enable a durable
  [history view](../HistoryViews) for that - it reports `Source == View` and is
  never truncated below its configured retention age.

## Feature doc

- [Change history](../../docs/lattice/change-history.md)
- [Durable per-key history views](../../docs/lattice/history-views.md)
