# HistoryViews

## What it shows

An opt-in **durable per-key history view**: an append-only materialised view
that records every revision of every key in a source tree, surviving source WAL
garbage collection. This sample enables a runtime history view over an `orders`
tree with `FullValue` retention, writes five successive values to one key, then
reads the timeline back with `ScanEntryHistoryAsync`. Because a history view is
enabled, `Source` is `View` (not the best-effort `WalWindow` fallback),
`Truncated` is always `false`, and - thanks to `FullValue` retention - each
revision carries its value bytes so the historical values print verbatim, even
though a plain `GetAsync` only ever returns the latest one.

## Run it

```
dotnet run --project samples/HistoryViews
```

## Expected output

```
Silo starting... ready.

History retention for 'orders': mode=FullValue, window=00:00:00
Durable history view 'orders-history' created (forward-only).

Writing 5 successive revisions to key 'order-42':
  set 'order-42' = 'placed'
  set 'order-42' = 'paid'
  set 'order-42' = 'packed'
  set 'order-42' = 'shipped'
  set 'order-42' = 'delivered'

View apply lag after catch-up: 0

Plain GetAsync('order-42') -> 'delivered' (latest only)

ScanEntryHistoryAsync('order-42') - the durable revision timeline:
  #1 hlc=HLC(639186132104706995:0) kind=Set value='placed' (shape=FullValue)
  #2 hlc=HLC(639186132104937326:0) kind=Set value='paid' (shape=FullValue)
  #3 hlc=HLC(639186132104984736:0) kind=Set value='packed' (shape=FullValue)
  #4 hlc=HLC(639186132104995873:0) kind=Set value='shipped' (shape=FullValue)
  #5 hlc=HLC(639186132105004997:0) kind=Set value='delivered' (shape=FullValue)

Source=View (View = durable history view, survives WAL GC)
Truncated=False (always false on the View path - bounded only by retention age)
Total durable revisions read: 5
```

(Clock values differ per run. `window=00:00:00` means no age bound - revisions
never expire.)

## When to use

- You need a **complete, retention-bounded** audit timeline that outlives WAL
  garbage collection.
- You want point-in-time values served straight from the history (`FullValue` or
  `Hybrid` retention), or compact change detection (`MetadataOnly`).

## When not to use

- For quick, ad-hoc inspection with zero setup - the best-effort WAL-window
  fallback (see [ChangeHistory](../ChangeHistory)) needs no view and no extra
  storage.
- History is **forward-only**: it begins at view creation, so it cannot recover
  revisions that predate enabling the view.

## Feature doc

- [Durable per-key history views](../../docs/lattice/history-views.md)
- [Change history](../../docs/lattice/change-history.md)
