# Resize

## What it shows

`ILattice.ResizeAsync` changes a tree's structural sizing
(`MaxLeafKeys` / `MaxInternalChildren`) on a tree that already holds data. It
runs **online**: reads and writes stay available while it drains the source into
a freshly-sized destination tree (shadow-forwarding live writes) and atomically
swaps the alias. Every entry is preserved. This sample populates a tree past a
single leaf, resizes its leaf capacity from the default 128 to 256, polls
`IsResizeCompleteAsync` until the swap finishes, and confirms the data survived
and the tree is still writable.

## Run it

```
dotnet run --project samples/Resize
```

## Expected output

(The resize duration varies run to run.)

```
== Resize sample ==

Populated 'catalog' with 500 entries (default MaxLeafKeys=128).

Calling ResizeAsync(newMaxLeafKeys: 256, newMaxInternalChildren: 64)...
Resize completed in 6.7s.

CountAsync after resize -> 500 (unchanged).
item:0250 = value-250
item:new  = post-resize (written after the resize)
-> same tree, wider leaves, data intact and still writable.

Done.
```

## When to use

- The current fan-out no longer suits the workload (e.g. leaves are too small
  for the value sizes or access pattern) and you need to re-paginate an existing,
  populated tree without taking it offline.
- To start a brand-new tree with non-default sizing: call `ResizeAsync` on the
  empty tree, which hits the in-place empty-tree fast path.

## When not to use

- Changing the shard count - that requires re-hashing keys and is done with
  `ReshardAsync`, not `ResizeAsync`.
- Casually or on the hot path. A resize on a populated tree copies the whole
  tree (roughly 2x storage during the window) and adds a forward hop to every
  write until the swap; prefer an off-peak window for large trees.

## Feature doc

[docs/lattice/tree-sizing.md](../../docs/lattice/tree-sizing.md)
