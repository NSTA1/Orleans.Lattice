# B+ Tree Structure

## Node Structure

Each shard is a standard B+ tree with a configurable branching factor (default: 128 keys per leaf, 128 children per internal node).

### Internal Nodes

An internal node stores a sorted list of `(SeparatorKey, ChildGrainId)` entries. The first entry always has a `null` separator and acts as the leftmost catch-all:

```mermaid
block-beta
    columns 5
    block:Internal["Internal Node"]
        columns 5
        NULL["∅ → Child₀"]
        SEP1["'fox' → Child₁"]
        SEP2["'monkey' → Child₂"]
        SEP3["'rabbit' → Child₃"]
        space
    end
```

Routing walks the separator list from right to left and picks the first child whose separator is ≤ the search key:

| Lookup key | Selected child | Reason |
|---|---|---|
| `"ant"` | Child₀ | `"ant"` < `"fox"`, falls through to leftmost |
| `"fox"` | Child₁ | `"fox"` ≥ `"fox"` |
| `"lion"` | Child₁ | Walk from right: `"lion"` < `"rabbit"`, `"lion"` < `"monkey"`, `"lion"` ≥ `"fox"` ✓ → Child₁ |
| `"zebra"` | Child₃ | `"zebra"` ≥ `"rabbit"` |

### Leaf Nodes

Each leaf stores entries in a `SortedDictionary<string, LwwValue<byte[]>>` and maintains `NextSibling` and `PrevSibling` pointers forming a doubly-linked list for forward and reverse range scans:

```mermaid
flowchart LR
    subgraph Leaf1["Leaf (a-f)"]
        E1["'ant' → 0x..."]
        E2["'cat' → 0x..."]
        E3["'fox' → 0x..."]
    end

    subgraph Leaf2["Leaf (g-m)"]
        E4["'goat' → 0x..."]
        E5["'lion' → 0x..."]
        E6["'monkey' → 0x..."]
    end

    Leaf1 -- "NextSibling" --> Leaf2
```

## Leaf Splits

When a leaf exceeds `MaxLeafKeys` (128) entries after an insert, it splits using a **two-phase** pattern that is crash-safe:

```mermaid
sequenceDiagram
    participant Client
    participant Root as ShardRootGrain
    participant Leaf as LeafGrain (original)
    participant New as LeafGrain (new sibling)
    participant Parent as InternalGrain

    Client->>Root: SetAsync("key", value)
    Root->>Leaf: SetAsync("key", value)

    Note over Leaf: Entry count > 128 → split triggered

    rect rgb(240, 248, 255)
    Note over Leaf: Phase 1 — persist intent
    Leaf->>Leaf: SplitState = SplitInProgress
    Leaf->>Leaf: Record SplitKey, SplitSiblingId, split entries
    Leaf->>Leaf: Trim local entries to left half
    Leaf->>Leaf: WriteStateAsync()
    end

    rect rgb(240, 255, 240)
    Note over Leaf: Phase 2 — cross-grain ops
    Leaf->>New: SetTreeIdAsync(treeId)
    Leaf->>New: MergeEntriesAsync(upper half)
    Leaf->>New: SetNextSiblingAsync(oldNextSibling)
    Leaf->>Leaf: SplitState = SplitComplete
    end

    Leaf-->>Root: SplitResult { PromotedKey, NewSiblingId }
    Root->>Parent: AcceptSplitAsync(promotedKey, newSiblingId)

    Note over Parent: Inserts new separator + child reference

    alt Parent also overflows
        Parent-->>Root: SplitResult (cascading)
        Root->>Root: PromoteRootAsync — create new root above
    end
```

1. **Phase 1 (persist intent):** The leaf finds the **median key**, records the split metadata (`SplitKey`, `SplitSiblingId`, right-half entries) and trims its own entries to the left half — all in a single `WriteStateAsync` call.
2. **Phase 2 (cross-grain ops):** The new sibling is populated via `MergeEntriesAsync` (an idempotent bulk merge), sibling pointers are updated, and `SplitState` advances to `SplitComplete`.
3. A `SplitResult` containing the promoted key and new sibling's `GrainId` is returned up the call stack.
4. The parent internal node inserts the new separator. If *it* overflows, the split cascades further (internal nodes use the same two-phase pattern).
5. If the split reaches the shard root, a new internal root is created above the old one via a two-phase `PromoteRootAsync`, increasing tree depth by one.

**Recovery:** If a grain crashes between Phase 1 and Phase 2, the next call to `SetAsync` detects `SplitState == SplitInProgress` and resumes Phase 2 (`CompleteSplitAsync`). After recovery completes, the caller's write is routed to the correct leaf — locally if the key falls below the split key, or forwarded to the new sibling otherwise. This ensures **no writes are lost** during a crash mid-split.

## Idempotent Split Propagation

`AcceptSplitAsync` on internal nodes checks for duplicate `(separatorKey, childId)` pairs before inserting. If the same split result is delivered twice (e.g. crash recovery, message retry), the duplicate is detected and skipped. Combined with the monotonic `SplitState` on leaf and internal nodes, this makes the entire split protocol idempotent end-to-end.

Internal nodes themselves use the same two-phase split pattern as leaves. If an internal node crashes mid-split, the next `AcceptSplitAsync` call resumes the incomplete split before processing the caller's promotion — routing it to the correct node (locally or to the new sibling) based on the split key.
