using System.Collections.Immutable;
using Orleans.Concurrency;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The cross-tree barriers on this receiver that a tree belongs to (issue
/// #4684), keyed by the receiver-side tree id. A barrier registers itself under
/// every tree of its wait set before it persists the wait set, and withdraws
/// once it has decided, so an import of the tree can find every barrier that
/// may still wait for it. An entry can outlive its barrier's decision (a
/// withdrawal that failed): a reader checks the barrier itself. The grain also
/// keeps the tree's latest snapshot import per origin, which a barrier reads to
/// decide whether the import settled the tree's part of its operation.
/// </summary>
[Alias(TypeAliases.ICrossTreeBarrierIndexGrain)]
internal interface ICrossTreeBarrierIndexGrain : IGrainWithStringKey
{
    /// <summary>Every registered barrier key.</summary>
    [AlwaysInterleave]
    Task<ImmutableArray<string>> GetAsync();

    /// <summary>Durably registers <paramref name="barrierKey"/>. Idempotent.</summary>
    Task AddAsync(string barrierKey);

    /// <summary>Durably withdraws <paramref name="barrierKey"/>. Idempotent.</summary>
    Task RemoveAsync(string barrierKey);

    /// <summary>
    /// Durably records the tree's latest snapshot import from
    /// <paramref name="originClusterId"/>. An import whose export epoch is below
    /// the recorded one is ignored.
    /// </summary>
    Task RecordImportAsync(string originClusterId, CrossTreeImportRecord import);

    /// <summary>The tree's latest snapshot import from <paramref name="originClusterId"/>, if any.</summary>
    [AlwaysInterleave]
    Task<CrossTreeImportRecord?> GetImportAsync(string originClusterId);

    /// <summary>
    /// Durably lists <paramref name="barrierKey"/> as a decided tombstone the
    /// tree took part in (issue #4733), swept when the tree's purge frontier
    /// advances. Idempotent.
    /// </summary>
    Task AddTombstoneAsync(string barrierKey);

    /// <summary>Durably unlists <paramref name="barrierKey"/>. Idempotent.</summary>
    Task RemoveTombstoneAsync(string barrierKey);

    /// <summary>Every listed tombstone.</summary>
    [AlwaysInterleave]
    Task<ImmutableArray<string>> GetTombstonesAsync();
}
