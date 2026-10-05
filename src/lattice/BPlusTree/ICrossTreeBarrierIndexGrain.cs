using System.Collections.Immutable;
using Orleans.Concurrency;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The cross-tree barriers on this receiver that a tree belongs to (issue
/// #4684), keyed by the receiver-side tree id. A barrier registers itself under
/// every tree of its wait set before it persists the wait set, and withdraws
/// once it has decided, so an import of the tree can find every barrier that
/// may still wait for it. An entry can outlive its barrier's decision (a
/// withdrawal that failed): a reader checks the barrier itself.
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
}
