using Orleans.Concurrency;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The cross-tree purge frontier an origin cluster has advertised to this
/// receiver (issue #4733), keyed by the origin cluster id: per origin tree, a
/// decision sequence at or below which the origin stores no cross-tree
/// decision of the tree and never will again. A decided barrier tombstone is
/// dropped once every participant's frontier reaches the operation's sequence,
/// because no arrival of the operation can reach this receiver after that.
/// Frontiers only advance: each is the maximum ever advertised.
/// </summary>
[Alias(TypeAliases.ICrossTreePurgeFrontierGrain)]
internal interface ICrossTreePurgeFrontierGrain : IGrainWithStringKey
{
    /// <summary>
    /// Durably raises each tree's frontier to the advertised value when it is
    /// higher, then has every tombstone listed under a tree whose frontier
    /// advanced settle itself. Call it only with a frontier the origin itself
    /// advertised on an authenticated call.
    /// </summary>
    Task AdvanceAsync(IReadOnlyDictionary<string, long> frontiers);

    /// <summary>Every tree's frontier. Pure read.</summary>
    [AlwaysInterleave]
    Task<IReadOnlyDictionary<string, long>> GetAsync();
}
