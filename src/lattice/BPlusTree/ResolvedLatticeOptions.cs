namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Internal resolved view of <see cref="LatticeOptions"/> that re-surfaces the
/// structural sizing fields (<see cref="MaxLeafKeys"/>, <see cref="MaxInternalChildren"/>,
/// <see cref="ShardCount"/>) sourced from the tree registry pin rather than
/// from <c>IOptionsMonitor&lt;LatticeOptions&gt;</c>.
/// <para>
/// Produced exclusively by <see cref="LatticeOptionsResolver.ResolveAsync"/>.
/// Consumers type their local <c>options</c> variable as
/// <see cref="ResolvedLatticeOptions"/> so existing call-site syntax
/// (<c>options.MaxLeafKeys</c>, <c>options.ShardCount</c>) keeps compiling.
/// </para>
/// </summary>
internal sealed class ResolvedLatticeOptions : LatticeOptions
{
    /// <summary>Maximum number of keys per leaf node before a split is triggered. Pinned in the registry.</summary>
    public required int MaxLeafKeys { get; init; }

    /// <summary>Maximum number of children per internal node before a split is triggered. Pinned in the registry.</summary>
    public required int MaxInternalChildren { get; init; }

    /// <summary>Number of independent physical shards the key space is divided into. Pinned in the registry.</summary>
    public required int ShardCount { get; init; }
}

