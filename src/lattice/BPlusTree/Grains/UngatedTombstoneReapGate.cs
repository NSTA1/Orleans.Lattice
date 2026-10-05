namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ITombstoneReapGate"/>: every tree reaps on the grace
/// period alone. A host that replicates trees registers a gate that bounds the
/// reap on the replication frontier (issue #4615).
/// </summary>
internal sealed class UngatedTombstoneReapGate : ITombstoneReapGate
{
    private static readonly Task<HybridLogicalClock?> Ungated = Task.FromResult<HybridLogicalClock?>(null);

    /// <inheritdoc />
    public Task<HybridLogicalClock?> GetReapCeilingAsync(string treeId, CancellationToken cancellationToken = default) => Ungated;
}
