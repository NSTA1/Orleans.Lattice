namespace Orleans.Lattice.Operations;

/// <summary>The persisted state of one <see cref="LatticeOperationGrain"/>.</summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeOperationGrainState)]
internal sealed class LatticeOperationGrainState
{
    /// <summary>The operation record, or <see langword="null"/> when none exists.</summary>
    [Id(0)] public LatticeOperationRecord? Record { get; set; }

    /// <summary>When the runner last reported or heartbeated (UTC).</summary>
    [Id(1)] public DateTimeOffset LastHeartbeatUtc { get; set; }

    /// <summary>The last index mutation awaiting acknowledgement; persisted with the record.</summary>
    [Id(2)] public LatticeOperationRecord? PendingIndexRecord { get; set; }

    /// <summary>Whether the pending mutation removes an expired operation.</summary>
    [Id(3)] public bool PendingIndexRemoval { get; set; }

    /// <summary>Whether this state was written with durable index reconciliation.</summary>
    [Id(4)] public bool IndexOutboxInitialized { get; set; }
}
