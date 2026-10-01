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
}
