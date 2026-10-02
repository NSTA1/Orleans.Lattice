namespace Orleans.Lattice.Operations;

/// <summary>One entry of the per-tenant operation index.</summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeOperationIndexEntry)]
internal sealed class LatticeOperationIndexEntry
{
    /// <summary>The operation id.</summary>
    [Id(0)] public string OperationId { get; set; } = string.Empty;

    /// <summary>The operation kind.</summary>
    [Id(1)] public string Kind { get; set; } = string.Empty;

    /// <summary>When it started (UTC).</summary>
    [Id(2)] public DateTimeOffset StartedAtUtc { get; set; }

    /// <summary>When it finished (UTC), or <see langword="null"/> while it runs.</summary>
    [Id(3)] public DateTimeOffset? FinishedAtUtc { get; set; }
}
