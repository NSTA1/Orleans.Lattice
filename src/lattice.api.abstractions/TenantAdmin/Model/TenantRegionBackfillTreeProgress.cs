namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>Observable receiver-side bootstrap progress for one tenant tree.</summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantRegionBackfillTreeProgress)]
[Immutable]
public sealed record TenantRegionBackfillTreeProgress
{
    /// <summary>The locally configured replicated tree id.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The receiver bootstrap phase.</summary>
    [Id(1)] public required string Phase { get; init; }

    /// <summary>Snapshot entries applied by the current attempt.</summary>
    [Id(2)] public long EntriesApplied { get; init; }

    /// <summary>The source cluster selected for this bootstrap, when known.</summary>
    [Id(3)] public string? SourceClusterId { get; init; }

    /// <summary>Whether reads are fenced because the receiver may hold a partial import.</summary>
    [Id(4)] public bool ReadFenced { get; init; }

    /// <summary>The tenant-offline dead-letter entries still awaiting replay.</summary>
    [Id(5)] public int PendingDeadLetters { get; init; }
}
