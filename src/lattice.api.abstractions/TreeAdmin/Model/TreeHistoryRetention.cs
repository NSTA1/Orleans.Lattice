namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// A tree's effective durable-history retention policy: the resolved
/// <see cref="TreeHistoryRetentionMode"/> applied to LWW value bytes and the
/// age-bound window after which a revision row expires. Returned by the retention
/// read verb and echoed back by the retention set verb (which reads the effective
/// policy after applying the change), so a caller always sees the resolved shape
/// rather than the raw override it supplied. A pure projection with no side effects.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeHistoryRetention)]
[Immutable]
public sealed record TreeHistoryRetention
{
    /// <summary>The tree id whose retention policy this reports.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// The retention mode applied to LWW value bytes, resolved from the tree's
    /// persisted override or the documented default
    /// (<see cref="TreeHistoryRetentionMode.MetadataOnly"/>) when none is set.
    /// </summary>
    [Id(1)] public TreeHistoryRetentionMode Mode { get; init; }

    /// <summary>
    /// The age after which a revision row expires, or <see cref="System.TimeSpan.Zero"/>
    /// when no age bound is configured.
    /// </summary>
    [Id(2)] public TimeSpan Window { get; init; }
}
