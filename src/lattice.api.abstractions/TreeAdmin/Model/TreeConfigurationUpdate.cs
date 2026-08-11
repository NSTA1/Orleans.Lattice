namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// A partial update to a tree's per-tree registry configuration. Each of the three
/// independent runtime knobs (publish-events, projection-digest maintenance,
/// durable-history retention) carries its own <c>Apply*</c> flag so a caller can
/// update one dimension without disturbing the others: a dimension is written only
/// when its <c>Apply*</c> flag is <see langword="true"/>, and a <see langword="null"/>
/// value on an applied dimension <b>clears</b> that override so the knob falls back to
/// the silo-wide option.
/// </summary>
/// <remarks>
/// This apply-flag shape distinguishes "leave unchanged" (the <c>Apply*</c> flag is
/// <see langword="false"/>) from "clear the override" (the <c>Apply*</c> flag is
/// <see langword="true"/> and the value is <see langword="null"/>), which a bare
/// nullable value could not express. Structural sizing (shard count, node fan-out) is
/// deliberately not settable here - it is pinned at tree creation and mutated only by
/// the resize / reshard operations.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeConfigurationUpdate)]
[Immutable]
public sealed record TreeConfigurationUpdate
{
    /// <summary>
    /// When <see langword="true"/>, write the <see cref="PublishEvents"/> override
    /// (a <see langword="null"/> value clears it); when <see langword="false"/>,
    /// leave the tree's publish-events override unchanged.
    /// </summary>
    [Id(0)] public bool ApplyPublishEvents { get; init; }

    /// <summary>
    /// The per-tree publish-events override to pin, or <see langword="null"/> to
    /// clear it (fall back to the silo-wide option). Honoured only when
    /// <see cref="ApplyPublishEvents"/> is <see langword="true"/>.
    /// </summary>
    [Id(1)] public bool? PublishEvents { get; init; }

    /// <summary>
    /// When <see langword="true"/>, write the <see cref="MaintainProjectionDigest"/>
    /// override (a <see langword="null"/> value clears it); when
    /// <see langword="false"/>, leave the tree's projection-digest override unchanged.
    /// </summary>
    [Id(2)] public bool ApplyMaintainProjectionDigest { get; init; }

    /// <summary>
    /// The per-tree projection-digest-maintenance override to pin, or
    /// <see langword="null"/> to clear it (fall back to the silo-wide option).
    /// Honoured only when <see cref="ApplyMaintainProjectionDigest"/> is
    /// <see langword="true"/>. Note the permanent-disable latch, once set, supersedes
    /// any <see langword="true"/> pinned here.
    /// </summary>
    [Id(3)] public bool? MaintainProjectionDigest { get; init; }

    /// <summary>
    /// When <see langword="true"/>, write the durable-history retention override
    /// (<see cref="HistoryRetentionMode"/> and <see cref="HistoryRetentionWindowTicks"/>,
    /// each cleared independently by a <see langword="null"/> value); when
    /// <see langword="false"/>, leave the tree's history-retention override unchanged.
    /// </summary>
    [Id(4)] public bool ApplyHistoryRetention { get; init; }

    /// <summary>
    /// The per-tree durable-history retention mode to pin, or <see langword="null"/>
    /// to clear it (fall back to metadata-only). Honoured only when
    /// <see cref="ApplyHistoryRetention"/> is <see langword="true"/>.
    /// </summary>
    [Id(5)] public HistoryRetentionMode? HistoryRetentionMode { get; init; }

    /// <summary>
    /// The per-tree durable-history age bound in ticks to pin, or
    /// <see langword="null"/> to clear it (no age bound). Must be strictly positive
    /// when supplied. Honoured only when <see cref="ApplyHistoryRetention"/> is
    /// <see langword="true"/>.
    /// </summary>
    [Id(6)] public long? HistoryRetentionWindowTicks { get; init; }
}
