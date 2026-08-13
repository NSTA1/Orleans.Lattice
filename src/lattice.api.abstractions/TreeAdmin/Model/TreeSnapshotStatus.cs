namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The read-only status of a tree's snapshot machinery, returned by the snapshot
/// capture verb and the standalone status read. Reports whether a snapshot is
/// currently in flight for the source tree, echoing the destination tree id and
/// mode requested by the trigger that produced it. A pure projection with no side
/// effects.
/// <para>
/// A snapshot is self-completing and reminder-durable (it drains the source tree
/// shard-by-shard into the destination and, in <see cref="TreeSnapshotMode.Online"/>
/// mode, shadow-forwards live writes until the drain converges, then clears itself),
/// so this status surfaces the observable idle/in-flight signal rather than the
/// coordinator's internal phase machine.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeSnapshotStatus)]
[Immutable]
public sealed record TreeSnapshotStatus
{
    /// <summary>The source tree id whose snapshot status this reports.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// <see langword="true"/> when a snapshot is currently in flight for the source
    /// tree; <see langword="false"/> when the coordinator is idle (either no snapshot
    /// has ever been initiated, or the last one has run to completion).
    /// </summary>
    [Id(1)] public bool InProgress { get; init; }

    /// <summary>
    /// The destination tree id requested by the capture trigger that produced this
    /// status, or <see langword="null"/> for a standalone status read (the
    /// coordinator's in-flight destination is not publicly surfaced).
    /// </summary>
    [Id(2)] public string? RequestedDestinationTreeId { get; init; }

    /// <summary>
    /// The snapshot mode requested by the capture trigger that produced this status,
    /// or <see langword="null"/> for a standalone status read.
    /// </summary>
    [Id(3)] public TreeSnapshotMode? RequestedMode { get; init; }
}
