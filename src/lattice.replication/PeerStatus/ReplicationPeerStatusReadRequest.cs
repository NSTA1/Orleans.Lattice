namespace Orleans.Lattice.Replication;

/// <summary>
/// One bounded read of the per-peer replication telemetry state: optional exact
/// tree and peer filters, an exclusive <see cref="After"/> cursor, and a row
/// <see cref="Limit"/>. Tree ids are read and ordered exactly as recorded (the
/// effective, possibly tenant-qualified id); nothing is rendered away. Each silo
/// answers with at most <see cref="EffectiveLimit"/> rows, sorted in
/// <see cref="ReplicationPeerStatusOrder"/>, so the payload of one read is bounded
/// by the limit rather than by the size of the estate.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationPeerStatusReadRequest)]
[Immutable]
internal sealed record ReplicationPeerStatusReadRequest
{
    /// <summary>The largest <see cref="Limit"/> a read honours; larger values are clamped.</summary>
    public const int MaxLimit = 1000;

    /// <summary>
    /// When set, only rows whose effective tree id equals this value (ordinal)
    /// are returned.
    /// </summary>
    [Id(0)] public string? TreeId { get; init; }

    /// <summary>
    /// When set, only rows whose peer cluster id equals this value (ordinal) are
    /// returned.
    /// </summary>
    [Id(1)] public string? Peer { get; init; }

    /// <summary>
    /// The exclusive lower bound: only rows ordered strictly after this key are
    /// returned. <see langword="null"/> reads from the start.
    /// </summary>
    [Id(2)] public ReplicationPeerStatusCursor? After { get; init; }

    /// <summary>
    /// The maximum number of rows to return. Clamped by
    /// <see cref="EffectiveLimit"/> to <c>[1, <see cref="MaxLimit"/>]</c>.
    /// </summary>
    [Id(3)] public int Limit { get; init; }

    /// <summary>The limit a read applies: <see cref="Limit"/> clamped to <c>[1, <see cref="MaxLimit"/>]</c>.</summary>
    public int EffectiveLimit => Math.Clamp(Limit, 1, MaxLimit);
}
