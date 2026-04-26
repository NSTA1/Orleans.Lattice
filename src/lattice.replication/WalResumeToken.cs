namespace Orleans.Lattice.Replication;

/// <summary>
/// Internal opaque resume token exchanged on the transport-side seam
/// (the gRPC push transport will consume this) when a peer needs to
/// resume mid-stream from a precise per-shard offset rather than a
/// hybrid-logical-clock cursor.
/// <para>
/// <b>Cursor-shape decision.</b> The public
/// <see cref="IChangeFeed"/> contract is HLC-cursor-shaped — that
/// preserves transitive replication HLC fidelity, aligns with the
/// per-origin high-water-mark dedup table, and matches the
/// shape a future cross-tree materialiser needs (no notion of per-shard
/// offset). HLC cursors are therefore the canonical resume shape on
/// every public surface.
/// </para>
/// <para>
/// Per-shard offsets are exposed only on the internal transport-side
/// seam where they trivially are monotonic per shard, match the WAL
/// <see cref="WalEntry.Offset"/> shape 1:1, and remove HLC-skew edge
/// cases at reconnect time. Receivers store this token alongside their
/// per-origin HWM purely as a diagnostic fast-path; the HWM remains the
/// authoritative dedup key.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.WalResumeToken)]
[Immutable]
internal readonly record struct WalResumeToken
{
    /// <summary>The per-tree shard index this token resumes against.</summary>
    [Id(0)] public int ShardIndex { get; init; }

    /// <summary>
    /// Inclusive lower-bound offset to resume from. The next emitted
    /// entry will have <see cref="WalEntry.Offset"/> equal to
    /// <see cref="Offset"/> + 1.
    /// </summary>
    [Id(1)] public long Offset { get; init; }
}
