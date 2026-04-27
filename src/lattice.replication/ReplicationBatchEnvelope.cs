namespace Orleans.Lattice.Replication;

/// <summary>
/// Versioned, Orleans-serialisable wire envelope wrapping a batch of
/// <see cref="ReplogEntry"/> records for cross-cluster replication
/// transport. The envelope is the canonical on-the-wire shape produced
/// by <see cref="IReplicationBatchEncoder"/> implementations and stuffed
/// into <see cref="ReplicationBatch.Payload"/> at dispatch time.
/// <para>
/// The shape is intentionally flat: routing metadata
/// (<see cref="TreeName"/>, <see cref="OriginClusterId"/>) is duplicated
/// from the surrounding <see cref="ReplicationBatch"/> so a receiver
/// that decodes the bytes in isolation - e.g. a debugging tool, a
/// disk-cached batch, or a forwarded payload - can recover the routing
/// context without reconstructing the call envelope. <see cref="WireVersion"/>
/// is the schema-evolution lever: every breaking change to the envelope
/// shape bumps it, so a receiver can fail fast on a payload it cannot
/// safely decode rather than silently mis-applying truncated state.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationBatchEnvelope)]
[Immutable]
public readonly record struct ReplicationBatchEnvelope
{
    /// <summary>
    /// The wire-format version this envelope was authored against.
    /// Receivers compare against
    /// <see cref="IReplicationBatchEncoder.CurrentWireVersion"/> and
    /// reject payloads carrying a strictly greater value rather than
    /// guess at the layout. Hand-constructed envelopes default to
    /// <c>0</c>; the canonical encoder stamps
    /// <see cref="CurrentVersion"/> at encode time when the caller
    /// supplies <c>0</c>.
    /// </summary>
    [Id(0)] public int WireVersion { get; init; }

    /// <summary>
    /// Logical tree id the entries were captured from. Mirrors
    /// <see cref="ReplicationBatch.TreeName"/> on the surrounding call
    /// envelope; receivers route the per-tree apply pipeline on this
    /// value.
    /// </summary>
    [Id(1)] public string TreeName { get; init; }

    /// <summary>
    /// Stable identifier of the originating cluster. Mirrors
    /// <see cref="ReplicationBatch.OriginClusterId"/> on the surrounding
    /// call envelope; receivers use it to attribute origin and break
    /// replication cycles.
    /// </summary>
    [Id(2)] public string OriginClusterId { get; init; }

    /// <summary>
    /// The captured <see cref="ReplogEntry"/> records, in commit order.
    /// May be empty (heartbeat / keep-alive batch). Never
    /// <see langword="null"/> on a value produced by the canonical
    /// encoder; hand-constructed envelopes that leave this default
    /// decode as <see langword="null"/> after a round-trip and the
    /// canonical decoder treats that as an empty list.
    /// </summary>
    [Id(3)] public IReadOnlyList<ReplogEntry> Entries { get; init; }

    /// <summary>
    /// The current wire-format version stamped by the canonical
    /// <see cref="OrleansBinaryReplicationBatchEncoder"/>. Bumped on every
    /// breaking change to the envelope shape; consumers compare strictly
    /// (greater than is rejected, less-than-or-equal is accepted) so
    /// older receivers fail fast on newer producers rather than
    /// mis-decoding.
    /// </summary>
    public const int CurrentVersion = 1;

    /// <summary>
    /// Diagnostic minor version stamped on the canonical wire format.
    /// Bumped when a strictly additive change ships - e.g. a new
    /// <c>[Id]</c> slot on <see cref="ReplogEntry"/> that legacy peers
    /// safely decode as null - so logs and traces can correlate the
    /// producer's exact envelope shape without inflating
    /// <see cref="CurrentVersion"/> (which is reserved for breaking
    /// changes that older receivers must reject). Has no effect on the
    /// wire-format alias and is not consulted by the encoder /
    /// decoder.
    /// </summary>
    public const int CurrentMinorVersion = 1;
}
