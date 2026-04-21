namespace Orleans.Lattice;

/// <summary>
/// A metadata-only notification about a change to a Lattice tree. Published on
/// the Orleans stream <c>orleans.lattice.events</c> (per-tree stream id =
/// <see cref="TreeId"/>) when <see cref="LatticeOptions.PublishEvents"/> is
/// enabled. Does <em>not</em> carry the written value — consumers call
/// <c>GetAsync</c> / <c>GetWithVersionAsync</c> to read the current state.
/// <para>
/// Delivery is Orleans-provider-dependent: the <c>MemoryStreams</c> provider
/// gives best-effort at-most-once delivery with no durability (events are lost
/// if an agent is not attached when publication occurs); <c>EventHub</c> /
/// <c>AzureQueue</c> providers give durable at-least-once. Events are not
/// guaranteed to arrive in HLC order — consumers that need causal ordering
/// should sort by <see cref="AtUtc"/> within a small window or re-read via
/// <c>GetWithVersionAsync</c>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeTreeEvent)]
[Immutable]
public readonly record struct LatticeTreeEvent
{
    /// <summary>Classification of the change — see <see cref="LatticeTreeEventKind"/>.</summary>
    [Id(0)] public LatticeTreeEventKind Kind { get; init; }

    /// <summary>The logical tree id that produced this event. Matches the stream id.</summary>
    [Id(1)] public string TreeId { get; init; }

    /// <summary>
    /// The key affected by this event, or <c>null</c> for tree-level events.
    /// For <see cref="LatticeTreeEventKind.DeleteRange"/> this carries the literal
    /// <c>"startInclusive..endExclusive"</c> range string. For
    /// <see cref="LatticeTreeEventKind.AtomicWriteCompleted"/> this is <c>null</c> —
    /// individual saga writes emit per-key <see cref="LatticeTreeEventKind.Set"/> /
    /// <see cref="LatticeTreeEventKind.Delete"/> events stamped with the
    /// <see cref="OperationId"/>.
    /// </summary>
    [Id(2)] public string? Key { get; init; }

    /// <summary>
    /// The physical shard index the event originated from, or <c>null</c> when the
    /// event is not shard-scoped (e.g. tree-lifecycle or global coordinator events).
    /// </summary>
    [Id(3)] public int? ShardIndex { get; init; }

    /// <summary>
    /// Correlation id for events produced by a multi-key saga (atomic write) or
    /// other bulk operation. <c>null</c> for stand-alone writes. Subscribers may
    /// use this to group per-key <see cref="LatticeTreeEventKind.Set"/> /
    /// <see cref="LatticeTreeEventKind.Delete"/> events under their parent
    /// <see cref="LatticeTreeEventKind.AtomicWriteCompleted"/> event.
    /// </summary>
    [Id(4)] public string? OperationId { get; init; }

    /// <summary>Wall-clock UTC timestamp captured at publish time on the silo.</summary>
    [Id(5)] public DateTimeOffset AtUtc { get; init; }
}
