namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Stable string identifiers for the delta payload formats produced by
/// the typed CRDT accessors. Carried on
/// <see cref="LatticeMutation.DeltaKind"/> so consumers (notably the
/// replication-side observer) can dispatch on the encoding without
/// parsing the payload bytes.
/// </summary>
/// <remarks>
/// The strings are part of the <em>internal</em> producer/consumer
/// contract between this assembly and <c>Orleans.Lattice.Replication</c>
/// (which has <c>InternalsVisibleTo</c> on this assembly). They are not
/// a public API surface - external observers should treat unknown kinds
/// as opaque.
/// </remarks>
internal static class CrdtDeltaKinds
{
    /// <summary>An OR-Set add: a fresh dot was attached to one element.</summary>
    public const string OrSetAdd = "ol.crdt.ors.add";

    /// <summary>An OR-Set remove: every observed dot for one element was tombstoned.</summary>
    public const string OrSetRemove = "ol.crdt.ors.rm";

    /// <summary>An OR-Set state merge: the supplied other-state was unioned in.</summary>
    public const string OrSetMerge = "ol.crdt.ors.mrg";

    /// <summary>A PN-Counter increment: per-replica positive component advanced.</summary>
    public const string PnCounterIncrement = "ol.crdt.pnc.inc";

    /// <summary>A PN-Counter decrement: per-replica negative component advanced.</summary>
    public const string PnCounterDecrement = "ol.crdt.pnc.dec";

    /// <summary>A PN-Counter state merge: the supplied other-state was pointwise-max'd in.</summary>
    public const string PnCounterMerge = "ol.crdt.pnc.mrg";

    /// <summary>A version-vector tick: one replica's HLC entry advanced.</summary>
    public const string VersionVectorTick = "ol.crdt.vvc.tick";

    /// <summary>A version-vector state merge: the supplied other-state was pointwise-max'd in.</summary>
    public const string VersionVectorMerge = "ol.crdt.vvc.mrg";

    /// <summary>An MV-Register set: a fresh dot was minted by one replica, dropping every entry the writer had observed.</summary>
    public const string MvRegisterSet = "ol.crdt.mvr.set";

    /// <summary>An MV-Register state merge: the supplied other-state was merged via dot-context dominance.</summary>
    public const string MvRegisterMerge = "ol.crdt.mvr.mrg";
}

/// <summary>
/// JSON-friendly payload records for the delta encodings emitted by the
/// typed CRDT accessors (<see cref="OrSetAccessor"/>,
/// <see cref="PnCounterAccessor"/>, <see cref="VersionVectorAccessor"/>).
/// </summary>
/// <remarks>
/// <para>
/// The payloads are JSON-serialised via <see cref="JsonLatticeSerializer{T}"/>
/// with default options so they round-trip without depending on Orleans
/// codec generation. They are deliberately decoupled from the typed
/// delta DTOs that live in <c>Orleans.Lattice.Replication</c>: this
/// keeps the core library free of any replication dependency, and lets
/// the replication-side observer translate from the producer-side
/// payload to its own public delta record without forcing one schema on
/// both packages.
/// </para>
/// <para>
/// Records use positional constructors so <see cref="System.Text.Json.JsonSerializer"/>
/// can deserialize them without reflection-on-init-only-properties
/// caveats.
/// </para>
/// </remarks>
internal static class CrdtDeltaPayloads
{
    /// <summary>An OR-Set add: a single fresh dot attached to <c>Element</c>.</summary>
    internal sealed record OrSetAddDelta(byte[] Element, string ReplicaId, long Counter);

    /// <summary>An OR-Set remove: every previously-observed dot for <c>Element</c> tombstoned.</summary>
    internal sealed record OrSetRemoveDelta(byte[] Element, OrSetDotPayload[] ObservedDots);

    /// <summary>An OR-Set merge: the union of the merged-in state's adds and tombstones.</summary>
    internal sealed record OrSetMergeDelta(
        Dictionary<string, OrSetDotPayload[]> Adds,
        Dictionary<string, OrSetDotPayload[]> Tombstones);

    /// <summary>A causal dot expressed as JSON-friendly primitives.</summary>
    internal sealed record OrSetDotPayload(string ReplicaId, long Counter);

    /// <summary>A PN-Counter increment authored by <c>ReplicaId</c>.</summary>
    internal sealed record PnCounterIncrementDelta(string ReplicaId, long Amount);

    /// <summary>A PN-Counter decrement authored by <c>ReplicaId</c>.</summary>
    internal sealed record PnCounterDecrementDelta(string ReplicaId, long Amount);

    /// <summary>A PN-Counter merge: the merged-in per-replica positive and negative components.</summary>
    internal sealed record PnCounterMergeDelta(
        Dictionary<string, long> Increments,
        Dictionary<string, long> Decrements);

    /// <summary>A version-vector tick: <c>ReplicaId</c> advanced to the supplied HLC.</summary>
    internal sealed record VersionVectorTickDelta(string ReplicaId, long WallClockTicks, int HlcCounter);

    /// <summary>A version-vector merge: the merged-in per-replica HLC entries.</summary>
    internal sealed record VersionVectorMergeDelta(Dictionary<string, HlcPayload> Entries);

    /// <summary>An HLC expressed as JSON-friendly primitives.</summary>
    internal sealed record HlcPayload(long WallClockTicks, int Counter);

    /// <summary>
    /// An MV-Register set: a fresh dot <c>(ReplicaId, Counter)</c> with <c>Value</c>
    /// stamped by the author, plus the writer's post-write dot context so a
    /// downstream replication consumer can decide which prior dots the
    /// write observed-and-superseded.
    /// </summary>
    internal sealed record MvRegisterSetDelta(
        string ReplicaId,
        long Counter,
        byte[] Value,
        Dictionary<string, long> Context);

    /// <summary>An MV-Register merge: the merged-in dot-tagged entries and context.</summary>
    internal sealed record MvRegisterMergeDelta(
        MvRegisterEntryPayload[] Entries,
        Dictionary<string, long> Context);

    /// <summary>An MV-Register entry expressed as JSON-friendly primitives.</summary>
    internal sealed record MvRegisterEntryPayload(string ReplicaId, long Counter, byte[] Value);
}
