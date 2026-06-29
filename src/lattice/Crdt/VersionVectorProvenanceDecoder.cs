using System.Collections.Generic;
using System.Text;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// <see cref="ICrdtProvenanceDecoder"/> for the version-vector shape
/// (<see cref="LatticeMergeMode.VersionVector"/>). Turns a
/// <see cref="VersionVector"/>'s stored state or a sequence of
/// <see cref="VersionVectorDelta"/> author deltas into
/// <see cref="CrdtMemberChange"/> events, one per replica entry.
/// <para>
/// <strong>Causal frontier, not an add/remove history.</strong> A version
/// vector is the per-replica high-water mark of observed causality, so it has
/// no removals: this decoder emits only <see cref="CrdtMemberChangeKind.Added"/>
/// events, one per replica entry, with <see cref="CrdtMemberChange.Element"/>
/// set to the replica id encoded as UTF-8 bytes,
/// <see cref="CrdtMemberChange.Ordinal"/> set to that entry's
/// <see cref="HybridLogicalClock.Counter"/>, and
/// <see cref="CrdtMemberChange.WallClock"/> set to the entry's clock itself.
/// Unlike the other folded-state decoders, the wall clock <em>is</em> populated
/// on the state path because the vector stores a real
/// <see cref="HybridLogicalClock"/> per replica; the decoded events are a
/// snapshot of the causal frontier, not a record of individual operations.
/// </para>
/// </summary>
public sealed class VersionVectorProvenanceDecoder : ICrdtProvenanceDecoder
{
    /// <summary>A shared, stateless instance. The decoder holds no per-call state.</summary>
    public static VersionVectorProvenanceDecoder Instance { get; } = new();

    /// <inheritdoc />
    public LatticeMergeMode Mode => LatticeMergeMode.VersionVector;

    /// <summary>
    /// Decodes an ordered <see cref="VersionVectorDelta"/> sequence into
    /// member-change events. Each delta contributes one
    /// <see cref="CrdtMemberChangeKind.Added"/> event per replica entry, ordered
    /// deterministically by replica within a delta and in the supplied order
    /// across deltas. Each event's <see cref="CrdtMemberChange.WallClock"/> is
    /// the entry's own <see cref="HybridLogicalClock"/>.
    /// </summary>
    /// <param name="deltas">
    /// The ordered author-delta sequence; each entry's <c>Delta</c> must be a
    /// <see cref="VersionVectorDelta"/>.
    /// </param>
    /// <returns>The decoded member-change events.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="deltas"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberChange> DecodeDeltas(IReadOnlyList<CrdtProvenanceDelta> deltas)
    {
        ArgumentNullException.ThrowIfNull(deltas);
        if (deltas.Count == 0) return Array.Empty<CrdtMemberChange>();

        var total = 0;
        for (var i = 0; i < deltas.Count; i++)
        {
            var delta = (VersionVectorDelta)deltas[i].Delta;
            if (delta.Entries is { Count: > 0 } entries) total += entries.Count;
        }
        if (total == 0) return Array.Empty<CrdtMemberChange>();

        var result = new List<CrdtMemberChange>(total);
        for (var i = 0; i < deltas.Count; i++)
        {
            var delta = (VersionVectorDelta)deltas[i].Delta;
            var start = result.Count;
            Emit(result, delta.Entries);
            result.Sort(start, result.Count - start, CrdtMemberChangeCausalComparer.Instance);
        }
        return result;
    }

    /// <summary>
    /// Reconstructs member-change events from a folded
    /// <see cref="VersionVector"/>: one <see cref="CrdtMemberChangeKind.Added"/>
    /// event per replica entry, ordered deterministically by replica. Each
    /// event's <see cref="CrdtMemberChange.WallClock"/> is the entry's own
    /// <see cref="HybridLogicalClock"/> - the causal frontier is part of the
    /// stored state, so a real wall clock is available even without an owning
    /// mutation.
    /// </summary>
    /// <param name="state">The <see cref="VersionVector"/> to decode.</param>
    /// <returns>The reconstructed member-change events.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberChange> DecodeState(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var vector = (VersionVector)state;
        var entries = vector.Entries;
        if (entries.Count == 0) return Array.Empty<CrdtMemberChange>();

        var result = new List<CrdtMemberChange>(entries.Count);
        Emit(result, entries);
        result.Sort(CrdtMemberChangeCausalComparer.Instance);
        return result;
    }

    /// <summary>
    /// Projects a folded <see cref="VersionVector"/> into its current causal
    /// frontier: one <see cref="CrdtMemberValue"/> per replica entry, carrying the
    /// replica id (as UTF-8 element bytes and as
    /// <see cref="CrdtMemberValue.ReplicaId"/>) and that replica's high-water
    /// counter as <see cref="CrdtMemberValue.Ordinal"/>, ordered by replica. A
    /// version vector has no removals, so its current value is simply its frontier.
    /// </summary>
    /// <param name="state">The <see cref="VersionVector"/> to project.</param>
    /// <returns>One member per replica frontier entry.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberValue> DecodeCurrentValue(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var vector = (VersionVector)state;
        var entries = vector.Entries;
        if (entries.Count == 0) return Array.Empty<CrdtMemberValue>();

        var replicas = new List<string>(entries.Count);
        foreach (var replicaId in entries.Keys) replicas.Add(replicaId);
        replicas.Sort(StringComparer.Ordinal);

        var result = new List<CrdtMemberValue>(replicas.Count);
        foreach (var replicaId in replicas)
        {
            var clock = entries[replicaId];
            result.Add(new CrdtMemberValue
            {
                Element = Encoding.UTF8.GetBytes(replicaId),
                ReplicaId = replicaId,
                Ordinal = clock.Counter,
            });
        }

        return result;
    }

    private static void Emit(List<CrdtMemberChange> sink, Dictionary<string, HybridLogicalClock>? entries)
    {
        if (entries is not { Count: > 0 }) return;
        foreach (var (replicaId, clock) in entries)
        {
            sink.Add(new CrdtMemberChange
            {
                Element = Encoding.UTF8.GetBytes(replicaId),
                Kind = CrdtMemberChangeKind.Added,
                ReplicaId = replicaId,
                Ordinal = clock.Counter,
                WallClock = clock,
            });
        }
    }
}
