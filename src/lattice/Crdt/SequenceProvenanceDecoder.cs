using System.Collections.Generic;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// <see cref="ICrdtProvenanceDecoder"/> for the replicated-growable-array
/// sequence shape (<see cref="LatticeMergeMode.Sequence"/>). Turns an
/// <see cref="Rga"/>'s stored state or a sequence of <see cref="RgaDelta"/>
/// author deltas into ordered <see cref="CrdtMemberChange"/> events.
/// <para>
/// An RGA retains full element-level provenance durably: every insert carries
/// a unique <c>(replica, counter)</c> dot and the node value bytes, and a
/// remove tombstones a single dot. The mapping is a clean fit - an insert is an
/// <see cref="CrdtMemberChangeKind.Added"/> event whose
/// <see cref="CrdtMemberChange.Element"/> is the inserted node's value bytes,
/// and a remove is a <see cref="CrdtMemberChangeKind.Removed"/> event keyed by
/// the tombstoned dot.
/// </para>
/// <para>
/// <strong>Element bytes on a removed event.</strong> An
/// <see cref="RgaDelta"/> tombstone carries only the removed dot, not the value
/// that was at that position, so a delta-path removed event has an empty
/// <see cref="CrdtMemberChange.Element"/>. The folded-state path does still
/// hold the node, so a state-path removed event carries the tombstoned node's
/// value bytes (which may themselves be empty for a placeholder node).
/// </para>
/// </summary>
public sealed class SequenceProvenanceDecoder : ICrdtProvenanceDecoder
{
    /// <summary>A shared, stateless instance. The decoder holds no per-call state.</summary>
    public static SequenceProvenanceDecoder Instance { get; } = new();

    /// <inheritdoc />
    public LatticeMergeMode Mode => LatticeMergeMode.Sequence;

    /// <summary>
    /// Decodes an ordered <see cref="RgaDelta"/> sequence into member-change
    /// events in operation order. Within a single delta, inserts precede
    /// removes; across deltas, the supplied order is the causal order, so an
    /// insert-then-remove of the same position surfaces both events in order.
    /// Each event carries the originating delta's wall-clock stamp when one was
    /// supplied. A removed event has an empty element because a tombstone
    /// carries only the removed dot.
    /// </summary>
    /// <param name="deltas">
    /// The ordered author-delta sequence; each entry's <c>Delta</c> must be an
    /// <see cref="RgaDelta"/>.
    /// </param>
    /// <returns>The decoded member-change events, in operation order.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="deltas"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberChange> DecodeDeltas(IReadOnlyList<CrdtProvenanceDelta> deltas)
    {
        ArgumentNullException.ThrowIfNull(deltas);
        if (deltas.Count == 0) return Array.Empty<CrdtMemberChange>();

        var total = 0;
        for (var i = 0; i < deltas.Count; i++)
        {
            var delta = (RgaDelta)deltas[i].Delta;
            if (delta.Inserts is { Count: > 0 } inserts) total += inserts.Count;
            if (delta.Tombstones is { Count: > 0 } tombstones) total += tombstones.Count;
        }
        if (total == 0) return Array.Empty<CrdtMemberChange>();

        var result = new List<CrdtMemberChange>(total);
        for (var i = 0; i < deltas.Count; i++)
        {
            var entry = deltas[i];
            var delta = (RgaDelta)entry.Delta;
            var wallClock = entry.WallClock;

            var inserts = delta.Inserts;
            if (inserts is { Count: > 0 })
            {
                for (var j = 0; j < inserts.Count; j++)
                {
                    var node = inserts[j];
                    result.Add(new CrdtMemberChange
                    {
                        Element = node.Value ?? Array.Empty<byte>(),
                        Kind = CrdtMemberChangeKind.Added,
                        ReplicaId = node.ReplicaId,
                        Ordinal = node.Counter,
                        WallClock = wallClock,
                    });
                }
            }

            var tombstones = delta.Tombstones;
            if (tombstones is { Count: > 0 })
            {
                for (var j = 0; j < tombstones.Count; j++)
                {
                    var dot = tombstones[j];
                    result.Add(new CrdtMemberChange
                    {
                        Element = Array.Empty<byte>(),
                        Kind = CrdtMemberChangeKind.Removed,
                        ReplicaId = dot.ReplicaId,
                        Ordinal = dot.Counter,
                        WallClock = wallClock,
                    });
                }
            }
        }
        return result;
    }

    /// <summary>
    /// Reconstructs member-change events from a folded <see cref="Rga"/>: each
    /// live node yields an <see cref="CrdtMemberChangeKind.Added"/> event and
    /// each tombstoned node a <see cref="CrdtMemberChangeKind.Removed"/> event,
    /// both carrying the node's value bytes. Events are ordered deterministically
    /// by replica then causal ordinal then kind. Because no owning mutation is
    /// available, <see cref="CrdtMemberChange.WallClock"/> is always
    /// <see langword="null"/>.
    /// </summary>
    /// <param name="state">The <see cref="Rga"/> to decode.</param>
    /// <returns>The reconstructed member-change events.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberChange> DecodeState(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var rga = (Rga)state;
        var nodes = rga.Nodes;
        if (nodes.Count == 0) return Array.Empty<CrdtMemberChange>();

        var result = new List<CrdtMemberChange>(nodes.Count);
        for (var i = 0; i < nodes.Count; i++)
        {
            var node = nodes[i];
            result.Add(new CrdtMemberChange
            {
                Element = node.Value ?? Array.Empty<byte>(),
                Kind = node.IsTombstone ? CrdtMemberChangeKind.Removed : CrdtMemberChangeKind.Added,
                ReplicaId = node.ReplicaId,
                Ordinal = node.Counter,
                WallClock = null,
            });
        }
        result.Sort(CrdtMemberChangeCausalComparer.Instance);
        return result;
    }
}
