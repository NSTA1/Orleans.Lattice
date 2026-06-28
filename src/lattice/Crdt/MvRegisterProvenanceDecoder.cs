using System.Collections.Generic;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// <see cref="ICrdtProvenanceDecoder"/> for the multi-value register shape
/// (<see cref="LatticeMergeMode.MvRegister"/>). Turns an
/// <see cref="MvRegister"/>'s stored state or a sequence of
/// <see cref="MvRegisterDelta"/> author deltas into
/// <see cref="CrdtMemberChange"/> events.
/// <para>
/// <strong>Concurrent-value provenance.</strong> A multi-value register keeps
/// every concurrent dot-tagged write as a live value until a future write
/// observes and supersedes it. This decoder maps each live value to an
/// <see cref="CrdtMemberChangeKind.Added"/> event whose
/// <see cref="CrdtMemberChange.Element"/> is the value bytes and whose
/// <see cref="CrdtMemberChange.Ordinal"/> is the dot counter, so concurrent
/// writes from different replicas are all represented (no last-writer-wins
/// collapse).
/// </para>
/// <para>
/// <strong>Superseded values are not byte-recoverable.</strong> When a write
/// supersedes an earlier value the earlier bytes are dropped at write time and
/// only the dot context (the per-replica high-water counter) survives. This
/// decoder therefore emits a <see cref="CrdtMemberChangeKind.Removed"/> event
/// with an <em>empty</em> element for each replica present in the dot context
/// that has no surviving live entry - recording that the replica's value was
/// observed-and-superseded at that counter without being able to recover what
/// the value was. A replica that still has a live entry contributes only its
/// <see cref="CrdtMemberChangeKind.Added"/> event; the intermediate superseded
/// counters for that same replica are not individually recoverable.
/// </para>
/// </summary>
public sealed class MvRegisterProvenanceDecoder : ICrdtProvenanceDecoder
{
    /// <summary>A shared, stateless instance. The decoder holds no per-call state.</summary>
    public static MvRegisterProvenanceDecoder Instance { get; } = new();

    /// <inheritdoc />
    public LatticeMergeMode Mode => LatticeMergeMode.MvRegister;

    /// <summary>
    /// Decodes an ordered <see cref="MvRegisterDelta"/> sequence into
    /// member-change events: each delta contributes one
    /// <see cref="CrdtMemberChangeKind.Added"/> event per carried entry and one
    /// <see cref="CrdtMemberChangeKind.Removed"/> event (empty element) per
    /// context replica without a surviving entry, ordered deterministically
    /// within a delta and in the supplied order across deltas. Each event
    /// carries the originating delta's wall-clock stamp when one was supplied.
    /// </summary>
    /// <param name="deltas">
    /// The ordered author-delta sequence; each entry's <c>Delta</c> must be an
    /// <see cref="MvRegisterDelta"/>.
    /// </param>
    /// <returns>The decoded member-change events.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="deltas"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberChange> DecodeDeltas(IReadOnlyList<CrdtProvenanceDelta> deltas)
    {
        ArgumentNullException.ThrowIfNull(deltas);
        if (deltas.Count == 0) return Array.Empty<CrdtMemberChange>();

        var result = new List<CrdtMemberChange>();
        for (var i = 0; i < deltas.Count; i++)
        {
            var entry = deltas[i];
            var delta = (MvRegisterDelta)entry.Delta;
            var start = result.Count;
            Emit(result, delta.Entries, delta.Context, entry.WallClock);
            result.Sort(start, result.Count - start, CrdtMemberChangeCausalComparer.Instance);
        }
        return result.Count == 0 ? Array.Empty<CrdtMemberChange>() : result;
    }

    /// <summary>
    /// Reconstructs member-change events from a folded <see cref="MvRegister"/>:
    /// one <see cref="CrdtMemberChangeKind.Added"/> event per live entry and one
    /// <see cref="CrdtMemberChangeKind.Removed"/> event (empty element) per
    /// context replica without a surviving entry, ordered deterministically by
    /// replica then ordinal then kind. Because no owning mutation is available,
    /// <see cref="CrdtMemberChange.WallClock"/> is always
    /// <see langword="null"/>.
    /// </summary>
    /// <param name="state">The <see cref="MvRegister"/> to decode.</param>
    /// <returns>The reconstructed member-change events.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberChange> DecodeState(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var register = (MvRegister)state;
        var result = new List<CrdtMemberChange>(register.Entries.Count + register.Context.Count);
        Emit(result, register.Entries, register.Context, null);
        if (result.Count == 0) return Array.Empty<CrdtMemberChange>();
        result.Sort(CrdtMemberChangeCausalComparer.Instance);
        return result;
    }

    private static void Emit(
        List<CrdtMemberChange> sink,
        IReadOnlyList<MvRegisterEntry>? entries,
        IReadOnlyDictionary<string, long>? context,
        HybridLogicalClock? wallClock)
    {
        HashSet<string>? liveReplicas = null;
        if (entries is { Count: > 0 })
        {
            liveReplicas = new HashSet<string>(StringComparer.Ordinal);
            for (var i = 0; i < entries.Count; i++)
            {
                var e = entries[i];
                liveReplicas.Add(e.ReplicaId);
                sink.Add(new CrdtMemberChange
                {
                    Element = e.Value ?? Array.Empty<byte>(),
                    Kind = CrdtMemberChangeKind.Added,
                    ReplicaId = e.ReplicaId,
                    Ordinal = e.Counter,
                    WallClock = wallClock,
                });
            }
        }

        if (context is { Count: > 0 })
        {
            foreach (var (replicaId, counter) in context)
            {
                if (liveReplicas is not null && liveReplicas.Contains(replicaId)) continue;
                sink.Add(new CrdtMemberChange
                {
                    Element = Array.Empty<byte>(),
                    Kind = CrdtMemberChangeKind.Removed,
                    ReplicaId = replicaId,
                    Ordinal = counter,
                    WallClock = wallClock,
                });
            }
        }
    }
}
