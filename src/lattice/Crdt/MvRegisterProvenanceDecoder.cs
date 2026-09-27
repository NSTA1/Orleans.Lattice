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

        // One pre-pass to size the result exactly so the hot append loop never
        // reallocates, matching the shape the OR-Set and RW-Set decoders already
        // use. The bound is free: each delta's entry and context counts are
        // already-materialised collection counts, not a re-scan of their
        // contents. It is exact - Emit contributes at most one event per entry
        // and one per context replica, and fewer only when a context replica
        // still has a live entry.
        var total = 0;
        for (var i = 0; i < deltas.Count; i++)
        {
            var delta = (MvRegisterDelta)deltas[i].Delta;
            if (delta.Entries is { Count: > 0 } entries) total += entries.Count;
            if (delta.Context is { Count: > 0 } context) total += context.Count;
        }
        if (total == 0) return Array.Empty<CrdtMemberChange>();

        var result = new List<CrdtMemberChange>(total);
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

    /// <summary>
    /// Projects a folded <see cref="MvRegister"/> into its current value(s). Each
    /// live entry yields one <see cref="CrdtMemberValue"/> carrying the value
    /// bytes and the entry's authoring replica and dot counter, ordered by replica
    /// then counter (matching <see cref="MvRegister.Values"/>). A single-valued
    /// register projects one member; a register with concurrent writes projects
    /// every conflicting value. The superseded-value provenance that
    /// <see cref="DecodeState(object)"/> records (empty-element removed events for
    /// context replicas without a live entry) is omitted: the current value is the
    /// set of live entries only.
    /// </summary>
    /// <param name="state">The <see cref="MvRegister"/> to project.</param>
    /// <returns>The current live value(s) as members.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <see langword="null"/>.</exception>
    public IReadOnlyList<CrdtMemberValue> DecodeCurrentValue(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var register = (MvRegister)state;
        var entries = register.Entries;
        if (entries.Count == 0) return Array.Empty<CrdtMemberValue>();

        // A multi-value register holds one value except while a concurrent write
        // is unresolved, and a one-element sequence is already sorted. Taking
        // that case directly skips the defensive copy the sort needs (a List and
        // its backing array) and the sort call itself, leaving only the single
        // result the caller asked for.
        if (entries.Count == 1)
        {
            var only = entries[0];
            return new CrdtMemberValue[]
            {
                new()
                {
                    Element = only.Value is null ? Array.Empty<byte>() : only.Value.AsSpan().ToArray(),
                    ReplicaId = only.ReplicaId,
                    Ordinal = only.Counter,
                },
            };
        }

        var ordered = new List<MvRegisterEntry>(entries);
        ordered.Sort(static (a, b) =>
        {
            var byReplica = string.CompareOrdinal(a.ReplicaId, b.ReplicaId);
            return byReplica != 0 ? byReplica : a.Counter.CompareTo(b.Counter);
        });

        var result = new List<CrdtMemberValue>(ordered.Count);
        for (var i = 0; i < ordered.Count; i++)
        {
            var e = ordered[i];
            result.Add(new CrdtMemberValue
            {
                // Copy: the projection is handed to an external caller, and
                // e.Value is the register's own stored buffer (see the
                // buffer-ownership remarks on ICrdt<TSelf>). An empty value
                // reuses the shared Array.Empty<byte>() singleton.
                Element = e.Value is null ? Array.Empty<byte>() : e.Value.AsSpan().ToArray(),
                ReplicaId = e.ReplicaId,
                Ordinal = e.Counter,
            });
        }

        return result;
    }

    private static void Emit(
        List<CrdtMemberChange> sink,
        IReadOnlyList<MvRegisterEntry>? entries,
        IReadOnlyDictionary<string, long>? context,
        HybridLogicalClock? wallClock)
    {
        // A multi-value register is single-valued in the steady state and
        // multi-valued only transiently, so the live-replica set the context
        // loop below tests against is almost always one element. Building a
        // HashSet for it costs three heap allocations (the set, its bucket
        // array, and its entry array) to answer a membership question a linear
        // scan over the same tiny list answers without allocating at all. The
        // set is therefore built only once the entry list is large enough for
        // the linear scan's O(entries) probe to stop being the cheaper option.
        HashSet<string>? liveReplicas = null;
        var entryCount = entries is null ? 0 : entries.Count;
        if (entryCount > 0)
        {
            if (entryCount > LinearLiveReplicaScanLimit)
            {
                liveReplicas = new HashSet<string>(entryCount, StringComparer.Ordinal);
            }

            for (var i = 0; i < entryCount; i++)
            {
                var e = entries![i];
                liveReplicas?.Add(e.ReplicaId);
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
                if (IsLiveReplica(liveReplicas, entries, entryCount, replicaId)) continue;
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

    /// <summary>
    /// The entry count above which <see cref="Emit"/> switches from a linear
    /// scan of the entry list to a <see cref="HashSet{T}"/>. A register holding
    /// more concurrent values than this is not a shape the steady state
    /// produces, so the set is built only for the pathological case that
    /// actually benefits from it.
    /// </summary>
    private const int LinearLiveReplicaScanLimit = 8;

    /// <summary>
    /// Answers whether <paramref name="replicaId"/> still has a live entry,
    /// through <paramref name="liveReplicas"/> when <see cref="Emit"/> built one
    /// and otherwise by an ordinal linear scan of the entry list itself.
    /// </summary>
    private static bool IsLiveReplica(
        HashSet<string>? liveReplicas,
        IReadOnlyList<MvRegisterEntry>? entries,
        int entryCount,
        string replicaId)
    {
        if (liveReplicas is not null) return liveReplicas.Contains(replicaId);
        for (var i = 0; i < entryCount; i++)
        {
            if (string.Equals(entries![i].ReplicaId, replicaId, StringComparison.Ordinal)) return true;
        }

        return false;
    }
}
