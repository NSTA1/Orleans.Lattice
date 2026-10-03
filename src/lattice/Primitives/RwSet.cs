using System.Buffers;

namespace Orleans.Lattice;

/// <summary>
/// A remove-wins observed-remove set CRDT - the set-granularity generalisation
/// of <see cref="RwFlag"/> (an <see cref="RwFlag"/> is a single-element
/// <see cref="RwSet"/>, exactly as <see cref="OrFlag"/> is to <see cref="OrSet"/>).
/// Per element it keeps three grow-only dot lists: add dots
/// (<see cref="Adds"/>), remove dots (<see cref="Removes"/>), and the remove
/// dots an observed add has tombstoned (<see cref="Tombstones"/>). An element
/// is a member if and only if it carries an add dot and no remove dot survives.
/// <para>
/// Concurrent add and remove of the same element converge <strong>remove-wins</strong>:
/// a remove that an add has not observed survives the merge and keeps the
/// element out, so a revoke is never silently resurrected by a concurrent
/// re-add - the natural primitive for membership revocation lists and
/// blocklists. This is the remove-wins counterpart of the add-wins
/// <see cref="OrSet"/>.
/// </para>
/// <para>
/// State-level <see cref="Merge(RwSet, RwSet)"/> is the pointwise union of every
/// replica's add, remove, and tombstone dots per element, making the CRDT
/// commutative, associative, and idempotent under arbitrary delivery order.
/// Element identity is by content (byte equality), encoded internally as a
/// base64 string for serialization stability. Empty arrays are valid elements;
/// <c>null</c> is rejected.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.RwSet)]
public sealed class RwSet : ICrdt<RwSet>
{
    // Below this many already-present dots a linear scan over the small list
    // beats allocating and populating a HashSet for the membership checks. An
    // element carries one dot per concurrent add / remove, overwhelmingly 1-2
    // in practice, so the linear path is the common case; the set is only built
    // once a key genuinely accumulates many concurrent dots. Mirrors
    // OrSet.DotLinearScanThreshold and RwFlag.DotLinearScanThreshold.
    private const int DotLinearScanThreshold = 4;

    // Elements whose base64 encoding fits in this many chars are keyed through
    // a stack buffer; larger elements rent from the shared pool. 256 chars
    // covers elements up to 192 bytes with no allocation.
    private const int MaxStackBase64Chars = 256;

    private static int Base64CharCount(int byteCount) => checked((byteCount + 2) / 3 * 4);

    /// <summary>
    /// Per-element add dots, keyed by the base64 encoding of the element bytes.
    /// An element requires at least one add dot to be a member; add dots are
    /// grow-only and are never cancelled (the remove side gates membership).
    /// </summary>
    [Id(0)]
    public Dictionary<string, List<OrSetDot>> Adds { get; set; }

    /// <summary>
    /// Per-element remove dots, keyed identically to <see cref="Adds"/>. A
    /// remove dot suppresses the element until an add observes it and cancels
    /// it via <see cref="Tombstones"/>.
    /// </summary>
    [Id(1)]
    public Dictionary<string, List<OrSetDot>> Removes { get; set; }

    /// <summary>
    /// Per-element observed-add tombstones: remove dots that an
    /// <see cref="Add(byte[], string, long)"/> has observed and cancelled. A
    /// dot in this map cancels the matching dot in <see cref="Removes"/> on
    /// merge.
    /// </summary>
    [Id(2)]
    public Dictionary<string, List<OrSetDot>> Tombstones { get; set; }

    /// <summary>Creates an empty remove-wins set.</summary>
    public RwSet()
    {
        Adds = [];
        Removes = [];
        Tombstones = [];
    }

    // Direct-assign constructor for the clone fast path: takes ownership of
    // already-built backing stores so the clone allocates no discarded
    // empty-collection shells from field initializers that an object
    // initializer would immediately overwrite.
    private RwSet(
        Dictionary<string, List<OrSetDot>> adds,
        Dictionary<string, List<OrSetDot>> removes,
        Dictionary<string, List<OrSetDot>> tombstones)
    {
        Adds = adds;
        Removes = removes;
        Tombstones = tombstones;
    }

    /// <summary>Returns <c>true</c> when no element is currently a member.</summary>
    public bool IsEmpty
    {
        get
        {
            if (Adds.Count == 0) return true;
            // No element has ever been removed: every add-carrying key is a
            // live member, so the emptiness check skips the per-key remove /
            // tombstone probe entirely. Append-only allow-lists and blocklists
            // that only ever grow stay in this branch for their whole lifetime.
            var noRemoves = Removes.Count == 0;
            foreach (var (key, dots) in Adds)
            {
                if (dots.Count == 0) continue;
                if (noRemoves || !HasLiveRemove(key)) return false;
            }
            return true;
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// An <see cref="RwSet"/> is bottom when no element is present - i.e.
    /// <see cref="IsEmpty"/>. Remove dots and tombstones may still be present
    /// and are preserved for causal-history purposes, but a containing
    /// composite (e.g. <see cref="OrMap{TKey, TValue}"/>) treats the slot as
    /// empty.
    /// </remarks>
    public bool IsBottom => IsEmpty;

    /// <summary>
    /// Adds <paramref name="element"/> with a fresh causal dot and tombstones
    /// every remove dot currently observed for it. Concurrent removes on other
    /// replicas (with dots not in the local <see cref="Removes"/> at the time
    /// of the add) survive a later merge because their dots are not tombstoned
    /// here, so they continue to suppress the element - remove wins.
    /// </summary>
    /// <param name="element">The element bytes to add. Must not be <c>null</c>.</param>
    /// <param name="replicaId">The replica authoring the add. Must be non-empty.</param>
    /// <param name="counter">The replica-local monotonic counter for the add dot.</param>
    public void Add(byte[] element, string replicaId, long counter)
    {
        ArgumentNullException.ThrowIfNull(element);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);

        var charCount = Base64CharCount(element.Length);
        char[]? rented = charCount > MaxStackBase64Chars ? ArrayPool<char>.Shared.Rent(charCount) : null;
        Span<char> buffer = rented ?? stackalloc char[MaxStackBase64Chars];
        try
        {
            Convert.TryToBase64Chars(element, buffer, out var written);
            var key = buffer[..written];
            var adds = Adds.GetAlternateLookup<ReadOnlySpan<char>>();
            if (!adds.TryGetValue(key, out var dots))
            {
                dots = [];
                Adds[new string(key)] = dots;
            }
            dots.Add(new OrSetDot { ReplicaId = replicaId, Counter = counter });

            // Tombstone the remove dots observed for this element so a
            // subsequent membership check sees the element present, unless a
            // concurrent unobserved remove survives.
            var removeLookup = Removes.GetAlternateLookup<ReadOnlySpan<char>>();
            if (!removeLookup.TryGetValue(key, out var removeDots) || removeDots.Count == 0)
            {
                OrSetDotCompaction.CompactMaxPerReplica(dots);
                return;
            }
            var tombLookup = Tombstones.GetAlternateLookup<ReadOnlySpan<char>>();
            if (!tombLookup.TryGetValue(key, out var tomb))
            {
                tomb = [];
                Tombstones[new string(key)] = tomb;
            }
            AddObservedTombstones(tomb, removeDots);
            CompactSlot(dots, removeDots, tomb);
        }
        finally
        {
            if (rented is not null) ArrayPool<char>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Removes <paramref name="element"/> with a fresh causal remove dot. The
    /// remove dominates any concurrent add that has not observed it (remove
    /// wins) and keeps the element out until an add observes and cancels this
    /// dot. Returns <c>true</c> when the element was previously a member.
    /// </summary>
    /// <param name="element">The element bytes to remove. Must not be <c>null</c>.</param>
    /// <param name="replicaId">The replica authoring the remove. Must be non-empty.</param>
    /// <param name="counter">The replica-local monotonic counter for the remove dot.</param>
    public bool Remove(byte[] element, string replicaId, long counter)
    {
        ArgumentNullException.ThrowIfNull(element);
        ArgumentException.ThrowIfNullOrEmpty(replicaId);

        var charCount = Base64CharCount(element.Length);
        char[]? rented = charCount > MaxStackBase64Chars ? ArrayPool<char>.Shared.Rent(charCount) : null;
        Span<char> buffer = rented ?? stackalloc char[MaxStackBase64Chars];
        try
        {
            Convert.TryToBase64Chars(element, buffer, out var written);
            var key = buffer[..written];
            var wasMember = ContainsKey(key);
            var removeLookup = Removes.GetAlternateLookup<ReadOnlySpan<char>>();
            if (!removeLookup.TryGetValue(key, out var removeDots))
            {
                removeDots = [];
                Removes[new string(key)] = removeDots;
            }
            removeDots.Add(new OrSetDot { ReplicaId = replicaId, Counter = counter });
            OrSetDotCompaction.CompactMaxPerReplica(removeDots);
            return wasMember;
        }
        finally
        {
            if (rented is not null) ArrayPool<char>.Shared.Return(rented);
        }
    }

    /// <summary>Returns <c>true</c> when <paramref name="element"/> is currently a member.</summary>
    public bool Contains(byte[] element)
    {
        ArgumentNullException.ThrowIfNull(element);

        var charCount = Base64CharCount(element.Length);
        char[]? rented = charCount > MaxStackBase64Chars ? ArrayPool<char>.Shared.Rent(charCount) : null;
        Span<char> buffer = rented ?? stackalloc char[MaxStackBase64Chars];
        try
        {
            Convert.TryToBase64Chars(element, buffer, out var written);
            return ContainsKey(buffer[..written]);
        }
        finally
        {
            if (rented is not null) ArrayPool<char>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Enumerates the live members of the set in deterministic order. Order is
    /// the ordinal sort of each element's base64 encoding (the internal key
    /// form), which is stable across replicas but is not the same as ordering
    /// by the raw element bytes.
    /// </summary>
    public IEnumerable<byte[]> Elements()
    {
        if (Adds.Count == 0) yield break;

        // No element has ever been removed: every add-carrying key is live, so
        // skip the per-key remove/tombstone probe when collecting survivors.
        var noRemoves = Removes.Count == 0;
        var live = new List<string>(Adds.Count);
        foreach (var (key, dots) in Adds)
        {
            if (dots.Count == 0) continue;
            if (noRemoves || !HasLiveRemove(key)) live.Add(key);
        }
        live.Sort(OrdinalStringOrder.Comparison);
        foreach (var key in live)
        {
            yield return Convert.FromBase64String(key);
        }
    }

    /// <summary>Returns the number of live members.</summary>
    public int Count
    {
        get
        {
            // No element has ever been removed: every add-carrying key is live,
            // so count them without the per-key remove/tombstone probe.
            var noRemoves = Removes.Count == 0;
            var n = 0;
            foreach (var (key, dots) in Adds)
            {
                if (dots.Count == 0) continue;
                if (noRemoves || !HasLiveRemove(key)) n++;
            }
            return n;
        }
    }

    /// <summary>
    /// The eager counterpart to <see cref="Elements"/>: the identical
    /// deterministic projection, materialised once into an exactly-sized array
    /// after a <em>single</em> survivor scan.
    /// <para>
    /// Prefer this over <c>set.Elements().ToArray()</c> or
    /// <c>[.. set.Elements()]</c> on a read path that materialises the whole
    /// set. <see cref="Elements"/> is a <c>yield return</c> iterator, so it
    /// hides its element count from the materialiser: the builder cannot size
    /// the destination, and instead fills a chain of segments and copies the
    /// whole projection once more into the final array - on top of the iterator
    /// state machine itself. Presizing from <see cref="Count"/> instead would be
    /// no better, because <see cref="Count"/> re-runs the very same survivor
    /// scan, so the whole remove/tombstone probe would be paid twice. This form
    /// pays neither: one scan, one exactly-sized destination.
    /// </para>
    /// </summary>
    internal byte[][] SnapshotElements()
    {
        if (Adds.Count == 0) return Array.Empty<byte[]>();

        // No element has ever been removed: every add-carrying key is live, so
        // skip the per-key remove/tombstone probe when collecting survivors.
        var noRemoves = Removes.Count == 0;
        var live = new List<string>(Adds.Count);
        foreach (var (key, dots) in Adds)
        {
            if (dots.Count == 0) continue;
            if (noRemoves || !HasLiveRemove(key)) live.Add(key);
        }
        if (live.Count == 0) return Array.Empty<byte[]>();

        live.Sort(OrdinalStringOrder.Comparison);
        var values = new byte[live.Count][];
        for (var i = 0; i < live.Count; i++)
        {
            values[i] = Convert.FromBase64String(live[i]);
        }
        return values;
    }

    /// <summary>
    /// Lattice merge: pointwise union of <see cref="Adds"/>,
    /// <see cref="Removes"/>, and <see cref="Tombstones"/> per element.
    /// Commutative, associative, idempotent.
    /// </summary>
    public static RwSet Merge(RwSet left, RwSet right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var result = left.Clone();
        result.MergeFrom(right);
        return result;
    }

    /// <summary>
    /// In-place lattice merge: applies the pointwise union of
    /// <paramref name="other"/>'s add, remove, and tombstone dots into this
    /// set. Equivalent to <see cref="Merge(RwSet, RwSet)"/> followed by
    /// replacing the receiver, but avoids the intermediate clone.
    /// </summary>
    public void MergeFrom(RwSet other)
    {
        ArgumentNullException.ThrowIfNull(other);
        MergeMap(Adds, other.Adds);
        MergeMap(Removes, other.Removes);
        MergeMap(Tombstones, other.Tombstones);
        Compact();
    }

    /// <summary>Creates a deep copy of this set.</summary>
    public RwSet Clone()
    {
        // Presize each backing dictionary to its source key count so the
        // entry-by-entry fill never triggers an intermediate rehash grow, and
        // hand the filled dictionaries to the direct-assign constructor so the
        // clone allocates no discarded empty-collection shells.
        var adds = new Dictionary<string, List<OrSetDot>>(Adds.Count);
        foreach (var (key, dots) in Adds) adds[key] = [.. dots];
        var removes = new Dictionary<string, List<OrSetDot>>(Removes.Count);
        foreach (var (key, dots) in Removes) removes[key] = [.. dots];
        var tombstones = new Dictionary<string, List<OrSetDot>>(Tombstones.Count);
        foreach (var (key, dots) in Tombstones) tombstones[key] = [.. dots];
        return new RwSet(adds, removes, tombstones);
    }

    /// <summary>
    /// Folds an <see cref="RwSetDelta"/> into this set: every dot in
    /// <see cref="RwSetDelta.Adds"/> is unioned into <see cref="Adds"/>, every
    /// dot in <see cref="RwSetDelta.Removes"/> into <see cref="Removes"/>, and
    /// every dot in <see cref="RwSetDelta.Tombstones"/> into
    /// <see cref="Tombstones"/>. The merge is commutative, associative, and
    /// idempotent against arrival order and duplicate delivery - applying the
    /// same delta twice yields the same state because the per-(element, dot)
    /// sets are unions.
    /// </summary>
    /// <param name="delta">
    /// The typed CRDT delta authored by the producing call site. Empty
    /// collections are valid; <c>null</c> collections are treated as empty.
    /// </param>
    public void MergeDelta(RwSetDelta delta)
    {
        UnionDeltaDots(Adds, delta.Adds);
        UnionDeltaDots(Removes, delta.Removes);
        UnionDeltaDots(Tombstones, delta.Tombstones);
        Compact();
    }

    private bool ContainsKey(ReadOnlySpan<char> key)
    {
        var adds = Adds.GetAlternateLookup<ReadOnlySpan<char>>();
        if (!adds.TryGetValue(key, out var addDots) || addDots.Count == 0) return false;
        if (Removes.Count == 0) return true;
        var removeLookup = Removes.GetAlternateLookup<ReadOnlySpan<char>>();
        if (!removeLookup.TryGetValue(key, out var removeDots) || removeDots.Count == 0) return true;
        List<OrSetDot>? tomb = null;
        if (Tombstones.Count > 0)
        {
            Tombstones.GetAlternateLookup<ReadOnlySpan<char>>().TryGetValue(key, out tomb);
        }
        return !HasLiveDot(removeDots, tomb);
    }

    /// <summary>
    /// Whether <paramref name="key"/> carries a remove dot no observed-add
    /// tombstone cancels. Every caller consumes a boolean, so the walk stops at
    /// the first survivor rather than counting them all.
    /// </summary>
    private bool HasLiveRemove(string key)
    {
        if (Removes.Count == 0) return false;
        if (!Removes.TryGetValue(key, out var removeDots) || removeDots.Count == 0) return false;
        Tombstones.TryGetValue(key, out var tomb);
        return HasLiveDot(removeDots, tomb);
    }

    private static void AddObservedTombstones(List<OrSetDot> tomb, List<OrSetDot> observed)
    {
        foreach (var dot in observed)
        {
            if (!OrSetDotCompaction.Covers(tomb, in dot)) tomb.Add(dot);
        }
    }

    /// <summary>
    /// Collapses every element's dot history to its bounded normal form: at most
    /// one add, remove, and tombstone dot per replica for each element.
    /// Idempotent, and never changes <see cref="Contains(byte[])"/> or
    /// <see cref="Count"/>.
    /// <para>
    /// Cancelled remove dots are retained for causal history; one dot per replica
    /// on each side already bounds state at O(replicas). Running this on every
    /// mutation and merge makes the fix self-healing: a set written by an older
    /// build collapses the first time any state or delta merges into it.
    /// </para>
    /// <para>
    /// Each map is swept independently. Compaction of one element's dot list
    /// depends on nothing but that list, so grouping an add list with its remove
    /// and tombstone lists bought nothing: it cost two hash probes for every add
    /// key and a third for every remove key - over base64 element keys, so full
    /// string hashes - and then compacted remove and tombstone lists that the
    /// sweeps below compact anyway. This runs on every mutation and merge, so
    /// those probes were paid per element on the replication fold path.
    /// </para>
    /// </summary>
    internal void Compact()
    {
        foreach (var dots in Adds.Values)
        {
            OrSetDotCompaction.CompactMaxPerReplica(dots);
        }

        foreach (var dots in Removes.Values)
        {
            OrSetDotCompaction.CompactMaxPerReplica(dots);
        }

        foreach (var dots in Tombstones.Values)
        {
            OrSetDotCompaction.CompactMaxPerReplica(dots);
        }
    }

    private static void CompactSlot(List<OrSetDot>? adds, List<OrSetDot>? removes, List<OrSetDot>? tomb)
    {
        if (adds is not null) OrSetDotCompaction.CompactMaxPerReplica(adds);
        if (removes is not null) OrSetDotCompaction.CompactMaxPerReplica(removes);
        if (tomb is not null) OrSetDotCompaction.CompactMaxPerReplica(tomb);
    }

    /// <summary>
    /// Whether <paramref name="dots"/> holds at least one dot that
    /// <paramref name="tomb"/> does not cancel. Asking "any" rather than "how
    /// many" is a free early exit for callers that only consume a boolean,
    /// which is all of them here.
    /// </summary>
    private static bool HasLiveDot(List<OrSetDot> dots, List<OrSetDot>? tomb)
        => tomb is null ? dots.Count > 0 : OrSetDotCompaction.AnyLive(dots, tomb);

    private static void MergeMap(Dictionary<string, List<OrSetDot>> target, Dictionary<string, List<OrSetDot>> source)
    {
        foreach (var (key, dots) in source)
        {
            if (!target.TryGetValue(key, out var existing))
            {
                target[key] = [.. dots];
                continue;
            }
            if (dots.Count <= DotLinearScanThreshold)
            {
                // Small incoming dot list (the steady-state delta / replication
                // fold case): at most DotLinearScanThreshold appends, so the
                // linear Contains stays O(existing) and never grows quadratic.
                // Only the incoming side must be small - the previous guard also
                // required the accumulated list to be small, allocating a
                // HashSet over the whole existing list every time a churned key
                // with a long dot history absorbed even a 1-2-dot delta.
                foreach (var d in dots)
                {
                    if (!existing.Contains(d)) existing.Add(d);
                }
                continue;
            }
            var seen = OrSetDotSet.Build(existing, dots.Count);
            foreach (var d in dots)
            {
                if (seen.Add(d)) existing.Add(d);
            }
        }
    }

    /// <summary>
    /// Folds a delta's per-element dots into the accumulated map. Two trims
    /// over the walk that shipped before, both on the replication apply path.
    /// <para>
    /// First, the source is resolved to its backing span where its runtime
    /// shape allows it. A delta's collection is declared
    /// <see cref="IReadOnlyList{T}"/> because it is serialised public surface,
    /// so the previous loop paid an interface call for the indexer <b>and</b>
    /// another for the re-read of <c>Count</c> in the loop condition, on every
    /// dot, with <see cref="OrSetDeltaDot"/> copied whole out of each.
    /// </para>
    /// <para>
    /// Second, the base64 scratch rental is hoisted to the whole walk. The
    /// previous shape opened an exception-handling region <i>per element</i>
    /// and rented and returned a pooled buffer per oversized element, so a
    /// delta carrying n large elements paid n rent/return round trips where
    /// one suffices. The rental grows monotonically and is returned once; the
    /// buffer is only ever read back as <c>buffer[..written]</c>, so content
    /// left by a previous element is never observable.
    /// </para>
    /// </summary>
    private static void UnionDeltaDots(Dictionary<string, List<OrSetDot>> target, IReadOnlyList<OrSetDeltaDot>? source)
    {
        if (source is not { Count: > 0 }) return;
        Span<char> scratch = stackalloc char[MaxStackBase64Chars];
        var lookup = target.GetAlternateLookup<ReadOnlySpan<char>>();
        char[]? rented = null;
        try
        {
            if (CrdtDeltaListSpan.TryGetSpan(source, out var span))
            {
                for (var i = 0; i < span.Length; i++)
                {
                    ref readonly var dot = ref span[i];
                    AddDeltaDot(target, lookup, dot.Element, dot.ReplicaId, dot.Counter, scratch, ref rented);
                }
            }
            else
            {
                var count = source.Count;
                for (var i = 0; i < count; i++)
                {
                    var dot = source[i];
                    AddDeltaDot(target, lookup, dot.Element, dot.ReplicaId, dot.Counter, scratch, ref rented);
                }
            }
        }
        finally
        {
            if (rented is not null) ArrayPool<char>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Keys one delta dot by the base64 encoding of its element and folds it
    /// into the accumulated map, using <paramref name="scratch"/> when the
    /// encoding fits and a caller-owned pooled rental when it does not.
    /// <para>
    /// The rental is threaded by reference rather than taken and returned here
    /// so that a delta carrying n oversized elements pays one rent/return round
    /// trip instead of n, and so that the exception-handling region guarding it
    /// is opened once for the whole walk instead of once per element. It grows
    /// monotonically and the previous rental is returned before it is replaced,
    /// so the pool is never leaked into. Content left by a previous element is
    /// never observable: the buffer is only ever read back as
    /// <c>buffer[..written]</c>.
    /// </para>
    /// </summary>
    private static void AddDeltaDot(
        Dictionary<string, List<OrSetDot>> target,
        Dictionary<string, List<OrSetDot>>.AlternateLookup<ReadOnlySpan<char>> lookup,
        byte[]? element,
        string replicaId,
        long counter,
        Span<char> scratch,
        ref char[]? rented)
    {
        if (element is null) return;
        var charCount = Base64CharCount(element.Length);
        Span<char> buffer;
        if (charCount > scratch.Length)
        {
            if (rented is null || rented.Length < charCount)
            {
                if (rented is not null) ArrayPool<char>.Shared.Return(rented);
                rented = ArrayPool<char>.Shared.Rent(charCount);
            }
            buffer = rented;
        }
        else
        {
            buffer = scratch;
        }
        Convert.TryToBase64Chars(element, buffer, out var written);
        var key = buffer[..written];
        if (!lookup.TryGetValue(key, out var dots))
        {
            dots = [];
            target[new string(key)] = dots;
        }
        var entry = new OrSetDot { ReplicaId = replicaId, Counter = counter };
        if (!dots.Contains(entry)) dots.Add(entry);
    }
}
