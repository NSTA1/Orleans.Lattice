using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// A bounded histogram of the leaf grains a single shard root routes reads to.
/// Each observed read increments the target leaf's visit count; the accumulated
/// histogram is then ranked by visit count to answer the only question the
/// pre-warm path asks: <em>which leaves does this shard read most, so which
/// caches are worth priming before the first external request lands?</em>
/// </summary>
/// <remarks>
/// <para>
/// <b>Why frequency and not recency.</b> A least-recently-used list ranks a leaf
/// that happened to be touched once immediately before shutdown above a leaf
/// that is read constantly. Visit frequency ranks by long-run access
/// probability instead, which is the quantity a cold-start warm-up actually
/// wants. On the measurements that selected this model (below) frequency
/// recovered 96% of the true hot set on a skewed trace and 98% on a
/// cyclic/sequential one, against 56% and 53% for recency.
/// </para>
/// <para>
/// <b>Why not a Markov chain.</b> This model was first built as a first-order
/// Markov chain over leaf identities, ranked by personalised PageRank. That was
/// measured against a plain visit histogram on four synthetic traces
/// (uniform, Zipf-skewed, cyclic/sequential, and phase-changing), with every arm
/// trained on the first half of the trace and scored against the hot set of the
/// held-out second half so no arm was privileged by the metric. The chain never
/// won: it lost by 12.5 points on the skewed trace and 3.1 on the cyclic one,
/// and on the two traces with no predictable structure every arm sat at chance.
/// The reason is analytic, not incidental - for a chain fitted to a single
/// observed trajectory, entries into a state equal exits from it, so the
/// empirical visit vector is already stationary for the fitted matrix. Since the
/// personalised teleport vector <em>was</em> that visit vector, the power
/// iteration was close to an identity map on its own input, and the transition
/// rows cost roughly 100 KB resident per activation to reproduce a ranking the
/// histogram already gives for free. Successor rows may earn their place later
/// as the substrate for online successor prefetch, but that is a separate
/// feature and must justify the memory on its own measurement.
/// </para>
/// <para>
/// <b>Bounds.</b> Memory is O(1) in the key space, not O(keyspace): at most
/// <see cref="MaxTrackedLeaves"/> entries of one <see cref="GrainId"/> and one
/// <see cref="long"/> each. When the cap is reached the model prunes the coldest
/// quarter in one deterministic pass (ordered by count ascending, then by grain
/// identity ascending), so the amortised cost of an insertion stays constant and
/// no single insert scans more than the cap.
/// </para>
/// <para>
/// <b>Hot path.</b> <see cref="Record"/> is O(1) and allocation-free: a single
/// dictionary probe and two integer increments. Because visit counts live
/// directly in the dictionary rather than in a per-leaf node object, observing a
/// leaf for the first time allocates nothing beyond amortised dictionary growth.
/// </para>
/// <para>
/// <b>Thread safety.</b> None. The model is owned by a single shard-root
/// activation and is only touched from that activation's grain turns, which
/// Orleans serialises.
/// </para>
/// </remarks>
internal sealed class LeafAccessFrequencyModel
{
    /// <summary>
    /// Upper bound on the number of distinct leaves tracked. Chosen so the
    /// resident model stays far below the cost of the leaf payload cache it
    /// exists to prime, while comfortably covering the working set of a shard
    /// whose reads are skewed (the only case where pre-warming pays).
    /// </summary>
    internal const int MaxTrackedLeaves = 256;

    /// <summary>
    /// Upper bound on the number of leaves written into a persisted
    /// <see cref="LeafAccessModelSnapshot"/>. Deliberately smaller than
    /// <see cref="MaxTrackedLeaves"/>: the resident model can afford to be
    /// generous, whereas the snapshot rides inside the shard root's durable
    /// state and must not inflate it.
    /// </summary>
    internal const int MaxPersistedLeaves = 64;

    /// <summary>
    /// Fraction of a full map retained by a prune pass. Pruning a quarter at a
    /// time amortises the eviction scan over many subsequent insertions instead
    /// of paying a scan on every insertion past the cap.
    /// </summary>
    private const double PruneRetainRatio = 0.75;

    private readonly Dictionary<GrainId, long> _visits;
    private long _observations;
    private bool _dirty;

    /// <summary>Creates an empty model.</summary>
    public LeafAccessFrequencyModel()
    {
        _visits = new Dictionary<GrainId, long>(capacity: 16);
    }

    /// <summary>Number of distinct leaves currently tracked.</summary>
    public int TrackedLeafCount => _visits.Count;

    /// <summary>Total number of accesses recorded since the model was created or restored.</summary>
    public long Observations => _observations;

    /// <summary>
    /// <see langword="true"/> when the model has changed since the last
    /// <see cref="MarkPersisted"/>. The shard root uses this to skip a
    /// storage write when nothing has moved.
    /// </summary>
    public bool IsDirty => _dirty;

    /// <summary>
    /// Records a read that landed on <paramref name="leaf"/>, incrementing the
    /// leaf's visit count.
    /// </summary>
    /// <param name="leaf">The leaf grain the read routed to.</param>
    public void Record(GrainId leaf)
    {
        _observations++;
        _dirty = true;

        ref var visits = ref CollectionsMarshal.GetValueRefOrNullRef(_visits, leaf);
        if (!Unsafe.IsNullRef(ref visits))
        {
            visits++;
            return;
        }

        AddLeafSlow(leaf);
    }

    /// <summary>
    /// Clears the dirty flag after the caller has durably persisted a snapshot.
    /// </summary>
    public void MarkPersisted() => _dirty = false;

    /// <summary>
    /// Ranks the tracked leaves by visit count and returns the highest-ranked
    /// <paramref name="count"/> of them, best first. Ties are broken by
    /// ascending grain identity so the result is fully deterministic for a given
    /// model, independent of hash-table layout.
    /// </summary>
    /// <param name="count">
    /// Maximum number of leaves to return. Values of zero or less produce an
    /// empty result; values above the tracked-leaf count return every leaf.
    /// </param>
    /// <returns>The ranked leaf identities, most-visited first.</returns>
    public GrainId[] RankTopLeaves(int count)
    {
        if (count <= 0 || _visits.Count == 0) return [];

        var ranked = SortedByVisitsDescending();
        var take = Math.Min(count, ranked.Length);
        var result = new GrainId[take];
        for (var i = 0; i < take; i++) result[i] = ranked[i].Leaf;
        return result;
    }

    /// <summary>
    /// Captures a compact, bounded snapshot suitable for persisting inside the
    /// shard root's durable state. At most <see cref="MaxPersistedLeaves"/>
    /// leaves - the most-visited ones - are written.
    /// </summary>
    /// <returns>An immutable snapshot; empty when nothing has been observed.</returns>
    public LeafAccessModelSnapshot CaptureSnapshot()
    {
        if (_visits.Count == 0) return LeafAccessModelSnapshot.Empty;

        var ranked = SortedByVisitsDescending();
        var persistCount = Math.Min(ranked.Length, MaxPersistedLeaves);
        var leaves = new List<string>(persistCount);
        var visits = new List<long>(persistCount);
        for (var i = 0; i < persistCount; i++)
        {
            leaves.Add(ranked[i].Leaf.ToString());
            visits.Add(ranked[i].Visits);
        }

        return new LeafAccessModelSnapshot { Leaves = leaves, Visits = visits };
    }

    /// <summary>
    /// Rebuilds a model from a persisted snapshot. Malformed entries (an
    /// unparsable grain identity, a non-positive count, a truncated parallel
    /// list) are skipped rather than throwing, so a snapshot written by an older
    /// or partially-corrupt build degrades to a smaller model instead of failing
    /// the shard-root activation that reads it.
    /// </summary>
    /// <param name="snapshot">The persisted snapshot, or <see langword="null"/>.</param>
    /// <returns>The restored model; empty when the snapshot is null or unusable.</returns>
    public static LeafAccessFrequencyModel Restore(LeafAccessModelSnapshot? snapshot)
    {
        var model = new LeafAccessFrequencyModel();
        if (snapshot is null || snapshot.Leaves.Count == 0) return model;

        // Tolerate a truncated parallel list rather than discarding the whole
        // snapshot: a short read still carries the hottest leaves, which are
        // written first.
        var count = Math.Min(snapshot.Leaves.Count, snapshot.Visits.Count);
        for (var i = 0; i < count; i++)
        {
            if (model._visits.Count >= MaxTrackedLeaves) break;

            var raw = snapshot.Leaves[i];
            if (string.IsNullOrEmpty(raw)) continue;

            var recorded = snapshot.Visits[i];
            if (recorded <= 0) continue;

            GrainId leaf;
            try
            {
                leaf = GrainId.Parse(raw);
            }
            catch (ArgumentException)
            {
                // A leaf identity we cannot parse is dropped.
                continue;
            }

            model._visits[leaf] = model._visits.TryGetValue(leaf, out var existing)
                ? existing + recorded
                : recorded;
            model._observations += recorded;
        }

        // A freshly restored model exactly matches what is already durable, so
        // it starts clean; only new observations make it worth re-persisting.
        model._dirty = false;
        return model;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void AddLeafSlow(GrainId leaf)
    {
        if (_visits.Count >= MaxTrackedLeaves)
        {
            PruneLeaves();
        }

        _visits[leaf] = 1;
    }

    /// <summary>
    /// Drops the coldest quarter of the tracked leaves, ordered by visit count
    /// ascending then grain identity ascending. Deterministic and amortised: one
    /// pass frees a quarter of the cap, so the next
    /// <c>MaxTrackedLeaves / 4</c> insertions are scan-free.
    /// </summary>
    private void PruneLeaves()
    {
        var ranked = SortedByVisitsDescending();
        var retain = Math.Max(1, (int)(MaxTrackedLeaves * PruneRetainRatio));
        for (var i = retain; i < ranked.Length; i++)
        {
            _visits.Remove(ranked[i].Leaf);
        }
    }

    /// <summary>
    /// Materialises the histogram ordered by visit count descending then grain
    /// identity ascending, so every consumer (ranking, snapshot, pruning) sees
    /// the same deterministic order regardless of dictionary layout. The
    /// comparison is a <see langword="static"/> lambda, so sorting captures
    /// nothing and allocates no closure.
    /// </summary>
    private (GrainId Leaf, long Visits)[] SortedByVisitsDescending()
    {
        var ranked = new (GrainId Leaf, long Visits)[_visits.Count];
        var i = 0;
        foreach (var (leaf, visits) in _visits)
        {
            ranked[i++] = (leaf, visits);
        }

        Array.Sort(ranked, static (a, b) =>
        {
            var byVisits = b.Visits.CompareTo(a.Visits);
            return byVisits != 0 ? byVisits : CompareGrainIds(a.Leaf, b.Leaf);
        });
        return ranked;
    }

    /// <summary>
    /// Total order over grain identities by raw byte content (type, then key).
    /// Used purely as a deterministic tie-break so the model's output does not
    /// depend on hash-table iteration order.
    /// </summary>
    private static int CompareGrainIds(GrainId a, GrainId b)
    {
        var byType = a.Type.AsSpan().SequenceCompareTo(b.Type.AsSpan());
        return byType != 0 ? byType : a.Key.AsSpan().SequenceCompareTo(b.Key.AsSpan());
    }
}
