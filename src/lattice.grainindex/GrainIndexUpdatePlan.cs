namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The reconciliation between the index entries a grain used to contribute and
/// the ones it contributes now: the entries to write, the stale keys to
/// tombstone, and the projection that becomes the next diff's baseline.
/// </summary>
/// <remarks>
/// <para>
/// A plan is applied as one all-or-nothing batch, so a grain whose property
/// value moved never has both its old and its new entry visible at once, and
/// never has neither. That is the whole point of tombstoning: without it a
/// grain that turns 18 would keep answering a scan for <c>Age == 17</c>
/// forever.
/// </para>
/// <para>
/// An unchanged re-projection yields an <see cref="IsEmpty"/> plan, which the
/// maintainer applies by doing nothing at all - no batch, no round trip.
/// </para>
/// </remarks>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.GrainIndexUpdatePlan)]
public sealed class GrainIndexUpdatePlan
{
    private static readonly KeyValuePair<string, byte[]>[] NoUpserts = [];
    private static readonly string[] NoDeletes = [];

    [NonSerialized]
    private List<KeyValuePair<string, byte[]>>? _upsertList;

    /// <summary>Initialises a plan.</summary>
    /// <param name="projection">The projection this plan brings the tree in line with. Must not be <c>null</c>.</param>
    /// <param name="upserts">The entries to write. Must not be <c>null</c>.</param>
    /// <param name="deletes">The stale keys to tombstone. Must not be <c>null</c>.</param>
    /// <param name="entryDelta">
    /// The net change this plan makes to the number of entries the index holds.
    /// Defaults to the upsert-minus-tombstone count, which is exact unless an
    /// upsert rewrites an entry whose key did not move; the
    /// <see cref="Between"/> and <see cref="Removing"/> factories always supply
    /// the exact value, so a plan built by the projection path never relies on
    /// that approximation.
    /// </param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexUpdatePlan(
        GrainIndexProjection projection,
        IReadOnlyList<KeyValuePair<string, byte[]>> upserts,
        IReadOnlyList<string> deletes,
        int? entryDelta = null)
    {
        ArgumentNullException.ThrowIfNull(projection);
        ArgumentNullException.ThrowIfNull(upserts);
        ArgumentNullException.ThrowIfNull(deletes);
        Projection = projection;
        Upserts = upserts;
        Deletes = deletes;
        EntryDelta = entryDelta ?? upserts.Count - deletes.Count;
    }

    /// <summary>
    /// The projection the tree holds once this plan is applied. Persist it as
    /// the baseline for the next diff.
    /// </summary>
    [Id(0)] public GrainIndexProjection Projection { get; }

    /// <summary>The entries to write, keyed by their on-tree key.</summary>
    [Id(1)] public IReadOnlyList<KeyValuePair<string, byte[]>> Upserts { get; }

    /// <summary>
    /// The keys to tombstone: entries this grain used to contribute whose
    /// property value has since moved, so their key no longer exists in the
    /// current projection.
    /// </summary>
    [Id(2)] public IReadOnlyList<string> Deletes { get; }

    /// <summary>
    /// The net change this plan makes to the number of entries the index holds:
    /// positive when the grain contributes more entries than it did, negative
    /// when it contributes fewer, and zero when a value merely moved.
    /// </summary>
    /// <remarks>
    /// It cannot be derived from <see cref="Upserts"/> and
    /// <see cref="Deletes"/> alone, because an unordered property's entry keeps
    /// a stable key when its value changes: that is one upsert, no tombstone,
    /// and no new entry. The diff knows both projections and so records the
    /// exact figure here, which is what lets the entry-count instrument stay
    /// accurate instead of drifting upwards on in-place rewrites.
    /// </remarks>
    [Id(3)] public int EntryDelta { get; }

    /// <summary>
    /// <c>true</c> when the projection is unchanged, so applying the plan is a
    /// no-op.
    /// </summary>
    public bool IsEmpty => Upserts.Count == 0 && Deletes.Count == 0;

    /// <summary>
    /// Reconciles <paramref name="current"/> against <paramref name="previous"/>:
    /// every entry that is new or whose payload changed becomes an upsert, and
    /// every key the grain used to contribute but no longer does becomes a
    /// tombstone.
    /// </summary>
    /// <param name="previous">
    /// The projection last written for this grain. Pass
    /// <see cref="GrainIndexProjection.Empty(string)"/> for a grain that has
    /// never been indexed. Must not be <c>null</c>.
    /// </param>
    /// <param name="current">The projection of the grain's current state. Must not be <c>null</c>.</param>
    /// <returns>The plan that moves the tree from <paramref name="previous"/> to <paramref name="current"/>.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public static GrainIndexUpdatePlan Between(GrainIndexProjection previous, GrainIndexProjection current)
    {
        ArgumentNullException.ThrowIfNull(previous);
        ArgumentNullException.ThrowIfNull(current);

        var before = previous.Entries;
        var after = current.Entries;

        List<KeyValuePair<string, byte[]>>? upserts = null;
        List<string>? deletes = null;

        // Both projections come from the same definition, so entry i describes
        // the same property in each. The index-aligned probe therefore settles
        // the common case in one comparison and the linear fallback only runs
        // when the property set itself changed. Neither path allocates a lookup
        // structure: an index projects a handful of properties, so a set would
        // cost more than the scan it saves.
        for (var i = 0; i < after.Count; i++)
        {
            var entry = after[i];
            int match = i < before.Count && KeysEqual(before[i].Key, entry.Key)
                ? i
                : IndexOfKey(before, entry.Key);

            if (match < 0 || !before[match].Equals(entry))
                (upserts ??= new List<KeyValuePair<string, byte[]>>(after.Count)).Add(new(entry.Key, entry.Value));
        }

        for (var i = 0; i < before.Count; i++)
        {
            var key = before[i].Key;
            if (i < after.Count && KeysEqual(after[i].Key, key))
                continue;

            if (IndexOfKey(after, key) < 0)
                (deletes ??= new List<string>(before.Count)).Add(key);
        }

        var plan = new GrainIndexUpdatePlan(
            current,
            (IReadOnlyList<KeyValuePair<string, byte[]>>?)upserts ?? NoUpserts,
            (IReadOnlyList<string>?)deletes ?? NoDeletes,
            after.Count - before.Count);
        plan._upsertList = upserts;
        return plan;
    }

    /// <summary>
    /// The plan that withdraws a grain from the index entirely - every entry it
    /// contributed is tombstoned and nothing is written. Use it when the grain
    /// is deleted or its state cleared.
    /// </summary>
    /// <param name="previous">The projection last written for the grain. Must not be <c>null</c>.</param>
    /// <returns>A delete-only plan whose <see cref="Projection"/> is empty.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="previous"/> is <c>null</c>.</exception>
    public static GrainIndexUpdatePlan Removing(GrainIndexProjection previous)
    {
        ArgumentNullException.ThrowIfNull(previous);

        var before = previous.Entries;
        if (before.Count == 0)
            return new GrainIndexUpdatePlan(GrainIndexProjection.Empty(previous.GrainKey), NoUpserts, NoDeletes, 0);

        var deletes = new string[before.Count];
        for (var i = 0; i < before.Count; i++)
            deletes[i] = before[i].Key;

        return new GrainIndexUpdatePlan(
            GrainIndexProjection.Empty(previous.GrainKey),
            NoUpserts,
            deletes,
            -before.Count);
    }

    /// <summary>
    /// The upserts in the exact shape the atomic bulk-write seam takes, reusing
    /// the list the diff already built so applying a plan copies nothing. A
    /// plan that arrived over the wire materialises the list once, on first use.
    /// </summary>
    internal List<KeyValuePair<string, byte[]>> UpsertList =>
        _upsertList ??= Upserts as List<KeyValuePair<string, byte[]>>
            ?? new List<KeyValuePair<string, byte[]>>(Upserts);

    private static bool KeysEqual(string left, string right) =>
        string.Equals(left, right, StringComparison.Ordinal);

    private static int IndexOfKey(IReadOnlyList<GrainIndexEntry> entries, string key)
    {
        for (var i = 0; i < entries.Count; i++)
        {
            if (KeysEqual(entries[i].Key, key))
                return i;
        }

        return -1;
    }
}
