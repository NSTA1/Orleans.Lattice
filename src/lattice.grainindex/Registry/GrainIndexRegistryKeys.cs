namespace Orleans.Lattice.GrainIndex.Registry;

/// <summary>
/// The key layout of the grain-index registry tree.
/// <para>
/// One tree holds every kind of per-index bookkeeping, so each kind takes a
/// distinct leading segment and each key sorts underneath it. That keeps a scan
/// of one kind a contiguous range read - the whole reason the registry is a tree
/// rather than a grain - and leaves room for the kinds added by later work
/// without re-keying what is already stored.
/// </para>
/// </summary>
/// <remarks>
/// <para>
/// The segments in use are:
/// </para>
/// <list type="bullet">
/// <item>
/// <description>
/// <c>def/{indexName}</c> - the persisted definition and its fingerprint, one
/// entry per index. Written by the startup reconciler.
/// </description>
/// </item>
/// <item>
/// <description>
/// <c>seen/{indexName}/{encodedGrainKey}</c> - the activation-path marker
/// recording that an indexed grain has already been projected, so an activation
/// does not re-project a grain the backfill has covered. Reserved here so the
/// layout is fixed before the first writer exists.
/// </description>
/// </item>
/// <item>
/// <description>
/// <c>ckpt/{indexName}</c> - the backfill resume checkpoint, so a rebuild that
/// is interrupted restarts where it stopped rather than from the beginning.
/// Reserved on the same terms.
/// </description>
/// </item>
/// <item>
/// <description>
/// <c>pend/{indexName}/{encodedGrainKey}</c> - the durable pending-projection
/// outbox entry recording that a grain's index write was intended but is not
/// yet known to have landed. Written before the index write and removed with
/// the seen marker in the same atomic batch once it has.
/// </description>
/// </item>
/// </list>
/// <para>
/// The per-kind prefixes are distinct strings rather than a shared separator
/// scheme, so no key of one kind can ever prefix-match a scan of another. Within
/// a kind, a range scan for one index runs from
/// <see cref="SeenPrefix(string)"/> to <see cref="SeenPrefixEnd(string)"/>; that
/// pairing assumes an index name contains no <c>/</c>, which is also what keeps
/// an index's backing tree name a single segment under
/// <see cref="GrainIndexTreeNames.ReservedPrefix"/>.
/// </para>
/// </remarks>
internal static class GrainIndexRegistryKeys
{
    /// <summary>The leading segment of every persisted-definition key.</summary>
    internal const string DefinitionSegment = "def/";

    /// <summary>The leading segment of every activation-path seen-marker key.</summary>
    internal const string SeenSegment = "seen/";

    /// <summary>The leading segment of every backfill-checkpoint key.</summary>
    internal const string CheckpointSegment = "ckpt/";

    /// <summary>The leading segment of every pending-projection outbox key.</summary>
    internal const string PendingSegment = "pend/";

    /// <summary>
    /// The registry key holding the persisted definition and fingerprint of the
    /// index called <paramref name="indexName"/>.
    /// </summary>
    /// <param name="indexName">The logical index name. Must not be <c>null</c>.</param>
    /// <returns>The definition key.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    internal static string Definition(string indexName)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        return DefinitionSegment + indexName;
    }

    /// <summary>
    /// The inclusive lower bound of a range scan over every persisted
    /// definition.
    /// </summary>
    internal static string DefinitionPrefix() => DefinitionSegment;

    /// <summary>
    /// The exclusive upper bound of a range scan over every persisted
    /// definition.
    /// </summary>
    internal static string DefinitionPrefixEnd() => ExclusiveEnd(DefinitionSegment);

    /// <summary>
    /// The registry key marking that the grain whose encoded key is
    /// <paramref name="encodedGrainKey"/> has already been projected into the
    /// index called <paramref name="indexName"/>.
    /// </summary>
    /// <param name="indexName">The logical index name. Must not be <c>null</c>.</param>
    /// <param name="encodedGrainKey">The indexed grain's encoded key. Must not be <c>null</c>.</param>
    /// <returns>The seen-marker key.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    internal static string Seen(string indexName, string encodedGrainKey)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(encodedGrainKey);
        return string.Concat(SeenSegment, indexName, "/", encodedGrainKey);
    }

    /// <summary>
    /// The inclusive lower bound of a range scan over one index's seen markers.
    /// </summary>
    /// <param name="indexName">The logical index name. Must not be <c>null</c>.</param>
    /// <returns>The scan's start key.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    internal static string SeenPrefix(string indexName)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        return string.Concat(SeenSegment, indexName, "/");
    }

    /// <summary>
    /// The exclusive upper bound matching <see cref="SeenPrefix(string)"/>.
    /// </summary>
    /// <param name="indexName">The logical index name. Must not be <c>null</c>.</param>
    /// <returns>The scan's end key.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    internal static string SeenPrefixEnd(string indexName) => ExclusiveEnd(SeenPrefix(indexName));

    /// <summary>
    /// The registry key holding the backfill resume checkpoint of the index
    /// called <paramref name="indexName"/>.
    /// </summary>
    /// <param name="indexName">The logical index name. Must not be <c>null</c>.</param>
    /// <returns>The checkpoint key.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    internal static string Checkpoint(string indexName)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        return CheckpointSegment + indexName;
    }

    /// <summary>
    /// The registry key holding the pending-projection outbox entry for the
    /// grain whose encoded key is <paramref name="encodedGrainKey"/> in the
    /// index called <paramref name="indexName"/>.
    /// </summary>
    /// <remarks>
    /// The outbox mirrors the seen marker's shape, one entry per grain per
    /// index, so a grain that writes again before its previous index write
    /// landed replaces its own outstanding entry rather than queueing a second
    /// one. That is what keeps the outbox bounded by the number of grains with
    /// an unfinished write rather than by the write rate.
    /// </remarks>
    /// <param name="indexName">The logical index name. Must not be <c>null</c>.</param>
    /// <param name="encodedGrainKey">The indexed grain's encoded key. Must not be <c>null</c>.</param>
    /// <returns>The pending-projection key.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    internal static string Pending(string indexName, string encodedGrainKey)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(encodedGrainKey);
        return string.Concat(PendingSegment, indexName, "/", encodedGrainKey);
    }

    /// <summary>
    /// The inclusive lower bound of a range scan over every index's pending
    /// projections, which is how the outbox drain finds its work in one pass.
    /// </summary>
    internal static string PendingPrefix() => PendingSegment;

    /// <summary>
    /// The exclusive upper bound matching <see cref="PendingPrefix()"/>.
    /// </summary>
    internal static string PendingPrefixEnd() => ExclusiveEnd(PendingSegment);

    /// <summary>
    /// The inclusive lower bound of a range scan over one index's pending
    /// projections.
    /// </summary>
    /// <param name="indexName">The logical index name. Must not be <c>null</c>.</param>
    /// <returns>The scan's start key.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    internal static string PendingPrefix(string indexName)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        return string.Concat(PendingSegment, indexName, "/");
    }

    /// <summary>
    /// The exclusive upper bound matching <see cref="PendingPrefix(string)"/>.
    /// </summary>
    /// <param name="indexName">The logical index name. Must not be <c>null</c>.</param>
    /// <returns>The scan's end key.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    internal static string PendingPrefixEnd(string indexName) => ExclusiveEnd(PendingPrefix(indexName));

    /// <summary>
    /// The exclusive upper bound of the half-open range that covers every key
    /// starting with <paramref name="prefix"/>, produced by incrementing its
    /// final character.
    /// </summary>
    private static string ExclusiveEnd(string prefix)
    {
        // Every prefix here ends in '/' or a letter, none of which is the
        // maximum char, so incrementing the last character is a valid exclusive
        // bound over ordinal string ordering.
        var last = prefix[^1];
        return string.Concat(prefix.AsSpan(0, prefix.Length - 1), stackalloc char[] { (char)(last + 1) });
    }
}
