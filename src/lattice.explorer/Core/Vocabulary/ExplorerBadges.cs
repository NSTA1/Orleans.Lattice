using Orleans.Lattice.Explorer.Core.Catalog;

namespace Orleans.Lattice.Explorer.Core.Vocabulary;

/// <summary>
/// Builds the catalog's badges, expanded and explained, from the single
/// glossary.
/// </summary>
/// <remarks>
/// <para>
/// A badge is built, not written: the wording comes from
/// <see cref="ExplorerGlossary"/>, so the badge, its help disclosure and any
/// heading that names the same concept cannot drift apart.
/// </para>
/// <para>
/// Badges are rendered per catalog item, so this is a hot path and nothing here
/// allocates on it. The badges that carry no runtime value are built once as
/// statics. The two counting badges are pre-built for every count from zero to
/// <see cref="CachedCountLimit"/>, which covers every shard count and almost
/// every dead-letter count a catalog list shows, so the common case is a bounds
/// check and an array index. Only a count past that limit, or a badge embedding
/// a runtime string, composes anything - and then once per badge built, never
/// per render.
/// </para>
/// </remarks>
public static class ExplorerBadges
{
    /// <summary>
    /// The largest number of badges any one catalog item can produce, and
    /// therefore the smallest buffer
    /// <see cref="ForCatalogItem(CatalogItem, int, Span{ExplorerBadge})"/>
    /// accepts. A view is the widest case: aggregation, history, source tree,
    /// projection provider and projection version.
    /// </summary>
    public const int MaxCatalogBadges = 5;

    /// <summary>
    /// The highest count whose badge is pre-built. A count above it composes its
    /// text when the badge is built.
    /// </summary>
    public const int CachedCountLimit = 64;

    private const string ShardAbbreviation = "sh";
    private const string DeadLetterAbbreviation = "DLQ";
    private const string LifecyclePrefix = "Tree lifecycle: ";

    private static readonly ExplorerBadge[] CachedShardCounts = BuildShardCountCache();
    private static readonly ExplorerBadge[] CachedDeadLetterCounts = BuildDeadLetterCountCache();

    /// <summary>The badge for a tree that is live and serving.</summary>
    public static ExplorerBadge Active { get; } = Lifecycle(ExplorerTermIds.LifecycleActive, "active");

    /// <summary>The badge for a tree inside its post-deletion retention window.</summary>
    public static ExplorerBadge SoftDeleted { get; } = Lifecycle(ExplorerTermIds.LifecycleSoftDeleted, "soft-deleted");

    /// <summary>The badge for a tree whose data is being physically removed.</summary>
    public static ExplorerBadge Purging { get; } = Lifecycle(ExplorerTermIds.LifecyclePurging, "purging");

    /// <summary>
    /// The badge marking a grouped-reduce view. Replaces the bare <c>agg</c>,
    /// which is kept only as <see cref="ExplorerBadge.ShortText"/>.
    /// </summary>
    public static ExplorerBadge Aggregation { get; } = new()
    {
        TermId = ExplorerTermIds.AggregationView,
        Label = ExplorerGlossary.Get(ExplorerTermIds.AggregationView).Label,
        Text = "Aggregation",
        ShortText = "agg",
        Expansion = "Aggregation view",
    };

    /// <summary>
    /// The badge marking a change-history view. Replaces the bare
    /// <c>history</c>, which named the surface rather than the kind of view.
    /// </summary>
    public static ExplorerBadge History { get; } = new()
    {
        TermId = ExplorerTermIds.HistoryView,
        Label = ExplorerGlossary.Get(ExplorerTermIds.HistoryView).Label,
        Text = "History",
        ShortText = "history",
        Expansion = "Change-history view",
    };

    /// <summary>
    /// The badge marking a tag index. Replaces the bare <c>tag</c>, which is
    /// kept only as <see cref="ExplorerBadge.ShortText"/>.
    /// </summary>
    public static ExplorerBadge TagIndex { get; } = new()
    {
        TermId = ExplorerTermIds.TagIndexes,
        Label = ExplorerGlossary.Get(ExplorerTermIds.TagIndexes).Label,
        Text = "Tag index",
        ShortText = "tag",
        Expansion = "Tag index",
    };

    /// <summary>
    /// The shard-count badge: <c>64 shards</c>, abbreviating to <c>64 sh</c>.
    /// </summary>
    /// <param name="count">The number of shards. Zero is allowed and reads as <c>0 shards</c>.</param>
    /// <returns>The badge.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
    public static ExplorerBadge ShardCount(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);

        return count <= CachedCountLimit ? CachedShardCounts[count] : BuildShardCount(count);
    }

    /// <summary>
    /// The dead-letter-count badge: <c>3 dead-lettered</c>, abbreviating to
    /// <c>3 DLQ</c> and expanding to <c>3 dead-lettered writes</c>.
    /// </summary>
    /// <param name="count">The number of dead-lettered writes. Zero is allowed; the catalog simply does not render a zero badge.</param>
    /// <returns>The badge.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
    public static ExplorerBadge DeadLetterCount(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);

        return count <= CachedCountLimit ? CachedDeadLetterCounts[count] : BuildDeadLetterCount(count);
    }

    /// <summary>The badge naming the tree a view projects from.</summary>
    /// <param name="sourceTreeId">The source tree's id.</param>
    /// <returns>The badge.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sourceTreeId"/> is null.</exception>
    public static ExplorerBadge SourceTree(string sourceTreeId)
    {
        ArgumentNullException.ThrowIfNull(sourceTreeId);
        return Value(ExplorerTermIds.SourceTree, sourceTreeId);
    }

    /// <summary>The badge naming the host-registered provider that computes a view.</summary>
    /// <param name="providerKey">The provider key.</param>
    /// <returns>The badge.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="providerKey"/> is null.</exception>
    public static ExplorerBadge ProjectionProvider(string providerKey)
    {
        ArgumentNullException.ThrowIfNull(providerKey);
        return Value(ExplorerTermIds.ProjectionProvider, providerKey);
    }

    /// <summary>The badge naming the projection version a view's rows were built with.</summary>
    /// <param name="version">The projection version.</param>
    /// <returns>The badge.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="version"/> is null.</exception>
    public static ExplorerBadge ProjectionVersion(string version)
    {
        ArgumentNullException.ThrowIfNull(version);
        return Value(ExplorerTermIds.ProjectionVersion, version);
    }

    /// <summary>
    /// The badge for a tree lifecycle state as the cluster spells it
    /// (<c>Active</c>, <c>SoftDeleted</c>, <c>Purging</c>), matched without
    /// regard to case.
    /// </summary>
    /// <param name="lifecycle">The lifecycle value, or <see langword="null"/>.</param>
    /// <returns>The badge, or <see langword="null"/> for an absent or unrecognised value.</returns>
    public static ExplorerBadge? ForLifecycle(string? lifecycle)
    {
        var term = ExplorerGlossary.ForLifecycle(lifecycle);
        if (term is null)
        {
            return null;
        }

        if (string.Equals(term.Id, ExplorerTermIds.LifecycleActive, StringComparison.Ordinal))
        {
            return Active;
        }

        return string.Equals(term.Id, ExplorerTermIds.LifecycleSoftDeleted, StringComparison.Ordinal)
            ? SoftDeleted
            : Purging;
    }

    /// <summary>
    /// Fills <paramref name="destination"/> with the badges a catalog item
    /// shows, in render order, and returns how many were written.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The buffer is the caller's, so a list can hold one
    /// <c>ExplorerBadge[<see cref="MaxCatalogBadges"/>]</c> for its lifetime and
    /// render every row through it without allocating.
    /// </para>
    /// <para>
    /// This is display code driven by values that arrived over the wire, so it
    /// never throws over a value it merely cannot render: a non-positive shard
    /// or dead-letter count, or an unrecognised lifecycle, contributes no badge.
    /// The individual factories are stricter, because there a bad argument is a
    /// programming error rather than a cluster's report.
    /// </para>
    /// </remarks>
    /// <param name="item">The catalog item.</param>
    /// <param name="deadLetterCount">The item's dead-letter count; zero or less renders no badge.</param>
    /// <param name="destination">A buffer of at least <see cref="MaxCatalogBadges"/> slots.</param>
    /// <returns>The number of badges written to <paramref name="destination"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="item"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="destination"/> holds fewer than <see cref="MaxCatalogBadges"/> slots.</exception>
    public static int ForCatalogItem(CatalogItem item, int deadLetterCount, Span<ExplorerBadge> destination)
    {
        ArgumentNullException.ThrowIfNull(item);

        if (destination.Length < MaxCatalogBadges)
        {
            throw new ArgumentException(
                "A catalog item can produce up to " + MaxCatalogBadges + " badges, so the destination must hold at least that many.",
                nameof(destination));
        }

        var written = 0;

        switch (item.Kind)
        {
            case CatalogKind.Trees:
                if (ForLifecycle(item.Lifecycle) is { } lifecycle)
                {
                    destination[written++] = lifecycle;
                }

                if (item.ShardCount is int shards and > 0)
                {
                    destination[written++] = ShardCount(shards);
                }

                if (deadLetterCount > 0)
                {
                    destination[written++] = DeadLetterCount(deadLetterCount);
                }

                break;

            case CatalogKind.TagIndexes:
                destination[written++] = TagIndex;

                if (item.ShardCount is int tagShards and > 0)
                {
                    destination[written++] = ShardCount(tagShards);
                }

                break;

            default:
                if (item.IsAggregation)
                {
                    destination[written++] = Aggregation;
                }

                if (item.IsHistory)
                {
                    destination[written++] = History;
                }

                if (item.SourceTreeId is not null)
                {
                    destination[written++] = SourceTree(item.SourceTreeId);
                }

                if (item.ProjectionProviderKey is not null)
                {
                    destination[written++] = ProjectionProvider(item.ProjectionProviderKey);
                }

                if (item.ProjectionVersion is not null)
                {
                    destination[written++] = ProjectionVersion(item.ProjectionVersion);
                }

                break;
        }

        return written;
    }

    private static ExplorerBadge Lifecycle(string termId, string shortText)
    {
        var term = ExplorerGlossary.Get(termId);
        return new ExplorerBadge
        {
            TermId = termId,
            Label = term.Label,
            Text = term.Label,
            ShortText = shortText,
            Expansion = LifecyclePrefix + shortText,
        };
    }

    private static ExplorerBadge Value(string termId, string value)
    {
        var term = ExplorerGlossary.Get(termId);
        return new ExplorerBadge
        {
            TermId = termId,
            Label = term.Label,
            Text = value,
            ShortText = value,
            Expansion = term.Label + ": " + value,
            Value = value,
            IsMuted = true,
        };
    }

    private static ExplorerBadge[] BuildShardCountCache()
    {
        var cache = new ExplorerBadge[CachedCountLimit + 1];
        for (var count = 0; count < cache.Length; count++)
        {
            cache[count] = BuildShardCount(count);
        }

        return cache;
    }

    private static ExplorerBadge[] BuildDeadLetterCountCache()
    {
        var cache = new ExplorerBadge[CachedCountLimit + 1];
        for (var count = 0; count < cache.Length; count++)
        {
            cache[count] = BuildDeadLetterCount(count);
        }

        return cache;
    }

    private static ExplorerBadge BuildShardCount(int count)
    {
        // One composed string reused as both the readable text and the
        // expansion, so a shard badge costs two strings rather than three.
        var text = count + (count == 1 ? " shard" : " shards");
        return new ExplorerBadge
        {
            TermId = ExplorerTermIds.ShardCount,
            Label = ExplorerGlossary.Get(ExplorerTermIds.ShardCount).Label,
            Text = text,
            ShortText = count + " " + ShardAbbreviation,
            Expansion = text,
            Count = count,
        };
    }

    private static ExplorerBadge BuildDeadLetterCount(int count)
    {
        return new ExplorerBadge
        {
            TermId = ExplorerTermIds.DeadLetterCount,
            Label = ExplorerGlossary.Get(ExplorerTermIds.DeadLetterCount).Label,
            Text = count + " dead-lettered",
            ShortText = count + " " + DeadLetterAbbreviation,
            Expansion = count + (count == 1 ? " dead-lettered write" : " dead-lettered writes"),
            Count = count,
        };
    }
}
