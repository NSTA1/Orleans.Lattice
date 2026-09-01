using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Vocabulary;

namespace Orleans.Lattice.Explorer.Tests.Vocabulary;

/// <summary>
/// Tests for the badge expansions that replace <c>64 sh</c> and <c>agg</c>.
/// </summary>
/// <remarks>
/// The rule being enforced is the issue's: an abbreviation may stay, but it must
/// carry an accessible expansion, and neither may be empty. The reuse assertions
/// exist because badges are built per catalog item on every render, so the
/// pre-built ones must be reused rather than recomposed.
/// </remarks>
[TestFixture]
public class ExplorerBadgesTests
{
    private static ExplorerBadge[] Buffer() => new ExplorerBadge[ExplorerBadges.MaxCatalogBadges];

    private static CatalogItem Tree(
        string id = "orders",
        string? lifecycle = "Active",
        int? shardCount = 64) =>
        new()
        {
            Id = id,
            Kind = CatalogKind.Trees,
            Lifecycle = lifecycle,
            ShardCount = shardCount,
        };

    private static CatalogItem View(
        bool isAggregation = false,
        bool isHistory = false,
        string? sourceTreeId = null,
        string? providerKey = null,
        string? version = null) =>
        new()
        {
            Id = "view-totals",
            DisplayName = "totals",
            Kind = CatalogKind.Views,
            IsAggregation = isAggregation,
            IsHistory = isHistory,
            SourceTreeId = sourceTreeId,
            ProjectionProviderKey = providerKey,
            ProjectionVersion = version,
        };

    private static CatalogItem TagIndex(int? shardCount = 8) =>
        new()
        {
            Id = "tag-region",
            IndexName = "region",
            Kind = CatalogKind.TagIndexes,
            ShardCount = shardCount,
        };

    private static IReadOnlyList<ExplorerBadge> AllStaticBadges() =>
    [
        ExplorerBadges.Active,
        ExplorerBadges.SoftDeleted,
        ExplorerBadges.Purging,
        ExplorerBadges.Aggregation,
        ExplorerBadges.History,
        ExplorerBadges.TagIndex,
    ];

    // ------------------------------------------------------------ the copy rule

    [Test]
    public void Every_badge_carries_a_non_empty_text_short_text_and_expansion()
    {
        var badges = AllStaticBadges()
            .Append(ExplorerBadges.ShardCount(64))
            .Append(ExplorerBadges.DeadLetterCount(3))
            .Append(ExplorerBadges.SourceTree("orders"))
            .Append(ExplorerBadges.ProjectionProvider("totals-v2"))
            .Append(ExplorerBadges.ProjectionVersion("3"))
            .ToArray();

        Assert.Multiple(() =>
        {
            foreach (var badge in badges)
            {
                Assert.That(badge.Text, Is.Not.Empty, badge.TermId);
                Assert.That(badge.ShortText, Is.Not.Empty, badge.TermId);
                Assert.That(badge.Expansion, Is.Not.Empty, badge.TermId);
                Assert.That(badge.Label, Is.Not.Empty, badge.TermId);
            }
        });
    }

    [Test]
    public void Every_badge_resolves_to_a_glossary_term_with_an_explanation()
    {
        var badges = AllStaticBadges()
            .Append(ExplorerBadges.ShardCount(1))
            .Append(ExplorerBadges.DeadLetterCount(1))
            .Append(ExplorerBadges.SourceTree("orders"))
            .Append(ExplorerBadges.ProjectionProvider("p"))
            .Append(ExplorerBadges.ProjectionVersion("1"))
            .ToArray();

        Assert.Multiple(() =>
        {
            foreach (var badge in badges)
            {
                Assert.That(badge.Term, Is.Not.Null, badge.TermId);
                Assert.That(badge.Explanation, Is.Not.Null.And.Not.Empty, badge.TermId);
                Assert.That(badge.Label, Is.EqualTo(badge.Term!.Label), badge.TermId);
            }
        });
    }

    [Test]
    public void An_abbreviated_badge_reports_that_it_needs_its_expansion_carried()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerBadges.ShardCount(64).IsAbbreviated, Is.True);
            Assert.That(ExplorerBadges.Aggregation.IsAbbreviated, Is.True);
            Assert.That(ExplorerBadges.History.IsAbbreviated, Is.True);
            Assert.That(ExplorerBadges.DeadLetterCount(3).IsAbbreviated, Is.True);
        });
    }

    [Test]
    public void A_badge_whose_short_form_says_everything_is_not_abbreviated()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerBadges.TagIndex.IsAbbreviated, Is.True, "'tag' says less than 'Tag index'");
            Assert.That(ExplorerBadges.SourceTree("orders").IsAbbreviated, Is.True);
            Assert.That(
                new ExplorerBadge
                {
                    TermId = ExplorerTermIds.Shard,
                    Label = "Shard",
                    Text = "Shard",
                    ShortText = "Shard",
                    Expansion = "Shard",
                }.IsAbbreviated,
                Is.False);
        });
    }

    // ------------------------------------------------------------- ShardCount

    [Test]
    public void ShardCount_reads_as_a_shard_count_and_abbreviates_to_the_old_form()
    {
        var badge = ExplorerBadges.ShardCount(64);

        Assert.Multiple(() =>
        {
            Assert.That(badge.Text, Is.EqualTo("64 shards"));
            Assert.That(badge.ShortText, Is.EqualTo("64 sh"));
            Assert.That(badge.Expansion, Is.EqualTo("64 shards"));
            Assert.That(badge.Count, Is.EqualTo(64));
            Assert.That(badge.TermId, Is.EqualTo(ExplorerTermIds.ShardCount));
            Assert.That(badge.IsMuted, Is.False);
        });
    }

    [Test]
    public void ShardCount_of_one_is_singular()
    {
        Assert.That(ExplorerBadges.ShardCount(1).Text, Is.EqualTo("1 shard"));
        Assert.That(ExplorerBadges.ShardCount(1).Expansion, Is.EqualTo("1 shard"));
    }

    [Test]
    public void ShardCount_of_zero_is_plural()
    {
        Assert.That(ExplorerBadges.ShardCount(0).Text, Is.EqualTo("0 shards"));
    }

    [Test]
    public void ShardCount_past_the_cache_still_composes_correctly()
    {
        var badge = ExplorerBadges.ShardCount(ExplorerBadges.CachedCountLimit + 1);

        Assert.Multiple(() =>
        {
            Assert.That(badge.Count, Is.EqualTo(ExplorerBadges.CachedCountLimit + 1));
            Assert.That(badge.Text, Is.EqualTo((ExplorerBadges.CachedCountLimit + 1) + " shards"));
            Assert.That(badge.ShortText, Is.EqualTo((ExplorerBadges.CachedCountLimit + 1) + " sh"));
        });
    }

    [Test]
    public void ShardCount_within_the_cache_returns_the_same_strings_every_time()
    {
        // The cache is what keeps a per-item badge off the allocation path.
        var first = ExplorerBadges.ShardCount(8);
        var second = ExplorerBadges.ShardCount(8);

        Assert.Multiple(() =>
        {
            Assert.That(first.Text, Is.SameAs(second.Text));
            Assert.That(first.ShortText, Is.SameAs(second.ShortText));
            Assert.That(first.Expansion, Is.SameAs(second.Expansion));
        });
    }

    [Test]
    public void ShardCount_reuses_one_string_for_its_text_and_its_expansion()
    {
        var badge = ExplorerBadges.ShardCount(12);

        Assert.That(badge.Text, Is.SameAs(badge.Expansion));
    }

    [Test]
    public void ShardCount_rejects_a_negative_count()
    {
        Assert.That(() => ExplorerBadges.ShardCount(-1), Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    // --------------------------------------------------------- DeadLetterCount

    [Test]
    public void DeadLetterCount_expands_the_DLQ_abbreviation()
    {
        var badge = ExplorerBadges.DeadLetterCount(3);

        Assert.Multiple(() =>
        {
            Assert.That(badge.Text, Is.EqualTo("3 dead-lettered"));
            Assert.That(badge.ShortText, Is.EqualTo("3 DLQ"));
            Assert.That(badge.Expansion, Is.EqualTo("3 dead-lettered writes"));
            Assert.That(badge.Count, Is.EqualTo(3));
        });
    }

    [Test]
    public void DeadLetterCount_of_one_is_singular_in_its_expansion()
    {
        Assert.That(ExplorerBadges.DeadLetterCount(1).Expansion, Is.EqualTo("1 dead-lettered write"));
    }

    [Test]
    public void DeadLetterCount_past_the_cache_still_composes_correctly()
    {
        var count = ExplorerBadges.CachedCountLimit + 7;

        Assert.That(ExplorerBadges.DeadLetterCount(count).ShortText, Is.EqualTo(count + " DLQ"));
    }

    [Test]
    public void DeadLetterCount_within_the_cache_returns_the_same_strings_every_time()
    {
        Assert.That(ExplorerBadges.DeadLetterCount(2).Text, Is.SameAs(ExplorerBadges.DeadLetterCount(2).Text));
    }

    [Test]
    public void DeadLetterCount_rejects_a_negative_count()
    {
        Assert.That(() => ExplorerBadges.DeadLetterCount(-1), Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    // ------------------------------------------------------------ value badges

    [Test]
    public void SourceTree_shows_the_id_and_expands_to_name_the_relationship()
    {
        var badge = ExplorerBadges.SourceTree("orders");

        Assert.Multiple(() =>
        {
            Assert.That(badge.Text, Is.EqualTo("orders"));
            Assert.That(badge.ShortText, Is.EqualTo("orders"));
            Assert.That(badge.Expansion, Is.EqualTo("Source tree: orders"));
            Assert.That(badge.Value, Is.EqualTo("orders"));
            Assert.That(badge.IsMuted, Is.True, "secondary context is rendered muted");
        });
    }

    [Test]
    public void SourceTree_does_not_copy_the_value_it_was_given()
    {
        var id = "orders";

        Assert.That(ExplorerBadges.SourceTree(id).Text, Is.SameAs(id));
    }

    [Test]
    public void ProjectionProvider_expands_to_name_the_field()
    {
        Assert.That(ExplorerBadges.ProjectionProvider("totals-v2").Expansion, Is.EqualTo("Projection provider: totals-v2"));
    }

    [Test]
    public void ProjectionVersion_expands_to_name_the_field()
    {
        Assert.That(ExplorerBadges.ProjectionVersion("3").Expansion, Is.EqualTo("Projection version: 3"));
    }

    [Test]
    public void An_empty_value_is_accepted_and_still_expands()
    {
        Assert.That(ExplorerBadges.SourceTree(string.Empty).Expansion, Is.EqualTo("Source tree: "));
    }

    [Test]
    public void Value_badges_reject_a_null_value()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => ExplorerBadges.SourceTree(null!), Throws.ArgumentNullException);
            Assert.That(() => ExplorerBadges.ProjectionProvider(null!), Throws.ArgumentNullException);
            Assert.That(() => ExplorerBadges.ProjectionVersion(null!), Throws.ArgumentNullException);
        });
    }

    // -------------------------------------------------------- constant badges

    [Test]
    public void Aggregation_replaces_agg_with_a_real_label()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerBadges.Aggregation.Text, Is.EqualTo("Aggregation"));
            Assert.That(ExplorerBadges.Aggregation.ShortText, Is.EqualTo("agg"));
            Assert.That(ExplorerBadges.Aggregation.Expansion, Is.EqualTo("Aggregation view"));
        });
    }

    [Test]
    public void History_names_the_kind_of_view_rather_than_the_surface()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerBadges.History.Text, Is.EqualTo("History"));
            Assert.That(ExplorerBadges.History.ShortText, Is.EqualTo("history"));
            Assert.That(ExplorerBadges.History.Expansion, Is.EqualTo("Change-history view"));
        });
    }

    [Test]
    public void TagIndex_replaces_tag_with_a_real_label()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerBadges.TagIndex.Text, Is.EqualTo("Tag index"));
            Assert.That(ExplorerBadges.TagIndex.ShortText, Is.EqualTo("tag"));
        });
    }

    [Test]
    public void Lifecycle_badges_expand_to_name_the_concept_the_bare_word_did_not()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerBadges.Active.Expansion, Is.EqualTo("Tree lifecycle: active"));
            Assert.That(ExplorerBadges.SoftDeleted.Expansion, Is.EqualTo("Tree lifecycle: soft-deleted"));
            Assert.That(ExplorerBadges.Purging.Expansion, Is.EqualTo("Tree lifecycle: purging"));
        });
    }

    [Test]
    public void Constant_badges_are_the_same_instance_on_every_read()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerBadges.Active.Text, Is.SameAs(ExplorerBadges.Active.Text));
            Assert.That(ExplorerBadges.Aggregation.Expansion, Is.SameAs(ExplorerBadges.Aggregation.Expansion));
        });
    }

    // ----------------------------------------------------------- ForLifecycle

    [Test]
    public void ForLifecycle_maps_each_state_the_cluster_reports()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerBadges.ForLifecycle("Active"), Is.EqualTo(ExplorerBadges.Active));
            Assert.That(ExplorerBadges.ForLifecycle("SoftDeleted"), Is.EqualTo(ExplorerBadges.SoftDeleted));
            Assert.That(ExplorerBadges.ForLifecycle("Purging"), Is.EqualTo(ExplorerBadges.Purging));
        });
    }

    [Test]
    public void ForLifecycle_ignores_case()
    {
        Assert.That(ExplorerBadges.ForLifecycle("purging"), Is.EqualTo(ExplorerBadges.Purging));
    }

    [Test]
    public void ForLifecycle_null_empty_or_unknown_returns_no_badge()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerBadges.ForLifecycle(null), Is.Null);
            Assert.That(ExplorerBadges.ForLifecycle(string.Empty), Is.Null);
            Assert.That(ExplorerBadges.ForLifecycle("Exploded"), Is.Null);
        });
    }

    // --------------------------------------------------------- ForCatalogItem

    [Test]
    public void ForCatalogItem_writes_a_trees_badge_set_in_render_order()
    {
        var buffer = Buffer();
        var written = ExplorerBadges.ForCatalogItem(Tree(), deadLetterCount: 2, buffer);

        Assert.Multiple(() =>
        {
            Assert.That(written, Is.EqualTo(3));
            Assert.That(buffer[0], Is.EqualTo(ExplorerBadges.Active));
            Assert.That(buffer[1].Count, Is.EqualTo(64));
            Assert.That(buffer[2].TermId, Is.EqualTo(ExplorerTermIds.DeadLetterCount));
        });
    }

    [Test]
    public void ForCatalogItem_omits_a_zero_dead_letter_badge()
    {
        var buffer = Buffer();
        var written = ExplorerBadges.ForCatalogItem(Tree(), deadLetterCount: 0, buffer);

        Assert.That(written, Is.EqualTo(2));
    }

    [Test]
    public void ForCatalogItem_omits_a_missing_lifecycle_and_shard_count()
    {
        var buffer = Buffer();
        var written = ExplorerBadges.ForCatalogItem(
            Tree(lifecycle: null, shardCount: null),
            deadLetterCount: 0,
            buffer);

        Assert.That(written, Is.Zero);
    }

    [Test]
    public void ForCatalogItem_skips_values_it_cannot_render_rather_than_throwing()
    {
        // Display code fed from the wire: a nonsensical count contributes no
        // badge instead of taking the catalog list down with it.
        var buffer = Buffer();
        var written = ExplorerBadges.ForCatalogItem(
            Tree(lifecycle: "Exploded", shardCount: -4),
            deadLetterCount: -1,
            buffer);

        Assert.That(written, Is.Zero);
    }

    [Test]
    public void ForCatalogItem_writes_the_widest_view_badge_set()
    {
        var buffer = Buffer();
        var written = ExplorerBadges.ForCatalogItem(
            View(isAggregation: true, isHistory: true, sourceTreeId: "orders", providerKey: "p", version: "3"),
            deadLetterCount: 0,
            buffer);

        Assert.Multiple(() =>
        {
            Assert.That(written, Is.EqualTo(ExplorerBadges.MaxCatalogBadges));
            Assert.That(buffer[0], Is.EqualTo(ExplorerBadges.Aggregation));
            Assert.That(buffer[1], Is.EqualTo(ExplorerBadges.History));
            Assert.That(buffer[2].Value, Is.EqualTo("orders"));
            Assert.That(buffer[3].Value, Is.EqualTo("p"));
            Assert.That(buffer[4].Value, Is.EqualTo("3"));
        });
    }

    [Test]
    public void ForCatalogItem_writes_nothing_for_a_bare_view()
    {
        var buffer = Buffer();

        Assert.That(ExplorerBadges.ForCatalogItem(View(), deadLetterCount: 0, buffer), Is.Zero);
    }

    [Test]
    public void ForCatalogItem_writes_a_tag_index_badge_set()
    {
        var buffer = Buffer();
        var written = ExplorerBadges.ForCatalogItem(TagIndex(), deadLetterCount: 0, buffer);

        Assert.Multiple(() =>
        {
            Assert.That(written, Is.EqualTo(2));
            Assert.That(buffer[0], Is.EqualTo(ExplorerBadges.TagIndex));
            Assert.That(buffer[1].Count, Is.EqualTo(8));
        });
    }

    [Test]
    public void ForCatalogItem_writes_only_the_tag_badge_when_a_tag_index_has_no_shard_count()
    {
        var buffer = Buffer();

        Assert.That(ExplorerBadges.ForCatalogItem(TagIndex(shardCount: null), 0, buffer), Is.EqualTo(1));
    }

    [Test]
    public void ForCatalogItem_ignores_a_dead_letter_count_on_a_kind_that_has_no_queue()
    {
        var buffer = Buffer();

        Assert.That(ExplorerBadges.ForCatalogItem(TagIndex(shardCount: null), 9, buffer), Is.EqualTo(1));
    }

    [Test]
    public void ForCatalogItem_leaves_the_slots_it_did_not_write_untouched()
    {
        var buffer = Buffer();
        var written = ExplorerBadges.ForCatalogItem(TagIndex(shardCount: null), 0, buffer);

        Assert.Multiple(() =>
        {
            Assert.That(written, Is.EqualTo(1));
            Assert.That(buffer[1].IsEmpty, Is.True, "a caller must respect the returned count");
        });
    }

    [Test]
    public void ForCatalogItem_can_refill_one_buffer_across_many_items()
    {
        // The allocation-free rendering pattern: one buffer for the list's
        // lifetime, refilled per row.
        var buffer = Buffer();

        var trees = ExplorerBadges.ForCatalogItem(Tree(), 1, buffer);
        var tags = ExplorerBadges.ForCatalogItem(TagIndex(), 0, buffer);

        Assert.Multiple(() =>
        {
            Assert.That(trees, Is.EqualTo(3));
            Assert.That(tags, Is.EqualTo(2));
            Assert.That(buffer[0], Is.EqualTo(ExplorerBadges.TagIndex));
        });
    }

    [Test]
    public void ForCatalogItem_rejects_a_null_item()
    {
        var buffer = Buffer();
        var localBuffer = buffer;

        Assert.That(
            () => ExplorerBadges.ForCatalogItem(null!, 0, localBuffer),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ForCatalogItem_rejects_a_buffer_that_cannot_hold_the_widest_set()
    {
        var tooSmall = new ExplorerBadge[ExplorerBadges.MaxCatalogBadges - 1];
        var item = Tree();

        Assert.That(
            () => ExplorerBadges.ForCatalogItem(item, 0, tooSmall),
            Throws.ArgumentException);
    }

    // --------------------------------------------------------- the default badge

    [Test]
    public void The_default_badge_reports_itself_empty_and_resolves_no_term()
    {
        var empty = default(ExplorerBadge);

        Assert.Multiple(() =>
        {
            Assert.That(empty.IsEmpty, Is.True);
            Assert.That(empty.Term, Is.Null);
            Assert.That(empty.Explanation, Is.Null);
            Assert.That(empty.DocsLink, Is.Null);
        });
    }

    [Test]
    public void A_built_badge_is_not_empty()
    {
        Assert.That(ExplorerBadges.ShardCount(3).IsEmpty, Is.False);
    }

    [Test]
    public void A_badge_exposes_where_to_read_more()
    {
        Assert.That(ExplorerBadges.ShardCount(3).DocsLink, Is.EqualTo(ExplorerDocsLinks.TreeSizing));
    }
}
