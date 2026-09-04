using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Token parsing, ordering, clamp, and history-bound edge cases for state API queries.
/// </summary>
public sealed partial class LatticeStateApiEdgeCaseTests
{
    [Test]
    public void Tag_member_token_without_a_separator_resumes_after_the_tree_with_an_empty_key()
    {
        var values = DecodeTagMemberToken("tree-only");

        Assert.Multiple(() =>
        {
            Assert.That(values.TreeId, Is.EqualTo("tree-only"));
            Assert.That(values.Key, Is.Empty);
        });
    }

    [Test]
    public void Tag_member_token_with_a_separator_preserves_separator_characters_inside_the_key()
    {
        var values = DecodeTagMemberToken("tree\0key\0tail");

        Assert.Multiple(() =>
        {
            Assert.That(values.TreeId, Is.EqualTo("tree"));
            Assert.That(values.Key, Is.EqualTo("key\0tail"));
        });
    }

    [Test]
    public void Tag_member_comparison_orders_by_tree_before_key()
    {
        Assert.Multiple(() =>
        {
            Assert.That(CompareTaggedKey("a", "z", "b", "a"), Is.LessThan(0));
            Assert.That(CompareTaggedKey("b", "a", "a", "z"), Is.GreaterThan(0));
            Assert.That(CompareTaggedKey("tree", "a", "tree", "b"), Is.LessThan(0));
            Assert.That(CompareTaggedKey("tree", "same", "tree", "same"), Is.Zero);
        });
    }

    [Test]
    public void Dead_letter_offset_tokens_ignore_null_empty_malformed_and_negative_values()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DecodeOffset(null), Is.Zero);
            Assert.That(DecodeOffset(string.Empty), Is.Zero);
            Assert.That(DecodeOffset("not-an-offset"), Is.Zero);
            Assert.That(DecodeOffset("-1"), Is.Zero);
            Assert.That(DecodeOffset("3"), Is.EqualTo(3));
        });
    }

    [Test]
    public void Dead_letter_source_projection_maps_unknown_values_to_unknown()
    {
        var mapped = InvokeStatic<DeadLetterSourceKind>("MapSource", unchecked((LatticeSchemaDeadLetterSource)999));

        Assert.That(mapped, Is.EqualTo(DeadLetterSourceKind.Unknown));
    }

    [Test]
    public void Query_clamp_helpers_apply_defaults_maximums_and_requested_values()
    {
        var query = CreateQuery(apiOptions: new LatticeApiStateOptions
        {
            DefaultScanPageSize = 10,
            MaxScanPageSize = 50,
            DefaultScanValuePreviewBytes = 3,
            MaxScanValuePreviewBytes = 9,
            DefaultHistoryPageSize = 4,
            MaxHistoryPageSize = 8,
            DefaultHistoryValuePreviewBytes = 5,
            MaxHistoryValuePreviewBytes = 7,
        });

        Assert.Multiple(() =>
        {
            Assert.That(InvokeInstance<int>(query, "ClampPageSize", 0), Is.EqualTo(10));
            Assert.That(InvokeInstance<int>(query, "ClampPageSize", 51), Is.EqualTo(50));
            Assert.That(InvokeInstance<int>(query, "ClampPageSize", 12), Is.EqualTo(12));
            Assert.That(InvokeInstance<int>(query, "ClampScanPreviewBudget", 0), Is.EqualTo(3));
            Assert.That(InvokeInstance<int>(query, "ClampScanPreviewBudget", 10), Is.EqualTo(9));
            Assert.That(InvokeInstance<int>(query, "ClampScanPreviewBudget", 6), Is.EqualTo(6));
            Assert.That(InvokeInstance<int>(query, "ClampHistoryLimit", 0), Is.EqualTo(4));
            Assert.That(InvokeInstance<int>(query, "ClampHistoryLimit", 9), Is.EqualTo(8));
            Assert.That(InvokeInstance<int>(query, "ClampHistoryLimit", 6), Is.EqualTo(6));
            Assert.That(InvokeInstance<int>(query, "ClampHistoryPreviewBudget", 0), Is.EqualTo(5));
            Assert.That(InvokeInstance<int>(query, "ClampHistoryPreviewBudget", 8), Is.EqualTo(7));
            Assert.That(InvokeInstance<int>(query, "ClampHistoryPreviewBudget", 6), Is.EqualTo(6));
        });
    }

    [Test]
    public void Query_clamp_helpers_never_exceed_the_maximum_when_the_default_is_misconfigured_above_it()
    {
        // Nothing validates LatticeApiStateOptions, so an operator can configure a
        // default that exceeds the maximum. A non-positive request (the "use the
        // default" arm) must still be capped to the maximum rather than returning
        // the raw, over-max default - which would let a page size or preview budget
        // exceed the configured ceiling.
        var query = CreateQuery(apiOptions: new LatticeApiStateOptions
        {
            DefaultScanPageSize = 1000,
            MaxScanPageSize = 50,
            DefaultScanValuePreviewBytes = 1000,
            MaxScanValuePreviewBytes = 9,
            DefaultHistoryPageSize = 1000,
            MaxHistoryPageSize = 8,
            DefaultHistoryValuePreviewBytes = 1000,
            MaxHistoryValuePreviewBytes = 7,
        });

        Assert.Multiple(() =>
        {
            Assert.That(InvokeInstance<int>(query, "ClampPageSize", 0), Is.EqualTo(50));
            Assert.That(InvokeInstance<int>(query, "ClampScanPreviewBudget", 0), Is.EqualTo(9));
            Assert.That(InvokeInstance<int>(query, "ClampHistoryLimit", 0), Is.EqualTo(8));
            Assert.That(InvokeInstance<int>(query, "ClampHistoryPreviewBudget", 0), Is.EqualTo(7));
        });
    }

    [Test]
    public void History_bound_maps_view_truncated_and_fallback_sources()
    {
        Assert.Multiple(() =>
        {
            Assert.That(MapHistoryBound(new EntryHistoryPage { Source = EntryHistorySource.View }), Is.EqualTo(EntryHistoryBound.BoundedByAge));
            Assert.That(MapHistoryBound(new EntryHistoryPage { Source = EntryHistorySource.WalWindow, Truncated = true }), Is.EqualTo(EntryHistoryBound.Truncated));
            Assert.That(MapHistoryBound(new EntryHistoryPage { Source = EntryHistorySource.WalWindow, Truncated = false }), Is.EqualTo(EntryHistoryBound.WalWindowFallback));
        });
    }
}
