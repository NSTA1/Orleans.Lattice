using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.Plugins.Selection;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// The shared badge run every per-selection surface renders (issue #1855).
/// </summary>
/// <remarks>
/// <para>
/// The invariant worth pinning is the pair: a badge renders its readable text,
/// and when that text still says less than the full expansion the expansion is
/// carried in the DOM for a screen reader. That is what replaced the
/// abbreviation-plus-<c>title</c> shape, which was invisible on touch and
/// unreachable by keyboard.
/// </para>
/// <para>
/// The count is asserted to be clamped because the buffer is caller-owned and
/// reused: a surface that refills fewer slots than last time must not be able to
/// render a stale one, and an over-large count must not read an unwritten slot.
/// </para>
/// </remarks>
[TestFixture]
public sealed class SelectionBadgeListTests
{
    [Test]
    public async Task Render_with_no_buffer_writes_nothing()
    {
        var html = await RenderAsync(null, count: 3);

        Assert.That(html, Does.Not.Contain("lx-selection-badges"));
    }

    [Test]
    public async Task Render_with_a_zero_count_writes_nothing()
    {
        var html = await RenderAsync([ExplorerBadges.TagIndex], count: 0);

        Assert.That(html, Does.Not.Contain("lx-selection-badges"));
    }

    [Test]
    public async Task Render_with_a_negative_count_writes_nothing()
    {
        var html = await RenderAsync([ExplorerBadges.TagIndex], count: -1);

        Assert.That(html, Does.Not.Contain("lx-selection-badges"));
    }

    [Test]
    public async Task Render_writes_the_readable_text_of_every_counted_badge()
    {
        var html = await RenderAsync(
            [ExplorerBadges.TagIndex, ExplorerBadges.ShardCount(64)],
            count: 2);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Tag index"));
            Assert.That(html, Does.Contain("64 shards"));
            Assert.That(html, Does.Not.Contain(">tag<"), "the abbreviation is not what is rendered");
            Assert.That(html, Does.Not.Contain("64 sh<"));
        });
    }

    [Test]
    public async Task Render_carries_the_expansion_accessibly_when_it_says_more_than_the_text()
    {
        var html = await RenderAsync([ExplorerBadges.Aggregation], count: 1);

        Assert.Multiple(() =>
        {
            Assert.That(ExplorerBadges.Aggregation.Text, Is.EqualTo("Aggregation"));
            Assert.That(ExplorerBadges.Aggregation.Expansion, Is.EqualTo("Aggregation view"));
            Assert.That(html, Does.Contain("lx-visually-hidden"));
            Assert.That(html, Does.Contain("Aggregation view"));
        });
    }

    [Test]
    public async Task Render_omits_the_hidden_expansion_when_it_repeats_the_text()
    {
        var badge = ExplorerBadges.ShardCount(3);

        var html = await RenderAsync([badge], count: 1);

        Assert.Multiple(() =>
        {
            Assert.That(badge.Text, Is.EqualTo(badge.Expansion));
            Assert.That(html, Does.Not.Contain("lx-visually-hidden"),
                "repeating the visible text for a screen reader is noise, not access");
        });
    }

    [Test]
    public async Task Render_stops_at_the_count_the_caller_filled()
    {
        // The third slot is the uninitialised default a reused buffer leaves
        // behind; honouring the count is what keeps it out of the markup.
        var buffer = new ExplorerBadge[3];
        buffer[0] = ExplorerBadges.TagIndex;
        buffer[1] = ExplorerBadges.Aggregation;

        var html = await RenderAsync(buffer, count: 1);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Tag index"));
            Assert.That(html, Does.Not.Contain("Aggregation"));
        });
    }

    [Test]
    public async Task Render_clamps_a_count_past_the_end_of_the_buffer()
    {
        var html = await RenderAsync([ExplorerBadges.TagIndex], count: 9);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Tag index"));
            Assert.That(html, Does.Contain("lx-selection-badges"));
        });
    }

    [Test]
    public async Task Render_marks_a_muted_badge_with_the_muted_class()
    {
        var muted = ExplorerBadges.SourceTree("orders");
        var loud = ExplorerBadges.TagIndex;

        var mutedHtml = await RenderAsync([muted], count: 1);
        var loudHtml = await RenderAsync([loud], count: 1);

        Assert.Multiple(() =>
        {
            Assert.That(muted.IsMuted, Is.True, "a source-tree badge is context, not status");
            Assert.That(mutedHtml, Does.Contain("lx-badge-muted"));
            Assert.That(loud.IsMuted, Is.False);
            Assert.That(loudHtml, Does.Not.Contain("lx-badge-muted"));
        });
    }

    private static Task<string> RenderAsync(ExplorerBadge[]? badges, int count) =>
        SelectionViewRenderHarness.RenderComponentAsync<SelectionBadgeList>(
            new Dictionary<string, object?>
            {
                ["Badges"] = badges,
                ["Count"] = count,
            });
}
