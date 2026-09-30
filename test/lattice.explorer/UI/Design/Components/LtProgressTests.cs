using Bunit;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// The progress primitive (issue 3958): a determinate bar is an ARIA progressbar
/// with its percentage between 0 and 100, rounded down so it never claims done
/// early; an unknown total draws an indeterminate bar with its phase and no
/// invented figure; a small total is cut into one segment per unit; and a change
/// of phase - never a moving percentage - is announced politely.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtProgressTests : ShellDesignTestContext
{
    [Test]
    public void A_determinate_bar_is_a_progressbar_with_its_percentage_and_description()
    {
        var cut = Render<LtProgress>(p => p
            .Add(x => x.Label, "Purge progress")
            .Add(x => x.Phase, "Purging shards")
            .Add(x => x.Value, 3)
            .Add(x => x.Maximum, 8)
            .Add(x => x.Detail, "3 of 8 shards purged."));

        var bar = cut.Find("[role=progressbar]");
        Assert.Multiple(() =>
        {
            Assert.That(bar.GetAttribute("aria-label"), Is.EqualTo("Purge progress"));
            Assert.That(bar.GetAttribute("aria-valuemin"), Is.EqualTo("0"));
            Assert.That(bar.GetAttribute("aria-valuemax"), Is.EqualTo("100"));
            Assert.That(bar.GetAttribute("aria-valuenow"), Is.EqualTo("37"), "3 of 8 is 37.5, rounded down");
            Assert.That(bar.GetAttribute("aria-valuetext"), Is.EqualTo("37%, Purging shards, 3 of 8 shards purged."));
            Assert.That(cut.Find(".lt-progress").GetAttribute("data-lt-progress"), Is.EqualTo("determinate"));
            Assert.That(cut.Find(".lt-progress__phase").TextContent, Is.EqualTo("Purging shards"));
            Assert.That(cut.Find(".lt-progress__figure").TextContent, Is.EqualTo("37%"));
            Assert.That(cut.Find(".lt-progress__fill").GetAttribute("style"), Is.EqualTo("inline-size: 37%"));
            Assert.That(cut.Find(".lt-progress__detail").TextContent, Is.EqualTo("3 of 8 shards purged."));
        });
    }

    [Test]
    [TestCase(7, 8, 87)]
    [TestCase(8, 8, 100)]
    [TestCase(12, 8, 100)]
    [TestCase(-1, 8, 0)]
    public void The_percentage_never_reaches_100_before_every_unit_is_done_and_is_clamped(long value, long maximum, int expected)
    {
        var cut = Render<LtProgress>(p => p.Add(x => x.Label, "Progress").Add(x => x.Value, value).Add(x => x.Maximum, maximum));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Instance.Percent, Is.EqualTo(expected));
            Assert.That(cut.Find("[role=progressbar]").GetAttribute("aria-valuenow"), Is.EqualTo(expected.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        });
    }

    [Test]
    [TestCase(null)]
    [TestCase(0L)]
    [TestCase(-4L)]
    public void An_unknown_total_draws_an_indeterminate_bar_with_its_phase_and_no_figure(long? maximum)
    {
        var cut = Render<LtProgress>(p => p
            .Add(x => x.Label, "Undo progress")
            .Add(x => x.Phase, "Undoing the resize")
            .Add(x => x.Value, 5)
            .Add(x => x.Maximum, maximum));

        var bar = cut.Find("[role=progressbar]");
        Assert.Multiple(() =>
        {
            Assert.That(cut.Instance.IsDeterminate, Is.False);
            Assert.That(cut.Instance.Percent, Is.Null);
            Assert.That(bar.HasAttribute("aria-valuenow"), Is.False, "an indeterminate progressbar has no current value");
            Assert.That(bar.GetAttribute("aria-valuetext"), Is.EqualTo("Undoing the resize"));
            Assert.That(cut.Find(".lt-progress").GetAttribute("data-lt-progress"), Is.EqualTo("indeterminate"));
            Assert.That(cut.FindAll(".lt-progress__figure"), Is.Empty);
            Assert.That(cut.FindAll(".lt-progress__fill"), Is.Empty);
        });
    }

    [Test]
    public void Without_a_phase_the_label_heads_the_bar_and_the_description_still_says_something()
    {
        var cut = Render<LtProgress>(p => p.Add(x => x.Label, "Reshard progress"));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-progress__phase").TextContent, Is.EqualTo("Reshard progress"));
            Assert.That(cut.Find("[role=progressbar]").GetAttribute("aria-valuetext"), Is.EqualTo("In progress"));
            Assert.That(cut.FindAll(".lt-progress__detail"), Is.Empty);
        });
    }

    [Test]
    public void A_small_total_is_cut_into_one_segment_per_unit()
    {
        var cut = Render<LtProgress>(p => p.Add(x => x.Label, "Progress").Add(x => x.Value, 1).Add(x => x.Maximum, 7));

        Assert.That(cut.Find(".lt-progress__ticks").GetAttribute("style"), Is.EqualTo("--lt-progress-segments: 7"));
    }

    [Test]
    [TestCase(1L)]
    [TestCase(LtProgress.MaximumSegments + 1)]
    public void A_single_unit_or_a_large_total_is_drawn_continuous(long maximum)
    {
        var cut = Render<LtProgress>(p => p.Add(x => x.Label, "Progress").Add(x => x.Value, 0).Add(x => x.Maximum, maximum));

        Assert.That(cut.FindAll(".lt-progress__ticks"), Is.Empty);
    }

    [Test]
    public void A_change_of_phase_is_announced_once_and_a_moving_percentage_is_not()
    {
        var cut = Render<LtProgress>(p => p.Add(x => x.Label, "Resize progress").Add(x => x.Phase, "Copying the tree").Add(x => x.Value, 1).Add(x => x.Maximum, 7));
        var live = () => cut.Find("[aria-live=polite]").TextContent;
        Assert.That(live(), Is.Empty, "the phase a page opens on is not announced over the page");

        cut.Render(p => p.Add(x => x.Value, 3));
        Assert.That(live(), Is.Empty, "a moving percentage is not announced");

        cut.Render(p => p.Add(x => x.Phase, "Retiring the old copy").Add(x => x.Value, 6));
        Assert.That(live(), Is.EqualTo("Retiring the old copy"));
    }

    [Test]
    public void A_bar_without_a_label_is_rejected()
    {
        Assert.That(() => Render<LtProgress>(p => p.Add(x => x.Label, " ")), Throws.InvalidOperationException);
    }
}
