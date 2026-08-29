using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Plugins.Data;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// The retained view the value drill-down surface reopens a selection on. A
/// value type carried across the plugin's domain seam, so its shape and its
/// value equality are contract.
/// </summary>
[TestFixture]
public sealed class DataRetainedViewTests
{
    [Test]
    public void It_carries_the_retained_prefix_page_size_scan_mode_and_index()
    {
        var view = new DataRetainedView("orders:", 50, EntryScanMode.Snapshot, "region");

        Assert.Multiple(() =>
        {
            Assert.That(view.KeyPrefix, Is.EqualTo("orders:"));
            Assert.That(view.PageSize, Is.EqualTo(50));
            Assert.That(view.ScanMode, Is.EqualTo(EntryScanMode.Snapshot));
            Assert.That(view.TagIndexName, Is.EqualTo("region"));
        });
    }

    [Test]
    public void An_unretained_index_is_null_rather_than_empty()
    {
        // The view branches on null to mean "nothing was chosen", so an empty
        // string would read as a chosen index with no name.
        var view = new DataRetainedView(string.Empty, 25, EntryScanMode.Live, null);

        Assert.That(view.TagIndexName, Is.Null);
    }

    [Test]
    public void Two_identical_retained_views_are_equal()
    {
        var left = new DataRetainedView("a", 25, EntryScanMode.Live, "region");
        var right = new DataRetainedView("a", 25, EntryScanMode.Live, "region");

        Assert.Multiple(() =>
        {
            Assert.That(left, Is.EqualTo(right));
            Assert.That(left.GetHashCode(), Is.EqualTo(right.GetHashCode()));
        });
    }

    [Test]
    public void A_differing_scan_mode_makes_two_retained_views_unequal()
    {
        var live = new DataRetainedView("a", 25, EntryScanMode.Live, "region");
        var snapshot = new DataRetainedView("a", 25, EntryScanMode.Snapshot, "region");

        Assert.That(live, Is.Not.EqualTo(snapshot));
    }

    [Test]
    public void The_default_value_is_an_unretained_view()
    {
        var view = default(DataRetainedView);

        Assert.Multiple(() =>
        {
            Assert.That(view.KeyPrefix, Is.Null);
            Assert.That(view.PageSize, Is.Zero);
            Assert.That(view.ScanMode, Is.EqualTo(default(EntryScanMode)));
            Assert.That(view.TagIndexName, Is.Null);
        });
    }
}
