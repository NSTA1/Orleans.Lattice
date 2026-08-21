using System.Text;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// End-to-end integration tests that drive the view maintainer through the public
/// <see cref="ILatticeView"/> surface: writes flow into a source tree, the
/// maintainer tails the WAL, and the materialised aggregation / filter view is
/// read back after a deterministic read-your-writes barrier.
/// </summary>
[TestFixture]
[Category("Integration")]
public partial class ViewMaintainerIntegrationTests
{
    private static readonly TimeSpan Barrier = TimeSpan.FromSeconds(30);

    private ViewClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ViewClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private async Task<ILatticeView> ViewAsync(string name)
    {
        var view = await _fixture.ViewFactory.GetAsync(name);
        Assert.That(view, Is.Not.Null, $"view '{name}' should be registered");
        return view!;
    }

    [Test]
    public async Task Count_view_materialises_group_cardinality()
    {
        var src = _fixture.Source(ViewClusterFixture.CountSource);
        await src.SetAsync("k1", ViewClusterFixture.AggValue("red"));
        await src.SetAsync("k2", ViewClusterFixture.AggValue("red"));
        await src.SetAsync("k3", ViewClusterFixture.AggValue("blue"));

        var view = await ViewAsync(ViewClusterFixture.CountView);
        await view.WaitForSourceHeadAsync(Barrier);

        Assert.That(await view.GetAggregateInt64Async("red"), Is.EqualTo(2L));
        Assert.That(await view.GetAggregateInt64Async("blue"), Is.EqualTo(1L));
    }

    [Test]
    public async Task Sum_view_materialises_group_total()
    {
        var src = _fixture.Source(ViewClusterFixture.SumSource);
        await src.SetAsync("a", ViewClusterFixture.AggValue("g", 10.5));
        await src.SetAsync("b", ViewClusterFixture.AggValue("g", 4.5));

        var view = await ViewAsync(ViewClusterFixture.SumView);
        await view.WaitForSourceHeadAsync(Barrier);

        Assert.That(await view.GetAggregateDoubleAsync("g"), Is.EqualTo(15.0));
    }

    [Test]
    public async Task Min_view_materialises_group_minimum()
    {
        var src = _fixture.Source(ViewClusterFixture.MinSource);
        await src.SetAsync("a", ViewClusterFixture.AggValue("g", 5));
        await src.SetAsync("b", ViewClusterFixture.AggValue("g", 2));
        await src.SetAsync("c", ViewClusterFixture.AggValue("g", 9));

        var view = await ViewAsync(ViewClusterFixture.MinView);
        await view.WaitForSourceHeadAsync(Barrier);

        Assert.That(await view.GetAggregateDoubleAsync("g"), Is.EqualTo(2.0));
    }

    [Test]
    public async Task Max_view_materialises_group_maximum()
    {
        var src = _fixture.Source(ViewClusterFixture.MaxSource);
        await src.SetAsync("a", ViewClusterFixture.AggValue("g", 5));
        await src.SetAsync("b", ViewClusterFixture.AggValue("g", 2));
        await src.SetAsync("c", ViewClusterFixture.AggValue("g", 9));

        var view = await ViewAsync(ViewClusterFixture.MaxView);
        await view.WaitForSourceHeadAsync(Barrier);

        Assert.That(await view.GetAggregateDoubleAsync("g"), Is.EqualTo(9.0));
    }

    [Test]
    public async Task SetUnion_view_materialises_distinct_member_cardinality()
    {
        var src = _fixture.Source(ViewClusterFixture.SetUnionSource);
        await src.SetAsync("a", ViewClusterFixture.AggValue("g", 0, "x"));
        await src.SetAsync("b", ViewClusterFixture.AggValue("g", 0, "y"));
        await src.SetAsync("c", ViewClusterFixture.AggValue("g", 0, "x"));

        var view = await ViewAsync(ViewClusterFixture.SetUnionView);
        await view.WaitForSourceHeadAsync(Barrier);

        Assert.That(await view.GetAggregateInt64Async("g"), Is.EqualTo(2L));
    }

    [Test]
    public async Task Count_view_retracts_on_source_delete()
    {
        var src = _fixture.Source(ViewClusterFixture.CountSource);
        await src.SetAsync("d1", ViewClusterFixture.AggValue("green"));
        await src.SetAsync("d2", ViewClusterFixture.AggValue("green"));

        var view = await ViewAsync(ViewClusterFixture.CountView);
        await view.WaitForSourceHeadAsync(Barrier);
        Assert.That(await view.GetAggregateInt64Async("green"), Is.EqualTo(2L));

        await src.DeleteAsync("d1");
        await view.WaitForSourceHeadAsync(Barrier);

        Assert.That(await view.GetAggregateInt64Async("green"), Is.EqualTo(1L));
    }

    [Test]
    public async Task Sum_view_regroups_when_source_value_changes_group()
    {
        var src = _fixture.Source(ViewClusterFixture.SumSource);
        await src.SetAsync("rg", ViewClusterFixture.AggValue("grpA", 8));

        var view = await ViewAsync(ViewClusterFixture.SumView);
        await view.WaitForSourceHeadAsync(Barrier);
        Assert.That(await view.GetAggregateDoubleAsync("grpA"), Is.EqualTo(8.0));

        await src.SetAsync("rg", ViewClusterFixture.AggValue("grpB", 8));
        await view.WaitForSourceHeadAsync(Barrier);

        Assert.That(await view.GetAggregateDoubleAsync("grpB"), Is.EqualTo(8.0));
        Assert.That(await view.GetAggregateDoubleAsync("grpA"), Is.Null);
    }

    [Test]
    public async Task Filter_view_keeps_matching_and_drops_failing_entries()
    {
        var src = _fixture.Source(ViewClusterFixture.FilterSource);
        await src.SetAsync("adult", JsonLatticeSerializer<ViewPerson>.Default.Serialize(new ViewPerson("Alice", 30)));
        await src.SetAsync("minor", JsonLatticeSerializer<ViewPerson>.Default.Serialize(new ViewPerson("Bob", 12)));

        var view = await ViewAsync(ViewClusterFixture.FilterView);
        await view.WaitForSourceHeadAsync(Barrier);

        Assert.That(await view.GetAsync("adult"), Is.Not.Null);
        Assert.That(await view.GetAsync("minor"), Is.Null);
    }

    [Test]
    public async Task Filter_view_count_keys_and_entries_reflect_live_rows()
    {
        var src = _fixture.Source(ViewClusterFixture.FilterSource);
        await src.SetAsync("ce-1", JsonLatticeSerializer<ViewPerson>.Default.Serialize(new ViewPerson("A", 40)));
        await src.SetAsync("ce-2", JsonLatticeSerializer<ViewPerson>.Default.Serialize(new ViewPerson("B", 41)));

        var view = await ViewAsync(ViewClusterFixture.FilterView);
        await view.WaitForSourceHeadAsync(Barrier);

        var keys = new List<string>();
        await foreach (var k in view.KeysAsync("ce-", "ce-\uffff"))
        {
            keys.Add(k);
        }

        var entries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in view.EntriesAsync("ce-", "ce-\uffff"))
        {
            entries.Add(e);
        }

        Assert.Multiple(() =>
        {
            Assert.That(keys, Does.Contain("ce-1").And.Contain("ce-2"));
            Assert.That(entries.Select(e => e.Key), Does.Contain("ce-1").And.Contain("ce-2"));
            Assert.That(view.ViewName, Is.EqualTo(ViewClusterFixture.FilterView));
        });
    }

    [Test]
    public async Task GetLagAsync_is_zero_after_barrier()
    {
        var src = _fixture.Source(ViewClusterFixture.CountSource);
        await src.SetAsync("lag-1", ViewClusterFixture.AggValue("lagg"));

        var view = await ViewAsync(ViewClusterFixture.CountView);
        await view.WaitForSourceHeadAsync(Barrier);

        Assert.That(await view.GetLagAsync(), Is.EqualTo(0L));
    }

    [Test]
    public async Task ComputeDigestAsync_is_stable_across_calls()
    {
        var src = _fixture.Source(ViewClusterFixture.SumSource);
        await src.SetAsync("dig-1", ViewClusterFixture.AggValue("digg", 3));

        var view = await ViewAsync(ViewClusterFixture.SumView);
        await view.WaitForSourceHeadAsync(Barrier);

        var d1 = await view.ComputeDigestAsync();
        var d2 = await view.ComputeDigestAsync();

        // Record-struct equality compares Hash by reference; ContentEquals is the
        // documented byte-for-byte comparison for a drift check.
        Assert.That(d1.ContentEquals(d2), Is.True);
    }

    [Test]
    public async Task ReconcileAsync_returns_false_when_view_matches_source()
    {
        var src = _fixture.Source(ViewClusterFixture.MaxSource);
        await src.SetAsync("rec-1", ViewClusterFixture.AggValue("recg", 7));

        var view = await ViewAsync(ViewClusterFixture.MaxView);
        await view.WaitForSourceHeadAsync(Barrier);

        Assert.That(await view.ReconcileAsync(), Is.False);
    }

    [Test]
    public async Task RebuildAsync_preserves_materialised_values()
    {
        var src = _fixture.Source(ViewClusterFixture.CountSource);
        await src.SetAsync("rb-1", ViewClusterFixture.AggValue("rebuildg"));
        await src.SetAsync("rb-2", ViewClusterFixture.AggValue("rebuildg"));

        var view = await ViewAsync(ViewClusterFixture.CountView);
        await view.WaitForSourceHeadAsync(Barrier);
        Assert.That(await view.GetAggregateInt64Async("rebuildg"), Is.EqualTo(2L));

        await view.RebuildAsync();

        Assert.That(await view.GetAggregateInt64Async("rebuildg"), Is.EqualTo(2L));
    }

    [Test]
    public async Task WaitForSourceHlcAsync_completes_for_applied_target()
    {
        var src = _fixture.Source(ViewClusterFixture.SumSource);
        await src.SetAsync("hlc-1", ViewClusterFixture.AggValue("hlcg", 1));

        var view = await ViewAsync(ViewClusterFixture.SumView);
        // Head barrier first so the maintainer is caught up; then a zero-target
        // HLC wait must return immediately.
        await view.WaitForSourceHeadAsync(Barrier);
        await view.WaitForSourceHlcAsync(HybridLogicalClock.Zero, Barrier);

        Assert.That(await view.GetAggregateDoubleAsync("hlcg"), Is.EqualTo(1.0));
    }
}
