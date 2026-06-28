using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Views;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// End-to-end coverage for the per-key history read path
/// (<see cref="ILattice.ScanEntryHistoryAsync"/>) over the durable history
/// view. A single silo registers <c>AddLattice</c> + <c>AddLatticeViews</c>, a
/// runtime history view is created the operator way, and the read method is driven
/// straight off the source grain: it prefix-scans the view tree, returns a key's
/// revisions in hybrid-logical-clock order with the right kind / origin / value per
/// the configured retention mode, pages through a continuation token, and keeps the
/// timeline readable after the source key's live state has been overwritten and
/// deleted (the view path never touches the source write-ahead log, so it reports a
/// clean, bounded-by-age, never-truncated history).
/// </summary>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class EntryHistoryReadIntegrationTests
{
    private TestCluster _cluster = null!;

    private IServiceProvider SiloServices =>
        _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    private ILatticeViewFactory Factory => SiloServices.GetRequiredService<ILatticeViewFactory>();

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeViews();
            siloBuilder.Services.ConfigureAll<LatticeViewOptions>(o =>
            {
                o.CoalesceWindow = TimeSpan.FromMinutes(5);
                o.ReadHandleCacheTtl = TimeSpan.FromMilliseconds(50);
                o.OldGenerationReclaimGrace = TimeSpan.FromMilliseconds(200);
            });
        }
    }

    private async Task CreateHistoryViewAsync(string sourceTreeId, string viewName)
    {
        var source = _cluster.Client.GetGrain<ILattice>(sourceTreeId);
        Factory.Create(source, viewName, LatticeHistoryView.Definition(viewName, SiloServices));

        // Establish the maintainer's WAL cursor before any source write so each
        // mutation is tailed as its own revision rather than folded into the
        // initial current-state backfill.
        var maintainer = _cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
    }

    private async Task DrainToZeroAsync(string viewName)
    {
        var maintainer = _cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
        for (var attempt = 0; attempt < 50; attempt++)
        {
            await maintainer.DrainAsync();
            if (await maintainer.GetLagAsync() == 0)
            {
                return;
            }

            await Task.Delay(20);
        }

        Assert.Fail($"History view '{viewName}' did not catch up to the source head.");
    }

    [Test]
    public async Task ScanEntryHistoryAsync_view_returns_revisions_in_hlc_order()
    {
        const string tree = "read-order-src";
        const string view = "read-order-view";
        var source = _cluster.Client.GetGrain<ILattice>(tree);
        await CreateHistoryViewAsync(tree, view);

        await source.SetAsync("k", new byte[] { 1 });
        await source.SetAsync("k", new byte[] { 2 });
        await source.SetAsync("k", new byte[] { 3 });
        await DrainToZeroAsync(view);

        var page = await source.ScanEntryHistoryAsync("k", null, null, 100, null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Source, Is.EqualTo(EntryHistorySource.View));
            Assert.That(page.Truncated, Is.False, "the view path is bounded by age, never truncated");
            Assert.That(page.Revisions, Has.Count.EqualTo(3));
            Assert.That(page.Revisions.All(r => r.Kind == HistoryRowKind.Set), Is.True);
            Assert.That(page.Revisions.All(r => r.SourceKey == "k"), Is.True);
            Assert.That(page.Revisions.Select(r => r.Hlc).ToList(), Is.Ordered);
        });
    }

    [Test]
    public async Task ScanEntryHistoryAsync_view_returns_only_the_requested_key()
    {
        const string tree = "read-isolate-src";
        const string view = "read-isolate-view";
        var source = _cluster.Client.GetGrain<ILattice>(tree);
        await CreateHistoryViewAsync(tree, view);

        await source.SetAsync("a", new byte[] { 1 });
        await source.SetAsync("b", new byte[] { 2 });
        await source.SetAsync("a", new byte[] { 3 });
        await DrainToZeroAsync(view);

        var page = await source.ScanEntryHistoryAsync("a", null, null, 100, null);

        Assert.That(page.Revisions, Has.Count.EqualTo(2));
        Assert.That(page.Revisions.All(r => r.SourceKey == "a"), Is.True);
    }

    [Test]
    public async Task ScanEntryHistoryAsync_view_pages_through_continuation_without_gaps_or_dupes()
    {
        const string tree = "read-page-src";
        const string view = "read-page-view";
        var source = _cluster.Client.GetGrain<ILattice>(tree);
        await CreateHistoryViewAsync(tree, view);

        const int total = 7;
        for (var i = 0; i < total; i++)
        {
            await source.SetAsync("k", new byte[] { (byte)i });
        }

        await DrainToZeroAsync(view);

        var seen = new List<HybridLogicalClock>();
        string? continuation = null;
        for (var guard = 0; guard < 20; guard++)
        {
            var page = await source.ScanEntryHistoryAsync("k", null, null, 2, continuation);
            seen.AddRange(page.Revisions.Select(r => r.Hlc));
            continuation = page.Continuation;
            if (continuation is null)
            {
                break;
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(seen, Has.Count.EqualTo(total), "every revision is returned exactly once across pages");
            Assert.That(seen, Is.Ordered);
            Assert.That(seen.Distinct().Count(), Is.EqualTo(total), "no revision is returned twice");
        });
    }

    [Test]
    public async Task ScanEntryHistoryAsync_view_metadata_only_strips_value_keeps_fingerprint()
    {
        const string tree = "read-meta-src";
        const string view = "read-meta-view";
        var source = _cluster.Client.GetGrain<ILattice>(tree);
        await CreateHistoryViewAsync(tree, view);

        await source.SetAsync("k", new byte[] { 9, 9, 9, 9 });
        await DrainToZeroAsync(view);

        var page = await source.ScanEntryHistoryAsync("k", null, null, 100, null);

        Assert.That(page.Revisions, Has.Count.EqualTo(1));
        var revision = page.Revisions[0];
        Assert.Multiple(() =>
        {
            Assert.That(revision.ValuePreview, Is.Null, "metadata-only is the default and strips value bytes");
            Assert.That(revision.ValueLength, Is.EqualTo(4));
            Assert.That(revision.ValueHash, Is.Not.Zero);
            Assert.That(revision.RetentionShape, Is.EqualTo(HistoryRetentionMode.MetadataOnly));
        });
    }

    [Test]
    public async Task ScanEntryHistoryAsync_view_full_value_returns_value_preview()
    {
        const string tree = "read-full-src";
        const string view = "read-full-view";
        var source = _cluster.Client.GetGrain<ILattice>(tree);
        await CreateHistoryViewAsync(tree, view);

        await source.SetHistoryRetentionAsync(HistoryRetentionMode.FullValue, null);
        await source.SetAsync("k", new byte[] { 5, 6, 7 });
        await DrainToZeroAsync(view);

        var page = await source.ScanEntryHistoryAsync("k", null, null, 100, null);

        Assert.That(page.Revisions, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(page.Revisions[0].ValuePreview, Is.EqualTo(new byte[] { 5, 6, 7 }));
            Assert.That(page.Revisions[0].RetentionShape, Is.EqualTo(HistoryRetentionMode.FullValue));
        });
    }

    [Test]
    public async Task ScanEntryHistoryAsync_view_records_delete_as_its_own_revision()
    {
        const string tree = "read-del-src";
        const string view = "read-del-view";
        var source = _cluster.Client.GetGrain<ILattice>(tree);
        await CreateHistoryViewAsync(tree, view);

        await source.SetAsync("k", new byte[] { 1 });
        await source.DeleteAsync("k");
        await DrainToZeroAsync(view);

        var page = await source.ScanEntryHistoryAsync("k", null, null, 100, null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Revisions, Has.Count.EqualTo(2));
            Assert.That(page.Revisions[0].Kind, Is.EqualTo(HistoryRowKind.Set));
            Assert.That(page.Revisions[1].Kind, Is.EqualTo(HistoryRowKind.Delete));
        });
    }

    [Test]
    public async Task ScanEntryHistoryAsync_view_survives_source_overwrite_and_delete()
    {
        const string tree = "read-durable-src";
        const string view = "read-durable-view";
        var source = _cluster.Client.GetGrain<ILattice>(tree);
        await CreateHistoryViewAsync(tree, view);

        await source.SetHistoryRetentionAsync(HistoryRetentionMode.FullValue, null);
        await source.SetAsync("k", new byte[] { 1 });
        await source.SetAsync("k", new byte[] { 2 });
        await source.SetAsync("k", new byte[] { 3 });
        await source.DeleteAsync("k");
        await DrainToZeroAsync(view);

        // The source key no longer exists; the timeline outlives the live state
        // because the read comes from the durable view tree, not the source WAL.
        Assert.That(await source.ExistsAsync("k"), Is.False);

        var page = await source.ScanEntryHistoryAsync("k", null, null, 100, null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Source, Is.EqualTo(EntryHistorySource.View));
            Assert.That(page.Truncated, Is.False);
            Assert.That(page.Revisions, Has.Count.EqualTo(4));
            Assert.That(page.Revisions.Count(r => r.Kind == HistoryRowKind.Set), Is.EqualTo(3));
            Assert.That(page.Revisions.Count(r => r.Kind == HistoryRowKind.Delete), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task ScanEntryHistoryAsync_view_honours_hlc_bounds()
    {
        const string tree = "read-bounds-src";
        const string view = "read-bounds-view";
        var source = _cluster.Client.GetGrain<ILattice>(tree);
        await CreateHistoryViewAsync(tree, view);

        for (var i = 0; i < 5; i++)
        {
            await source.SetAsync("k", new byte[] { (byte)i });
        }

        await DrainToZeroAsync(view);

        var all = await source.ScanEntryHistoryAsync("k", null, null, 100, null);
        Assert.That(all.Revisions, Has.Count.EqualTo(5));

        var from = all.Revisions[1].Hlc;
        var to = all.Revisions[3].Hlc;

        var bounded = await source.ScanEntryHistoryAsync("k", from, to, 100, null);

        Assert.Multiple(() =>
        {
            Assert.That(bounded.Revisions, Has.Count.EqualTo(3));
            Assert.That(bounded.Revisions[0].Hlc, Is.EqualTo(from));
            Assert.That(bounded.Revisions[^1].Hlc, Is.EqualTo(to));
        });
    }
}
