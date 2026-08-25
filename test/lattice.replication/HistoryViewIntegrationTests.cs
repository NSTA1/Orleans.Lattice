using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Views;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// End-to-end integration coverage for the durable per-key history substrate: a
/// runtime-created accumulative history view that re-keys every source mutation
/// into an append-only revision row, the retention modes that shape stored LWW
/// value bytes, and the accumulative guard that keeps an unconstrained range
/// delete from wiping the timeline. A single silo registers <c>AddLattice</c> +
/// <c>AddLatticeViews</c>; the history view is created the runtime way.
/// </summary>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class HistoryViewIntegrationTests
{
    private TestCluster _cluster = null!;

    private IServiceProvider SiloServices =>
        _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    private ILatticeViewFactory Factory => SiloServices.GetRequiredService<ILatticeViewFactory>();

    private HistoryRowCodec Codec => SiloServices.GetRequiredService<HistoryRowCodec>();

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

    private async Task<ILatticeView> CreateHistoryViewAsync(string sourceTreeId, string viewName)
    {
        var source = _cluster.Client.GetGrain<ILattice>(sourceTreeId);
        var view = Factory.Create(source, viewName, LatticeHistoryView.Definition(viewName, SiloServices));

        // Establish the maintainer's WAL cursor before any source write so each
        // subsequent mutation is tailed and recorded as its own revision rather than
        // being folded into the initial current-state backfill. A real operator
        // enables history first, then mutations flow.
        var maintainer = _cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
        return view;
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

    private async Task<IReadOnlyList<HistoryRow>> ReadHistoryAsync(string viewName)
    {
        var maintainer = _cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        var treeId = await maintainer.GetActiveTreeIdAsync();
        var viewTree = _cluster.Client.GetGrain<ILattice>(treeId);

        var rows = new List<HistoryRow>();
        using var scope = ViewReadContext.BeginScope();
        await foreach (var entry in viewTree.ScanEntriesAsync())
        {
            rows.Add(Codec.Decode(entry.Value));
        }

        return rows;
    }


    [Test]
    public async Task History_view_records_one_revision_per_write_in_order()
    {
        const string tree = "hist-order-src";
        const string view = "hist-order-view";
        var source = _cluster.Client.GetGrain<ILattice>(tree);
        await CreateHistoryViewAsync(tree, view);

        await source.SetAsync("k", new byte[] { 1 });
        await source.SetAsync("k", new byte[] { 2 });
        await source.SetAsync("k", new byte[] { 3 });
        await DrainToZeroAsync(view);

        var rows = await ReadHistoryAsync(view);

        Assert.That(rows, Has.Count.EqualTo(3));
        Assert.That(rows.All(r => r.Kind == HistoryRowKind.Set), Is.True);
        Assert.That(rows.All(r => r.SourceKey == "k"), Is.True);
        // Stored under {key}/{hlc}, so the scan returns them in chronological order.
        Assert.That(rows.Select(r => r.Timestamp).ToList(), Is.Ordered);
    }

    [Test]
    public async Task History_view_default_metadata_only_strips_value_keeps_fingerprint()
    {
        const string tree = "hist-meta-src";
        const string view = "hist-meta-view";
        var source = _cluster.Client.GetGrain<ILattice>(tree);
        await CreateHistoryViewAsync(tree, view);

        await source.SetAsync("k", new byte[] { 9, 9, 9, 9 });
        await DrainToZeroAsync(view);

        var rows = await ReadHistoryAsync(view);

        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0].Value, Is.Null, "metadata-only is the default and strips value bytes");
        Assert.That(rows[0].ValueLength, Is.EqualTo(4));
        Assert.That(rows[0].ValueHash, Is.Not.Zero);
        Assert.That(rows[0].RetentionShape, Is.EqualTo(HistoryRetentionMode.MetadataOnly));
    }

    [Test]
    public async Task Full_value_retention_keeps_value_bytes()
    {
        const string tree = "hist-full-src";
        const string view = "hist-full-view";
        var source = _cluster.Client.GetGrain<ILattice>(tree);
        await CreateHistoryViewAsync(tree, view);

        await source.SetHistoryRetentionAsync(HistoryRetentionMode.FullValue, null);
        await source.SetAsync("k", new byte[] { 5, 6, 7 });
        await DrainToZeroAsync(view);

        var rows = await ReadHistoryAsync(view);

        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0].Value, Is.EqualTo(new byte[] { 5, 6, 7 }));
        Assert.That(rows[0].RetentionShape, Is.EqualTo(HistoryRetentionMode.FullValue));
    }

    [Test]
    public async Task Delete_is_recorded_as_a_revision_not_a_removal()
    {
        const string tree = "hist-del-src";
        const string view = "hist-del-view";
        var source = _cluster.Client.GetGrain<ILattice>(tree);
        await CreateHistoryViewAsync(tree, view);

        await source.SetAsync("k", new byte[] { 1 });
        await source.DeleteAsync("k");
        await DrainToZeroAsync(view);

        var rows = await ReadHistoryAsync(view);

        Assert.That(rows, Has.Count.EqualTo(2), "the delete appends a revision rather than removing the prior one");
        Assert.That(rows.Count(r => r.Kind == HistoryRowKind.Set), Is.EqualTo(1));
        Assert.That(rows.Count(r => r.Kind == HistoryRowKind.Delete), Is.EqualTo(1));
    }

    [Test]
    public async Task Unconstrained_range_delete_records_marker_and_keeps_prior_revisions()
    {
        const string tree = "hist-range-src";
        const string view = "hist-range-view";
        var source = _cluster.Client.GetGrain<ILattice>(tree);
        await CreateHistoryViewAsync(tree, view);

        await source.SetAsync("a", new byte[] { 1 });
        await source.SetAsync("b", new byte[] { 2 });
        await DrainToZeroAsync(view);

        await source.DeleteRangeAsync("a", "z");
        await DrainToZeroAsync(view);

        var rows = await ReadHistoryAsync(view);

        // The two original Set revisions survive (an accumulative view never wipes)
        // and a range-tombstone marker is appended.
        Assert.That(rows.Count(r => r.Kind == HistoryRowKind.Set), Is.EqualTo(2));
        Assert.That(rows.Count(r => r.Kind == HistoryRowKind.RangeTombstone), Is.EqualTo(1));
        var marker = rows.Single(r => r.Kind == HistoryRowKind.RangeTombstone);
        Assert.That(marker.SourceKey, Is.EqualTo("a"));
        Assert.That(marker.EndKey, Is.EqualTo("z"));
    }

    [Test]
    public async Task Explicit_rebuild_clears_history_to_current_source_state()
    {
        const string tree = "hist-rebuild-src";
        const string view = "hist-rebuild-view";
        var source = _cluster.Client.GetGrain<ILattice>(tree);
        await CreateHistoryViewAsync(tree, view);

        await source.SetAsync("k", new byte[] { 1 });
        await source.SetAsync("k", new byte[] { 2 });
        await DrainToZeroAsync(view);
        Assert.That(await ReadHistoryAsync(view), Has.Count.EqualTo(2));

        var maintainer = _cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.RebuildAsync();
        await DrainToZeroAsync(view);

        var rows = await ReadHistoryAsync(view);
        Assert.That(rows, Has.Count.EqualTo(1), "an explicit rebuild re-derives from current source state, collapsing prior revisions");
        Assert.That(rows[0].SourceKey, Is.EqualTo("k"));
    }

    [Test]
    public async Task Get_set_history_retention_round_trips_on_the_public_surface()
    {
        const string tree = "hist-cfg-src";
        var source = _cluster.Client.GetGrain<ILattice>(tree);

        var initial = await source.GetHistoryRetentionAsync();
        Assert.That(initial.Mode, Is.EqualTo(HistoryRetentionMode.MetadataOnly));
        Assert.That(initial.Window, Is.EqualTo(TimeSpan.Zero));

        await source.SetHistoryRetentionAsync(HistoryRetentionMode.Hybrid, TimeSpan.FromDays(7));

        var updated = await source.GetHistoryRetentionAsync();
        Assert.That(updated.Mode, Is.EqualTo(HistoryRetentionMode.Hybrid));
        Assert.That(updated.Window, Is.EqualTo(TimeSpan.FromDays(7)));
    }

    [Test]
    public void Set_history_retention_rejects_nonpositive_window()
    {
        var source = _cluster.Client.GetGrain<ILattice>("hist-cfg-bad-src");
        Assert.That(
            async () => await source.SetHistoryRetentionAsync(null, TimeSpan.Zero),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task History_view_can_be_disabled_by_deleting_the_runtime_view()
    {
        const string tree = "hist-disable-src";
        const string view = "hist-disable-view";
        var source = _cluster.Client.GetGrain<ILattice>(tree);
        await CreateHistoryViewAsync(tree, view);
        await source.SetAsync("k", new byte[] { 1 });
        await DrainToZeroAsync(view);

        await Factory.DeleteAsync(view);

        var remaining = await Factory.GetAsync(view);
        Assert.That(remaining, Is.Null, "deleting the runtime history view disables history for the tree");
    }
}
