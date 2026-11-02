using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Views;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// The built-in change-history runtime provider (#5611) end to end: a host that
/// registers no provider of its own can list it, create a change-history view
/// through it, read the timeline from the view, re-hydrate it from its durable
/// record, and drop it again, releasing the source WAL pin.
/// </summary>
[TestFixture]
[Category("Integration")]
public class HistoryRuntimeViewProviderIntegrationTests
{
    private TestCluster _cluster = null!;

    private IServiceProvider SiloServices
        => ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services;

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
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public void The_history_provider_is_offered_for_operator_creation()
    {
        var catalog = SiloServices.GetRequiredService<RuntimeViewProjectionProviderCatalog>();
        var offered = catalog.HostProviders.SingleOrDefault(p => p.ProviderKey == LatticeHistoryView.ProviderKey);

        Assert.That(offered, Is.Not.Null, "AddLatticeViews offers the history provider with no host provider registered");
        Assert.That(offered!.Description, Is.SameAs(HistoryRuntimeViewProvider.Description));
    }

    [Test]
    public async Task A_view_created_through_the_descriptor_keeps_every_revision_and_survives_rehydration()
    {
        const string sourceTreeId = "hist-provider-src";
        const string viewName = "hist-provider-view";
        var factory = SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _cluster.Client.GetGrain<ILattice>(sourceTreeId);

        await factory.CreateAsync(source, viewName, LatticeHistoryView.Descriptor);
        var maintainer = _cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
        await source.SetAsync("k", [1]);
        await source.SetAsync("k", [2]);
        await source.SetAsync("k", [3]);
        await maintainer.WaitForSourceHeadAsync(TimeSpan.FromSeconds(30));

        var page = await source.ScanEntryHistoryAsync("k", null, null, 10, null);
        var record = (await _cluster.Client.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).ListAsync())
            .Single(r => r.ViewName == viewName);
        var rehydrated = RuntimeViewRehydrator.Resolve(
            record,
            SiloServices,
            SiloServices.GetRequiredService<RuntimeViewProjectionProviderCatalog>(),
            NullLogger.Instance);

        Assert.Multiple(() =>
        {
            Assert.That(page.Source, Is.EqualTo(EntryHistorySource.View), "the timeline is read from the history view");
            Assert.That(page.Revisions, Has.Count.EqualTo(3), "the view kept every revision");
            Assert.That(record.ProjectionProviderKey, Is.EqualTo(LatticeHistoryView.ProviderKey),
                "the view is persisted against the built-in provider");
            Assert.That(record.Accumulative, Is.True);
            Assert.That(rehydrated, Is.Not.Null, "the durable record re-hydrates through the built-in provider");
            Assert.That(rehydrated!.Projection, Is.InstanceOf<HistoryLatticeViewProjection>());
            Assert.That(rehydrated.Accumulative, Is.True);
        });
    }

    [Test]
    public async Task A_history_read_on_a_silo_whose_catalog_never_saw_the_view_still_reads_the_view()
    {
        const string sourceTreeId = "hist-provider-remote-src";
        const string viewName = "hist-provider-remote";
        var factory = SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _cluster.Client.GetGrain<ILattice>(sourceTreeId);

        await factory.CreateAsync(source, viewName, LatticeHistoryView.Descriptor);
        var maintainer = _cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
        await source.SetAsync("k", [1]);
        await source.SetAsync("k", [2]);
        await maintainer.WaitForSourceHeadAsync(TimeSpan.FromSeconds(30));

        // A runtime view lives in the catalog of the silo that created it; another silo's
        // catalog has no entry for it until its maintainer re-hydrates there. Model that
        // silo by dropping the local entry: the durable registry still holds the view.
        SiloServices.GetRequiredService<IViewCatalog>().Remove(viewName);
        var page = await source.ScanEntryHistoryAsync("k", null, null, 10, null);
        var again = await source.ScanEntryHistoryAsync("k", null, null, 10, null);

        // A remembered resolution must never outlive the view: once it is dropped,
        // the next read falls back to the write-ahead-log window rather than reading
        // a decommissioned view's empty generation.
        await factory.DeleteAsync(viewName);
        var dropped = await source.ScanEntryHistoryAsync("k", null, null, 10, null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Source, Is.EqualTo(EntryHistorySource.View),
                "the read finds the history view through the durable registry, not only the local catalog");
            Assert.That(page.Revisions, Has.Count.EqualTo(2));
            Assert.That(again.Source, Is.EqualTo(EntryHistorySource.View), "a repeat read still finds the view");
            Assert.That(again.Revisions, Has.Count.EqualTo(2));
            Assert.That(dropped.Source, Is.Not.EqualTo(EntryHistorySource.View), "a dropped view is never read");
        });
    }

    [Test]
    public void Creating_with_a_payload_that_carries_settings_is_refused()
    {
        var factory = SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _cluster.Client.GetGrain<ILattice>("hist-provider-refused-src");

        Assert.That(
            async () => await factory.CreateAsync(
                source,
                "hist-provider-refused",
                new LatticeRuntimeViewProjectionDescriptor(LatticeHistoryView.ProviderKey, "{\"retention\":\"full\"}"u8)),
            Throws.ArgumentException.With.Message.Contains("empty JSON object"));
    }

    [Test]
    public async Task Dropping_the_view_releases_its_source_wal_pin()
    {
        const string sourceTreeId = "hist-provider-drop-src";
        const string viewName = "hist-provider-drop";
        var factory = SiloServices.GetRequiredService<ILatticeViewFactory>();
        var cursors = SiloServices.GetRequiredService<IWalCursorRegistry>();
        var source = _cluster.Client.GetGrain<ILattice>(sourceTreeId);

        await factory.CreateAsync(source, viewName, LatticeHistoryView.Descriptor);
        var maintainer = _cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
        await source.SetAsync("k", [1]);
        await maintainer.WaitForSourceHeadAsync(TimeSpan.FromSeconds(30));
        var pinnedBefore = (await cursors.SnapshotAsync(sourceTreeId)).Any(c => c.ConsumerId == $"view:{viewName}");

        await factory.DeleteAsync(viewName);
        var pinnedAfter = (await cursors.SnapshotAsync(sourceTreeId)).Any(c => c.ConsumerId == $"view:{viewName}");

        Assert.Multiple(async () =>
        {
            Assert.That(pinnedBefore, Is.True, "the live view pins its source WAL");
            Assert.That(pinnedAfter, Is.False, "dropping the view releases the pin; the orphan sweep is not needed");
            Assert.That(await factory.GetAsync(viewName), Is.Null, "the view is gone");
        });
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeViews();
        }
    }
}
