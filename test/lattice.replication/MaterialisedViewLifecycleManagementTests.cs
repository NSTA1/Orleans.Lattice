using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Views;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// End-to-end lifecycle coverage for materialised views: durable runtime-view
/// registration (a runtime <see cref="ILatticeViewFactory.Create"/> survives a
/// silo restart / catalog loss) and the view deletion API
/// (<see cref="ILatticeViewFactory.DeleteAsync"/>). A single silo registers a
/// startup-declared view (used only to prove that deleting a declared view is
/// rejected); every other view is created at runtime.
/// </summary>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public class MaterialisedViewLifecycleManagementTests
{
    private TestCluster _cluster = null!;

    private const string StartupViewName = "mv-lifecycle-startup-view";
    private const string StartupSourceTreeId = "mv-lifecycle-startup-src";

    private IServiceProvider SiloServices =>
        _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

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

    /// <summary>
    /// A deterministic, DI-constructable identity projection. Re-hydration resolves
    /// the projection from the silo service provider by this concrete type, so it
    /// must be public and constructable without configuration.
    /// </summary>
    public sealed class IdentityViewProjection : ILatticeViewProjection
    {
        public string ProjectionVersion => "identity-lifecycle-v1";

        public IEnumerable<ViewWrite> Project(LatticeMutation mutation)
        {
            switch (mutation.Kind)
            {
                case MutationKind.Set:
                    yield return ViewWrite.Upsert(mutation.Key, mutation.Value!, mutation.Timestamp, mutation.ExpiresAtTicks, mutation.Key);
                    break;
                case MutationKind.Delete:
                case MutationKind.Tombstone:
                    yield return ViewWrite.Delete(mutation.Key, mutation.Timestamp, mutation.Key);
                    break;
                default:
                    yield break;
            }
        }
    }

    /// <summary>
    /// A deterministic, DI-constructable count-by-group aggregation projection that
    /// groups every source key under one fixed group, used to exercise the
    /// aggregation re-hydration branch (the registry persists <c>IsAggregation</c>
    /// and the rehydrator resolves an <see cref="ILatticeAggregationProjection"/>).
    /// </summary>
    public sealed class CountAllAggregationProjection : ILatticeAggregationProjection
    {
        public const string GroupKey = "all";

        public string ProjectionVersion => "count-all-lifecycle-v1";

        public AggregationKind Aggregation => AggregationKind.Count;

        public IEnumerable<AggregationContribution> Project(LatticeMutation mutation)
        {
            switch (mutation.Kind)
            {
                case MutationKind.Set:
                    yield return AggregationContribution.Membership(GroupKey, mutation.Key, mutation.Timestamp);
                    break;
                case MutationKind.Delete:
                case MutationKind.Tombstone:
                    yield return AggregationContribution.Retract(mutation.Key, mutation.Timestamp);
                    break;
                default:
                    yield break;
            }
        }
    }

    private ILatticeViewFactory Factory => SiloServices.GetRequiredService<ILatticeViewFactory>();

    private ILatticeView CreateRuntimeView(string sourceTreeId, string viewName)
    {
        var source = _cluster.Client.GetGrain<ILattice>(sourceTreeId);
        return Factory.Create(source, viewName, new LatticeViewDefinition(viewName, new IdentityViewProjection()));
    }

    private ILatticeView CreateRuntimeAggregationView(string sourceTreeId, string viewName)
    {
        var source = _cluster.Client.GetGrain<ILattice>(sourceTreeId);
        return Factory.Create(source, viewName, new LatticeViewDefinition(viewName, new CountAllAggregationProjection()));
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

        Assert.Fail($"View '{viewName}' did not catch up to the source head.");
    }

    private async Task<ILattice> ActiveViewTreeAsync(string viewName)
    {
        var maintainer = _cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        var treeId = await maintainer.GetActiveTreeIdAsync();
        return _cluster.Client.GetGrain<ILattice>(treeId);
    }

    // White-box helper: read a key straight from a view's active backing tree the
    // way an authorised internal reader (the maintainer or an ILatticeView handle)
    // would, by opening a ViewReadContext scope. This bypasses the public
    // read-guard so these lifecycle assertions exercise backing-tree state (e.g.
    // a soft-deleted generation still throwing) rather than the guard itself.
    private static async Task<byte[]?> ReadActiveViewKeyAsync(ILattice viewTree, string key)
    {
        using var scope = ViewReadContext.BeginScope();
        return await viewTree.GetAsync(key);
    }

    private IViewRegistryGrain RegistryGrain =>
        _cluster.Client.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey);

    private async Task WaitForDurableRegistrationAsync(string viewName)
    {
        for (var attempt = 0; attempt < 50; attempt++)
        {
            if ((await RegistryGrain.ListAsync()).Any(r => r.ViewName == viewName))
            {
                return;
            }

            await Task.Delay(20);
        }

        Assert.Fail($"View '{viewName}' durable registration did not land.");
    }

    [Test]
    public async Task Create_persists_a_durable_runtime_registration()
    {
        const string tree = "mv-reg-src";
        const string view = "mv-reg-view";
        _ = CreateRuntimeView(tree, view);
        await DrainToZeroAsync(view);

        var records = await RegistryGrain.ListAsync();
        var record = records.FirstOrDefault(r => r.ViewName == view);

        Assert.That(record, Is.Not.Null, "Create should persist a durable runtime registration");
        Assert.Multiple(() =>
        {
            Assert.That(record!.SourceTreeId, Is.EqualTo(tree));
            Assert.That(record.IsAggregation, Is.False);
            Assert.That(record.ProjectionVersion, Is.EqualTo(new IdentityViewProjection().ProjectionVersion));
            // The persisted identity is the projection's version-free full name, so
            // a package bump does not strand the view on re-hydration.
            Assert.That(record.ProjectionTypeName, Is.EqualTo(typeof(IdentityViewProjection).FullName));
            Assert.That(record.ProjectionTypeName, Does.Not.Contain("Version="));
        });
    }

    [Test]
    public async Task Runtime_view_resumes_maintaining_after_catalog_loss()
    {
        const string tree = "mv-resume-src";
        const string view = "mv-resume-view";
        var source = _cluster.Client.GetGrain<ILattice>(tree);
        _ = CreateRuntimeView(tree, view);

        await source.SetAsync("k1", "v1"u8.ToArray());
        await DrainToZeroAsync(view);
        Assert.That(await ReadActiveViewKeyAsync(await ActiveViewTreeAsync(view), "k1"), Is.Not.Null, "the view should materialise the first source write");

        // Simulate a silo restart's effect on the in-memory catalog: the durable
        // checkpoint and registry survive, but the catalog entry is gone. A
        // maintainer woken by its keepalive reminder must re-hydrate from the
        // durable registry and resume - without the application re-calling Create.
        SiloServices.GetRequiredService<IViewCatalog>().Remove(view);
        Assert.That(SiloServices.GetRequiredService<IViewCatalog>().TryGet(view), Is.Null, "precondition: the catalog entry is gone");

        // EnsureActiveAsync is the reminder-driven activation path.
        await _cluster.Client.GetGrain<IViewMaintainerGrain>(view).EnsureActiveAsync();

        await source.SetAsync("k2", "v2"u8.ToArray());
        await DrainToZeroAsync(view);

        var viewTree = await ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(SiloServices.GetRequiredService<IViewCatalog>().TryGet(view), Is.Not.Null, "re-hydration should re-register the view in the catalog");
            Assert.That(await ReadActiveViewKeyAsync(viewTree, "k1"), Is.Not.Null, "the pre-restart entry should still be materialised");
            Assert.That(await ReadActiveViewKeyAsync(viewTree, "k2"), Is.Not.Null, "the resumed maintainer should apply a post-restart source write");
        });
    }

    [Test]
    public async Task DeleteAsync_removes_registration_catalog_and_backing_tree()
    {
        const string tree = "mv-del-src";
        const string view = "mv-del-view";
        var source = _cluster.Client.GetGrain<ILattice>(tree);
        _ = CreateRuntimeView(tree, view);
        await source.SetAsync("d1", "v1"u8.ToArray());
        await DrainToZeroAsync(view);

        var viewTreeBefore = await ActiveViewTreeAsync(view);
        Assert.That(await ReadActiveViewKeyAsync(viewTreeBefore, "d1"), Is.Not.Null, "precondition: the view holds the source entry");

        await Factory.DeleteAsync(view);

        var records = await RegistryGrain.ListAsync();
        Assert.Multiple(() =>
        {
            Assert.That(records.Any(r => r.ViewName == view), Is.False, "the durable runtime registration should be removed");
            Assert.That(SiloServices.GetRequiredService<IViewCatalog>().TryGet(view), Is.Null, "the catalog entry should be removed");
        });

        // The backing tree is soft-deleted, so a read against it now throws even
        // under an authorised view-read scope.
        Assert.ThrowsAsync<InvalidOperationException>(
            () => ReadActiveViewKeyAsync(viewTreeBefore, "d1"),
            "the backing view tree should be deleted");

        // The view name is free to be re-created. It rebuilds onto a fresh backing
        // tree (never reusing the soft-deleted generation) and re-derives its
        // contents from the still-present source.
        _ = CreateRuntimeView(tree, view);
        await DrainToZeroAsync(view);
        var recreated = await ActiveViewTreeAsync(view);
        Assert.That(await ReadActiveViewKeyAsync(recreated, "d1"), Is.Not.Null, "a re-created view rebuilds onto a fresh tree and re-derives from the source");
    }

    [Test]
    public async Task DeleteAsync_is_idempotent()
    {
        const string tree = "mv-del-idem-src";
        const string view = "mv-del-idem-view";
        _ = CreateRuntimeView(tree, view);
        await DrainToZeroAsync(view);

        await Factory.DeleteAsync(view);
        Assert.DoesNotThrowAsync(() => Factory.DeleteAsync(view), "re-deleting an already-deleted view must be an idempotent no-op");
    }

    [Test]
    public void DeleteAsync_nonexistent_view_is_a_noop()
    {
        Assert.DoesNotThrowAsync(
            () => Factory.DeleteAsync("mv-never-created-view"),
            "deleting a view that was never created must be an idempotent no-op");
    }

    [Test]
    public void DeleteAsync_startup_declared_view_is_rejected()
    {
        Assert.ThrowsAsync<InvalidOperationException>(
            () => Factory.DeleteAsync(StartupViewName),
            "deleting a startup-declared view must be rejected because the declaration re-creates it on the next start");
    }

    [Test]
    public async Task DeleteAsync_releases_source_wal_pin_even_after_catalog_loss()
    {
        const string tree = "mv-pin-src";
        const string view = "mv-pin-view";
        const string viewConsumerId = "view:" + view;
        var source = _cluster.Client.GetGrain<ILattice>(tree);
        _ = CreateRuntimeView(tree, view);
        await source.SetAsync("p1", "v1"u8.ToArray());
        await DrainToZeroAsync(view);

        var cursors = SiloServices.GetRequiredService<IWalCursorRegistry>();
        var pinnedBefore = await cursors.SnapshotAsync(tree);
        Assert.That(pinnedBefore.Any(c => c.ConsumerId == viewConsumerId), Is.True, "precondition: the active view pins the source WAL under its own consumer id");

        // The durable registration is persisted by a fire-and-forget task kicked off
        // by Create; wait for it to land so the decommission's registry fallback has
        // a record to resolve the source tree id from.
        await WaitForDurableRegistrationAsync(view);

        // Simulate the maintainer being decommissioned with no catalog entry (it
        // can activate fresh on a silo that never saw the runtime Create). The
        // source tree id must then be recovered from the durable registry, or the
        // view's WAL cursor pin would leak and hold the source WAL GC forever.
        SiloServices.GetRequiredService<IViewCatalog>().Remove(view);

        await Factory.DeleteAsync(view);

        var pinnedAfter = await cursors.SnapshotAsync(tree);
        Assert.That(pinnedAfter.Any(c => c.ConsumerId == viewConsumerId), Is.False, "deleting the view must release its source WAL cursor pin even when the catalog entry is gone");
    }

    [Test]
    public async Task Aggregation_view_persists_registration_and_resumes_after_catalog_loss()
    {
        const string tree = "mv-agg-src";
        const string view = "mv-agg-view";
        var source = _cluster.Client.GetGrain<ILattice>(tree);
        var handle = CreateRuntimeAggregationView(tree, view);

        await source.SetAsync("a1", "v1"u8.ToArray());
        await source.SetAsync("a2", "v2"u8.ToArray());
        await DrainToZeroAsync(view);
        Assert.That(await handle.GetAggregateInt64Async(CountAllAggregationProjection.GroupKey), Is.EqualTo(2), "the aggregation view should count both source entries");

        var record = (await RegistryGrain.ListAsync()).FirstOrDefault(r => r.ViewName == view);
        Assert.That(record, Is.Not.Null);
        Assert.That(record!.IsAggregation, Is.True, "an aggregation view's durable registration must flag it as an aggregation");

        // Re-hydrate through the aggregation branch of the rehydrator.
        SiloServices.GetRequiredService<IViewCatalog>().Remove(view);
        await _cluster.Client.GetGrain<IViewMaintainerGrain>(view).EnsureActiveAsync();

        await source.SetAsync("a3", "v3"u8.ToArray());
        await DrainToZeroAsync(view);
        Assert.That(await handle.GetAggregateInt64Async(CountAllAggregationProjection.GroupKey), Is.EqualTo(3), "the resumed aggregation maintainer should fold in a post-restart source write");

        await Factory.DeleteAsync(view);
        Assert.That((await RegistryGrain.ListAsync()).Any(r => r.ViewName == view), Is.False, "deleting an aggregation view should remove its durable registration");
    }

    [Test]
    public async Task DeleteTreeAsync_on_a_source_with_a_runtime_view_is_rejected()
    {
        const string tree = "mv-srcdel-runtime-src";
        const string view = "mv-srcdel-runtime-view";
        var source = _cluster.Client.GetGrain<ILattice>(tree);
        _ = CreateRuntimeView(tree, view);
        await DrainToZeroAsync(view);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => source.DeleteTreeAsync(),
            "deleting a source tree that still has a dependent view must be rejected");
        Assert.That(ex!.Message, Does.Contain(view), "the rejection should name the blocking view");

        // After the dependent view is deleted, the source tree is free to delete.
        await Factory.DeleteAsync(view);
        Assert.DoesNotThrowAsync(
            () => source.DeleteTreeAsync(),
            "once the dependent view is gone the source tree should delete");
    }

    [Test]
    public void DeleteTreeAsync_on_a_source_with_a_startup_view_is_rejected()
    {
        var source = _cluster.Client.GetGrain<ILattice>(StartupSourceTreeId);
        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => source.DeleteTreeAsync(),
            "deleting a source tree that backs a startup-declared view must be rejected");
        Assert.That(ex!.Message, Does.Contain(StartupViewName), "the rejection should name the blocking startup view");
    }

    [Test]
    public async Task DeleteTreeAsync_on_a_source_with_no_views_succeeds()
    {
        const string tree = "mv-srcdel-noview-src";
        var source = _cluster.Client.GetGrain<ILattice>(tree);
        await source.SetAsync("k", "v"u8.ToArray());

        Assert.DoesNotThrowAsync(
            () => source.DeleteTreeAsync(),
            "a tree with no dependent views must delete normally");
    }

    [Test]
    public void Create_over_a_view_tree_source_is_rejected()
    {
        var viewSource = _cluster.Client.GetGrain<ILattice>("view-some-existing-view");
        Assert.Throws<InvalidOperationException>(
            () => Factory.Create(viewSource, "mv-chained-view", new LatticeViewDefinition("mv-chained-view", new IdentityViewProjection())),
            "creating a view whose source is itself a view tree must be rejected");
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeViews(views => views.AddView(
                StartupViewName,
                StartupSourceTreeId,
                new IdentityViewProjection()));

            // Keep the background drain timer dormant so convergence is driven
            // deterministically via explicit DrainAsync, and shrink the read-handle
            // cache so a re-created view's fresh tree is resolved promptly.
            siloBuilder.Services.ConfigureAll<LatticeViewOptions>(o =>
            {
                o.CoalesceWindow = TimeSpan.FromMinutes(5);
                o.ReadHandleCacheTtl = TimeSpan.FromMilliseconds(50);
                o.OldGenerationReclaimGrace = TimeSpan.FromMilliseconds(200);
            });
        }
    }
}
