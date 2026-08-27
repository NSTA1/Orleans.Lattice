using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantUsageMeteringService"/>, the cadence driver whose
/// absence made every per-tenant quota inert (issue #1688).
/// </summary>
/// <remarks>
/// <para>
/// <see cref="TenantUsagePublisher"/> is the only thing that writes a usage sample,
/// and it is documented as the "cadence-driven side of the accounting layer (the
/// caller supplies both the cadence and a monotonic stamp)" - but no production
/// caller supplied that cadence, so no sample ever landed and
/// <see cref="LatticeTenantAdmissionController"/> permanently took its documented
/// "fail open until the first sample lands" branch. These tests pin that a cycle
/// actually meters, that it attributes usage to the owning tenant, and that the
/// reserved default tenant is skipped.
/// </para>
/// <para>
/// The loop is driven directly through <c>MeterOnceAsync</c> so every assertion is
/// deterministic - no timer, no cluster.
/// </para>
/// </remarks>
[TestFixture]
public sealed class TenantUsageMeteringServiceTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Globex = TenantId.Parse("globex");

    /// <summary>An in-memory registry returning a fixed tenant roster.</summary>
    private sealed class FakeRegistry(params TenantId[] tenants) : ITenantRegistry
    {
        public Task<TenantRecord?> GetAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
            Task.FromResult<TenantRecord?>(null);

        public Task<bool> ExistsAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
            Task.FromResult(true);

        public async IAsyncEnumerable<TenantRecord> ListAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var tenant in tenants)
            {
                yield return TenantRecord.Create(
                    tenant,
                    TenantStatus.Active,
                    TenantQuotas.Unbounded,
                    TenantPlacement.Shared,
                    HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                    "test");
            }

            await Task.CompletedTask;
        }

        public Task<TenantRecord> PutAsync(TenantRecord record, CancellationToken cancellationToken = default) =>
            Task.FromResult(record);

        public Task<bool> DeleteAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
            Task.FromResult(true);
    }

    /// <summary>Records what the publisher was handed, so a cycle's output is observable.</summary>
    private sealed class RecordingStore : ITenantUsageStore
    {
        public List<TenantUsageRecord> Published { get; } = [];

        public Task<TenantUsageRecord?> GetAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
            Task.FromResult<TenantUsageRecord?>(null);

        public async IAsyncEnumerable<TenantUsageRecord> ListAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }

        public Task<TenantUsageRecord> PublishAsync(TenantUsageRecord record, CancellationToken cancellationToken = default)
        {
            Published.Add(record);
            return Task.FromResult(record);
        }
    }

    private static TenantUsageMeteringService Create(
        ITenantRegistry registry,
        RecordingStore store,
        IGrainFactory grainFactory,
        TimeSpan? interval = null)
    {
        var options = Substitute.For<IOptionsMonitor<TenantUsageAccountingOptions>>();
        options.CurrentValue.Returns(new TenantUsageAccountingOptions
        {
            MeterInterval = interval ?? TimeSpan.FromSeconds(30),
            // Publish every movement so a test's first cycle is never suppressed by
            // the hysteresis band.
            PublishMinAbsoluteDelta = 0,
            PublishMinRelativeDelta = 0,
        });

        var publisher = new TenantUsagePublisher(
            store,
            Options.Create(new Orleans.Configuration.ClusterOptions { ClusterId = "cluster-a" }),
            options);

        return new TenantUsageMeteringService(
            registry,
            publisher,
            grainFactory,
            TimeProvider.System,
            options,
            NullLogger<TenantUsageMeteringService>.Instance);
    }

    /// <summary>Wires a registry grain returning <paramref name="treeIds"/> for any prefix, and a usage report per tree.</summary>
    private static IGrainFactory GrainFactoryWith(
        IReadOnlyList<string> treeIds,
        long bytesPerTree = 100,
        long keysPerTree = 10)
    {
        var grainFactory = Substitute.For<IGrainFactory>();

        var registryGrain = Substitute.For<ILatticeRegistry>();
        registryGrain.GetAllTreeIdsAsync(Arg.Any<string?>()).Returns(call =>
        {
            var prefix = call.Arg<string?>();
            return Task.FromResult<IReadOnlyList<string>>(
                string.IsNullOrEmpty(prefix)
                    ? treeIds
                    : treeIds.Where(id => id.StartsWith(prefix, StringComparison.Ordinal)).ToList());
        });
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registryGrain);

        foreach (var treeId in treeIds)
        {
            var tree = Substitute.For<ILattice>();
            tree.GetStorageUsageAsync(Arg.Any<CancellationToken>()).Returns(
                Task.FromResult(new TreeStorageUsageReport
                {
                    TreeId = treeId,
                    TotalBytes = bytesPerTree,
                    LiveKeys = keysPerTree,
                    LeafStateBytes = bytesPerTree,
                }));
            grainFactory.GetGrain<ILattice>(treeId).Returns(tree);
        }

        return grainFactory;
    }

    [Test]
    public async Task A_metering_cycle_publishes_a_usage_slot_for_each_tenant()
    {
        var store = new RecordingStore();
        var service = Create(
            new FakeRegistry(Acme, Globex),
            store,
            GrainFactoryWith(["t/acme/orders", "t/globex/secrets"]));

        await service.MeterOnceAsync(CancellationToken.None);

        // The whole point: without this driver nothing ever published, so admission
        // never had a sample to admit against and quotas could not bind.
        Assert.That(store.Published, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task Usage_is_attributed_to_the_owning_tenant_only()
    {
        var store = new RecordingStore();
        var service = Create(
            new FakeRegistry(Acme),
            store,
            GrainFactoryWith(["t/acme/orders", "t/acme/users", "t/globex/secrets"], bytesPerTree: 100, keysPerTree: 5));

        await service.MeterOnceAsync(CancellationToken.None);

        var record = store.Published.Single();
        var sample = record.LocalSample("cluster-a");
        Assert.Multiple(() =>
        {
            Assert.That(record.Id, Is.EqualTo(Acme));
            Assert.That(sample.Bytes, Is.EqualTo(200), "only acme's two trees are folded in, not globex's");
            Assert.That(sample.Keys, Is.EqualTo(10));
        });
    }

    [Test]
    public async Task The_reserved_default_tenant_is_not_metered()
    {
        var store = new RecordingStore();
        var service = Create(
            new FakeRegistry(TenantId.Default, Acme),
            store,
            GrainFactoryWith(["t/acme/orders"]));

        await service.MeterOnceAsync(CancellationToken.None);

        // The default tenant is unbounded and cannot be given quotas, so a slot for
        // it would be one nothing ever consults.
        Assert.That(store.Published.Select(r => r.Id), Is.EqualTo(new[] { Acme }));
    }

    [Test]
    public async Task A_tenant_with_no_trees_publishes_nothing()
    {
        var store = new RecordingStore();
        var service = Create(new FakeRegistry(Acme), store, GrainFactoryWith([]));

        await service.MeterOnceAsync(CancellationToken.None);

        // An empty roll-up has not moved from the empty baseline, so the hysteresis
        // gate suppresses it. That is correct and harmless: a tenant with no trees
        // has no footprint to exceed, so leaving admission on its fail-open branch
        // for it changes nothing. The first cycle after real data lands moves off
        // empty, clears any band, and publishes.
        Assert.That(store.Published, Is.Empty);
    }

    [Test]
    public async Task The_first_cycle_with_real_usage_publishes_and_arms_enforcement()
    {
        var store = new RecordingStore();
        var service = Create(
            new FakeRegistry(Acme),
            store,
            GrainFactoryWith(["t/acme/orders"], bytesPerTree: 4096, keysPerTree: 32));

        await service.MeterOnceAsync(CancellationToken.None);

        // This is the transition that makes a quota bind: once a sample lands the
        // admission controller stops taking its fail-open branch for this tenant.
        var sample = store.Published.Single().LocalSample("cluster-a");
        Assert.Multiple(() =>
        {
            Assert.That(sample.Bytes, Is.EqualTo(4096));
            Assert.That(sample.Keys, Is.EqualTo(32));
        });
    }

    [Test]
    public async Task An_unreadable_tree_does_not_abandon_the_tenants_rollup()
    {
        var grainFactory = GrainFactoryWith(["t/acme/good"]);
        var broken = Substitute.For<ILattice>();
        broken.GetStorageUsageAsync(Arg.Any<CancellationToken>())
            .Returns<Task<TreeStorageUsageReport>>(_ => throw new InvalidOperationException("tree is down"));
        grainFactory.GetGrain<ILattice>("t/acme/broken").Returns(broken);

        var registryGrain = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registryGrain.GetAllTreeIdsAsync(Arg.Any<string?>())
            .Returns(Task.FromResult<IReadOnlyList<string>>(["t/acme/good", "t/acme/broken"]));

        var store = new RecordingStore();
        var service = Create(new FakeRegistry(Acme), store, grainFactory);

        await service.MeterOnceAsync(CancellationToken.None);

        Assert.That(store.Published, Has.Count.EqualTo(1),
            "one unreadable tree contributes nothing but must not fault the cycle");
    }

    [Test]
    public void Constructor_rejects_a_null_dependency()
    {
        var store = new RecordingStore();
        var options = Substitute.For<IOptionsMonitor<TenantUsageAccountingOptions>>();
        options.CurrentValue.Returns(new TenantUsageAccountingOptions());
        var publisher = new TenantUsagePublisher(
            store, Options.Create(new Orleans.Configuration.ClusterOptions()), options);
        var grainFactory = Substitute.For<IGrainFactory>();

        Assert.Multiple(() =>
        {
            Assert.That(() => new TenantUsageMeteringService(
                null!, publisher, grainFactory, TimeProvider.System, options,
                NullLogger<TenantUsageMeteringService>.Instance), Throws.ArgumentNullException);
            Assert.That(() => new TenantUsageMeteringService(
                new FakeRegistry(), null!, grainFactory, TimeProvider.System, options,
                NullLogger<TenantUsageMeteringService>.Instance), Throws.ArgumentNullException);
            Assert.That(() => new TenantUsageMeteringService(
                new FakeRegistry(), publisher, null!, TimeProvider.System, options,
                NullLogger<TenantUsageMeteringService>.Instance), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task A_non_positive_interval_disables_metering()
    {
        var store = new RecordingStore();
        var service = Create(new FakeRegistry(Acme), store, GrainFactoryWith(["t/acme/orders"]), TimeSpan.Zero);

        await service.StartAsync(CancellationToken.None);

        Assert.That(service.Loop, Is.Null, "a zero cadence opts the deployment out of metering entirely");
        await service.StopAsync(CancellationToken.None);
    }
}
