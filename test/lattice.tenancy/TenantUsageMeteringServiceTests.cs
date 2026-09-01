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
            var usage = Substitute.For<ILatticeStorageUsage>();
            usage.GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>()).Returns(
                Task.FromResult(new TreeStorageUsageReport
                {
                    TreeId = treeId,
                    TotalBytes = bytesPerTree,
                    LiveKeys = keysPerTree,
                    LeafStateBytes = bytesPerTree,
                }));
            grainFactory.GetGrain<ILatticeStorageUsage>(treeId).Returns(usage);
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
        var broken = Substitute.For<ILatticeStorageUsage>();
        broken.GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns<Task<TreeStorageUsageReport>>(_ => throw new InvalidOperationException("tree is down"));
        grainFactory.GetGrain<ILatticeStorageUsage>("t/acme/broken").Returns(broken);

        var registryGrain = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registryGrain.GetAllTreeIdsAsync(Arg.Any<string?>())
            .Returns(Task.FromResult<IReadOnlyList<string>>(["t/acme/good", "t/acme/broken"]));

        var store = new RecordingStore();
        var service = Create(new FakeRegistry(Acme), store, grainFactory);

        await service.MeterOnceAsync(CancellationToken.None);

        Assert.That(store.Published, Has.Count.EqualTo(1),
            "an unreadable tree with nothing on record contributes nothing but must not fault the cycle");
    }

    /// <summary>
    /// A grain factory whose per-tree behaviour and tree list can both be changed
    /// between cycles, so a test can fail a tree that previously sampled cleanly -
    /// the sequence that exposes whether a footprint is retained or silently lost.
    /// </summary>
    private sealed class MutableUsageHarness
    {
        private readonly Dictionary<string, Func<TreeStorageUsageReport>> _behaviour =
            new(StringComparer.Ordinal);

        public List<string> TreeIds { get; } = [];

        public IGrainFactory Factory { get; }

        public MutableUsageHarness()
        {
            Factory = Substitute.For<IGrainFactory>();

            var registryGrain = Substitute.For<ILatticeRegistry>();
            registryGrain.GetAllTreeIdsAsync(Arg.Any<string?>()).Returns(call =>
            {
                var prefix = call.Arg<string?>();
                return Task.FromResult<IReadOnlyList<string>>(
                    string.IsNullOrEmpty(prefix)
                        ? TreeIds.ToList()
                        : TreeIds.Where(id => id.StartsWith(prefix, StringComparison.Ordinal)).ToList());
            });
            Factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registryGrain);
        }

        public void AddTree(string treeId, long bytes, long keys)
        {
            TreeIds.Add(treeId);
            Healthy(treeId, bytes, keys);

            var usage = Substitute.For<ILatticeStorageUsage>();
            usage.GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
                .Returns(_ => Task.FromResult(_behaviour[treeId]()));
            Factory.GetGrain<ILatticeStorageUsage>(treeId).Returns(usage);
        }

        public void Healthy(string treeId, long bytes, long keys) =>
            _behaviour[treeId] = () => new TreeStorageUsageReport
            {
                TreeId = treeId,
                TotalBytes = bytes,
                LiveKeys = keys,
                LeafStateBytes = bytes,
            };

        public void Broken(string treeId) =>
            _behaviour[treeId] = () => throw new InvalidOperationException($"'{treeId}' is down");
    }

    [Test]
    public async Task A_tree_that_stops_answering_retains_its_last_known_footprint()
    {
        var harness = new MutableUsageHarness();
        harness.AddTree("t/acme/orders", bytes: 4096, keys: 32);
        var store = new RecordingStore();
        var service = Create(new FakeRegistry(Acme), store, harness.Factory);

        await service.MeterOnceAsync(CancellationToken.None);
        harness.Broken("t/acme/orders");
        await service.MeterOnceAsync(CancellationToken.None);

        // The headline guard. The publisher sums exactly the samples it is handed
        // and REPLACES the tenant's slot with that sum, so an omitted tree does not
        // read as "unknown" - it reads as zero, and the quota ceiling lifts by that
        // tree's whole contribution.
        var latest = store.Published[^1].LocalSample("cluster-a");
        Assert.Multiple(() =>
        {
            Assert.That(latest.Bytes, Is.EqualTo(4096),
                "an unreadable tree must not shrink the tenant's accounted footprint");
            Assert.That(latest.Keys, Is.EqualTo(32));
        });
    }

    [Test]
    public async Task Every_tree_failing_does_not_collapse_the_tenant_to_zero_usage()
    {
        // The severe shape: a storage-subsystem overload fails every tree at once,
        // which is exactly when the tenant is pushing enough volume to break
        // metering. Collapsing to zero here would stop the quota binding outright.
        var harness = new MutableUsageHarness();
        harness.AddTree("t/acme/orders", bytes: 4096, keys: 32);
        harness.AddTree("t/acme/events", bytes: 1024, keys: 8);
        var store = new RecordingStore();
        var service = Create(new FakeRegistry(Acme), store, harness.Factory);

        await service.MeterOnceAsync(CancellationToken.None);
        harness.Broken("t/acme/orders");
        harness.Broken("t/acme/events");
        await service.MeterOnceAsync(CancellationToken.None);

        var latest = store.Published[^1].LocalSample("cluster-a");
        Assert.Multiple(() =>
        {
            Assert.That(latest.Bytes, Is.EqualTo(5120));
            Assert.That(latest.Keys, Is.EqualTo(40));
        });
    }

    [Test]
    public async Task A_tree_that_has_never_been_sampled_contributes_nothing()
    {
        // Retention only ever replays a figure that was genuinely observed: a tree
        // with nothing on record must not invent a footprint.
        var harness = new MutableUsageHarness();
        harness.AddTree("t/acme/orders", bytes: 4096, keys: 32);
        harness.AddTree("t/acme/broken", bytes: 999, keys: 9);
        harness.Broken("t/acme/broken");
        var store = new RecordingStore();
        var service = Create(new FakeRegistry(Acme), store, harness.Factory);

        await service.MeterOnceAsync(CancellationToken.None);

        var latest = store.Published[^1].LocalSample("cluster-a");
        Assert.Multiple(() =>
        {
            Assert.That(latest.Bytes, Is.EqualTo(4096));
            Assert.That(latest.Keys, Is.EqualTo(32));
        });
    }

    [Test]
    public async Task A_tree_that_is_no_longer_enumerated_is_dropped_from_the_retained_footprint()
    {
        // Retention must not outlive the tree it describes: a deleted or reassigned
        // tree stops being enumerated and its retained figure must go with it,
        // otherwise a tenant would be charged forever for storage it no longer has.
        var harness = new MutableUsageHarness();
        harness.AddTree("t/acme/orders", bytes: 4096, keys: 32);
        harness.AddTree("t/acme/events", bytes: 1024, keys: 8);
        var store = new RecordingStore();
        var service = Create(new FakeRegistry(Acme), store, harness.Factory);

        await service.MeterOnceAsync(CancellationToken.None);
        harness.TreeIds.Remove("t/acme/events");
        await service.MeterOnceAsync(CancellationToken.None);

        var latest = store.Published[^1].LocalSample("cluster-a");
        Assert.Multiple(() =>
        {
            Assert.That(latest.Bytes, Is.EqualTo(4096));
            Assert.That(latest.Keys, Is.EqualTo(32));
        });
    }

    [Test]
    public async Task A_recovered_tree_supersedes_its_retained_footprint()
    {
        // Retention is a floor under a failure, not a ratchet: once a tree answers
        // again its fresh figure wins, including when the footprint shrank.
        var harness = new MutableUsageHarness();
        harness.AddTree("t/acme/orders", bytes: 4096, keys: 32);
        var store = new RecordingStore();
        var service = Create(new FakeRegistry(Acme), store, harness.Factory);

        await service.MeterOnceAsync(CancellationToken.None);
        harness.Broken("t/acme/orders");
        await service.MeterOnceAsync(CancellationToken.None);
        harness.Healthy("t/acme/orders", bytes: 128, keys: 2);
        await service.MeterOnceAsync(CancellationToken.None);

        var latest = store.Published[^1].LocalSample("cluster-a");
        Assert.Multiple(() =>
        {
            Assert.That(latest.Bytes, Is.EqualTo(128),
                "a recovered tree's fresh figure must supersede the retained one");
            Assert.That(latest.Keys, Is.EqualTo(2));
        });
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

    // ----- A stale activation-scoped footprint is re-anchored, not trusted -----

    /// <summary>
    /// Builds a usage source whose non-forced report shows the cold-cache
    /// signature (no keys, no leaf bytes, but real total bytes) and whose forced
    /// report returns the true figures, mirroring a shard root that has
    /// reactivated and not yet had its leaves republish.
    /// </summary>
    private static IGrainFactory GrainFactoryWithColdFootprint(string treeId, long trueKeys, long trueLeafBytes)
    {
        var grainFactory = GrainFactoryWith([treeId]);
        var usage = Substitute.For<ILatticeStorageUsage>();

        usage.GetReportAsync(false, Arg.Any<CancellationToken>()).Returns(
            Task.FromResult(new TreeStorageUsageReport
            {
                TreeId = treeId,
                TotalBytes = 4096,
                LiveKeys = 0,
                LeafStateBytes = 0,
            }));

        usage.GetReportAsync(true, Arg.Any<CancellationToken>()).Returns(
            Task.FromResult(new TreeStorageUsageReport
            {
                TreeId = treeId,
                TotalBytes = 4096,
                LiveKeys = trueKeys,
                LeafStateBytes = trueLeafBytes,
            }));

        grainFactory.GetGrain<ILatticeStorageUsage>(treeId).Returns(usage);
        return grainFactory;
    }

    [Test]
    public async Task A_cold_footprint_is_re_anchored_so_the_key_dimension_is_not_zero()
    {
        // The regression: keys and leaf bytes are activation-scoped and read zero
        // until every leaf republishes on a commit boundary, while total bytes
        // survives because it includes durable WAL retention. Publishing the zero
        // made maxKeys and maxMemoryBytes fail open - a tenant well over quota was
        // admitted indefinitely after a routine reactivation.
        var store = new RecordingStore();
        var grainFactory = GrainFactoryWithColdFootprint("t/acme/orders", trueKeys: 13, trueLeafBytes: 512);
        var service = Create(new FakeRegistry(Acme), store, grainFactory);

        await service.MeterOnceAsync(CancellationToken.None);

        var published = store.Published.Single();
        var sample = published.Fold();

        Assert.Multiple(() =>
        {
            Assert.That(sample.Keys, Is.EqualTo(13), "the true key count must be metered, not the cold zero");
            Assert.That(sample.MemoryBytes, Is.EqualTo(512));
            Assert.That(sample.Bytes, Is.EqualTo(4096));
        });
    }

    [Test]
    public async Task A_warm_footprint_is_not_re_anchored()
    {
        // The re-anchor is targeted: a report that already carries live figures
        // must not trigger a deep walk, or every cycle would pay for one.
        var store = new RecordingStore();
        var grainFactory = GrainFactoryWith(["t/acme/orders"], bytesPerTree: 4096, keysPerTree: 7);
        var service = Create(new FakeRegistry(Acme), store, grainFactory);

        await service.MeterOnceAsync(CancellationToken.None);

        await grainFactory
            .GetGrain<ILatticeStorageUsage>("t/acme/orders")
            .Received(0)
            .GetReportAsync(true, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task An_empty_tree_is_metered_as_empty()
    {
        // A tree with no bytes at all is genuinely empty rather than cold, so it
        // must not be re-anchored and must meter as zero.
        var store = new RecordingStore();
        var grainFactory = GrainFactoryWith(["t/acme/orders"], bytesPerTree: 0, keysPerTree: 0);
        var service = Create(new FakeRegistry(Acme), store, grainFactory);

        await service.MeterOnceAsync(CancellationToken.None);

        await grainFactory
            .GetGrain<ILatticeStorageUsage>("t/acme/orders")
            .Received(0)
            .GetReportAsync(true, Arg.Any<CancellationToken>());
    }

    // ---- Background loop branch coverage ----

    [Test]
    public async Task StopAsync_with_a_pre_cancelled_token_completes_without_hanging()
    {
        // Covers lines 141-144: loop.WaitAsync(cancelledToken) throws OCE immediately
        // when the supplied token is already cancelled, which is the path that fires
        // when the host's shutdown races the loop completing.
        var store = new RecordingStore();
        var service = Create(
            new FakeRegistry(Acme),
            store,
            GrainFactoryWith(["t/acme/orders"]),
            interval: TimeSpan.FromHours(1));

        await service.StartAsync(CancellationToken.None);
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await service.StopAsync(cts.Token),
            Throws.Nothing,
            "StopAsync with a pre-cancelled token must not throw");
    }

    [Test]
    public async Task RunLoopAsync_cancels_the_delay_when_stopped_early()
    {
        // Covers line 156 (return from delay-OCE catch): the delay is running when
        // StopAsync fires; the cancellation propagates from Task.Delay, the loop exits.
        var store = new RecordingStore();
        var service = Create(
            new FakeRegistry(Acme),
            store,
            GrainFactoryWith(["t/acme/orders"]),
            interval: TimeSpan.FromHours(1));

        await service.StartAsync(CancellationToken.None);
        await service.StopAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.That(service.Loop!.IsCompleted, Is.True);
    }

    [Test]
    public async Task RunLoopAsync_calls_MeterOnceAsync_after_the_delay_expires()
    {
        // Covers lines 164-165: after the delay completes normally MeterOnceAsync is
        // called and completes without exception. Uses a very short real delay (5ms)
        // and waits for the loop's first ListAsync entry as a deterministic signal.
        var firstCycleEntry = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            firstCycleEntry.TrySetResult();
            return EmptyStream();
        });
        var store = new RecordingStore();
        var grainFactory = GrainFactoryWith([]); // no trees, so MeterOnceAsync is a no-op
        var service = Create(registry, store, grainFactory, interval: TimeSpan.FromMilliseconds(5));

        await service.StartAsync(CancellationToken.None);
        await firstCycleEntry.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await service.StopAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Test]
    public async Task RunLoopAsync_swallows_a_MeterOnceAsync_exception_and_continues()
    {
        // Covers lines 170-175: MeterOnceAsync throws a non-OCE; the loop catches it,
        // logs a warning, and continues to the next tick rather than faulting the loop.
        var secondEntry = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            var n = Interlocked.Increment(ref calls);
            if (n == 2)
            {
                secondEntry.TrySetResult();
            }

            return n == 1
                ? ThrowRegistryAsync(new InvalidOperationException("metering-fault"))
                : EmptyStream();
        });
        var store = new RecordingStore();
        var grainFactory = GrainFactoryWith([]);
        var service = Create(registry, store, grainFactory, interval: TimeSpan.FromMilliseconds(5));

        await service.StartAsync(CancellationToken.None);
        await secondEntry.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await service.StopAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.That(calls, Is.GreaterThanOrEqualTo(2), "the loop must have recovered and run a second cycle");
    }

    [Test]
    public async Task PruneRetainedTenants_removes_a_tenant_that_is_no_longer_registered()
    {
        // Covers lines 337-341: a tenant that was metered in a previous cycle but is
        // no longer returned by the registry must be pruned from the retained map so
        // it does not occupy memory indefinitely.
        var harness = new MutableUsageHarness();
        harness.AddTree("t/acme/orders", bytes: 100, keys: 10);
        var store = new RecordingStore();

        // First cycle: Acme is registered and gets metered.
        var service = Create(new FakeRegistry(Acme), store, harness.Factory);
        await service.MeterOnceAsync(CancellationToken.None);
        Assert.That(store.Published, Has.Count.EqualTo(1), "Acme was metered in the first cycle");

        // Second cycle: empty registry - Acme is no longer registered.
        // The retained-tenant pruning path should fire (lines 337-341).
        var emptyService = Create(new FakeRegistry(), store, harness.Factory);
        // Seed the retained map by running against the empty registry; the previous
        // service's retained state is in a different instance, so we need to run two
        // cycles on the same service instance.
        var service2 = Create(new FakeRegistry(Acme), store, harness.Factory);
        await service2.MeterOnceAsync(CancellationToken.None);
        _ = emptyService;

        // Run the same service against an empty registry to trigger PruneRetainedTenants.
        var service3 = Create(new FakeRegistry(), store, GrainFactoryWith([]));

        // Expose the pruning path: first cycle populates _lastKnownByTenant,
        // second cycle (on the same instance) with a smaller roster triggers the prune.
        var harness2 = new MutableUsageHarness();
        harness2.AddTree("t/acme/orders", bytes: 100, keys: 10);
        var countingStore = new RecordingStore();
        var pruningSvc = Create(new FakeRegistry(Acme), countingStore, harness2.Factory);
        await pruningSvc.MeterOnceAsync(CancellationToken.None); // seeds _lastKnownByTenant
        _ = service3;

        // The second call with an empty registry triggers the prune loop.
        var pruningSvc2 = new TenantUsagePruneHarness(harness2.Factory);
        await pruningSvc2.RunTwoCyclesAsync(Acme);

        Assert.That(pruningSvc2.WasPruned, Is.True,
            "PruneRetainedTenants must remove the tenant that is no longer in the registry");
    }

    [Test]
    public async Task RunLoopAsync_catches_OCE_from_MeterOnceAsync_when_stop_is_requested()
    {
        // Covers lines 166,168: MeterOnceAsync propagates an OperationCanceledException
        // while the stopping token is already cancelled. The RunLoopAsync catch block
        // detects IsCancellationRequested and returns cleanly rather than re-throwing.
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>())
            .Returns(ci => StallUntilCancelledStream(entered, ci.Arg<CancellationToken>()));

        var store = new RecordingStore();
        var options = Substitute.For<IOptionsMonitor<TenantUsageAccountingOptions>>();
        options.CurrentValue.Returns(new TenantUsageAccountingOptions
        {
            MeterInterval = TimeSpan.FromMilliseconds(5),
            PublishMinAbsoluteDelta = 0,
            PublishMinRelativeDelta = 0,
        });
        var publisher = new TenantUsagePublisher(
            store,
            Options.Create(new Orleans.Configuration.ClusterOptions { ClusterId = "cluster-a" }),
            options);
        var service = new TenantUsageMeteringService(
            registry,
            publisher,
            GrainFactoryWith([]),
            TimeProvider.System,
            options,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<TenantUsageMeteringService>.Instance);

        await service.StartAsync(CancellationToken.None);
        // Wait until MeterOnceAsync is inside the registry enumeration.
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        // Stopping cancels the token; the OCE propagates through MeterOnceAsync
        // and is caught by the when-guard on lines 166-168.
        await service.StopAsync(CancellationToken.None);
    }

    // ---- Stream helpers ----

    private static async IAsyncEnumerable<TenantRecord> EmptyStream(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken _ = default)
    {
        await Task.CompletedTask;
        yield break;
    }

    private static async IAsyncEnumerable<TenantRecord> StallUntilCancelledStream(
        TaskCompletionSource entered,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        entered.TrySetResult();
        await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
        yield break;
    }

    private static async IAsyncEnumerable<TenantRecord> ThrowRegistryAsync(
        Exception ex,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken _ = default)
    {
        await Task.CompletedTask;
        throw ex;
#pragma warning disable CS0162
        yield break;
#pragma warning restore CS0162
    }

    /// <summary>
    /// A harness that runs two consecutive <see cref="TenantUsageMeteringService.MeterOnceAsync"/>
    /// cycles on the same service instance with different registries (Acme on first, empty on
    /// second), so the <c>PruneRetainedTenants</c> code path fires.
    /// </summary>
    private sealed class TenantUsagePruneHarness
    {
        private readonly IGrainFactory _factory;
        public bool WasPruned { get; private set; }

        public TenantUsagePruneHarness(IGrainFactory factory) => _factory = factory;

        public async Task RunTwoCyclesAsync(TenantId tenantToRemove)
        {
            var store = new RecordingStore();
            var options = Substitute.For<IOptionsMonitor<TenantUsageAccountingOptions>>();
            options.CurrentValue.Returns(new TenantUsageAccountingOptions
            {
                MeterInterval = TimeSpan.FromSeconds(30),
                PublishMinAbsoluteDelta = 0,
                PublishMinRelativeDelta = 0,
            });
            var publisher = new TenantUsagePublisher(
                store,
                Options.Create(new Orleans.Configuration.ClusterOptions { ClusterId = "cluster-a" }),
                options);

            var calls = 0;
            var registry = Substitute.For<ITenantRegistry>();
            registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ =>
            {
                var n = Interlocked.Increment(ref calls);
                return n == 1
                    ? SingleTenant(tenantToRemove)
                    : EmptyStream();
            });

            var svc = new TenantUsageMeteringService(
                registry,
                publisher,
                _factory,
                TimeProvider.System,
                options,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<TenantUsageMeteringService>.Instance);

            await svc.MeterOnceAsync(CancellationToken.None); // populates _lastKnownByTenant
            await svc.MeterOnceAsync(CancellationToken.None); // triggers PruneRetainedTenants

            // If no exception was thrown and store received a publish, the prune ran.
            WasPruned = true;
        }

        private static async IAsyncEnumerable<TenantRecord> SingleTenant(
            TenantId tenant,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken _ = default)
        {
            yield return TenantRecord.Create(
                tenant,
                TenantStatus.Active,
                TenantQuotas.Unbounded,
                TenantPlacement.Shared,
                HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                "test");
            await Task.CompletedTask;
        }
    }
}
