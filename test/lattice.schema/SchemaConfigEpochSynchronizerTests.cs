using System.Collections.Immutable;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit coverage for <see cref="SchemaConfigEpochSynchronizer{TCache}"/>, which keeps
/// a silo's cached schema configuration current with changes committed on other
/// silos (#5238). The mutation observer fires only on the silo hosting the
/// committing leaf, so without it every other silo kept its cached answer.
/// </summary>
[TestFixture]
public sealed class SchemaConfigEpochSynchronizerTests
{
    private const string ConfigTree = "sys-schema-policy";

    private static readonly Guid Incarnation = Guid.NewGuid();

    [Test]
    public async Task PollOnceAsync_first_poll_drops_the_cache()
    {
        var rig = Rig.Create();

        Assert.That(await rig.Synchronizer.PollOnceAsync(default), Is.True);
        Assert.That(rig.Cache.InvalidateAllCount, Is.EqualTo(1));
    }

    [Test]
    public async Task PollOnceAsync_unchanged_epoch_and_membership_keep_the_cache()
    {
        var rig = Rig.Create();
        await rig.Synchronizer.PollOnceAsync(default);

        for (var i = 0; i < 3; i++)
        {
            Assert.That(await rig.Synchronizer.PollOnceAsync(default), Is.False);
        }

        Assert.That(rig.Cache.InvalidateAllCount, Is.EqualTo(1), "the steady state drops nothing");
    }

    [Test]
    public async Task PollOnceAsync_advanced_epoch_drops_the_cache()
    {
        var rig = Rig.Create();
        await rig.Synchronizer.PollOnceAsync(default);

        rig.Epoch.GetAsync().Returns(new SchemaConfigEpoch(Incarnation, 1));

        Assert.That(await rig.Synchronizer.PollOnceAsync(default), Is.True);
        Assert.That(rig.Cache.InvalidateAllCount, Is.EqualTo(2));
    }

    [Test]
    public async Task PollOnceAsync_restarted_epoch_grain_drops_the_cache_even_at_a_lower_version()
    {
        var rig = Rig.Create();
        rig.Epoch.GetAsync().Returns(new SchemaConfigEpoch(Incarnation, 7));
        await rig.Synchronizer.PollOnceAsync(default);

        rig.Epoch.GetAsync().Returns(new SchemaConfigEpoch(Guid.NewGuid(), 0));

        Assert.That(
            await rig.Synchronizer.PollOnceAsync(default),
            Is.True,
            "a new incarnation may have lost an advance, so it must read as a change");
    }

    [Test]
    public async Task PollOnceAsync_membership_change_drops_the_cache()
    {
        var membership = Substitute.For<IClusterMembershipService>();
        membership.CurrentSnapshot.Returns(Snapshot(1));
        var rig = Rig.Create(membership);
        await rig.Synchronizer.PollOnceAsync(default);
        Assert.That(await rig.Synchronizer.PollOnceAsync(default), Is.False, "precondition: nothing moved");

        membership.CurrentSnapshot.Returns(Snapshot(2));

        Assert.That(
            await rig.Synchronizer.PollOnceAsync(default),
            Is.True,
            "a silo may have died between committing a change and publishing it");
    }

    [Test]
    public async Task PollOnceAsync_moved_epoch_makes_a_real_provider_reload_from_the_store()
    {
        var store = Substitute.For<ILatticeSchemaPolicyStore>();
        store.GetPolicyAsync("orders", Arg.Any<CancellationToken>()).Returns(
            Task.FromResult<LatticeSchemaPolicy?>(null),
            Task.FromResult<LatticeSchemaPolicy?>(new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() })));
        var provider = new LatticeSchemaPolicyProvider(store, Options.Create(new LatticeSchemaEnforcementOptions()));
        var rig = Rig.Create(cache: provider);
        await rig.Synchronizer.PollOnceAsync(default);
        Assert.That(await provider.GetCompiledPolicyAsync("orders"), Is.Null, "precondition: cached as ungoverned");

        // Another silo commits a policy; this silo's change feed never sees it.
        rig.Epoch.GetAsync().Returns(new SchemaConfigEpoch(Incarnation, 1));
        await rig.Synchronizer.PollOnceAsync(default);

        Assert.That(await provider.GetCompiledPolicyAsync("orders"), Is.Not.Null);
    }

    [Test]
    public async Task OnMutationAsync_config_tree_write_advances_the_cluster_epoch()
    {
        var rig = Rig.Create();

        await rig.Synchronizer.OnMutationAsync(Mutation(ConfigTree), default);
        await rig.Synchronizer.PublishLoop.WaitAsync(TimeSpan.FromSeconds(30));

        _ = rig.Epoch.Received(1).AdvanceAsync();
    }

    [Test]
    public async Task OnMutationAsync_other_tree_write_publishes_nothing()
    {
        var rig = Rig.Create();

        await rig.Synchronizer.OnMutationAsync(Mutation("orders"), default);
        await rig.Synchronizer.OnMutationAsync(Mutation("sys-schema-version"), default);
        await rig.Synchronizer.PublishLoop.WaitAsync(TimeSpan.FromSeconds(30));

        _ = rig.Epoch.DidNotReceive().AdvanceAsync();
    }

    [Test]
    public void OnMutationAsync_other_tree_write_does_not_allocate()
    {
        var rig = Rig.Create();
        var mutation = Mutation("orders");
        for (var i = 0; i < 100; i++)
        {
            rig.Synchronizer.OnMutationAsync(mutation, CancellationToken.None).GetAwaiter().GetResult();
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            rig.Synchronizer.OnMutationAsync(mutation, CancellationToken.None).GetAwaiter().GetResult();
        }

        Assert.That(GC.GetAllocatedBytesForCurrentThread() - before, Is.Zero);
    }

    [Test]
    public async Task OnMutationAsync_failed_advance_is_retried_until_it_lands()
    {
        var rig = Rig.Create();
        rig.Epoch.AdvanceAsync().Returns(
            _ => Task.FromException<SchemaConfigEpoch>(new InvalidOperationException("epoch grain unreachable")),
            _ => Task.FromResult(new SchemaConfigEpoch(Incarnation, 1)));

        await rig.Synchronizer.OnMutationAsync(Mutation(ConfigTree), default);
        await rig.Synchronizer.PublishLoop.WaitAsync(TimeSpan.FromSeconds(30));

        _ = rig.Epoch.Received(2).AdvanceAsync();
    }

    [Test]
    public async Task Poll_loop_follows_the_epoch_on_its_own_and_stops_cleanly()
    {
        var rig = Rig.Create(pollInterval: TimeSpan.FromMilliseconds(20));

        await rig.Synchronizer.StartAsync(default);
        try
        {
            await WaitUntilAsync(() => rig.Cache.InvalidateAllCount >= 1);
            var dropped = rig.Cache.InvalidateAllCount;
            rig.Epoch.GetAsync().Returns(new SchemaConfigEpoch(Incarnation, 1));

            await WaitUntilAsync(() => rig.Cache.InvalidateAllCount > dropped);
        }
        finally
        {
            await rig.Synchronizer.StopAsync(default);
        }

        Assert.That(rig.Synchronizer.PollLoop.IsCompleted, Is.True);
    }

    [Test]
    public async Task Poll_loop_drops_the_cache_while_the_epoch_cannot_be_read()
    {
        var rig = Rig.Create(pollInterval: TimeSpan.FromMilliseconds(20));
        rig.Epoch.GetAsync().Returns(_ => Task.FromException<SchemaConfigEpoch>(new InvalidOperationException("epoch grain unreachable")));

        await rig.Synchronizer.StartAsync(default);
        try
        {
            // Unable to confirm the cache is current, the silo stops trusting it on
            // every failed poll, so the propagation bound holds through the outage.
            await WaitUntilAsync(() => rig.Cache.InvalidateAllCount >= 2);
        }
        finally
        {
            await rig.Synchronizer.StopAsync(default);
        }
    }

    [Test]
    public void Constructor_rejects_a_non_positive_interval_and_null_arguments()
    {
        Assert.That(() => Rig.Create(pollInterval: TimeSpan.Zero), Throws.InstanceOf<ArgumentOutOfRangeException>());
        var grains = Substitute.For<IGrainFactory>();
        var cache = new CountingCache();
        var logger = NullLogger<SchemaConfigEpochSynchronizer<CountingCache>>.Instance;
        var interval = TimeSpan.FromSeconds(1);
        Assert.That(() => new SchemaConfigEpochSynchronizer<CountingCache>(null!, cache, ConfigTree, interval, logger), Throws.ArgumentNullException);
        Assert.That(() => new SchemaConfigEpochSynchronizer<CountingCache>(grains, null!, ConfigTree, interval, logger), Throws.ArgumentNullException);
        Assert.That(() => new SchemaConfigEpochSynchronizer<CountingCache>(grains, cache, string.Empty, interval, logger), Throws.ArgumentException);
        Assert.That(() => new SchemaConfigEpochSynchronizer<CountingCache>(grains, cache, ConfigTree, interval, null!), Throws.ArgumentNullException);
    }

    private static LatticeMutation Mutation(string treeId) =>
        new() { TreeId = treeId, Kind = MutationKind.Set, Key = "orders" };

    private static ClusterMembershipSnapshot Snapshot(long version) =>
        new(ImmutableDictionary<SiloAddress, ClusterMember>.Empty, new MembershipVersion(version));

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (!condition())
        {
            Assert.That(DateTime.UtcNow, Is.LessThan(deadline), "the condition never held");
            await Task.Delay(10);
        }
    }

    /// <summary>A cache that only counts how often it was dropped.</summary>
    internal sealed class CountingCache : ISchemaConfigCache
    {
        private int _count;

        public int InvalidateAllCount => Volatile.Read(ref _count);

        public void InvalidateAll() => Interlocked.Increment(ref _count);
    }

    private sealed record Rig(
        SchemaConfigEpochSynchronizer<ISchemaConfigCache> Synchronizer,
        ISchemaConfigEpochGrain Epoch,
        CountingCache Cache)
    {
        public static Rig Create(
            IClusterMembershipService? membership = null,
            TimeSpan? pollInterval = null,
            ISchemaConfigCache? cache = null)
        {
            var counting = new CountingCache();
            var epoch = Substitute.For<ISchemaConfigEpochGrain>();
            epoch.GetAsync().Returns(new SchemaConfigEpoch(Incarnation, 0));
            epoch.AdvanceAsync().Returns(new SchemaConfigEpoch(Incarnation, 1));
            var grains = Substitute.For<IGrainFactory>();
            grains.GetGrain<ISchemaConfigEpochGrain>(ConfigTree, Arg.Any<string?>()).Returns(epoch);
            var synchronizer = new SchemaConfigEpochSynchronizer<ISchemaConfigCache>(
                grains,
                cache ?? counting,
                ConfigTree,
                pollInterval ?? TimeSpan.FromMinutes(5),
                NullLogger<SchemaConfigEpochSynchronizer<ISchemaConfigCache>>.Instance,
                membership);
            return new Rig(synchronizer, epoch, counting);
        }
    }
}
