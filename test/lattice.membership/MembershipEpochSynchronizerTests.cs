using System.Collections.Immutable;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit coverage for <see cref="MembershipEpochSynchronizer"/>, which keeps a
/// silo's <see cref="MembershipResolutionCache"/> current with membership writes
/// committed on other silos (#5518). Before it, the core change feed was the only
/// flush trigger and it fires only on the silo hosting the committing leaf, so a
/// removed member kept their old groups on every other silo for the cache lifetime.
/// </summary>
[TestFixture]
public sealed class MembershipEpochSynchronizerTests
{
    private static readonly DateTimeOffset Start = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly Guid Incarnation = Guid.NewGuid();
    private static readonly MembershipCacheKey Key = MembershipCacheKey.ForToken("tok");

    [Test]
    public async Task A_membership_write_committed_on_another_silo_flushes_the_cache_after_the_next_poll()
    {
        var rig = Rig.Create();
        Assert.That(await rig.Synchronizer.PollOnceAsync(default), Is.True, "the first poll observes the epoch");
        await rig.ResolveAsync();
        await rig.ResolveAsync();
        Assert.That(rig.Resolutions, Is.EqualTo(1), "precondition: the second resolution is served warm");

        // Another silo removes the member; this silo's change feed never sees it.
        Assert.That(await rig.Synchronizer.PollOnceAsync(default), Is.False, "an unmoved epoch flushes nothing");
        await rig.ResolveAsync();
        Assert.That(rig.Resolutions, Is.EqualTo(1), "nothing has told this silo about the write yet");

        rig.Epoch.GetAsync().Returns(new MembershipEpoch(Incarnation, 1));

        Assert.That(await rig.Synchronizer.PollOnceAsync(default), Is.True);
        await rig.ResolveAsync();
        Assert.That(rig.Resolutions, Is.EqualTo(2), "the cached pre-change resolution is dropped once the epoch moves");
    }

    [Test]
    public async Task An_unchanged_epoch_and_membership_never_flush()
    {
        var rig = Rig.Create();
        await rig.Synchronizer.PollOnceAsync(default);
        await rig.ResolveAsync();

        for (var i = 0; i < 3; i++)
        {
            Assert.That(await rig.Synchronizer.PollOnceAsync(default), Is.False);
        }

        await rig.ResolveAsync();
        Assert.That(rig.Resolutions, Is.EqualTo(1), "the steady state keeps the cache warm");
    }

    [Test]
    public async Task A_restarted_epoch_grain_flushes_even_at_a_lower_version()
    {
        var rig = Rig.Create();
        rig.Epoch.GetAsync().Returns(new MembershipEpoch(Incarnation, 7));
        await rig.Synchronizer.PollOnceAsync(default);

        rig.Epoch.GetAsync().Returns(new MembershipEpoch(Guid.NewGuid(), 0));

        Assert.That(
            await rig.Synchronizer.PollOnceAsync(default),
            Is.True,
            "a new incarnation may have lost an advance, so it must read as a change");
    }

    [Test]
    public async Task A_cluster_membership_change_flushes()
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
            "a silo may have died between committing a write and publishing it");
    }

    [Test]
    public async Task Before_the_first_poll_no_warm_entry_is_served()
    {
        var rig = Rig.Create();

        await rig.ResolveAsync();
        await rig.ResolveAsync();

        Assert.Multiple(() =>
        {
            Assert.That(rig.Resolutions, Is.EqualTo(2), "nothing has confirmed the cache current yet");
            Assert.That(rig.Cache.TryGetCached(Key, out _), Is.False);
        });
    }

    [Test]
    public async Task A_lapsed_confirmation_stops_serving_warm_entries_until_a_poll_succeeds_again()
    {
        var rig = Rig.Create();
        await rig.Synchronizer.PollOnceAsync(default);
        await rig.ResolveAsync();
        Assert.That(rig.Cache.TryGetCached(Key, out _), Is.True, "precondition: warm");

        rig.Epoch.GetAsync().Returns(_ => Task.FromException<MembershipEpoch>(new InvalidOperationException("epoch grain unreachable")));
        Assert.That(async () => await rig.Synchronizer.PollOnceAsync(default), Throws.InvalidOperationException);
        Assert.That(rig.Cache.TryGetCached(Key, out _), Is.True, "a failed poll leaves the earlier confirmation to run out");

        rig.Time.Advance(rig.Synchronizer.LeaseDuration);

        Assert.Multiple(() =>
        {
            Assert.That(rig.Cache.TryGetCached(Key, out _), Is.False, "a silo that cannot read the epoch stops serving cached groups");
            Assert.That(rig.Cache.Count, Is.EqualTo(1), "the entry is kept, only not served");
        });
        await rig.ResolveAsync();
        Assert.That(rig.Resolutions, Is.EqualTo(2), "the caller is resolved afresh from the directory");

        rig.Epoch.GetAsync().Returns(new MembershipEpoch(Incarnation, 0));
        await rig.Synchronizer.PollOnceAsync(default);

        Assert.That(rig.Cache.TryGetCached(Key, out _), Is.True, "a successful poll confirms the cache current again");
    }

    [Test]
    public async Task A_slow_poll_cannot_extend_the_confirmation_past_its_start()
    {
        var rig = Rig.Create();
        await rig.Synchronizer.PollOnceAsync(default);
        rig.Time.Advance(TimeSpan.FromSeconds(1));
        await rig.ResolveAsync();
        rig.Epoch.GetAsync().Returns(_ =>
        {
            rig.Time.Advance(rig.Synchronizer.LeaseDuration);
            return Task.FromResult(new MembershipEpoch(Incarnation, 0));
        });

        Assert.That(await rig.Synchronizer.PollOnceAsync(default), Is.False, "precondition: the epoch has not moved");

        Assert.That(
            rig.Cache.TryGetCached(Key, out _),
            Is.False,
            "the confirmation runs from before the read, so a read that took a whole lease vouches for nothing");
    }

    [Test]
    public void A_cache_that_no_synchronizer_keeps_current_serves_warm_entries_unconditionally()
    {
        var (cache, _) = Rig.CreateCache();

        Assert.That(cache.IsConfirmedCurrent(Start), Is.True);
    }

    [TestCase(MembershipConstants.EdgesTree)]
    [TestCase(MembershipConstants.GroupsTree)]
    public async Task A_membership_write_observed_here_advances_the_cluster_epoch(string tree)
    {
        var rig = Rig.Create();

        await rig.Synchronizer.OnMutationAsync(Mutation(tree), default);
        await rig.Synchronizer.PublishLoop.WaitAsync(TimeSpan.FromSeconds(30));

        _ = rig.Epoch.Received(1).AdvanceAsync();
    }

    [Test]
    public async Task A_write_to_any_other_tree_publishes_nothing()
    {
        var rig = Rig.Create();

        await rig.Synchronizer.OnMutationAsync(Mutation("orders"), default);
        await rig.Synchronizer.OnMutationAsync(new LatticeMutation(), default);
        await rig.Synchronizer.PublishLoop.WaitAsync(TimeSpan.FromSeconds(30));

        _ = rig.Epoch.DidNotReceive().AdvanceAsync();
    }

    [Test]
    public async Task A_failed_advance_is_retried_until_it_lands()
    {
        var rig = Rig.Create();
        rig.Epoch.AdvanceAsync().Returns(
            _ => Task.FromException<MembershipEpoch>(new InvalidOperationException("epoch grain unreachable")),
            _ => Task.FromResult(new MembershipEpoch(Incarnation, 1)));

        await rig.Synchronizer.OnMutationAsync(Mutation(MembershipConstants.EdgesTree), default);
        await rig.Synchronizer.PublishLoop.WaitAsync(TimeSpan.FromSeconds(30));

        _ = rig.Epoch.Received(2).AdvanceAsync();
    }

    [Test]
    public async Task The_poll_loop_follows_the_cluster_epoch_on_its_own_and_stops_cleanly()
    {
        var rig = Rig.Create(pollInterval: TimeSpan.FromMilliseconds(20), time: TimeProvider.System);

        await rig.Synchronizer.StartAsync(default);
        try
        {
            // The first poll flushes (it has no earlier epoch to compare with), so
            // warm the cache only once it has confirmed the cache current.
            await WaitUntilAsync(() => rig.Cache.IsConfirmedCurrent(DateTimeOffset.UtcNow));
            await rig.ResolveAsync();
            await WaitUntilAsync(() => rig.Cache.TryGetCached(Key, out _));

            rig.Epoch.GetAsync().Returns(new MembershipEpoch(Incarnation, 1));

            await WaitUntilAsync(() => rig.Cache.Count == 0);
        }
        finally
        {
            await rig.Synchronizer.StopAsync(default);
        }

        Assert.That(rig.Synchronizer.PollLoop.IsCompleted, Is.True);
    }

    [Test]
    public void The_poll_interval_must_be_positive() =>
        Assert.That(
            () => Rig.Create(pollInterval: TimeSpan.Zero),
            Throws.InstanceOf<ArgumentOutOfRangeException>());

    [Test]
    public void The_lease_is_a_fixed_number_of_poll_intervals()
    {
        var rig = Rig.Create(pollInterval: TimeSpan.FromSeconds(2));

        Assert.That(rig.Synchronizer.LeaseDuration, Is.EqualTo(TimeSpan.FromSeconds(2 * MembershipEpochSynchronizer.LeasePolls)));
    }

    [Test]
    public void AddLatticeMembership_registers_one_synchronizer_as_observer_and_hosted_service_in_charge_of_the_cache()
    {
        var builder = new PlainSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<IValidateOptions<LatticeOptions>>());
        builder.Services.AddSingleton(Substitute.For<IGrainFactory>());
        builder.Services.AddSingleton(Substitute.For<ILatticeViewFactory>());
        builder.Services.AddLogging();
        builder.AddLatticeMembership();
        builder.AddLatticeMembership();

        using var provider = builder.Services.BuildServiceProvider();
        var synchronizer = provider.GetRequiredService<MembershipEpochSynchronizer>();
        var cache = provider.GetRequiredService<MembershipResolutionCache>();

        // Only the factory registrations are materialised, so unrelated hosted
        // services the core registers by type are never constructed here.
        object[] Built<TService>() =>
            builder.Services
                .Where(d => d.ServiceType == typeof(TService) && d.ImplementationFactory is not null)
                .Select(d => d.ImplementationFactory!(provider))
                .ToArray();

        var hosted = Built<IHostedService>();
        var observers = Built<IMutationObserver>();

        Assert.Multiple(() =>
        {
            Assert.That(hosted.Count(s => ReferenceEquals(s, synchronizer)), Is.EqualTo(1));
            Assert.That(observers.Count(s => ReferenceEquals(s, synchronizer)), Is.EqualTo(1));
            Assert.That(observers.Count(s => ReferenceEquals(s, cache)), Is.EqualTo(1));
            Assert.That(cache.IsConfirmedCurrent(DateTimeOffset.UtcNow), Is.False, "the registered cache serves only what a poll confirmed");
        });
    }

    [Test]
    public void The_constructor_rejects_null_collaborators()
    {
        var (cache, _) = Rig.CreateCache();
        var grains = Substitute.For<IGrainFactory>();
        var logger = NullLogger<MembershipEpochSynchronizer>.Instance;

        Assert.Multiple(() =>
        {
            Assert.That(() => new MembershipEpochSynchronizer(null!, cache, logger), Throws.ArgumentNullException);
            Assert.That(() => new MembershipEpochSynchronizer(grains, null!, logger), Throws.ArgumentNullException);
            Assert.That(() => new MembershipEpochSynchronizer(grains, cache, null!), Throws.ArgumentNullException);
        });
    }

    private static LatticeMutation Mutation(string treeId) =>
        new() { TreeId = treeId, Kind = MutationKind.Set, Key = "k" };

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

    private sealed class Rig
    {
        private Rig(MembershipEpochSynchronizer synchronizer, IMembershipEpochGrain epoch, MembershipResolutionCache cache, ManualTimeProvider time)
        {
            Synchronizer = synchronizer;
            Epoch = epoch;
            Cache = cache;
            Time = time;
        }

        public MembershipEpochSynchronizer Synchronizer { get; }

        public IMembershipEpochGrain Epoch { get; }

        public MembershipResolutionCache Cache { get; }

        public ManualTimeProvider Time { get; }

        public int Resolutions { get; private set; }

        public static (MembershipResolutionCache Cache, ManualTimeProvider Time) CreateCache(TimeProvider? time = null)
        {
            var manual = new ManualTimeProvider(Start);
            var monitor = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
            monitor.CurrentValue.Returns(new LatticeMembershipOptions { ResolutionCacheTtl = TimeSpan.FromMinutes(5) });
            return (new MembershipResolutionCache(time ?? manual, monitor), manual);
        }

        public static Rig Create(
            IClusterMembershipService? membership = null,
            TimeSpan? pollInterval = null,
            TimeProvider? time = null)
        {
            var (cache, manual) = CreateCache(time);
            var epoch = Substitute.For<IMembershipEpochGrain>();
            epoch.GetAsync().Returns(new MembershipEpoch(Incarnation, 0));
            epoch.AdvanceAsync().Returns(new MembershipEpoch(Incarnation, 1));
            var grains = Substitute.For<IGrainFactory>();
            grains.GetGrain<IMembershipEpochGrain>(IMembershipEpochGrain.GrainKey, Arg.Any<string?>()).Returns(epoch);
            var synchronizer = new MembershipEpochSynchronizer(
                grains,
                cache,
                NullLogger<MembershipEpochSynchronizer>.Instance,
                membership,
                time ?? manual,
                pollInterval ?? TimeSpan.FromSeconds(1));
            return new Rig(synchronizer, epoch, cache, manual);
        }

        public async Task ResolveAsync() =>
            await Cache.ResolveAsync(
                Key,
                _ =>
                {
                    Resolutions++;
                    return new ValueTask<ResolvedSubject>(new ResolvedSubject(new LatticeSubject("alice", ["stewards"]), null));
                },
                default);
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class PlainSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
