using System.Collections.Immutable;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage for <see cref="ReplicationConfigEpochSynchronizer"/>, which keeps
/// a silo's compiled replication-configuration snapshot current with config
/// writes committed on other silos (#5462). Before it, the core change feed was
/// the only rebuild trigger and it fires only on the silo hosting the committing
/// leaf, so a tree enabled at runtime stayed "not enrolled" on every other silo:
/// that silo refused the peer's re-seed export and declined to coordinate a
/// restore of the tree.
/// </summary>
[TestFixture]
public sealed class ReplicationConfigEpochSynchronizerTests
{
    private const string Tree = "factory-orders";

    private static readonly Guid Incarnation = Guid.NewGuid();

    [Test]
    public async Task An_enable_committed_on_another_silo_is_compiled_after_the_next_poll()
    {
        var rig = Rig.Create();
        Assert.That(await rig.Synchronizer.PollOnceAsync(default), Is.True, "the first poll builds the snapshot");
        Assert.That(rig.IsEnrolled(), Is.False, "precondition: the tree is not enrolled");

        // Another silo enables the tree; this silo's change feed never sees it.
        rig.Store.Entries[Tree] = Enabled();
        Assert.That(await rig.Synchronizer.PollOnceAsync(default), Is.False, "an unmoved epoch rebuilds nothing");
        Assert.That(rig.IsEnrolled(), Is.False, "nothing has told this silo about the write yet");

        rig.Epoch.GetAsync().Returns(new ReplicationConfigEpoch(Incarnation, 1));

        Assert.Multiple(async () =>
        {
            Assert.That(await rig.Synchronizer.PollOnceAsync(default), Is.True);
            Assert.That(rig.IsEnrolled(), Is.True, "the enable is compiled once the epoch moves");
        });
    }

    [Test]
    public async Task An_unchanged_epoch_and_membership_never_rebuild()
    {
        var rig = Rig.Create();
        await rig.Synchronizer.PollOnceAsync(default);
        var epoch = rig.Maintainer.CurrentEpoch;

        for (var i = 0; i < 3; i++)
        {
            Assert.That(await rig.Synchronizer.PollOnceAsync(default), Is.False);
        }

        Assert.That(rig.Maintainer.CurrentEpoch, Is.EqualTo(epoch), "the steady state costs no rebuild");
    }

    [Test]
    public async Task A_restarted_epoch_grain_rebuilds_even_at_a_lower_version()
    {
        var rig = Rig.Create();
        rig.Epoch.GetAsync().Returns(new ReplicationConfigEpoch(Incarnation, 7));
        await rig.Synchronizer.PollOnceAsync(default);

        rig.Epoch.GetAsync().Returns(new ReplicationConfigEpoch(Guid.NewGuid(), 0));

        Assert.That(
            await rig.Synchronizer.PollOnceAsync(default),
            Is.True,
            "a new incarnation may have lost an advance, so it must read as a change");
    }

    [Test]
    public async Task A_membership_change_rebuilds()
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
    public async Task A_failed_rebuild_is_retried_on_the_next_poll()
    {
        var rig = Rig.Create();
        await rig.Synchronizer.PollOnceAsync(default);
        rig.Store.Entries[Tree] = Enabled();
        rig.Epoch.GetAsync().Returns(new ReplicationConfigEpoch(Incarnation, 1));
        rig.Store.FailuresRemaining = 1;

        Assert.That(async () => await rig.Synchronizer.PollOnceAsync(default), Throws.InvalidOperationException);
        Assert.That(rig.IsEnrolled(), Is.False, "precondition: the failed rebuild left the old snapshot in force");

        Assert.Multiple(async () =>
        {
            Assert.That(await rig.Synchronizer.PollOnceAsync(default), Is.True, "the same epoch is retried");
            Assert.That(rig.IsEnrolled(), Is.True);
        });
    }

    [Test]
    public async Task A_config_write_observed_here_advances_the_cluster_epoch()
    {
        var rig = Rig.Create();

        await rig.Synchronizer.OnMutationAsync(Mutation(LatticeSystemTreeNames.ReplicationConfig), default);
        await rig.Synchronizer.PublishLoop.WaitAsync(TimeSpan.FromSeconds(30));

        _ = rig.Epoch.Received(1).AdvanceAsync();
    }

    [Test]
    public async Task A_write_to_any_other_tree_publishes_nothing()
    {
        var rig = Rig.Create();

        await rig.Synchronizer.OnMutationAsync(Mutation(Tree), default);
        await rig.Synchronizer.PublishLoop.WaitAsync(TimeSpan.FromSeconds(30));

        _ = rig.Epoch.DidNotReceive().AdvanceAsync();
    }

    [Test]
    public async Task A_failed_advance_is_retried_until_it_lands()
    {
        var rig = Rig.Create();
        rig.Epoch.AdvanceAsync().Returns(
            _ => Task.FromException<ReplicationConfigEpoch>(new InvalidOperationException("epoch grain unreachable")),
            _ => Task.FromResult(new ReplicationConfigEpoch(Incarnation, 1)));

        await rig.Synchronizer.OnMutationAsync(Mutation(LatticeSystemTreeNames.ReplicationConfig), default);
        await rig.Synchronizer.PublishLoop.WaitAsync(TimeSpan.FromSeconds(30));

        _ = rig.Epoch.Received(2).AdvanceAsync();
    }

    [Test]
    public async Task The_poll_loop_follows_the_cluster_epoch_on_its_own_and_stops_cleanly()
    {
        var rig = Rig.Create(pollInterval: TimeSpan.FromMilliseconds(20));

        await rig.Synchronizer.StartAsync(default);
        try
        {
            await TestPoll.UntilAsync(() => rig.Maintainer.CurrentEpoch > 0, "the poll loop builds the snapshot");
            rig.Store.Entries[Tree] = Enabled();
            rig.Epoch.GetAsync().Returns(new ReplicationConfigEpoch(Incarnation, 1));

            await TestPoll.UntilAsync(() => rig.IsEnrolled(), "the poll loop compiles the enable once the epoch moves");
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
    public void The_public_constructor_polls_at_the_default_interval()
    {
        var synchronizer = new ReplicationConfigEpochSynchronizer(
            Substitute.For<IGrainFactory>(),
            new CompiledReplicationConfigSnapshotMaintainer(
                new FlakyConfigStore(), NullLogger<CompiledReplicationConfigSnapshotMaintainer>.Instance),
            NullLogger<ReplicationConfigEpochSynchronizer>.Instance);

        Assert.That(synchronizer.PollInterval, Is.EqualTo(ReplicationConfigEpochSynchronizer.DefaultPollInterval));
    }

    private static LatticeReplicationConfigEntry Enabled()
    {
        var entry = new LatticeReplicationConfigEntry();
        entry.Enable("west", 1);
        entry.SetMode("west", LatticeMergeMode.LwwRegister);
        return entry;
    }

    private static LatticeMutation Mutation(string treeId) =>
        new() { TreeId = treeId, Kind = MutationKind.Set, Key = "k" };

    private static ClusterMembershipSnapshot Snapshot(long version) =>
        new(ImmutableDictionary<SiloAddress, ClusterMember>.Empty, new MembershipVersion(version));

    private sealed record Rig(
        ReplicationConfigEpochSynchronizer Synchronizer,
        IReplicationConfigEpochGrain Epoch,
        FlakyConfigStore Store,
        CompiledReplicationConfigSnapshotMaintainer Maintainer)
    {
        public static Rig Create(IClusterMembershipService? membership = null, TimeSpan? pollInterval = null)
        {
            var store = new FlakyConfigStore();
            var maintainer = new CompiledReplicationConfigSnapshotMaintainer(
                store, NullLogger<CompiledReplicationConfigSnapshotMaintainer>.Instance);
            var epoch = Substitute.For<IReplicationConfigEpochGrain>();
            epoch.GetAsync().Returns(new ReplicationConfigEpoch(Incarnation, 0));
            epoch.AdvanceAsync().Returns(new ReplicationConfigEpoch(Incarnation, 1));
            var grains = Substitute.For<IGrainFactory>();
            grains.GetGrain<IReplicationConfigEpochGrain>(IReplicationConfigEpochGrain.GrainKey, Arg.Any<string?>())
                .Returns(epoch);
            var synchronizer = new ReplicationConfigEpochSynchronizer(
                grains,
                maintainer,
                NullLogger<ReplicationConfigEpochSynchronizer>.Instance,
                membership,
                timeProvider: null,
                pollInterval ?? TimeSpan.FromMinutes(5));
            return new Rig(synchronizer, epoch, store, maintainer);
        }

        public bool IsEnrolled() =>
            Maintainer.Current.TryGetTree(Tree, out var projection) && projection.Enabled;
    }

    /// <summary>An in-memory config store whose next scans can be made to fail.</summary>
    private sealed class FlakyConfigStore : ILatticeReplicationConfigStore
    {
        public Dictionary<string, LatticeReplicationConfigEntry> Entries { get; } = new(StringComparer.Ordinal);

        public int FailuresRemaining { get; set; }

        public Task<IReadOnlyDictionary<string, LatticeReplicationConfigEntry>> ReadEntriesAsync(
            CancellationToken cancellationToken = default)
        {
            if (FailuresRemaining > 0)
            {
                FailuresRemaining--;
                throw new InvalidOperationException("transient config scan failure");
            }

            var copy = new Dictionary<string, LatticeReplicationConfigEntry>(Entries, StringComparer.Ordinal);
            return Task.FromResult<IReadOnlyDictionary<string, LatticeReplicationConfigEntry>>(copy);
        }

        public Task<LatticeReplicationConfigEntry?> ReadEntryAsync(string treeId, CancellationToken cancellationToken = default) =>
            Task.FromResult(Entries.TryGetValue(treeId, out var entry) ? entry : null);

        public Task WriteEntryAsync(
            string treeId, string replicaId, LatticeReplicationConfigEntry entry, CancellationToken cancellationToken = default)
        {
            Entries[treeId] = entry;
            return Task.CompletedTask;
        }
    }
}
