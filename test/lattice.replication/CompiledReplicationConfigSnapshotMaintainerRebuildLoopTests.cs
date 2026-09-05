using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage for the background rebuild loop of
/// <see cref="CompiledReplicationConfigSnapshotMaintainer"/>: the coalescing
/// state machine that collapses a burst of config writes into one in-flight
/// rebuild plus at most one queued follow-up, and the catch arm that keeps a
/// failed rescan from tearing down the loop or the previous snapshot.
/// <para>
/// Both need a rebuild to be observably in flight, which the ordinary fake store
/// cannot express - it completes synchronously, so a burst is always serialised
/// and the coalescing branch is never taken. The store here is gated instead, so
/// a test can hold the first rescan open, queue a follow-up behind it, and then
/// release.
/// </para>
/// </summary>
[TestFixture]
public sealed class CompiledReplicationConfigSnapshotMaintainerRebuildLoopTests
{
    private static LatticeMutation ConfigMutation =>
        new() { TreeId = LatticeSystemTreeNames.ReplicationConfig };

    private static LatticeReplicationConfigEntry Enabled()
    {
        var entry = new LatticeReplicationConfigEntry();
        entry.Enable("site-a", 1);
        entry.SetMode("site-a", LatticeMergeMode.LwwRegister);
        return entry;
    }

    private static async Task<bool> WaitFor(Func<bool> condition, int timeoutMs = 10000)
    {
        var start = Environment.TickCount64;
        while (Environment.TickCount64 - start < timeoutMs)
        {
            if (condition())
            {
                return true;
            }

            await Task.Delay(20);
        }

        return condition();
    }

    [Test]
    public async Task A_burst_during_an_in_flight_rebuild_collapses_to_one_follow_up()
    {
        // Hold the first rescan open, then fire three more config-tree mutations
        // behind it. The contract is "at most one in-flight rebuild plus at most
        // one queued follow-up", so the three must collapse into a single
        // follow-up rescan - two store reads in total, not four.
        var store = new GatedConfigStore { BlockOnCall = 1 };
        var maintainer = new CompiledReplicationConfigSnapshotMaintainer(
            store, NullLogger<CompiledReplicationConfigSnapshotMaintainer>.Instance);

        await maintainer.OnMutationAsync(ConfigMutation, CancellationToken.None);
        Assert.That(await WaitFor(() => store.Calls >= 1), Is.True, "the first rebuild must start");

        store.Entries["orders"] = Enabled();
        for (var i = 0; i < 3; i++)
        {
            await maintainer.OnMutationAsync(ConfigMutation, CancellationToken.None);
        }

        Assert.That(store.Calls, Is.EqualTo(1), "the burst must not start a second concurrent rescan");

        store.Release();

        Assert.That(await WaitFor(() => maintainer.CurrentEpoch >= 2), Is.True,
            "the queued follow-up must run once the in-flight rebuild finishes");
        // Let any (incorrect) extra follow-up settle before counting.
        await Task.Delay(200);
        Assert.Multiple(() =>
        {
            Assert.That(store.Calls, Is.EqualTo(2), "three queued writes must collapse into exactly one follow-up");
            Assert.That(maintainer.CurrentEpoch, Is.EqualTo(2));
            Assert.That(maintainer.Current.TryGetTree("orders", out _), Is.True,
                "the follow-up rescan must capture the latest committed change");
        });
    }

    [Test]
    public async Task The_loop_goes_idle_and_can_be_rescheduled_after_a_coalesced_follow_up()
    {
        // After the follow-up drains, the coalescing state must be back to idle -
        // otherwise a later mutation would be swallowed and the snapshot would
        // silently stop tracking the config tree.
        var store = new GatedConfigStore { BlockOnCall = 1 };
        var maintainer = new CompiledReplicationConfigSnapshotMaintainer(
            store, NullLogger<CompiledReplicationConfigSnapshotMaintainer>.Instance);

        await maintainer.OnMutationAsync(ConfigMutation, CancellationToken.None);
        Assert.That(await WaitFor(() => store.Calls >= 1), Is.True);
        await maintainer.OnMutationAsync(ConfigMutation, CancellationToken.None);
        store.Release();
        Assert.That(await WaitFor(() => maintainer.CurrentEpoch >= 2), Is.True);

        store.Entries["late"] = Enabled();
        await maintainer.OnMutationAsync(ConfigMutation, CancellationToken.None);

        Assert.That(await WaitFor(() => maintainer.CurrentEpoch >= 3), Is.True,
            "a mutation after the loop went idle must schedule a fresh rebuild");
        Assert.That(maintainer.Current.TryGetTree("late", out _), Is.True);
    }

    [Test]
    public async Task A_failed_background_rescan_is_logged_and_leaves_the_previous_snapshot_in_effect()
    {
        var store = new GatedConfigStore();
        store.Entries["orders"] = Enabled();
        var logger = new CapturingLogger<CompiledReplicationConfigSnapshotMaintainer>();
        var maintainer = new CompiledReplicationConfigSnapshotMaintainer(store, logger);

        await maintainer.EnsureWarmAsync();
        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1));

        // The next background rescan faults.
        store.FaultOnCall = 2;
        await maintainer.OnMutationAsync(ConfigMutation, CancellationToken.None);

        Assert.That(await WaitFor(() => logger.Warnings.Count > 0), Is.True,
            "a failed background rescan must be logged");
        Assert.Multiple(() =>
        {
            Assert.That(logger.Warnings[0].Message, Does.Contain("previous snapshot remains in effect"));
            Assert.That(logger.Warnings[0].Exception, Is.TypeOf<InvalidOperationException>());
            Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1),
                "a failed rescan must not advance the epoch");
            Assert.That(maintainer.Current.TryGetTree("orders", out _), Is.True,
                "the previous snapshot must survive a failed rescan");
        });
    }

    [Test]
    public async Task The_loop_survives_a_failed_rescan_and_still_serves_later_mutations()
    {
        // The catch arm exists so one bad rescan does not permanently wedge the
        // maintainer: the loop must still return to idle and rebuild next time.
        var store = new GatedConfigStore { FaultOnCall = 1 };
        var logger = new CapturingLogger<CompiledReplicationConfigSnapshotMaintainer>();
        var maintainer = new CompiledReplicationConfigSnapshotMaintainer(store, logger);

        maintainer.EnsureWarmStarted();
        Assert.That(await WaitFor(() => logger.Warnings.Count > 0), Is.True);
        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(0), "the failed warm-up built nothing");

        store.Entries["orders"] = Enabled();
        await maintainer.OnMutationAsync(ConfigMutation, CancellationToken.None);

        Assert.That(await WaitFor(() => maintainer.CurrentEpoch >= 1), Is.True,
            "the maintainer must recover and rebuild on the next config mutation");
        Assert.That(maintainer.Current.TryGetTree("orders", out _), Is.True);
    }

    /// <summary>
    /// An <see cref="ILatticeReplicationConfigStore"/> whose reads a test can
    /// hold open (to make a rebuild observably in flight) or fault on a nominated
    /// call, so the maintainer's coalescing and catch arms become reachable.
    /// </summary>
    private sealed class GatedConfigStore : ILatticeReplicationConfigStore
    {
        private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _calls;

        public Dictionary<string, LatticeReplicationConfigEntry> Entries { get; } = new(StringComparer.Ordinal);

        /// <summary>1-based call ordinal to block until <see cref="Release"/>; 0 blocks nothing.</summary>
        public int BlockOnCall { get; init; }

        /// <summary>1-based call ordinal to fault; 0 faults nothing.</summary>
        public int FaultOnCall { get; set; }

        public int Calls => Volatile.Read(ref _calls);

        public void Release() => _gate.TrySetResult();

        public async Task<IReadOnlyDictionary<string, LatticeReplicationConfigEntry>> ReadEntriesAsync(
            CancellationToken cancellationToken = default)
        {
            var n = Interlocked.Increment(ref _calls);

            if (FaultOnCall == n)
            {
                throw new InvalidOperationException($"config store unavailable on call {n}");
            }

            if (BlockOnCall == n)
            {
                await _gate.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken).ConfigureAwait(false);
            }

            lock (Entries)
            {
                return new Dictionary<string, LatticeReplicationConfigEntry>(Entries, StringComparer.Ordinal);
            }
        }

        public Task<LatticeReplicationConfigEntry?> ReadEntryAsync(
            string treeId, CancellationToken cancellationToken = default)
        {
            lock (Entries)
            {
                return Task.FromResult(Entries.TryGetValue(treeId, out var entry) ? entry : null);
            }
        }

        public Task WriteEntryAsync(
            string treeId,
            string replicaId,
            LatticeReplicationConfigEntry entry,
            CancellationToken cancellationToken = default)
        {
            lock (Entries)
            {
                Entries[treeId] = entry;
            }

            return Task.CompletedTask;
            }
    }
}
