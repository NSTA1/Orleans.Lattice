using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4526 on a real in-process cluster: a snapshot bootstrap applies the
/// export one row at a time, so a reader part-way through used to observe a
/// committed atomic batch with some keys post-batch and others pre-batch. The
/// bootstrap now arms a read fence on every shard of the tree before applying the
/// first row and lifts it after the last; a failed drain keeps it up and is
/// re-driven automatically, and only an explicit operator override lifts it early.
/// <para>
/// The scenario is an in-place re-bootstrap: the receiver tree already holds the
/// pre-batch values of <c>k1</c> and <c>k2</c>, and the export carries the batch's
/// committed values for both. A gated snapshot source pauses the drain after the
/// first row so the test can read mid-import.
/// </para>
/// <para>
/// A second scenario changes the source generation during an export while writes
/// continue, proving that the unstable-export reconcile retry does not keep the
/// receiver read-fenced after the import has drained.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class BootstrapReadFenceIntegrationTests
{
    private const string ClusterId = "fence-receiver";
    private const string SourceCluster = "fence-source";

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        GatedSnapshotSource.Reset();
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [TearDown]
    public void ResetSource() => GatedSnapshotSource.Reset();

    private ILattice Tree(string name) => _cluster.Client.GetGrain<ILattice>(name);

    private ILatticeBootstrapCoordinator Coordinator => new LatticeBootstrapCoordinator(_cluster.Client);

    private ILatticeReplicationAdmin Admin()
    {
        var options = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions { ClusterId = ClusterId });
        return new LatticeReplicationAdmin(
            Coordinator, options, NullLogger<LatticeReplicationAdmin>.Instance, timeProvider: null, grainFactory: _cluster.Client);
    }

    private static string Str(byte[]? value) => value is null ? "<none>" : System.Text.Encoding.UTF8.GetString(value);

    private static byte[] Bytes(string value) => System.Text.Encoding.UTF8.GetBytes(value);

    private async Task SeedPreBatchAsync(string treeName)
    {
        await Tree(treeName).SetAsync("k1", Bytes("pre"));
        await Tree(treeName).SetAsync("k2", Bytes("pre"));
    }

    private static async Task<LatticeBootstrapState> WaitForPhaseAsync(
        ILatticeBootstrapCoordinator coordinator, string treeName, Func<BootstrapCoordinatorStatus, bool> done, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            var status = await coordinator.GetStatusAsync(treeName);
            if (done(status)) return status.Phase;
            if (DateTime.UtcNow > deadline)
                Assert.Fail($"Bootstrap of '{treeName}' did not reach the expected state; last phase {status.Phase}, fenced={status.ReadFenced}.");
            await Task.Delay(100);
        }
    }

    [Test]
    public async Task A_read_part_way_through_a_snapshot_drain_never_observes_a_partial_import()
    {
        var treeName = $"fence-midread-{Guid.NewGuid():N}";
        await SeedPreBatchAsync(treeName);
        GatedSnapshotSource.Arm(treeName, failFirstAttempts: 0);

        await Coordinator.BootstrapAsync(treeName, SourceCluster);
        await GatedSnapshotSource.FirstRowAppliedAsync(TimeSpan.FromSeconds(30));

        // k1's committed value is applied, k2's is not yet. Any read that
        // returned values here would show the batch torn.
        Exception? midRead = null;
        Dictionary<string, byte[]>? torn = null;
        try
        {
            torn = await Tree(treeName).GetManyAsync(["k1", "k2"]);
        }
        catch (Exception ex)
        {
            midRead = ex;
        }

        // Plain writes are not fenced: a write issued mid-drain completes.
        var write = Tree(treeName).SetAsync("k3", Bytes("written-mid-drain"));
        var completed = await Task.WhenAny(write, Task.Delay(TimeSpan.FromSeconds(10)));
        var status = await Coordinator.GetStatusAsync(treeName);

        GatedSnapshotSource.Release();
        await WaitForPhaseAsync(Coordinator, treeName, s => s.Phase == LatticeBootstrapState.LiveIncremental, TimeSpan.FromSeconds(60));
        var after = await Tree(treeName).GetManyAsync(["k1", "k2", "k3"]);

        Assert.Multiple(() =>
        {
            Assert.That(torn, Is.Null,
                $"a mid-drain read returned k1={Str(torn?.GetValueOrDefault("k1"))}, k2={Str(torn?.GetValueOrDefault("k2"))}: a partial import");
            Assert.That(midRead, Is.InstanceOf<LatticeTreeBootstrappingException>());
            Assert.That(completed, Is.SameAs(write), "a plain write issued mid-drain must complete");
            Assert.That(status.ReadFenced, Is.True, "the status reports the fence while the drain runs");
            Assert.That(status.EntriesApplied, Is.GreaterThanOrEqualTo(1));
            Assert.That(Str(after.GetValueOrDefault("k1")), Is.EqualTo("batch"));
            Assert.That(Str(after.GetValueOrDefault("k2")), Is.EqualTo("batch"));
            Assert.That(Str(after.GetValueOrDefault("k3")), Is.EqualTo("written-mid-drain"));
        });
    }

    [Test]
    public async Task An_unstable_export_under_continuous_writes_lifts_the_read_fence_when_the_drain_finishes()
    {
        var treeName = $"fence-unstable-{Guid.NewGuid():N}";
        await SeedPreBatchAsync(treeName);
        GatedSnapshotSource.Arm(treeName, failFirstAttempts: 0, unstableGeneration: true);

        await Coordinator.BootstrapAsync(treeName, SourceCluster);
        await GatedSnapshotSource.FirstRowAppliedAsync(TimeSpan.FromSeconds(30));
        Assert.That((await Coordinator.GetStatusAsync(treeName)).ReadFenced, Is.True);

        using var stopWriter = new CancellationTokenSource();
        var writes = 0;
        var writer = Task.Run(async () =>
        {
            while (!stopWriter.IsCancellationRequested)
            {
                var write = Interlocked.Increment(ref writes);
                await Tree(treeName).SetAsync($"live/{write}", Bytes(write.ToString()));
                await Task.Delay(10, stopWriter.Token);
            }
        });

        try
        {
            await Task.Delay(100, stopWriter.Token);
            Assert.That(writes, Is.GreaterThan(0), "the writer is active while the export is paused");
            GatedSnapshotSource.Release();
            await WaitForPhaseAsync(
                Coordinator,
                treeName,
                status => status.Phase == LatticeBootstrapState.LiveIncremental && !status.ReadFenced,
                TimeSpan.FromSeconds(30));
        }
        finally
        {
            GatedSnapshotSource.Release();
            stopWriter.Cancel();
            try
            {
                await writer;
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    [Test]
    public async Task A_drain_that_fails_part_way_keeps_the_fence_and_is_re_driven_until_it_completes()
    {
        var treeName = $"fence-redrive-{Guid.NewGuid():N}";
        await SeedPreBatchAsync(treeName);
        GatedSnapshotSource.Arm(treeName, failFirstAttempts: 1, gate: false);

        await Coordinator.BootstrapAsync(treeName, SourceCluster);
        await WaitForPhaseAsync(Coordinator, treeName, s => s.Phase == LatticeBootstrapState.Failed, TimeSpan.FromSeconds(30));
        var failed = await Coordinator.GetStatusAsync(treeName);
        var readWhileFailed = Assert.ThrowsAsync<LatticeTreeBootstrappingException>(
            () => Tree(treeName).GetAsync("k2"));

        await WaitForPhaseAsync(Coordinator, treeName, s => s.Phase == LatticeBootstrapState.LiveIncremental, TimeSpan.FromSeconds(60));
        var live = await Coordinator.GetStatusAsync(treeName);
        var after = await Tree(treeName).GetManyAsync(["k1", "k2"]);

        Assert.Multiple(() =>
        {
            Assert.That(failed.ReadFenced, Is.True, "a failed drain that applied part of the import keeps the fence");
            Assert.That(readWhileFailed, Is.Not.Null);
            Assert.That(live.ReadFenced, Is.False);
            Assert.That(live.RedriveAttempts, Is.GreaterThanOrEqualTo(1), "the failed bootstrap was re-driven automatically");
            Assert.That(Str(after.GetValueOrDefault("k1")), Is.EqualTo("batch"));
            Assert.That(Str(after.GetValueOrDefault("k2")), Is.EqualTo("batch"));
        });
    }

    [Test]
    public async Task An_operator_force_lift_exposes_the_partial_import_and_stops_the_re_drive()
    {
        var treeName = $"fence-forcelift-{Guid.NewGuid():N}";
        await SeedPreBatchAsync(treeName);
        GatedSnapshotSource.Arm(treeName, failFirstAttempts: int.MaxValue, gate: false);

        await Coordinator.BootstrapAsync(treeName, SourceCluster);
        await WaitForPhaseAsync(Coordinator, treeName, s => s.Phase == LatticeBootstrapState.Failed && s.ReadFenced, TimeSpan.FromSeconds(30));

        var admin = Admin();
        var lifted = await admin.ForceLiftBootstrapReadFenceAsync(treeName, "integration test: exercising the override");
        var again = await admin.ForceLiftBootstrapReadFenceAsync(treeName, "integration test: second call is a no-op");
        var status = await Coordinator.GetStatusAsync(treeName);
        var exposed = await Tree(treeName).GetManyAsync(["k1", "k2"]);

        Assert.Multiple(() =>
        {
            Assert.That(lifted, Is.True);
            Assert.That(again, Is.False);
            Assert.That(status.ReadFenced, Is.False);
            Assert.That(status.SourceClusterId, Is.Null, "the automatic re-drive is stopped");
            // The documented consequence of the override: the partial import is readable.
            Assert.That(Str(exposed.GetValueOrDefault("k1")), Is.EqualTo("batch"));
            Assert.That(Str(exposed.GetValueOrDefault("k2")), Is.EqualTo("pre"));
        });
    }

    [Test]
    public async Task A_prepared_row_imported_after_its_sagas_terminal_arrived_live_is_settled_not_torn()
    {
        // 8d4eaa41's interleaving: the export has the saga in flight, so it
        // ships prepared rows for k1 and k2. k2's is staged; then the live
        // stream delivers the saga's terminal - the receiver records it
        // Committed and k1's leaf applies it with no bucket - and only then
        // does the drain apply k1's prepared row. Without the settle (#4510)
        // the late-prepare refusal drops it, so after the fence lifts k2 reads
        // post-saga while k1 stays pre-saga, permanently.
        var treeName = $"fence-interleave-{Guid.NewGuid():N}";
        await SeedPreBatchAsync(treeName);
        var saga = Guid.NewGuid();
        GatedSnapshotSource.Arm(treeName, failFirstAttempts: 0, preparedSaga: saga);

        await Coordinator.BootstrapAsync(treeName, SourceCluster);
        await GatedSnapshotSource.FirstRowAppliedAsync(TimeSpan.FromSeconds(30));

        var apply = _cluster.Client.GetGrain<Orleans.Lattice.BPlusTree.IReplicationApplyGrain>(treeName);
        var terminalHlc = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks, Counter = 1 };
        var shards = new[] { "k1", "k2" }
            .Select(k => Orleans.Lattice.BPlusTree.LatticeSharding.GetShardIndex(k, Orleans.Lattice.BPlusTree.LatticeConstants.DefaultShardCount))
            .Distinct()
            .ToArray();
        foreach (var shard in shards)
        {
            await apply.ApplyTxTerminalAsync(saga, committed: true, shardIndex: shard, terminalHlc, SourceCluster);
        }

        GatedSnapshotSource.Release();
        await WaitForPhaseAsync(Coordinator, treeName, s => s.Phase == LatticeBootstrapState.LiveIncremental, TimeSpan.FromSeconds(60));
        var after = await Tree(treeName).GetManyAsync(["k1", "k2"]);

        Assert.Multiple(() =>
        {
            Assert.That(Str(after.GetValueOrDefault("k1")), Is.EqualTo("batch"),
                "k1's prepared row arrived after its saga's terminal and must be settled as committed");
            Assert.That(Str(after.GetValueOrDefault("k2")), Is.EqualTo("batch"));
        });
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IBootstrapSnapshotSource, GatedSnapshotSource>();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = ClusterId;
                opts.BootstrapTransientRetry = new BoundedExponentialRetryPolicyOptions
                {
                    MaxAttempts = 1,
                    InitialDelay = TimeSpan.Zero,
                    MaxDelay = TimeSpan.Zero,
                };
            });
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }

    /// <summary>
    /// A snapshot source exporting a committed batch over <c>k1</c> and
    /// <c>k2</c>. When gated it pauses after yielding <c>k1</c> (the drain applies
    /// a row before pulling the next) until released; it can fail a number of
    /// attempts with a non-transient fault after yielding <c>k1</c>. Static
    /// because the in-process silo constructs it.
    /// </summary>
    private sealed class GatedSnapshotSource : IBootstrapSnapshotSource
    {
        private static readonly object Sync = new();
        private static string? _tree;
        private static bool _gate;
        private static bool _unstableGeneration;
        private static int _failuresLeft;
        private static TaskCompletionSource _firstRowApplied = New();
        private static TaskCompletionSource _released = New();
        private static Guid _preparedSaga;

        private static TaskCompletionSource New() => new(TaskCreationOptions.RunContinuationsAsynchronously);

        public static void Arm(
            string tree,
            int failFirstAttempts,
            bool gate = true,
            Guid preparedSaga = default,
            bool unstableGeneration = false)
        {
            lock (Sync)
            {
                _tree = tree;
                _gate = gate;
                _unstableGeneration = unstableGeneration;
                _failuresLeft = failFirstAttempts;
                _preparedSaga = preparedSaga;
                _firstRowApplied = New();
                _released = New();
            }
        }

        public static void Release() => _released.TrySetResult();

        public static void Reset()
        {
            lock (Sync)
            {
                _tree = null;
                _unstableGeneration = false;
                _released.TrySetResult();
            }
        }

        public static async Task FirstRowAppliedAsync(TimeSpan timeout)
        {
            var reached = await Task.WhenAny(_firstRowApplied.Task, Task.Delay(timeout));
            Assert.That(reached, Is.SameAs(_firstRowApplied.Task),
                "PRECONDITION: the drain must have applied the first row and paused");
        }

        private static SnapshotEntry Prepared(string key, Guid saga, HybridLogicalClock stamp, int index) => new()
        {
            Key = key,
            Value = Bytes("batch"),
            Timestamp = stamp,
            IsPrepared = true,
            TransactionId = saga,
            AtomicBatchSize = 2,
            AtomicBatchIndex = index,
        };

        public Task<SnapshotStream> ExportAsync(string treeName, HybridLogicalClock asOfHlc, CancellationToken cancellationToken = default)
        {
            bool unstableGeneration;
            lock (Sync)
            {
                unstableGeneration = string.Equals(treeName, _tree, StringComparison.Ordinal) && _unstableGeneration;
            }

            if (!unstableGeneration)
            {
                return Task.FromResult(new SnapshotStream(treeName, asOfHlc, new VersionVector(), RowsAsync(treeName)));
            }

            var open = new SnapshotSourceGeneration
            {
                PhysicalTreeId = $"physical-{treeName}",
                ShardMapVersion = 1,
                Lineage = Guid.NewGuid(),
                DeleteEpoch = 0,
                IsDeleted = false,
            };
            return Task.FromResult(new SnapshotStream(treeName, asOfHlc, new VersionVector(), RowsAsync(treeName))
            {
                OpenGeneration = open,
                CloseGeneration = open with { Lineage = Guid.NewGuid() },
                OpenFrontier = new SnapshotSourceFrontier { Lineage = open.Lineage },
            });
        }

        private static async IAsyncEnumerable<SnapshotEntry> RowsAsync(string treeName)
        {
            var stamp = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks, Counter = 0 };
            Guid saga;
            lock (Sync) saga = string.Equals(treeName, _tree, StringComparison.Ordinal) ? _preparedSaga : Guid.Empty;
            if (saga != Guid.Empty)
            {
                // The export saw the saga in flight, so it ships both keys as
                // prepared rows; k2's is applied first, then the drain pauses.
                yield return Prepared("k2", saga, stamp, index: 1);
                _firstRowApplied.TrySetResult();
                await _released.Task;
                yield return Prepared("k1", saga, stamp, index: 0);
                yield break;
            }

            yield return new SnapshotEntry { Key = "k1", Value = Bytes("batch"), Timestamp = stamp };

            // The drain pulls the next row only after applying this one.
            bool fail;
            bool gate;
            lock (Sync)
            {
                if (!string.Equals(treeName, _tree, StringComparison.Ordinal))
                {
                    fail = false;
                    gate = false;
                }
                else
                {
                    fail = _failuresLeft > 0;
                    if (fail && _failuresLeft != int.MaxValue) _failuresLeft--;
                    gate = _gate;
                }
            }

            _firstRowApplied.TrySetResult();
            if (fail)
                throw new InvalidOperationException("injected: the snapshot stream broke after its first row");
            if (gate)
                await _released.Task;

            yield return new SnapshotEntry { Key = "k2", Value = Bytes("batch"), Timestamp = stamp };
        }
    }
}
