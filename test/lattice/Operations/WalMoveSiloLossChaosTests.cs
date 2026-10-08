using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Operations;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;
using Orleans.Lattice.Tests.Fakes;
using System.Diagnostics;

namespace Orleans.Lattice.Tests.Operations;

/// <summary>
/// Silo loss in the middle of a tracked WAL partition move (#4124). The move is held
/// part-way through its tail copy by a gated target provider, the silo running the
/// operation is killed, and the operation must then read as
/// <see cref="LatticeOperationState.Failed"/> from the survivor - never stay running -
/// while the partition keeps serving: whichever silo the move's admin grain lived on,
/// the source is either released by its quiesce lease or the move completes, and every
/// key written before and after the loss reads back.
/// </summary>
/// <remarks>
/// Grain state lives in a process-scope store shared by every silo, so the operation
/// record and the tree survive the killed silo as they would on a durable provider.
/// Waits are bounded polls on observable state, never fixed sleeps.
/// </remarks>
[TestFixture]
[Category("Chaos")]
public sealed class WalMoveSiloLossChaosTests
{
    private const string Tree = "chaos-wal-move";

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        GatedWal.Reset();
        IndexGate.Reset();
        var builder = new TestClusterBuilder(2);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        await SharedInMemoryWal.AssertAllSilosShareOneWalAsync(_cluster);
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        GatedWal.Release();
        IndexGate.Release();
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [Test]
    public async Task A_wal_move_whose_runner_silo_is_killed_fails_and_the_partition_keeps_serving()
    {
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(Tree, new TreeRegistryEntry { ShardCount = 1, WalPartitions = 1 });
        var tree = _cluster.Client.GetGrain<ILattice>(Tree);
        for (var i = 0; i < 20; i++)
        {
            await tree.SetAsync($"before-{i}", [(byte)i]);
        }

        var secondary = _cluster.SecondarySilos[0];
        var runner = ((InProcessSiloHandle)secondary).SiloHost.Services.GetRequiredService<LatticeOperationRunner>();
        var survivor = ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services.GetRequiredService<LatticeOperationRunner>();
        var operationId = LatticeOperationKey.NewId();
        var ticket = LatticeOperationTicket.For("default", operationId);
        var grainFactory = ((InProcessSiloHandle)secondary).SiloHost.Services.GetRequiredService<IGrainFactory>();

        await runner.StartAsync(
            new LatticeOperationStart
            {
                TenantId = "default",
                OperationId = operationId,
                Kind = "treeadmin.wal-move",
                TreeIds = [Tree],
                Phases = [LatticeMaintenanceProgress.Copying, LatticeMaintenanceProgress.Verifying, LatticeMaintenanceProgress.Flipping],
            },
            (_, ct) => grainFactory
                .GetGrain<ILatticeAdminTrackedGrain>(LatticeConstants.AdminGrainKey)
                .ExecuteWalMoveTrackedAsync(
                    Tree, 0, "gated", new WalMoveOptions { QuiesceLease = TimeSpan.FromSeconds(2), CopyPageSize = 4 }, ticket, ct),
            static receipt => LatticeOperationCompletion.Succeeded(receipt.TreeId));

        await GatedWal.Entered.Task.WaitAsync(TimeSpan.FromMinutes(1));
        await TestPoll.UntilAsync(
            async () => (await survivor.GetAsync("default", operationId))?.State == LatticeOperationState.Running,
            "the survivor to observe the move running");

        // Model the dead-index routing window deterministically, independent of
        // where Orleans happened to place the index in this run.
        IndexGate.BlockTerminalUpdates = true;
        await _cluster.KillSiloAsync(secondary);

        var longestPoll = TimeSpan.Zero;
        await TestPoll.UntilAsync(
            async () =>
            {
                var watch = Stopwatch.StartNew();
                var status = await survivor.GetAsync("default", operationId).WaitAsync(TimeSpan.FromSeconds(2));
                longestPoll = watch.Elapsed > longestPoll ? watch.Elapsed : longestPoll;
                return status?.State == LatticeOperationState.Failed;
            },
            "the move of the killed silo to be failed rather than left running",
            timeout: TimeSpan.FromMinutes(2),
            cadence: TimeSpan.FromMilliseconds(250));
        var record = await survivor.GetAsync("default", operationId);
        Assert.That(IndexGate.Entered.Task.IsCompleted, Is.True, "The failing status path must actually encounter the blocked index.");
        Assert.That(longestPoll, Is.LessThan(TimeSpan.FromSeconds(2)), "No poll may wait out the 30-second Orleans response timeout.");
        IndexGate.Release();

        // Let a surviving saga (if the admin grain lived on the primary) run on; the
        // partition must serve either way.
        GatedWal.Release();
        await TestPoll.UntilAsync(
            async () =>
            {
                try
                {
                    await tree.SetAsync("after", [42]);
                    return true;
                }
                catch (Exception)
                {
                    return false;
                }
            },
            "the partition to accept writes again after the loss",
            timeout: TimeSpan.FromMinutes(2),
            cadence: TimeSpan.FromMilliseconds(500));

        Assert.Multiple(async () =>
        {
            Assert.That(record!.FailureReason, Does.Contain("not supported"), "The loss is named, not a generic failure.");
            Assert.That(record.Phase, Is.EqualTo(LatticeMaintenanceProgress.Copying), "The failed record keeps the phase it reached.");
            Assert.That(record.CompletedUnits, Is.EqualTo(4), "The first copied page was banked before the loss.");
            Assert.That(record.TotalUnits, Is.EqualTo(20));
            Assert.That(await tree.GetAsync("after"), Is.EqualTo(new byte[] { 42 }));
            for (var i = 0; i < 20; i++)
            {
                Assert.That(await tree.GetAsync($"before-{i}"), Is.EqualTo(new[] { (byte)i }), $"before-{i}");
            }
        });
    }

    private static class IndexGate
    {
        public static volatile bool BlockTerminalUpdates;
        public static TaskCompletionSource Entered { get; private set; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private static TaskCompletionSource Gate { get; set; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public static void Reset()
        {
            BlockTerminalUpdates = false;
            Entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
            Gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public static void Release() => Gate.TrySetResult();
        public static Task WaitAsync() => Gate.Task;
    }

    private sealed class IndexStallFilter : IOutgoingGrainCallFilter
    {
        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            if (IndexGate.BlockTerminalUpdates
                && context.InterfaceMethod.DeclaringType == typeof(ILatticeOperationIndexGrain)
                && (context.InterfaceMethod.Name == nameof(ILatticeOperationIndexGrain.MarkFinishedAsync)
                    || (context.InterfaceMethod.Name == nameof(ILatticeOperationIndexGrain.ReconcileAsync)
                        && context.Request.GetArgument(0) is LatticeOperationRecord { IsTerminal: true })))
            {
                IndexGate.Entered.TrySetResult();
                await IndexGate.WaitAsync();
            }
            await context.Invoke();
        }
    }

    /// <summary>Process-wide WAL stores shared by both silos, plus the copy gate.</summary>
    private static class GatedWal
    {
        public static InMemoryWalStorageProvider Baseline { get; private set; } = new();

        public static InMemoryWalStorageProvider Target { get; private set; } = new();

        public static TaskCompletionSource Entered { get; private set; } = NewSignal();

        private static TaskCompletionSource Gate { get; set; } = NewSignal();

        public static Task WaitAsync(CancellationToken cancellationToken) => Gate.Task.WaitAsync(cancellationToken);

        public static void Reset()
        {
            Baseline = new InMemoryWalStorageProvider();
            Target = new InMemoryWalStorageProvider();
            Entered = NewSignal();
            Gate = NewSignal();
            _appends = 0;
        }

        private static int _appends;

        public static int CountAppend() => Interlocked.Increment(ref _appends);

        public static void Release() => Gate.TrySetResult();

        private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    /// <summary>
    /// The move target: an in-memory WAL whose second copy page blocks until released,
    /// so the move is reliably mid-copy - with progress already recorded - when the
    /// runner silo is killed.
    /// </summary>
    private sealed class GatedTargetWalProvider : IWalStorageProvider
    {
        private static IWalStorageProvider Inner => GatedWal.Target;

        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
            => Inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken);

        public async Task AppendEncodedBatchAsync(
            string treeId,
            int shardIndex,
            ReadOnlyMemory<ArraySegment<byte>> encodedEntries,
            ReadOnlyMemory<long> offsets,
            IWalRecordEncoder encoder,
            CancellationToken cancellationToken)
        {
            if (GatedWal.CountAppend() == 2)
            {
                GatedWal.Entered.TrySetResult();
                await GatedWal.WaitAsync(cancellationToken);
            }

            await Inner.AppendEncodedBatchAsync(treeId, shardIndex, encodedEntries, offsets, encoder, cancellationToken);
        }

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => Inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken);

        public Task<WalShardEncodedPage> ReadEncodedAsync(
            string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, IWalRecordEncoder encoder, CancellationToken cancellationToken)
            => Inner.ReadEncodedAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, encoder, cancellationToken);

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => Inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => Inner.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => Inner.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) =>
                silo.Services.AddKeyedSingleton<Orleans.Storage.IGrainStorage>(
                    name,
                    (_, _) => new Orleans.Lattice.Tests.BPlusTree.PublicApiContract.ProcessScopeMemoryGrainStorage()));
            siloBuilder.AddWalStorage(_ => GatedWal.Baseline);
            siloBuilder.AddLatticeWalStorageProvider("gated", _ => new GatedTargetWalProvider());
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IOutgoingGrainCallFilter, IndexStallFilter>();
            siloBuilder.Services.Configure<LatticeOperationOptions>(o =>
            {
                o.HeartbeatInterval = TimeSpan.FromSeconds(1);
                o.HeartbeatLease = TimeSpan.FromSeconds(15);
            });
        }
    }
}
