using System.Collections.Concurrent;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Concurrency;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Proves that <see cref="IShardRootGrain.IsSplittingAsync"/> does not
/// head-of-line-block behind a long non-reentrant turn on the shard-root
/// activation.
/// <para>
/// <see cref="IsSplittingAsync"/> is one of three probes the split and
/// healing coordinators fan out per shard and then await together:
/// </para>
/// <list type="bullet">
/// <item><description>
/// <c>HotShardMonitorGrain.RunSamplingPassAsync</c> fans out
/// <c>GetHotnessAsync</c>, <c>HasPendingBulkOperationAsync</c>, and
/// <c>IsSplittingAsync</c> across every shard and awaits each set with a
/// separate <c>Task.WhenAll</c>.
/// </description></item>
/// <item><description>
/// <c>ShardHealingOrchestratorGrain.ObserveLoadAsync</c> fans out the same
/// three probes in the same shape.
/// </description></item>
/// </list>
/// <para>
/// The first two probes are marked <see cref="AlwaysInterleaveAttribute"/>;
/// <c>IsSplittingAsync</c> was not. On a shard whose activation was busy in a
/// long non-reentrant turn (a paged scan, for instance) the two interleaved
/// probes returned and the third queued behind that turn until the 30s
/// response timeout fired. Because the fan-out is awaited with
/// <c>Task.WhenAll</c>, that single shard's timeout faulted the whole await
/// and aborted sampling for every other shard in the tree - so the monitor
/// stopped detecting hot shards entirely, and splitting never ran to relieve
/// the very load producing the long turns.
/// </para>
/// <para>
/// The behavioural test below is the load-bearing one: it parks a genuinely
/// non-reentrant turn on a real Orleans activation and asserts a concurrent
/// probe still completes. The reflection test is the wire-contract half -
/// without the attribute the behavioural interleave cannot hold, so pinning
/// it on the interface names exactly which edit broke the property when it
/// regresses. Neither substitutes for the other, and the reflection guard
/// deliberately covers all three members of the fan-out rather than
/// <c>IsSplittingAsync</c> alone: they are dispatched together and awaited
/// together, so they have to interleave together. A guard over the set
/// would have caught this defect when it was introduced.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ShardRootGrainIsSplittingInterleaveTests
{
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
        GatingShardRootStorage.Reset();
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    /// <summary>
    /// The three probes the coordinators fan out per shard must all be
    /// interleaved. <c>IsSplittingAsync</c> is the member this fixture was
    /// written for; the other two are included so that dropping the
    /// attribute from any member of the awaited set fails here.
    /// </summary>
    [TestCase(nameof(IShardRootGrain.IsSplittingAsync))]
    [TestCase(nameof(IShardRootGrain.GetHotnessAsync))]
    [TestCase(nameof(IShardRootGrain.HasPendingBulkOperationAsync))]
    public void Coordinator_fanout_probe_is_marked_AlwaysInterleave(string methodName)
    {
        var method = typeof(IShardRootGrain).GetMethod(methodName);

        Assert.That(method, Is.Not.Null,
            $"Expected to find method '{methodName}' on IShardRootGrain.");
        Assert.That(method!.GetCustomAttribute<AlwaysInterleaveAttribute>(inherit: false), Is.Not.Null,
            $"IShardRootGrain.{methodName} must be [AlwaysInterleave]. It is fanned out per shard by " +
            "HotShardMonitorGrain.RunSamplingPassAsync and ShardHealingOrchestratorGrain.ObserveLoadAsync " +
            "and awaited with Task.WhenAll, so if it queues behind a long non-reentrant turn on one shard " +
            "its 30s timeout faults the whole await and aborts sampling for every other shard in the tree.");
    }

    /// <summary>
    /// The behavioural guarantee. Parks a non-reentrant
    /// <see cref="IShardRootGrain.BeginSplitAsync"/> turn inside the grain
    /// storage write and asserts a concurrent
    /// <see cref="IShardRootGrain.IsSplittingAsync"/> completes without
    /// waiting for it.
    /// <para>
    /// <c>BeginSplitAsync</c> is the blocker precisely because it is the
    /// production scenario: a split is being initiated on this shard while
    /// the hot-shard monitor polls it. It carries no interleave attribute,
    /// so it holds the activation's non-reentrant turn, and it ends in a
    /// persisted state write that the gated storage below can park on.
    /// <c>SetManyAsync</c> would not work as a blocker - it is itself
    /// <c>[AlwaysInterleave]</c> and so never holds the turn.
    /// </para>
    /// </summary>
    [Test]
    public async Task IsSplittingAsync_completes_while_a_non_reentrant_turn_is_parked()
    {
        const string tree = "issplitting-interleave-tree";
        var grain = _cluster.Client.GetGrain<IShardRootGrain>($"{tree}/0");

        // Warm the activation so the probe below cannot be measuring
        // activation latency, and so any first-touch state write has
        // already happened before the gate is armed.
        await grain.IsSplittingAsync().WaitAsync(TimeSpan.FromSeconds(30));

        GatingShardRootStorage.Arm(tree);
        Task? blocker = null;
        try
        {
            // Enters the non-reentrant turn and parks inside the write that
            // persists the split intent, holding the activation's turn open.
            blocker = grain.BeginSplitAsync(targetShardIndex: 1, movedSlots: [0], virtualShardCount: 4);
            await GatingShardRootStorage.WriteEntered!.Task.WaitAsync(TimeSpan.FromSeconds(30));

            // With that turn parked, the probe must still answer. Without
            // [AlwaysInterleave] it queues behind the parked turn and only
            // completes when the gate is released below - which in
            // production is where the 30s response timeout fires.
            var probe = grain.IsSplittingAsync();
            var winner = await Task.WhenAny(probe, Task.Delay(TimeSpan.FromSeconds(10)));

            Assert.That(winner, Is.SameAs(probe),
                "IsSplittingAsync must interleave with a non-reentrant turn parked on the same shard-root " +
                "activation. It queued behind the parked BeginSplitAsync turn instead, which is the " +
                "head-of-line block that times out the coordinators' per-shard fan-out.");

            // The probe answered from the live activation, not from a stale
            // snapshot. BeginSplitAsync assigns SplitInProgress before the
            // parked write, so the interleaved reader observes the new value
            // one persisted write earlier than a queued reader would - the
            // staleness-window narrowing documented on the interface, made
            // observable here.
            Assert.That(await probe, Is.True,
                "The interleaved probe should observe the split record the parked turn already assigned.");
        }
        finally
        {
            GatingShardRootStorage.Release();
            if (blocker is not null)
            {
                await blocker.WaitAsync(TimeSpan.FromSeconds(30));
            }
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) =>
                silo.Services.AddKeyedSingleton<IGrainStorage>(
                    name,
                    (_, _) => new GatingShardRootStorage()));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.ConfigureLattice(o => o.WalPartitions = 1);
        }
    }

    /// <summary>
    /// In-memory <see cref="IGrainStorage"/> that, when armed for a tree,
    /// parks the write that persists a shard's split intent on a gate. That
    /// holds the writing grain's non-reentrant turn open for as long as the
    /// test needs, which is the only way to observe interleaving on a real
    /// activation: the attribute is honoured by the Orleans scheduler, which a
    /// unit-level harness does not run.
    /// <para>
    /// The gate deliberately keys on <c>SplitInProgress is not null</c> rather
    /// than on the state type alone. <c>BeginSplitAsync</c> calls
    /// <c>PrepareForOperationAsync</c> first, which can itself persist
    /// shard-root state before the split record is assigned; parking on that
    /// earlier write would hold the same non-reentrant turn but leave
    /// <c>SplitInProgress</c> still null, making the observed-value assertion
    /// non-deterministic. Keying on the split record pins the park to the one
    /// write that follows the assignment.
    /// </para>
    /// <para>
    /// The control state is static because the TestingHost silo runs
    /// in-process, and it is scoped to a unique tree id so it cannot leak
    /// into other fixtures.
    /// </para>
    /// </summary>
    private sealed class GatingShardRootStorage : IGrainStorage
    {
        private readonly ConcurrentDictionary<string, (string ETag, object State)> _store = new();

        internal static volatile TaskCompletionSource? WriteGate;
        internal static volatile TaskCompletionSource? WriteEntered;
        private static volatile string? _gatedTree;

        internal static void Arm(string tree)
        {
            _gatedTree = tree;
            WriteEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            WriteGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        internal static void Release() => WriteGate?.TrySetResult();

        internal static void Reset()
        {
            _gatedTree = null;
            WriteGate = null;
            WriteEntered = null;
        }

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            ArgumentNullException.ThrowIfNull(stateName);
            ArgumentNullException.ThrowIfNull(grainState);

            if (_store.TryGetValue(MakeKey(stateName, grainId), out var entry))
            {
                grainState.State = (T)entry.State;
                grainState.ETag = entry.ETag;
                grainState.RecordExists = true;
            }
            else
            {
                grainState.RecordExists = false;
            }
            return Task.CompletedTask;
        }

        public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            ArgumentNullException.ThrowIfNull(stateName);
            ArgumentNullException.ThrowIfNull(grainState);

            var gate = WriteGate;
            if (gate is not null &&
                grainState.State is ShardRootState shardRoot &&
                shardRoot.SplitInProgress is not null &&
                grainId.Key.ToString()?.StartsWith(_gatedTree ?? "\u0000", StringComparison.Ordinal) == true)
            {
                WriteEntered?.TrySetResult();
                await gate.Task.ConfigureAwait(false);
            }

            var newEtag = Guid.NewGuid().ToString("N");
            _store[MakeKey(stateName, grainId)] = (newEtag, grainState.State!);
            grainState.ETag = newEtag;
            grainState.RecordExists = true;
        }

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            ArgumentNullException.ThrowIfNull(stateName);
            ArgumentNullException.ThrowIfNull(grainState);

            _store.TryRemove(MakeKey(stateName, grainId), out _);
            grainState.ETag = null!;
            grainState.RecordExists = false;
            return Task.CompletedTask;
        }

        private static string MakeKey(string stateName, GrainId grainId) => $"{stateName}/{grainId}";
    }
}
