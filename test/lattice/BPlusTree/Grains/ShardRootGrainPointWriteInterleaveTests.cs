using System.Reflection;
using System.Text;
using Orleans.Concurrency;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Serialization.Invocation;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Proves that point writes on one shard root run concurrently (issue #812).
/// <para>
/// Before the change, <see cref="IShardRootGrain.SetAsync(string, byte[])"/> and its
/// expiry overload held the activation's non-reentrant turn across the whole
/// leaf and write-ahead-log round trip, so every point write aimed at a shard
/// queued behind the one in flight. On the set-point rig nearly all of the set
/// latency was that queue: the leaf commit took 20-32 ms while the shard stage
/// took 257 ms.
/// </para>
/// <para>
/// The behavioural tests park one point write inside its leaf call with an
/// incoming grain call filter and assert that a second point write on the same
/// shard root completes while the first is still parked. The attribute is
/// honoured by the Orleans scheduler, which a unit-level harness does not run,
/// so only a real activation can observe it. The reflection test is the
/// wire-contract half: it names the edit that broke the property when it
/// regresses.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ShardRootGrainPointWriteInterleaveTests
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
        GatingLeafSetFilter.Reset();
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    public void Point_SetAsync_is_marked_AlwaysInterleave(bool withExpiry)
    {
        var parameters = withExpiry
            ? new[] { typeof(string), typeof(byte[]), typeof(long) }
            : new[] { typeof(string), typeof(byte[]) };
        var method = typeof(IShardRootGrain).GetMethod(nameof(IShardRootGrain.SetAsync), parameters);

        Assert.That(method, Is.Not.Null, "Expected to find the IShardRootGrain.SetAsync overload.");
        Assert.That(method!.GetCustomAttribute<AlwaysInterleaveAttribute>(inherit: false), Is.Not.Null,
            "IShardRootGrain.SetAsync must be [AlwaysInterleave]. Without it every point write on a shard " +
            "queues behind the one in flight for its whole leaf and WAL round trip, which caps set-point " +
            "throughput at one write per commit per shard (issue #812).");
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Point_SetAsync_completes_while_another_point_write_on_the_same_shard_is_parked(bool withExpiry)
    {
        var tree = $"point-write-interleave-{(withExpiry ? "ttl" : "plain")}";
        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{tree}/0");

        // Warm the activation and create its root, so the probe below cannot be
        // measuring activation or first-write latency.
        await shard.SetAsync("warm", [0]).WaitAsync(TimeSpan.FromSeconds(30));

        var expiry = DateTime.UtcNow.AddHours(1).Ticks;
        GatingLeafSetFilter.Arm("parked");
        Task? blocker = null;
        try
        {
            blocker = withExpiry
                ? shard.SetAsync("parked", Encoding.UTF8.GetBytes("parked"), expiry)
                : shard.SetAsync("parked", Encoding.UTF8.GetBytes("parked"));
            await GatingLeafSetFilter.Entered!.Task.WaitAsync(TimeSpan.FromSeconds(30));

            // Without [AlwaysInterleave] this queues behind the parked write and
            // completes only when the gate is released below.
            var probe = withExpiry
                ? shard.SetAsync("probe", Encoding.UTF8.GetBytes("probe"), expiry)
                : shard.SetAsync("probe", Encoding.UTF8.GetBytes("probe"));
            var winner = await Task.WhenAny(probe, Task.Delay(TimeSpan.FromSeconds(10)));

            Assert.That(winner, Is.SameAs(probe),
                "A point write must not queue behind another point write parked on the same shard root.");
            Assert.That(blocker.IsCompleted, Is.False, "The parked write must still be parked when the probe completes.");
        }
        finally
        {
            GatingLeafSetFilter.Release();
            if (blocker is not null)
            {
                await blocker.WaitAsync(TimeSpan.FromSeconds(30));
            }
        }

        Assert.Multiple(async () =>
        {
            Assert.That(await shard.GetAsync("parked"), Is.EqualTo(Encoding.UTF8.GetBytes("parked")));
            Assert.That(await shard.GetAsync("probe"), Is.EqualTo(Encoding.UTF8.GetBytes("probe")));
        });
    }

    [Test]
    public async Task Serial_turn_waits_for_an_in_flight_point_write_and_observes_it()
    {
        var shard = _cluster.Client.GetGrain<IShardRootGrain>("point-write-quiesce/0");
        await shard.SetAsync("warm", [0]).WaitAsync(TimeSpan.FromSeconds(30));

        GatingLeafSetFilter.Arm("parked");
        Task? blocker = null;
        Task<byte[]?>? serialRead = null;
        try
        {
            blocker = shard.SetAsync("parked", Encoding.UTF8.GetBytes("parked"));
            await GatingLeafSetFilter.Entered!.Task.WaitAsync(TimeSpan.FromSeconds(30));

            // GetAsync is a serial turn. Orleans admits it while only
            // always-interleave calls are running, so without the quiesce guard
            // it would run part-way through the parked write. Split and fold
            // phase transitions are serial turns too, and they rely on no point
            // write straddling them.
            serialRead = shard.GetAsync("parked");
            var winner = await Task.WhenAny(serialRead, Task.Delay(TimeSpan.FromSeconds(3)));
            Assert.That(winner, Is.Not.SameAs(serialRead),
                "A serial shard-root turn must not start while a point write is in flight.");
        }
        finally
        {
            GatingLeafSetFilter.Release();
            if (blocker is not null)
            {
                await blocker.WaitAsync(TimeSpan.FromSeconds(30));
            }
        }

        var value = await serialRead!.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.That(value, Is.EqualTo(Encoding.UTF8.GetBytes("parked")),
            "The serial turn runs after the point write drains, so it observes the write.");
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.ConfigureLattice(o => o.WalPartitions = 1);
            siloBuilder.AddIncomingGrainCallFilter<GatingLeafSetFilter>();
        }
    }

    /// <summary>
    /// Parks the leaf <c>SetAsync</c> call for one armed key until released. The
    /// shard root's turn stays suspended on that call, which is the state a slow
    /// leaf commit leaves it in. The control state is static because the
    /// TestingHost silo runs in-process, and it matches a single key so it
    /// cannot park any other write.
    /// </summary>
    private sealed class GatingLeafSetFilter : IIncomingGrainCallFilter
    {
        internal static volatile TaskCompletionSource? Gate;
        internal static volatile TaskCompletionSource? Entered;
        private static volatile string? _key;

        internal static void Arm(string key)
        {
            Entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _key = key;
        }

        internal static void Release() => Gate?.TrySetResult();

        internal static void Reset()
        {
            _key = null;
            Gate?.TrySetResult();
            Gate = null;
            Entered = null;
        }

        public async Task Invoke(IIncomingGrainCallContext context)
        {
            ArgumentNullException.ThrowIfNull(context);

            var gate = Gate;
            if (gate is not null && IsGatedLeafSet(context.Request))
            {
                Entered?.TrySetResult();
                await gate.Task;
            }

            await context.Invoke();
        }

        private static bool IsGatedLeafSet(IInvokable request) =>
            _key is { } key
            && request.GetInterfaceType() == typeof(IBPlusLeafGrain)
            && request.GetMethodName() == nameof(IBPlusLeafGrain.SetAsync)
            && request.GetArgumentCount() > 0
            && request.GetArgument(0) is string argument
            && string.Equals(argument, key, StringComparison.Ordinal);
    }
}
