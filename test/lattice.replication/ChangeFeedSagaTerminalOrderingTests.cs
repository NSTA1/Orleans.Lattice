using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4511: <see cref="IChangeFeed"/> must never yield a saga terminal
/// ahead of a prepare it resolves. Runs the real <see cref="ChangeFeed"/>
/// over real two-partition WAL shard grains. The interleaving test wraps the
/// real grain references only to append, at a chosen moment, records the
/// real grains then store and serve.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ChangeFeedSagaTerminalOrderingTests
{
    private const string ClusterId = "feed-saga-site";
    private const int Partitions = 2;

    private TestCluster _cluster = null!;
    private IOptionsMonitor<LatticeReplicationOptions> _options = null!;
    private ILatticeMergeModeResolver _resolver = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        _options = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        _options.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions
        {
            ClusterId = ClusterId,
            ReplogPartitions = Partitions,
        });
        _resolver = Substitute.For<ILatticeMergeModeResolver>();
        _resolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task Subscribe_yields_prepare_appended_to_an_already_read_partition_before_its_terminal()
    {
        // The feed reads partition 0 (empty), then partition 1. Just before
        // partition 1 is read, the saga prepares on partition 0 and its
        // terminal lands on partition 1 - real-time order, prepare first.
        var tree = $"feed-race-{Guid.NewGuid():N}";
        var txid = Guid.NewGuid();
        var hooked = false;
        var factory = InterceptingFactory.Create(_cluster.Client, (key, grain) =>
            key == $"{tree}/1"
                ? InterceptingWalShard.Create(grain, async () =>
                {
                    if (hooked)
                    {
                        return;
                    }

                    hooked = true;
                    await Shard(tree, 0).AppendAsync(Prepare(tree, txid, "a", 100), CancellationToken.None);
                    await Shard(tree, 1).AppendAsync(Terminal(tree, txid, 200), CancellationToken.None);
                })
                : grain);
        var feed = new ChangeFeed(factory, _options, _resolver);

        var entries = await CollectAsync(feed.Subscribe(tree, ChangeFeedCursor.Initial));

        Assert.That(hooked, Is.True, "the interleaving hook never ran");
        AssertTerminalFollowsPrepares(entries, txid, expectedPrepares: 1);
    }

    [Test]
    public async Task Subscribe_yields_prepare_appended_after_its_tail_was_captured_before_terminal()
    {
        var tree = $"feed-tail-race-{Guid.NewGuid():N}";
        var txid = Guid.NewGuid();
        var appendCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var appended = 0;
        var factory = InterceptingFactory.Create(_cluster.Client, (key, grain) =>
        {
            if (key == $"{tree}/0")
            {
                return InterceptingWalShard.Create(
                    grain,
                    static () => Task.CompletedTask,
                    async (inner, cancellationToken) =>
                    {
                        await appendCompleted.Task.WaitAsync(cancellationToken);
                        return await inner.GetNextSequenceAsync(cancellationToken);
                    });
            }

            if (key == $"{tree}/1")
            {
                return InterceptingWalShard.Create(
                    grain,
                    static () => Task.CompletedTask,
                    async (inner, cancellationToken) =>
                    {
                        var capturedTail = await inner.GetNextSequenceAsync(cancellationToken);
                        if (Interlocked.Exchange(ref appended, 1) == 0)
                        {
                            await Shard(tree, 1).AppendAsync(
                                Prepare(tree, txid, "a", 100) with { AtomicBatchSize = 1 },
                                cancellationToken);
                            await Shard(tree, 0).AppendAsync(Terminal(tree, txid, 200), cancellationToken);
                            appendCompleted.TrySetResult();
                        }

                        return capturedTail;
                    });
            }

            return grain;
        });
        var feed = new ChangeFeed(factory, _options, _resolver);

        var entries = await CollectAsync(feed.Subscribe(tree, ChangeFeedCursor.Initial));

        Assert.That(appended, Is.EqualTo(1), "the tail-capture interleaving hook never ran");
        AssertTerminalFollowsPrepares(entries, txid, expectedPrepares: 1);
    }

    [Test]
    public async Task Subscribe_yields_terminal_after_a_prepare_with_a_later_hlc()
    {
        // Leaf clocks are independent, so a prepare on one partition can
        // carry a later HLC than its saga's terminal on another.
        var tree = $"feed-skew-{Guid.NewGuid():N}";
        var txid = Guid.NewGuid();
        await Shard(tree, 0).AppendAsync(Terminal(tree, txid, 100), CancellationToken.None);
        await Shard(tree, 1).AppendAsync(Prepare(tree, txid, "a", 200), CancellationToken.None);
        var feed = new ChangeFeed(_cluster.Client, _options, _resolver);

        var entries = await CollectAsync(feed.Subscribe(tree, HybridLogicalClock.Zero));

        AssertTerminalFollowsPrepares(entries, txid, expectedPrepares: 1);
    }

    [Test]
    public async Task Subscribe_keeps_hlc_order_among_non_terminal_records()
    {
        var tree = $"feed-mixed-{Guid.NewGuid():N}";
        var txid = Guid.NewGuid();
        await Shard(tree, 0).AppendAsync(Prepare(tree, txid, "b", 300), CancellationToken.None);
        await Shard(tree, 0).AppendAsync(Terminal(tree, txid, 150), CancellationToken.None);
        await Shard(tree, 1).AppendAsync(Prepare(tree, txid, "a", 100), CancellationToken.None);
        var feed = new ChangeFeed(_cluster.Client, _options, _resolver);

        var entries = await CollectAsync(feed.Subscribe(tree, ChangeFeedCursor.Initial));

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "a", "b", string.Empty }));
        AssertTerminalFollowsPrepares(entries, txid, expectedPrepares: 2);
    }

    private static void AssertTerminalFollowsPrepares(List<WalRecord> entries, Guid txid, int expectedPrepares)
    {
        var terminalIndex = entries.FindIndex(e => e.Op == MutationKind.TxCommit && e.TransactionId == txid);
        var prepareIndexes = entries
            .Select((e, i) => (e, i))
            .Where(x => x.e.IsPrepared && x.e.TransactionId == txid)
            .Select(x => x.i)
            .ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(terminalIndex, Is.GreaterThanOrEqualTo(0), "the terminal was not yielded");
            Assert.That(prepareIndexes, Has.Length.EqualTo(expectedPrepares), "a prepare was not yielded");
            Assert.That(prepareIndexes, Is.All.LessThan(terminalIndex), "the terminal was yielded ahead of a prepare");
        });
    }

    private IWalShardGrain Shard(string tree, int partition) =>
        _cluster.Client.GetGrain<IWalShardGrain>($"{tree}/{partition}");

    private static WalRecord Prepare(string tree, Guid txid, string key, long ticks) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Value = [1],
        Timestamp = new HybridLogicalClock { WallClockTicks = ticks },
        OriginClusterId = ClusterId,
        TransactionId = txid,
        IsPrepared = true,
    };

    private static WalRecord Terminal(string tree, Guid txid, long ticks) => new()
    {
        TreeId = tree,
        Op = MutationKind.TxCommit,
        Key = string.Empty,
        Timestamp = new HybridLogicalClock { WallClockTicks = ticks },
        OriginClusterId = ClusterId,
        TransactionId = txid,
    };

    private static async Task<List<WalRecord>> CollectAsync(IAsyncEnumerable<WalRecord> source)
    {
        var result = new List<WalRecord>();
        await foreach (var entry in source)
        {
            result.Add(entry);
        }

        return result;
    }

    /// <summary>Hands out the real grain references, wrapping chosen WAL shards.</summary>
    public class InterceptingFactory : DispatchProxy
    {
        private IGrainFactory _inner = null!;
        private Func<string, IWalShardGrain, IWalShardGrain> _wrap = null!;

        internal static IGrainFactory Create(IGrainFactory inner, Func<string, IWalShardGrain, IWalShardGrain> wrap)
        {
            var proxy = DispatchProxy.Create<IGrainFactory, InterceptingFactory>();
            var self = (InterceptingFactory)(object)proxy;
            self._inner = inner;
            self._wrap = wrap;
            return proxy;
        }

        /// <inheritdoc />
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            var result = targetMethod!.Invoke(_inner, args);
            return result is IWalShardGrain grain && args is [string key, ..] ? _wrap(key, grain) : result;
        }
    }

    /// <summary>Runs a hook before each read, then forwards to the real WAL shard grain.</summary>
    public class InterceptingWalShard : DispatchProxy
    {
        private IWalShardGrain _inner = null!;
        private Func<Task> _beforeRead = null!;
        private Func<IWalShardGrain, CancellationToken, ValueTask<long>>? _getNextSequence;

        internal static IWalShardGrain Create(
            IWalShardGrain inner,
            Func<Task> beforeRead,
            Func<IWalShardGrain, CancellationToken, ValueTask<long>>? getNextSequence = null)
        {
            var proxy = DispatchProxy.Create<IWalShardGrain, InterceptingWalShard>();
            var self = (InterceptingWalShard)(object)proxy;
            self._inner = inner;
            self._beforeRead = beforeRead;
            self._getNextSequence = getNextSequence;
            return proxy;
        }

        /// <inheritdoc />
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod!.Name == nameof(IWalShardGrain.ReadAsync))
            {
                return new ValueTask<WalShardPage>(ReadAfterHookAsync(args!));
            }

            if (targetMethod!.Name == nameof(IWalShardGrain.GetNextSequenceAsync)
                && _getNextSequence is not null)
            {
                return _getNextSequence(_inner, (CancellationToken)args![0]!);
            }

            return targetMethod.Invoke(_inner, args);
        }

        private async Task<WalShardPage> ReadAfterHookAsync(object?[] args)
        {
            await _beforeRead();
            return await _inner.ReadAsync((long)args[0]!, (int)args[1]!, (CancellationToken)args[2]!);
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o => o.WalPartitions = Partitions);
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = ClusterId);
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }
}
