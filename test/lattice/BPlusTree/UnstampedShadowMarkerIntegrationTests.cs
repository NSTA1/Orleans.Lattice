using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Real-grain detectors for issue #4545's residual: a destination-side shadow
/// marker that carries no marked prepare stamp (a CRDT-delta or resize-copy
/// sweep marker, or one from an older silo) reaching a leaf that never applied
/// the saga's terminal, after the saga has completed.
/// <para>
/// The self-verifying release cannot apply without a stamp, and the marker's
/// terminal has come and gone, so before the fix the marker gated the migrated
/// key until the registry stopped reporting the saga's decision: the
/// <c>ReadableOnceComplete</c> violation the shard-ownership retention model
/// finds at depth 14, for the marker kinds that carry no P.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class UnstampedShadowMarkerIntegrationTests
{
    private static readonly TimeSpan ClusterResponseTimeout = TimeSpan.FromSeconds(6);

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        builder.AddClientBuilderConfigurator<ClientConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private async Task<(ILattice Tree, IShardRootGrain Shard, string TreeId)> CreateSingleShardTreeAsync(string prefix)
    {
        var treeId = $"{prefix}-{Guid.NewGuid():N}";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            ShardCount = 1,
            MaxLeafKeys = 4,
            MaxInternalChildren = 4,
        });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        var physical = (await tree.GetRoutingAsync()).PhysicalTreeId;
        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{physical}/0");
        return (tree, shard, treeId);
    }

    private static async Task PrepareAsync(IShardRootGrain shard, Guid txid, string key, byte[] value)
    {
        var previous = LatticeTransactionContext.Current;
        LatticeTransactionContext.Set(txid);
        try
        {
            using (LatticePreparedContext.BeginScope())
            {
                await shard.SetAsync(key, value);
            }
        }
        finally
        {
            LatticeTransactionContext.Set(previous);
        }
    }

    private static async Task<(Exception? Failure, byte[]? Read, TimeSpan Elapsed)> TimedReadAsync(ILattice tree, string key)
    {
        var started = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            return (null, await tree.GetAsync(key), started.Elapsed);
        }
        catch (Exception ex)
        {
            return (ex, null, started.Elapsed);
        }
    }

    /// <summary>
    /// The saga's terminal settles the key on its leaf and the split source's
    /// own post-saga row arrives as a migration import. The saga completes. A
    /// leaf split then moves the key to a new sibling, and a delayed marker
    /// for the saga - one that carries no prepare stamp - is routed by key to
    /// that sibling, which never saw the terminal. The read must be served.
    /// </summary>
    [Test]
    public async Task An_unstamped_marker_reaching_a_split_sibling_after_its_saga_completed_does_not_hide_the_key()
    {
        var (tree, shard, treeId) = await CreateSingleShardTreeAsync("unstamped-split");
        const string participantKey = "k-a";
        const string key = "k-z";
        var sagaValue = Bytes("saga");

        var txid = Guid.NewGuid();
        var registry = TxRegistryRouting.GetRegistry(_cluster.Client, treeId, txid);
        await PrepareAsync(shard, txid, participantKey, Bytes("prepared"));
        await registry.MarkCommittedAsync(txid);
        await shard.AppendTxTerminalAsync(
            txid, committed: true, new Dictionary<string, byte[]> { [key] = sagaValue });

        var sourceRowStamp = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.AddSeconds(1).Ticks, Counter = 0 };
        await shard.MergeManyAsync(
            new Dictionary<string, LwwValue<byte[]>> { [key] = LwwValue<byte[]>.Create(sagaValue, sourceRowStamp) },
            isCrossShardMigration: true);

        // The saga's broadcast is done: it forgets its decision, which the
        // registry keeps reporting for the retention window.
        await registry.ForgetAsync(txid);
        Assert.That(await registry.GetStatusAsync(txid), Is.EqualTo(TxStatus.Committed),
            "precondition: the completed saga's decision is still reported");

        foreach (var k in new[] { "k-b", "k-c", "k-d", "k-e", "k-f" })
        {
            await tree.SetAsync(k, Bytes(k));
        }

        await shard.MarkSagaShadowAsync(txid, [key]);

        var (failure, read, elapsed) = await TimedReadAsync(tree, key);
        Assert.That(failure, Is.Null,
            $"the read failed after {elapsed.TotalSeconds:N1} s with {failure?.GetType().Name}: an unstamped marker "
            + "for a completed saga is hiding the key on a leaf its terminal will never reach");
        Assert.That(read, Is.EqualTo(sagaValue));
    }

    /// <summary>
    /// The control: while the saga has decided but not completed, a migrated
    /// row under an unstamped marker on a leaf that has not applied the
    /// terminal may still be the pre-saga value, so the gate must hold.
    /// </summary>
    [Test]
    public async Task An_unstamped_marker_still_gates_while_its_saga_has_not_completed()
    {
        var (tree, shard, treeId) = await CreateSingleShardTreeAsync("unstamped-open");
        const string key = "k-m";
        var txid = Guid.NewGuid();
        await TxRegistryRouting.GetRegistry(_cluster.Client, treeId, txid).MarkCommittedAsync(txid);

        await shard.MergeManyAsync(
            new Dictionary<string, LwwValue<byte[]>>
            {
                [key] = LwwValue<byte[]>.Create(Bytes("pre-saga"), HybridLogicalClock.Tick(HybridLogicalClock.Zero)),
            },
            isCrossShardMigration: true);
        await shard.MarkSagaShadowAsync(txid, [key]);

        var (failure, read, elapsed) = await TimedReadAsync(tree, key);
        Assert.That(read, Is.Null, "a possibly pre-saga row must never be served under a committed, open saga");
        Assert.That(failure, Is.Not.Null);
        Assert.That(elapsed, Is.LessThan(ClusterResponseTimeout), "the read budget bounds the refusal");
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.Configure<SiloMessagingOptions>(o =>
            {
                o.ResponseTimeout = ClusterResponseTimeout;
                o.SystemResponseTimeout = ClusterResponseTimeout;
            });
        }
    }

    private sealed class ClientConfigurator : IClientBuilderConfigurator
    {
        public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
        {
            clientBuilder.Services.Configure<ClientMessagingOptions>(o => o.ResponseTimeout = ClusterResponseTimeout);
        }
    }
}
