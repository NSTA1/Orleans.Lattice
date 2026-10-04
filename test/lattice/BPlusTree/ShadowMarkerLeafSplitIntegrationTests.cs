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
/// Real-grain reproduction of issue #4545: a destination-side saga shadow marker
/// that a leaf split carries onto a sibling which never sees the saga's terminal.
/// <para>
/// The trace the chaos fixture captured was: the saga's terminal lands on a
/// destination leaf, then a delayed shadow-marker install reaches the same leaf
/// (harmless there, because that leaf has already applied the terminal), then a
/// leaf split moves the marked key, and the marker, to a new sibling. The sibling
/// has not applied the terminal and never will, so its reshard read gate treats
/// the committed saga as undelivered and refuses every read of the migrated key
/// with a stale-routing signal until the decision ages out of the registry. The
/// reader spins until its response timeout.
/// </para>
/// <para>
/// This fixture drives exactly that sequence through the public shard-root
/// surface on a single-silo cluster with a short response timeout, so the hang
/// surfaces in seconds rather than at the default 30 s.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ShadowMarkerLeafSplitIntegrationTests
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

    /// <summary>
    /// Prepares <paramref name="key"/> under <paramref name="txid"/> on the shard,
    /// so the leaf holding it records a pending bucket and becomes a participant
    /// the saga's terminal reaches.
    /// </summary>
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

    [Test]
    public async Task A_marker_installed_after_its_terminal_does_not_strand_a_key_a_leaf_split_moves()
    {
        var (tree, shard, treeId) = await CreateSingleShardTreeAsync("shadow-split");
        const string preparedKey = "k-a";
        const string migratedKey = "k-z";
        var migratedValue = Bytes("post-saga");

        // 1. A committed saga whose prepare and terminal land on the leaf that
        //    will hold the migrated key: the leaf records the terminal.
        var txid = Guid.NewGuid();
        await PrepareAsync(shard, txid, preparedKey, Bytes("prepared"));
        await TxRegistryRouting.GetRegistry(_cluster.Client, treeId, txid).MarkCommittedAsync(txid);
        await shard.AppendTxTerminalAsync(txid, committed: true);

        // 2. A split drain migrates the saga's committed value for the migrated
        //    key onto the same leaf, as a migrated row.
        await shard.MergeManyAsync(
            new Dictionary<string, LwwValue<byte[]>>
            {
                [migratedKey] = LwwValue<byte[]>.Create(migratedValue, HybridLogicalClock.Tick(HybridLogicalClock.Zero)),
            },
            isCrossShardMigration: true);

        // 3. The delayed shadow-marker install reaches the leaf after the
        //    terminal. On this leaf it is harmless: the terminal already applied.
        await shard.MarkSagaShadowAsync(txid, [migratedKey]);
        Assert.That(await tree.GetAsync(migratedKey), Is.EqualTo(migratedValue),
            "precondition: the leaf that applied the terminal serves the migrated key");

        // 4. Writes below the migrated key overflow the leaf. It splits, and the
        //    migrated key - the largest - moves to the new sibling with the marker.
        foreach (var key in new[] { "k-b", "k-c", "k-d", "k-e", "k-f" })
        {
            await tree.SetAsync(key, Bytes(key));
        }

        // 5. The read must still be served. Before the fix the sibling carried the
        //    dead marker, never saw the terminal, and refused the read until the
        //    caller's response timeout.
        byte[]? read = null;
        Exception? failure = null;
        var started = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            read = await tree.GetAsync(migratedKey);
        }
        catch (Exception ex)
        {
            failure = ex;
        }
        started.Stop();

        Assert.That(failure, Is.Null,
            $"the read of the migrated key failed after {started.Elapsed.TotalSeconds:N1} s with "
            + $"{failure?.GetType().Name}: a shadow marker the leaf split carried is gating it");
        Assert.That(read, Is.EqualTo(migratedValue));
    }

    /// <summary>
    /// Installs a marker for a committed saga on a leaf that never applies the
    /// saga's terminal. That is indistinguishable, on the leaf, from the
    /// reactivation counter-trace the shard-ownership retention model found:
    /// the leaf applied the terminal, a reactivation forgot it, and a delayed
    /// marker install arrived. The marker carries the saga's marked prepare
    /// stamp P, as the shadow forward and the split sweep now send it.
    /// </summary>
    private async Task<(Exception? Failure, byte[]? Read, TimeSpan Elapsed)> ReadUnderOrphanedMarkerAsync(
        HybridLogicalClock rowStamp, HybridLogicalClock prepareStamp)
    {
        var (tree, shard, treeId) = await CreateSingleShardTreeAsync("shadow-selfcheck");
        const string key = "k-m";
        var txid = Guid.NewGuid();
        await TxRegistryRouting.GetRegistry(_cluster.Client, treeId, txid).MarkCommittedAsync(txid);

        await shard.MergeManyAsync(
            new Dictionary<string, LwwValue<byte[]>> { [key] = LwwValue<byte[]>.Create(Bytes("row"), rowStamp) },
            isCrossShardMigration: true);
        using (LatticeOriginalPrepareStampContext.With(new Dictionary<string, HybridLogicalClock> { [key] = prepareStamp }))
        {
            await shard.MarkSagaShadowAsync(txid, [key]);
        }

        TestContext.Out.WriteLine(
            $"gated row: stamp={rowStamp}, IsMigrated=true, marker P={prepareStamp}, "
            + $"row {(rowStamp.CompareTo(prepareStamp) >= 0 ? ">=" : "<")} P");

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

    [Test]
    public async Task A_marker_whose_terminal_this_leaf_never_sees_is_released_once_the_row_is_at_its_prepare_stamp()
    {
        var p = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks, Counter = 2 };

        var (failure, read, elapsed) = await ReadUnderOrphanedMarkerAsync(rowStamp: p, prepareStamp: p);

        Assert.That(failure, Is.Null,
            $"the read failed after {elapsed.TotalSeconds:N1} s with {failure?.GetType().Name}: the row already "
            + "holds the saga's value at its prepare stamp, so the marker must not gate it");
        Assert.That(read, Is.EqualTo(Bytes("row")));
    }

    /// <summary>
    /// The control: a migrated row below the saga's prepare stamp is the
    /// pre-saga value. Under a committed saga whose terminal never reached this
    /// leaf, serving it would lose the saga's write, so the gate must hold and
    /// the reader must surface the failure within its read budget.
    /// </summary>
    [Test]
    public async Task A_marker_whose_terminal_this_leaf_never_sees_still_gates_a_row_below_its_prepare_stamp()
    {
        var p = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks, Counter = 2 };
        var below = new HybridLogicalClock { WallClockTicks = p.WallClockTicks - 1, Counter = 0 };

        var (failure, read, elapsed) = await ReadUnderOrphanedMarkerAsync(rowStamp: below, prepareStamp: p);

        Assert.That(read, Is.Null, "a pre-saga row must never be served under a committed saga");
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
