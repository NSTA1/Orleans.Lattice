using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Wal;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Real-grain detector for issue #4654: a leaf whose own state row vanishes -
/// lost storage, or a row deleted outside the lattice - while the shard still
/// routes to it must not come up empty and report acknowledged keys absent.
/// <para>
/// The row is the leaf's only link to its tree, key range, checkpoint and
/// kept-snapshot record (issue #4634), so with it gone the leaf had nothing to
/// replay from and nothing to fail closed on: it activated unbound and served an
/// empty cache, and every key whose only durable copy was the trimmed WAL prefix
/// or the leaf's snapshot read as absent with no error.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class VanishedLeafRowIntegrationTests
{
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
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

    private static IServiceProvider Services => SiloServiceProviderCaptureForWalTests.Captured
        ?? throw new InvalidOperationException("Silo IServiceProvider was not captured by the fixture.");

    private async Task<(ILattice Tree, string TreeId, GrainId LeafId)> CreateSnapshottedTrimmedLeafAsync(string prefix)
    {
        var logical = $"{prefix}-{Guid.NewGuid():N}";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(logical, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 1024 });
        var tree = _cluster.Client.GetGrain<ILattice>(logical);
        var treeId = (await tree.GetRoutingAsync()).PhysicalTreeId;
        for (var i = 0; i < 20; i++)
        {
            await tree.SetAsync($"k-{i:D2}", Bytes($"v-{i}"));
        }

        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{treeId}/0");
        var leafId = (await shard.GetLeftmostLeafIdAsync())!.Value;
        var leaf = _cluster.Client.GetGrain<IBPlusLeafGrain>(leafId.GetGuidKey());

        // Cover the written prefix with a snapshot, so the next checkpoint flush
        // publishes the durable pin that licenses the WAL GC to trim it.
        var head = await _cluster.Client.GetGrain<ILeafReplayCoordinatorGrain>($"{treeId}/0").GetHeadOffsetAsync(CancellationToken.None);
        await leaf.SetCheckpointOffsetHintsAsync([head - 1]);
        await leaf.CaptureSnapshotAsync();
        await tree.SetAsync("k-after-capture", Bytes("x"));

        var trimmed = await TrimWalAsync(treeId);
        Assert.That(trimmed, Is.GreaterThan(0),
            "precondition: the WAL GC trimmed the prefix the snapshot covers; without a trim the test is vacuous");

        await leaf.ForceDeactivateAsync();
        await Task.Delay(200);
        return (tree, treeId, leafId);
    }

    private static async Task<long> TrimWalAsync(string treeId)
    {
        var registry = Services.GetRequiredService<IWalCursorRegistry>();
        var deadline = Environment.TickCount64 + 30_000;
        HybridLogicalClock? last = null;
        var stable = 0;
        while (Environment.TickCount64 < deadline)
        {
            var min = await registry.GetMinCursorAsync(treeId);
            if (min is { } floor && floor.CompareTo(HybridLogicalClock.Zero) > 0)
            {
                if (last is { } prev && floor.CompareTo(prev) == 0)
                {
                    if (++stable >= 2) break;
                }
                else
                {
                    stable = 0;
                }

                last = floor;
            }

            await Task.Delay(100);
        }

        var report = await Services.GetRequiredService<ILatticeWalGc>().RunOnceAsync(treeId);
        TestContext.Out.WriteLine($"gc: trimmed={report.EntriesTrimmed} state={report.CursorFloorState} blocking={report.BlockingConsumerId}");
        return report.EntriesTrimmed;
    }

    /// <summary>Deletes the leaf's own state row straight from grain storage, as lost storage would.</summary>
    private static async Task DeleteLeafRowAsync(GrainId leafId)
    {
        var storage = Services.GetRequiredKeyedService<IGrainStorage>(LatticeOptions.StorageProviderName);
        var row = new GrainState<LeafNodeState>();
        await storage.ReadStateAsync("leaf", leafId, row);
        Assert.That(row.RecordExists, Is.True, "precondition: the leaf row exists before it is deleted");
        await storage.ClearStateAsync("leaf", leafId, row);
    }

    private static async Task<(byte[]? Read, Exception? Failure)> TryGetAsync(ILattice tree, string key)
    {
        try
        {
            return (await tree.GetAsync(key), null);
        }
        catch (Exception ex)
        {
            return (null, ex);
        }
    }

    /// <summary>What a fault takes from the leaf before its next activation.</summary>
    public enum Loss
    {
        /// <summary>The state row alone; the row record and snapshot survive.</summary>
        Row,

        /// <summary>The state row and the snapshot; the row record survives.</summary>
        RowAndSnapshot,

        /// <summary>The state row and its row record: the double loss nothing on the leaf can see.</summary>
        RowAndRecord,

        /// <summary>
        /// A leaf written before row records existed (so it has none) and with no
        /// snapshot, whose row then vanishes.
        /// </summary>
        PreRecordRow,
    }

    private async Task InflictAsync(GrainId leafId, Loss loss)
    {
        if (loss is Loss.RowAndRecord or Loss.PreRecordRow)
        {
            await _cluster.Client.GetGrain<ILeafRowRecordGrain>(leafId.GetGuidKey()).ClearAsync();
        }

        if (loss is Loss.PreRecordRow)
        {
            // A leaf that predates row records has none and need not have a snapshot.
            await _cluster.Client.GetGrain<ILeafSnapshotStorageGrain>(leafId.GetGuidKey()).ClearAsync(CancellationToken.None);
        }

        await DeleteLeafRowAsync(leafId);

        if (loss is Loss.RowAndSnapshot or Loss.RowAndRecord)
        {
            await _cluster.Client.GetGrain<ILeafSnapshotStorageGrain>(leafId.GetGuidKey()).ClearAsync(CancellationToken.None);
        }
    }

    private static void AssertFailedClosed(byte[]? read, Exception? failure, string what)
    {
        TestContext.Out.WriteLine($"{what}: read={(read is null ? "null" : Encoding.UTF8.GetString(read))}; "
            + $"failure={failure?.GetType().Name}: {failure?.Message}");
        Assert.That(read is not null || failure is not null, Is.True,
            $"{what}: k-00 was acknowledged, and the leaf that owned it lost its state row: the leaf must fail "
            + "closed rather than come up empty and report the key absent");
        if (read is not null)
        {
            Assert.That(read, Is.EqualTo(Bytes("v-0")));
        }
        else
        {
            Assert.That(failure, Is.InstanceOf<ILatticeLeafUnavailable>(),
                $"{what}: the fault must be recognisable as an unavailable leaf, not an arbitrary error");
        }
    }

    [TestCase(Loss.Row, TestName = "A_leaf_whose_state_row_vanishes_does_not_silently_report_acknowledged_keys_absent")]
    [TestCase(Loss.RowAndSnapshot, TestName = "A_leaf_whose_state_row_and_snapshot_both_vanish_does_not_silently_report_acknowledged_keys_absent")]
    [TestCase(Loss.RowAndRecord, TestName = "A_leaf_whose_state_row_and_row_record_both_vanish_does_not_silently_report_acknowledged_keys_absent")]
    [TestCase(Loss.PreRecordRow, TestName = "A_leaf_written_before_row_records_existed_whose_row_vanishes_does_not_silently_report_acknowledged_keys_absent")]
    public async Task Vanished_leaf_row(Loss loss)
    {
        var (tree, _, leafId) = await CreateSnapshottedTrimmedLeafAsync("vanished-row");

        await InflictAsync(leafId, loss);

        var (read, failure) = await TryGetAsync(tree, "k-00");
        AssertFailedClosed(read, failure, loss.ToString());
    }

    /// <summary>
    /// A path that binds a leaf without creating it - the #1744 write-path re-bind,
    /// recovery's reseed, or any caller of the birth seams that carries no create
    /// intent - must not turn a leaf whose row and record were both lost into an
    /// empty bound one.
    /// </summary>
    [Test]
    public async Task A_binding_without_a_create_intent_is_refused_on_a_leaf_whose_row_was_lost()
    {
        var (tree, treeId, leafId) = await CreateSnapshottedTrimmedLeafAsync("no-intent-bind");
        await InflictAsync(leafId, Loss.RowAndRecord);
        var leaf = _cluster.Client.GetGrain<IBPlusLeafGrain>(leafId.GetGuidKey());

        var bind = Assert.ThrowsAsync<LeafStateRowLostException>(async () => await leaf.SetTreeIdAsync(treeId));
        Assert.That(bind!.Message, Does.Contain("path creating it"));

        var (read, failure) = await TryGetAsync(tree, "k-00");
        AssertFailedClosed(read, failure, "after the refused bind");
        Assert.That(await leaf.GetTreeIdAsync(), Is.Null, "the refused bind left the leaf unbound");
    }

    /// <summary>
    /// The #1744 write-path self-heal re-binds a routable leaf that has a row but
    /// no tree id. A leaf with no row at all is not something it may re-create, so
    /// a typed CRDT write to a leaf whose row and record were lost fails closed.
    /// </summary>
    [Test]
    public async Task The_write_path_self_heal_does_not_re_create_a_leaf_whose_row_was_lost()
    {
        var (tree, _, leafId) = await CreateSnapshottedTrimmedLeafAsync("no-heal");
        await InflictAsync(leafId, Loss.RowAndRecord);

        Exception? healFailure = null;
        try
        {
            await tree.OrFlag("k-00").EnableAsync("replica-1");
        }
        catch (Exception ex)
        {
            healFailure = ex;
        }

        Assert.That(healFailure, Is.InstanceOf<ILatticeLeafUnavailable>(),
            $"the self-heal must not bind and write into an empty re-created leaf; got {healFailure}");
        var (read, failure) = await TryGetAsync(tree, "k-00");
        AssertFailedClosed(read, failure, "after the refused self-heal");
    }

    /// <summary>
    /// The trade the fix makes, stated as a test: a leaf deliberately cleared while
    /// routing still names it (an interrupted purge) cannot be told apart from one
    /// whose row and record were lost, so it fails closed rather than read empty.
    /// </summary>
    [Test]
    public async Task A_deliberately_cleared_leaf_that_routing_still_names_fails_closed()
    {
        var (tree, _, leafId) = await CreateSnapshottedTrimmedLeafAsync("cleared-row");

        await _cluster.Client.GetGrain<IBPlusLeafGrain>(leafId.GetGuidKey()).ClearGrainStateAsync();
        await Task.Delay(200);

        var (read, failure) = await TryGetAsync(tree, "k-00");
        Assert.That(read, Is.Null);
        Assert.That(failure, Is.InstanceOf<ILatticeLeafUnavailable>());
    }

    /// <summary>
    /// The create paths still create: a tree that splits mints new leaves through
    /// the split's create intent and serves every key.
    /// </summary>
    [Test]
    public async Task Leaves_created_by_a_split_serve_their_keys()
    {
        var logical = $"split-born-{Guid.NewGuid():N}";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(logical, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 4 });
        var tree = _cluster.Client.GetGrain<ILattice>(logical);
        for (var i = 0; i < 40; i++)
        {
            await tree.SetAsync($"s-{i:D2}", Bytes($"v-{i}"));
        }

        for (var i = 0; i < 40; i++)
        {
            Assert.That(await tree.GetAsync($"s-{i:D2}"), Is.EqualTo(Bytes($"v-{i}")));
        }
    }
    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.AddWalCursorRegistry();
            siloBuilder.AddLatticeWalGc();
            siloBuilder.ConfigureLattice(o =>
            {
                o.DigestCoalescingWindowMs = 0;
                o.MaterialiserCheckpointInterval = TimeSpan.Zero;
                // One WAL partition, so the single leaf's pin covers the whole log
                // and no unwritten partition holds a blocking pin.
                o.WalPartitions = 1;
            });
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<SiloServiceProviderCaptureForWalTests>();
            siloBuilder.Services.AddHostedService(sp => sp.GetRequiredService<SiloServiceProviderCaptureForWalTests>());
        }
    }
}
