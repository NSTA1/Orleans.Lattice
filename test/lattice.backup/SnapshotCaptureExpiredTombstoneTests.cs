using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Serialization;
using Orleans.TestingHost;
using CaptureSagaCallGate = Orleans.Lattice.Backup.Tests.SnapshotCaptureSagaAtomicityTests.CaptureSagaCallGate;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Issue #4619: a capture must resolve a still-pending bucket of a committed saga
/// against the verdict the registry still records, even once the decision's
/// tombstone has outlived its retention.
/// <para>
/// Saga <c>t</c> commits over two shards. One shard applies its terminal; the
/// other still holds <c>t</c>'s pending bucket. <c>t</c>'s decision is forgotten
/// and its tombstone expires while the row is still stored, so a live read
/// reports it Indeterminate. The capture's decision snapshot (D0) used to take
/// that Indeterminate too, so the capture hid the pending key and held the batch
/// with one key post-saga and the other absent - a committed write missing from
/// every restore. D0 now carries the recorded verdict, exactly as the sweeps
/// settle such a bucket, so both keys are post-saga.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SnapshotCaptureExpiredTombstoneTests
{
    private static readonly TimeSpan Retention = TimeSpan.FromSeconds(2);

    private TestCluster _cluster = null!;

    private IServiceProvider SiloServices =>
        _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    private ILatticeBackupCaptureService Capture => SiloServices.GetRequiredService<ILatticeBackupCaptureService>();

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
        CaptureSagaCallGate.Reset();
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [SetUp]
    public void SetUp() => CaptureSagaCallGate.Reset();

    [TearDown]
    public void TearDown() => CaptureSagaCallGate.Reset();

    [Test]
    public async Task A_capture_holds_both_keys_of_a_committed_batch_whose_tombstone_expired_with_one_bucket_pending()
    {
        var treeId = $"expired-tombstone-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await tree.SetAsync("seed", Bytes("seed"));
        var routing = await tree.GetRoutingAsync(forceRefresh: true);
        var a = "key-000";
        var b = Enumerable.Range(1, 10_000).Select(i => $"key-{i:D3}").First(k => routing.Map.Resolve(k) != routing.Map.Resolve(a));

        var hold = CaptureSagaCallGate.ArmTerminalHold(
            heldShardKey: ShardKey(routing, b),
            freeShardKey: ShardKey(routing, a));
        var saga = tree.SetManyAtomicAsync([new(a, Bytes("post")), new(b, Bytes("post"))]);
        await WithTimeout(hold.Entered.Task, "the held shard's terminal never reached the filter");
        await WithTimeout(hold.FreeDone.Task, "the free shard's terminal never returned");

        var txid = await PendingTransactionAsync(routing, b);
        var registry = TxRegistryRouting.GetRegistry(_cluster.GrainFactory, treeId, txid);
        Assert.That(await registry.GetStatusAsync(txid), Is.EqualTo(TxStatus.Committed), "PRECONDITION: the saga committed");
        await registry.ForgetAsync(txid);
        await Task.Delay(Retention + TimeSpan.FromSeconds(1));
        Assert.That(await registry.GetStatusAsync(txid), Is.EqualTo(TxStatus.Indeterminate),
            "PRECONDITION: the tombstone has expired while its row is still stored");

        var backup = await WithTimeout(
            Capture.CaptureAsync(new LatticeBackupCaptureRequest("expired-tombstone", BackupScopeSelector.WholeTree(treeId))),
            "the capture blocked on a parked saga terminal");
        var entries = await DecodeAsync(backup.Manifest);

        hold.Release.TrySetResult();
        await WithTimeout(saga, "the saga never completed after its terminal was released");

        Assert.Multiple(() =>
        {
            Assert.That(ValueOf(entries, a), Is.EqualTo("post"), "PRECONDITION: the applied shard holds the batch post-saga");
            Assert.That(ValueOf(entries, b), Is.EqualTo("post"),
                "the pending key of the committed batch must be captured post-saga, not absent");
        });
    }

    /// <summary>The one saga transaction pending on the shard that owns <paramref name="key"/>.</summary>
    private async Task<Guid> PendingTransactionAsync(RoutingInfo routing, string key)
    {
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>(ShardKey(routing, key));
        var vsc = routing.Map.Slots.Length;
        var slot = ShardMap.GetVirtualSlot(key, vsc);
        var leafId = (await shard.GetLeafIdForKeyAsync(key))!.Value;
        var pending = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId)
            .GetPendingMutationsForSlotsAsync(new[] { slot }, vsc);
        return pending.Where(p => p.Key == key).Select(p => p.TransactionId).Distinct().Single();
    }

    private static string ShardKey(RoutingInfo routing, string key) =>
        $"{routing.PhysicalTreeId}/{routing.Map.Resolve(key)}";

    private async Task<List<LwwEntry>> DecodeAsync(BackupManifest manifest)
    {
        var sink = SiloServices.GetRequiredService<ILatticeBackupSink>();
        var serializer = SiloServices.GetRequiredService<Serializer>();
        var all = new List<LwwEntry>();
        foreach (var descriptor in manifest.ContentDescriptors)
        {
            await foreach (var chunk in sink.ReadArtifactAsync(descriptor.ArtifactId))
            {
                all.AddRange(serializer.Deserialize<LwwEntry[]>(chunk));
            }
        }

        return all;
    }

    private static string? ValueOf(List<LwwEntry> entries, string key) =>
        entries.SingleOrDefault(e => e.Key == key) is { IsTombstone: false, Value: { } value } ? Encoding.UTF8.GetString(value) : null;

    private static async Task WithTimeout(Task task, string because)
    {
        if (await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(60))) != task)
            Assert.Fail(because);
        await task;
    }

    private static async Task<T> WithTimeout<T>(Task<T> task, string because)
    {
        await WithTimeout((Task)task, because);
        return await task;
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o =>
            {
                o.MaxConcurrentSnapshotCaptures = 1;
                o.TxDecisionRetention = Retention;
            });
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeBackup();
            siloBuilder.AddOutgoingGrainCallFilter<CaptureSagaCallGate>();
        }
    }
}
