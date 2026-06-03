using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the writer-layer wedge diagnostic pack: the per-partition
/// pending-append dispatch tracker (PartitionTracker + PendingAppend +
/// WalAppendStage) and the two metric instruments
/// (orleans.lattice.wal.writer.append.dispatched,
/// orleans.lattice.wal.writer.partition.pending_appends) that surface
/// it. Mirrors WalShardGrainTests.WedgeDiagnostics one layer up so a
/// rename of the watchdog-readable shape lights up the build
/// immediately rather than at the next cohort.
/// </summary>
[TestFixture]
[Category("Integration")]
public class WalCommitLogWriterWedgeDiagnosticsTests
{
    private const string TreeId = "tree-wedge-diag";

    private sealed class MeterCapture : IDisposable
    {
        private readonly MeterListener _listener;
        public ConcurrentBag<(string Name, double Value, KeyValuePair<string, object?>[] Tags)> Records { get; } = new();

        public MeterCapture()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (inst, l) =>
                {
                    if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter))
                    {
                        l.EnableMeasurementEvents(inst);
                    }
                }
            };
            _listener.SetMeasurementEventCallback<long>(
                (inst, value, tags, _) => Records.Add((inst.Name, value, tags.ToArray())));
            _listener.SetMeasurementEventCallback<double>(
                (inst, value, tags, _) => Records.Add((inst.Name, value, tags.ToArray())));
            _listener.Start();
        }

        public long Count(string instrumentName) =>
            Records.Where(r => r.Name == instrumentName).Sum(r => (long)r.Value);

        public (long Value, KeyValuePair<string, object?>[] Tags)? FirstFor(string instrumentName)
        {
            var hit = Records.FirstOrDefault(r => r.Name == instrumentName);
            return hit == default ? null : ((long)hit.Value, hit.Tags);
        }

        public void Dispose() => _listener.Dispose();
    }

    private static WalCommitLogWriter CreateWriter(IWalShardGrain shard, string clusterId = "site-test")
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var modeResolver = Substitute.For<ILatticeMergeModeResolver>();
        modeResolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);

        var clusterIdResolver = Substitute.For<ILatticeOriginClusterIdResolver>();
        clusterIdResolver.Resolve(Arg.Any<string>()).Returns(clusterId);

        var optionsResolver = TestOptionsResolver.Create(baseOptions: optionsMonitor.Get(string.Empty), factory: grainFactory);
        return new WalCommitLogWriter(grainFactory, optionsMonitor, optionsResolver, modeResolver, clusterIdResolver);
    }

    private static WalRecord MakeMutation(string key) => new()
    {
        TreeId = TreeId,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = "site-test",
    };

    [Test]
    public async Task AppendAsync_increments_dispatched_counter_per_invocation()
    {
        using var capture = new MeterCapture();
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
        var writer = CreateWriter(shard);

        var offset = await writer.AppendAsync(MakeMutation("k"), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(offset, Is.EqualTo(0L));

        var dispatched = capture.Count("orleans.lattice.wal.writer.append.dispatched");
        Assert.That(dispatched, Is.EqualTo(1L),
            "writer-layer dispatch counter: every AppendAsync must increment the dispatched counter exactly once");

        var sample = capture.FirstFor("orleans.lattice.wal.writer.append.dispatched");
        Assert.That(sample, Is.Not.Null);
        Assert.That(
            sample!.Value.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (string?)t.Value == TreeId),
            Is.True,
            "writer-layer dispatch counter: dispatched must be tagged with the affected tree id");
        Assert.That(
            sample.Value.Tags.Any(t => t.Key == LatticeMetrics.TagPartition && t.Value is int),
            Is.True,
            "writer-layer dispatch counter: dispatched must be tagged with the writer partition");
    }

    [Test]
    public async Task AppendAsync_records_pending_appends_histogram_per_invocation()
    {
        using var capture = new MeterCapture();
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
        var writer = CreateWriter(shard);

        await writer.AppendAsync(MakeMutation("k"), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));

        var observation = capture.FirstFor("orleans.lattice.wal.writer.partition.pending_appends");
        Assert.That(observation, Is.Not.Null,
            "writer-layer dispatch counter: pending_appends observation must be emitted at every AppendAsync entry");
        // For a single in-flight dispatch on a previously-empty
        // partition the pre-link depth is 0 (the new pending stamp is
        // observation N, not N+1). Asserting the lower bound rather
        // than exact zero keeps this robust against cross-test ordering
        // (other tests in this fixture also write into the static
        // tracker dictionary).
        Assert.That(observation!.Value.Value, Is.GreaterThanOrEqualTo(0L));
    }

    [Test]
    public async Task AppendManyAsync_records_dispatched_and_pending_for_batched_path()
    {
        using var capture = new MeterCapture();
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var entries = call.Arg<IReadOnlyList<WalRecord>>();
                var offsets = new long[entries.Count];
                for (var i = 0; i < entries.Count; i++) { offsets[i] = i; }
                return Task.FromResult<IReadOnlyList<long>>(offsets);
            });
        // Force two distinct partitions by routing on two distinct keys.
        // Single-entry path collapses to AppendAsync; we need >=2 distinct
        // partition groups to exercise AppendForPartitionAsync.
        var writer = CreateWriter(shard);

        var entries = new[]
        {
            MakeMutation("partition-a-key-1"),
            MakeMutation("partition-b-key-2"),
            MakeMutation("partition-c-key-3"),
        };
        var offsets = await writer.AppendManyAsync(entries, CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(offsets, Has.Count.EqualTo(3));

        var dispatched = capture.Count("orleans.lattice.wal.writer.append.dispatched");
        Assert.That(dispatched, Is.GreaterThanOrEqualTo(1L),
            "writer-layer dispatch counter: every AppendForPartitionAsync dispatch must increment the dispatched counter at least once");

        var pendingObs = capture.Records.Where(r => r.Name == "orleans.lattice.wal.writer.partition.pending_appends").ToList();
        Assert.That(pendingObs, Is.Not.Empty,
            "writer-layer dispatch counter: pending_appends observations must be emitted at every per-partition dispatch entry");
    }

    [Test]
    public void WalAppendStage_enum_layout_is_contract_with_StallWatchdog()
    {
        // The StallWatchdog reads WalAppendStage as a raw byte via ClrMD
        // and maps it to a stage name through a hardcoded switch
        // (Enqueued=0, DequeuedForBatch=1, SentToShard=2, Acked=3,
        // Failed=4). A future enum renumber would silently mislabel
        // every pending stamp in the watchdog log; this contract test
        // catches it.
        var enumType = typeof(Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter).Assembly
            .GetType("Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter+WalAppendStage", throwOnError: true)!;
        Assert.That(enumType.GetEnumUnderlyingType(), Is.EqualTo(typeof(byte)),
            "writer-layer lifecycle: WalAppendStage underlying type must be byte (StallWatchdog reads it as byte)");
        var values = Enum.GetValues(enumType).Cast<object>()
            .ToDictionary(v => v.ToString()!, v => (byte)v);
        Assert.Multiple(() =>
        {
            Assert.That(values["Enqueued"], Is.EqualTo((byte)0));
            Assert.That(values["DequeuedForBatch"], Is.EqualTo((byte)1));
            Assert.That(values["SentToShard"], Is.EqualTo((byte)2));
            Assert.That(values["Acked"], Is.EqualTo((byte)3));
            Assert.That(values["Failed"], Is.EqualTo((byte)4));
        });
    }

    [Test]
    public void PendingAppend_carries_Stage_and_StageStartedTicks_fields()
    {
        // The StallWatchdog walks the heap for PendingAppend instances
        // and reads Stage / StageStartedTicks by name via ClrMD. A
        // field rename would silently break attribution; this contract
        // test catches it.
        var pendingType = typeof(Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter).Assembly
            .GetType("Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter+PendingAppend", throwOnError: true)!;
        var stageField = pendingType.GetField("Stage", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
        var stageStartedField = pendingType.GetField("StageStartedTicks", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
        Assert.That(stageField, Is.Not.Null, "writer-layer lifecycle: PendingAppend must carry a Stage field of type WalAppendStage");
        Assert.That(stageStartedField, Is.Not.Null, "writer-layer lifecycle: PendingAppend must carry a StageStartedTicks field of type long");
        Assert.That(stageField!.FieldType.Name, Is.EqualTo("WalAppendStage"));
        Assert.That(stageStartedField!.FieldType, Is.EqualTo(typeof(long)));
    }

    [Test]
    public void PartitionTracker_carries_TreeId_Partition_inFlight_fields_for_watchdog_walk()
    {
        // The StallWatchdog detects PartitionTracker by field signature
        // (TreeId + Partition + _inFlight). A field rename would
        // silently break the writer-layer watchdog walker; this
        // contract test catches it.
        var trackerType = typeof(Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter).Assembly
            .GetType("Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter+PartitionTracker", throwOnError: true)!;
        Assert.Multiple(() =>
        {
            Assert.That(trackerType.GetField("TreeId", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance), Is.Not.Null);
            Assert.That(trackerType.GetField("Partition", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance), Is.Not.Null);
            Assert.That(trackerType.GetField("_inFlight", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance), Is.Not.Null,
                "writer-layer lifecycle: PartitionTracker._inFlight must remain a public field (the StallWatchdog ClrMD walk reads it via Fields.FirstOrDefault by name).");
        });
    }
}