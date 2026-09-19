using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Regression tests for issue #3207: a WAL shard whose scan stops at its
/// <b>first</b> entry never reached a compaction evaluation at all.
/// <para>
/// The chain these pin is short and entirely structural. A shard's dead bytes
/// are measured against every compaction threshold in exactly one place, at the
/// end of <c>IWalStorageProvider.TrimAsync</c>, and that evaluation is
/// unconditional there - a trim that removes no entry still evaluates. So the
/// gate on the ratio and the absolute ceiling was never "did we trim", it was
/// "was <c>TrimAsync</c> called at all". <c>TrimShardAsync</c> returns before
/// calling it whenever the scan found no eligible entry, which is precisely
/// what a scan stopping on its first entry produces. The consequence is not
/// slow reclamation: it is that dead bytes are stranded <i>above</i> the
/// threshold rather than accumulating below it, with no path to evaluation, so
/// no value of any compaction option can reach them.
/// </para>
/// <para>
/// The load-bearing property is therefore <b>reachability</b>, and it is
/// distinct from the question of whether an evaluation, once reached, decides
/// to compact. These tests assert only that the shard is asked. A shard below
/// its configured thresholds is still entitled to decline; what it may not do
/// is never be consulted.
/// </para>
/// <para>
/// Two fixtures here would pass against the unfixed build and are deliberately
/// present as controls rather than as coverage: the mid-log floor re-proves the
/// case that already worked, and its value is that it fails if a remedy ever
/// stops trimming in order to make evaluation reachable.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeWalGcCompactionReachabilityTests
{
    private const string Tree = "tree";
    private const string LeafConsumer = "_lattice_materialiser_tree_leaf-1";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static WalEntry Entry(long offset, HybridLogicalClock ts) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = $"k{offset}",
            Value = new byte[] { 1 },
            Timestamp = ts,
            OriginClusterId = "site-a",
        },
    };

    private static IOptionsMonitor<LatticeOptions> Monitor()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var options = new LatticeOptions { WalPartitions = 1 };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static async Task<LatticeWalGc> CollectorAsync(
        IWalStorageProvider provider,
        long? checkpointOffset)
    {
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));

        var sc = new ServiceCollection();
        sc.AddSingleton(provider);

        var pinGrain = Substitute.For<IWalMaterialiserPinGrain>();
        pinGrain
            .GetPinsAsync()
            .Returns(Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(
                new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal) { [LeafConsumer] = Hlc(20) }));
        if (checkpointOffset is { } offset)
        {
            pinGrain
                .GetPinOffsetsAsync()
                .Returns(Task.FromResult<IReadOnlyDictionary<string, long>>(
                    new Dictionary<string, long>(StringComparer.Ordinal) { [LeafConsumer] = offset }));
        }

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);
        sc.AddSingleton(factory);

        return new LatticeWalGc(sc.BuildServiceProvider(), registry, Monitor());
    }

    /// <summary>
    /// The exact production signature from issue #3207. An earlier trim removed
    /// the prefix so the retained range begins at offset 5, while the durable
    /// leaf checkpoint is stranded at offset 2 - so the very first entry the
    /// scan examines is above the floor and the pass stops on it.
    /// <para>
    /// The second assertion is what makes the first mean anything. Because no
    /// entry was released, the trim call that carries the only other compaction
    /// evaluation is genuinely not made, so the fixture is verified to be in
    /// the defect's condition rather than merely asserted to be.
    /// </para>
    /// </summary>
    [Test]
    public async Task RunOnceAsync_evaluates_compaction_on_a_shard_whose_first_entry_is_above_the_offset_floor()
    {
        var inner = new InMemoryWalStorageProvider();
        await inner.AppendBatchAsync(
            Tree,
            0,
            new[] { Entry(5, Hlc(10)), Entry(6, Hlc(11)), Entry(7, Hlc(12)) },
            CancellationToken.None);
        var provider = new RecordingWalStorageProvider(inner);

        var report = await (await CollectorAsync(provider, checkpointOffset: 2)).RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.Zero,
                "The floor stops the scan on its first entry, so the pass releases nothing.");
            Assert.That(provider.Trimmed, Is.Empty,
                "Control on the fixture itself: the shard must not have been trimmed, because a trim would "
                + "have carried its own compaction evaluation and the test would prove nothing.");
            Assert.That(provider.Evaluated, Is.EqualTo(new[] { (Tree, 0) }),
                "A shard holding dead bytes behind a held floor must still be asked whether it is due for "
                + "reclamation. Without this the ratio and ceiling are never read at all, at any dead ratio, "
                + "for as long as the stop persists - which is why no compaction setting could reach it.");
        });
    }

    /// <summary>
    /// The same reachability, reached by an entirely different stop. Here the
    /// offset floor is generous and never fires; the HLC eligibility clause
    /// stops the scan on its first entry instead.
    /// <para>
    /// This is the test that pins the remedy's <b>shape</b> rather than its
    /// effect, and it is the one that fails against the obvious wrong fix.
    /// Conditioning the evaluation on the stop reason being the offset floor
    /// would rebuild the same unreachable-site defect one step along: the floor
    /// advances by a single entry, the arm becomes a different stop, and
    /// reclamation silently ceases again. The quantity that matters is that the
    /// shard holds dead bytes, which is independent of every stop reason.
    /// </para>
    /// </summary>
    [Test]
    public async Task RunOnceAsync_evaluates_compaction_when_the_scan_stops_for_a_reason_other_than_the_offset_floor()
    {
        var inner = new InMemoryWalStorageProvider();
        await inner.AppendBatchAsync(
            Tree,
            0,
            new[] { Entry(0, Hlc(100)), Entry(1, Hlc(101)), Entry(2, Hlc(102)) },
            CancellationToken.None);
        var provider = new RecordingWalStorageProvider(inner);

        var report = await (await CollectorAsync(provider, checkpointOffset: 10)).RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.Zero);
            Assert.That(provider.Trimmed, Is.Empty);
            Assert.That(provider.Evaluated, Is.EqualTo(new[] { (Tree, 0) }),
                "Reclamation of already-dead bytes has nothing to do with why live entries could not be "
                + "released, so keying it to one stop reason would leave every other stop stranded.");
        });
    }

    /// <summary>
    /// Control. A floor sitting <em>mid</em>-log is the case that already
    /// worked: the prefix below the floor is released, so <c>TrimAsync</c> runs
    /// and carries the evaluation with it as it always has.
    /// <para>
    /// It is asserted here so that a future change cannot obtain reachability
    /// by giving up trimming, which would swap one defect for a worse one.
    /// </para>
    /// </summary>
    [Test]
    public async Task RunOnceAsync_still_trims_and_does_not_need_a_separate_evaluation_when_the_floor_sits_mid_log()
    {
        var inner = new InMemoryWalStorageProvider();
        await inner.AppendBatchAsync(
            Tree,
            0,
            new[] { Entry(0, Hlc(10)), Entry(1, Hlc(11)), Entry(2, Hlc(12)) },
            CancellationToken.None);
        var provider = new RecordingWalStorageProvider(inner);

        var report = await (await CollectorAsync(provider, checkpointOffset: 1)).RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.EqualTo(2),
                "Offsets 0 and 1 are at or below the floor and HLC-eligible, so both are released.");
            Assert.That(provider.Trimmed, Is.EqualTo(new[] { (Tree, 0, 1L) }),
                "The trim must still happen - it is what carries the evaluation on the healthy path.");
            Assert.That(provider.Evaluated, Is.Empty,
                "A shard that was trimmed has already been evaluated inside the trim, so evaluating it again "
                + "would double the work on the one path that was never broken.");
        });
    }

    /// <summary>
    /// Control on the empty shard. Nothing was scanned, so nothing can be
    /// stranded; the evaluation is still offered because the provider, not the
    /// GC, is what knows whether a shard holds dead bytes, and for an empty one
    /// it costs a comparison and declines.
    /// </summary>
    [Test]
    public async Task RunOnceAsync_offers_an_evaluation_on_an_empty_shard_rather_than_reasoning_about_its_contents()
    {
        var provider = new RecordingWalStorageProvider(new InMemoryWalStorageProvider());

        var report = await (await CollectorAsync(provider, checkpointOffset: 10)).RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.Zero);
            Assert.That(provider.Trimmed, Is.Empty);
            Assert.That(provider.Evaluated, Is.EqualTo(new[] { (Tree, 0) }),
                "Whether a shard with no live entries still holds dead bytes is the provider's question, not "
                + "the GC's - a log-structured backend can hold a whole file of them behind an empty index.");
        });
    }

    /// <summary>
    /// Records which shards were trimmed and which were offered a compaction
    /// evaluation, so a test can assert the two are reached independently.
    /// Everything else delegates verbatim.
    /// </summary>
    private sealed class RecordingWalStorageProvider(IWalStorageProvider inner) : IWalStorageProvider
    {
        public List<(string Tree, int Shard, long Through)> Trimmed { get; } = [];

        public List<(string Tree, int Shard)> Evaluated { get; } = [];

        public Task AppendBatchAsync(
            string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
            => inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken);

        public IAsyncEnumerable<WalEntry> ReadAsync(
            string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken);

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => inner.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task TrimAsync(
            string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
        {
            Trimmed.Add((treeId, shardIndex, throughOffsetInclusive));
            return inner.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);
        }

        public Task EvaluateCompactionAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        {
            Evaluated.Add((treeId, shardIndex));
            return inner.EvaluateCompactionAsync(treeId, shardIndex, cancellationToken);
        }
    }
}
