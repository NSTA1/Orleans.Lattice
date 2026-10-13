using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression for overlapping atomic batches on one key. The source tree
/// resolves two batches that both prepared the same key by last-writer-wins
/// on the prepared entry's HLC, whichever commits first. The view must agree:
/// flushing completed batches in completion-discovery order, stamped with the
/// view tree's own clock, would let the older batch overwrite the newer one
/// and leave the view permanently diverged from its source.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewStagingOverlapOrderTests
{
    private const string Origin = "remote-origin";

    private MaterialisedViewClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new MaterialisedViewClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private sealed record ViewPerson(int Age, string Tag);

    private static byte[] Person(int age, string tag) =>
        JsonLatticeSerializer<ViewPerson>.Default.Serialize(new ViewPerson(age, tag));

    private static int ShardOf(string key) =>
        LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount);

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    private async Task<(ILattice Source, IViewMaintainerGrain Maintainer)> CreateViewAsync(string tree, string view)
    {
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var projection = new PredicateLatticeViewProjection(
            LatticePredicateTranslator.Translate<ViewPerson>(p => p.Age >= 18));
        _ = factory.Create(source, view, new LatticeViewDefinition(view, projection));
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.EnsureActiveAsync();
        return (source, maintainer);
    }

    private static async Task DrainToZeroAsync(IViewMaintainerGrain maintainer)
    {
        for (var attempt = 0; attempt < 50; attempt++)
        {
            await maintainer.DrainAsync();
            if (await maintainer.GetLagAsync() == 0)
            {
                return;
            }

            await Task.Delay(20);
        }

        Assert.Fail("View did not catch up to the source head.");
    }

    [Test]
    public async Task An_older_batch_completing_after_a_newer_batch_on_the_same_key_does_not_overwrite_it()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-atomic-overlap-src";
        const string view = "mv-atomic-overlap-view";
        var (source, maintainer) = await CreateViewAsync(tree, view);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        const string key = "overlap-k";
        var older = Guid.NewGuid();
        var newer = Guid.NewGuid();

        // Both batches prepare the same key; the newer one carries the higher
        // prepared HLC and commits first.
        await apply.ApplyPreparedSetAsync(
            key, Person(30, "older"), Hlc(100), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, older, atomicBatchSize: 1, atomicBatchIndex: 0);
        await apply.ApplyPreparedSetAsync(
            key, Person(40, "newer"), Hlc(200), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, newer, atomicBatchSize: 1, atomicBatchIndex: 0);
        await apply.ApplyTxTerminalAsync(newer, committed: true, ShardOf(key), Hlc(1000), Origin, atomicShardCount: 1);
        await maintainer.DrainAsync();

        // The older batch commits later, in a later drain pass.
        await apply.ApplyTxTerminalAsync(older, committed: true, ShardOf(key), Hlc(1001), Origin, atomicShardCount: 1);
        await DrainToZeroAsync(maintainer);

        var sourceValue = await source.GetAsync(key);
        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(sourceValue, Is.EqualTo(Person(40, "newer")),
                "The source resolves overlapping prepares by last-writer-wins on the prepared HLC.");
            Assert.That(await viewTree.GetAsync(key), Is.EqualTo(sourceValue),
                "The view must hold the source's surviving value, not the batch that completed last.");
        });
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task A_rebuild_replaying_an_older_batch_does_not_overwrite_a_newer_overlapping_batch_it_reflected(bool stagedAtRebuild)
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        var tree = $"mv-atomic-overlap-rebuild-src-{stagedAtRebuild}";
        var view = $"mv-atomic-overlap-rebuild-view-{stagedAtRebuild}";
        var (source, maintainer) = await CreateViewAsync(tree, view);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        const string key = "overlap-rebuild-k";
        var older = Guid.NewGuid();
        var newer = Guid.NewGuid();

        // The older batch prepares first and is still in flight when the rebuild
        // runs; the newer batch prepares the same key later and commits first.
        await apply.ApplyPreparedSetAsync(
            key, Person(30, "older"), Hlc(100), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, older, atomicBatchSize: 1, atomicBatchIndex: 0);
        await apply.ApplyPreparedSetAsync(
            key, Person(40, "newer"), Hlc(200), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, newer, atomicBatchSize: 1, atomicBatchIndex: 0);
        await apply.ApplyTxTerminalAsync(newer, committed: true, ShardOf(key), Hlc(1000), Origin, atomicShardCount: 1);
        if (stagedAtRebuild)
        {
            await maintainer.DrainAsync();
        }

        // The rebuild reflects the newer batch from source and replays the older
        // one from its prepare, re-reading the newer batch's prepare under the
        // replay fence as already resolved.
        await maintainer.RebuildAsync();
        await apply.ApplyTxTerminalAsync(older, committed: true, ShardOf(key), Hlc(1001), Origin, atomicShardCount: 1);
        await DrainToZeroAsync(maintainer);

        var sourceValue = await source.GetAsync(key);
        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(sourceValue, Is.EqualTo(Person(40, "newer")));
            Assert.That(await viewTree.GetAsync(key), Is.EqualTo(sourceValue),
                "A replayed older batch must not overwrite the newer batch the rebuild reflected.");
        });
    }

    [Test]
    public async Task A_rebuild_replaying_a_staged_batch_does_not_overwrite_a_newer_ordinary_write_it_reflected()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-atomic-rebuild-reflected-ordinary-src";
        var view = MaterialisedViewClusterFixture.NarrowBatchStagingViewName;
        var (source, maintainer) = await CreateViewAsync(tree, view);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);

        // The prepare, the commit terminal and the ordinary write share one WAL
        // partition, which the view reads two entries per pass.
        var partitions = await _fixture.SiloServices
            .GetRequiredService<LatticeOptionsResolver>()
            .GetWalPartitionsAsync(tree);
        var key = Enumerable.Range(0, 100_000)
            .Select(i => $"rebuild-ordinary-k-{i}")
            .First(k => WalPartitionHash.Compute(k, partitions) == ShardOf(k) % partitions);
        var txId = Guid.NewGuid();

        // The batch's prepare is read and staged; then it commits and a newer
        // ordinary write lands on the same key, so the source last-writer is the
        // ordinary write.
        await apply.ApplyPreparedSetAsync(
            key, Person(30, "atomic"), Hlc(100), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, txId, atomicBatchSize: 1, atomicBatchIndex: 0);
        await maintainer.DrainAsync();
        await apply.ApplyTxTerminalAsync(txId, committed: true, ShardOf(key), Hlc(1000), Origin, atomicShardCount: 1);
        await source.SetAsync(key, Person(50, "ordinary"));

        // The rebuild reflects both from source and replays the still-staged
        // batch under a replay fence at the head. The next pass re-reads only the
        // prepare and the commit, flushing the batch before it reaches the
        // ordinary write, which every later pass then reads fenced as reflected.
        await maintainer.RebuildAsync();
        await maintainer.DrainAsync();
        await DrainToZeroAsync(maintainer);

        var sourceValue = await source.GetAsync(key);
        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(sourceValue, Is.EqualTo(Person(50, "ordinary")));
            Assert.That(await viewTree.GetAsync(key), Is.EqualTo(sourceValue),
                "A replayed batch must not overwrite a newer ordinary write the rebuild reflected.");
        });
    }
    [Test]
    public async Task A_rebuild_whose_scan_is_clamped_above_a_staged_batch_still_supersedes_it_with_the_newer_batch_it_reflected()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-atomic-rebuild-clamped-supersession-src";
        var view = MaterialisedViewClusterFixture.TruncatedRebuildScanViewName;
        var (source, maintainer) = await CreateViewAsync(tree, view);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);

        // Every entry lands on one WAL partition; the view's rebuild scan examines
        // only that partition's last entry.
        var partitions = await _fixture.SiloServices
            .GetRequiredService<LatticeOptionsResolver>()
            .GetWalPartitionsAsync(tree);
        var key = Enumerable.Range(0, 100_000)
            .Select(i => $"rebuild-clamped-k-{i}")
            .First(k => WalPartitionHash.Compute(k, partitions) == ShardOf(k) % partitions);
        var partition = WalPartitionHash.Compute(key, partitions);
        var filler = Enumerable.Range(0, 100_000)
            .Select(i => $"rebuild-clamped-filler-{i}")
            .First(k => WalPartitionHash.Compute(k, partitions) == partition);
        var older = Guid.NewGuid();
        var newer = Guid.NewGuid();

        // The older batch prepares the key and is staged, holding the checkpoint
        // below it. A newer batch prepares the same key, commits and flushes; the
        // source keeps it whichever commits first, since it prepared later.
        await apply.ApplyPreparedSetAsync(
            key, Person(30, "older"), Hlc(100), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, older, atomicBatchSize: 1, atomicBatchIndex: 0);
        await maintainer.DrainAsync();
        await apply.ApplyPreparedSetAsync(
            key, Person(40, "newer"), Hlc(200), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, newer, atomicBatchSize: 1, atomicBatchIndex: 0);
        await apply.ApplyTxTerminalAsync(newer, committed: true, ShardOf(key), Hlc(300), Origin, atomicShardCount: 1);
        await maintainer.DrainAsync();

        // An unrelated write leaves the newer batch's entries outside the rebuild's
        // one-entry scan, which therefore sees neither its prepare of the key nor
        // the staged older batch's.
        await source.SetAsync(filler, Person(20, "filler"));
        await maintainer.RebuildAsync();

        // The older batch commits; the resumed tail re-reads it from its held floor,
        // passing the newer batch's prepare fenced as reflected, and flushes it.
        await apply.ApplyTxTerminalAsync(older, committed: true, ShardOf(key), Hlc(1000), Origin, atomicShardCount: 1);
        await DrainToZeroAsync(maintainer);

        var sourceValue = await source.GetAsync(key);
        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(sourceValue, Is.EqualTo(Person(40, "newer")));
            Assert.That(await viewTree.GetAsync(key), Is.EqualTo(sourceValue),
                "A replayed batch staged below a clamped rebuild scan must not overwrite the newer batch the rebuild reflected.");
        });
    }

    [Test]
    public async Task A_rebuild_replaying_a_staged_batch_keeps_a_reflected_supersession_when_another_replayed_batch_aborts()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-atomic-rebuild-release-supersession-src";
        var view = MaterialisedViewClusterFixture.NarrowBatchStagingViewName;
        var (source, maintainer) = await CreateViewAsync(tree, view);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);

        // Two keys on one shard, whose prepares, terminals and ordinary writes
        // share one WAL partition and so are read in log order.
        var partitions = await _fixture.SiloServices
            .GetRequiredService<LatticeOptionsResolver>()
            .GetWalPartitionsAsync(tree);
        var key = Enumerable.Range(0, 100_000)
            .Select(i => $"rebuild-release-k-{i}")
            .First(k => WalPartitionHash.Compute(k, partitions) == ShardOf(k) % partitions);
        var other = Enumerable.Range(0, 100_000)
            .Select(i => $"rebuild-release-o-{i}")
            .First(k => ShardOf(k) == ShardOf(key) && WalPartitionHash.Compute(k, partitions) == ShardOf(k) % partitions);
        var filler = Enumerable.Range(0, 100_000)
            .Select(i => $"rebuild-release-f-{i}")
            .First(k => ShardOf(k) % partitions == ShardOf(key) % partitions
                && WalPartitionHash.Compute(k, partitions) == ShardOf(key) % partitions);
        var aborted = Guid.NewGuid();
        var committed = Guid.NewGuid();

        // The aborting batch prepares the key; the committing batch prepares only
        // its other key so far. Both are read and staged.
        await apply.ApplyPreparedSetAsync(
            key, Person(30, "aborted"), Hlc(100), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, aborted, atomicBatchSize: 1, atomicBatchIndex: 0);
        await apply.ApplyPreparedSetAsync(
            other, Person(31, "committed-other"), Hlc(110), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, committed, atomicBatchSize: 2, atomicBatchIndex: 0);
        await maintainer.DrainAsync();

        // The first batch aborts; the second prepares the key, commits, and a newer
        // ordinary write then lands on the key, so the source last-writer is the
        // ordinary write.
        await source.SetAsync(filler, Person(40, "filler"));
        await apply.ApplyTxTerminalAsync(aborted, committed: false, ShardOf(key), Hlc(115), Origin, atomicShardCount: 1);
        await apply.ApplyPreparedSetAsync(
            key, Person(32, "committed"), Hlc(120), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, committed, atomicBatchSize: 2, atomicBatchIndex: 1);
        await apply.ApplyTxTerminalAsync(committed, committed: true, ShardOf(key), Hlc(130), Origin, atomicShardCount: 1);
        await source.SetAsync(key, Person(50, "ordinary"));

        // The rebuild reflects the ordinary write and records its supersession of
        // the key under the replay fence. The resumed tail re-stages both batches;
        // reading the abort releases the first batch, which must not forget the
        // reflected supersession the still-replayed second batch needs.
        await maintainer.RebuildAsync();
        await DrainToZeroAsync(maintainer);

        var sourceValue = await source.GetAsync(key);
        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(sourceValue, Is.EqualTo(Person(50, "ordinary")));
            Assert.That(await viewTree.GetAsync(key), Is.EqualTo(sourceValue),
                "Releasing an aborted replayed batch must not drop the reflected supersession another replayed batch needs.");
        });
    }
    [Test]
    public async Task Overlapping_batches_completing_in_one_drain_pass_resolve_by_prepared_hlc()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-atomic-overlap-pass-src";
        const string view = "mv-atomic-overlap-pass-view";
        var (source, maintainer) = await CreateViewAsync(tree, view);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        const string key = "overlap-pass-k";
        var older = Guid.NewGuid();
        var newer = Guid.NewGuid();

        await apply.ApplyPreparedSetAsync(
            key, Person(40, "newer"), Hlc(200), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, newer, atomicBatchSize: 1, atomicBatchIndex: 0);
        await apply.ApplyPreparedSetAsync(
            key, Person(30, "older"), Hlc(100), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, older, atomicBatchSize: 1, atomicBatchIndex: 0);
        await apply.ApplyTxTerminalAsync(newer, committed: true, ShardOf(key), Hlc(1000), Origin, atomicShardCount: 1);
        await apply.ApplyTxTerminalAsync(older, committed: true, ShardOf(key), Hlc(1001), Origin, atomicShardCount: 1);
        await DrainToZeroAsync(maintainer);

        var sourceValue = await source.GetAsync(key);
        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(sourceValue, Is.EqualTo(Person(40, "newer")));
            Assert.That(await viewTree.GetAsync(key), Is.EqualTo(sourceValue),
                "Batches completing in one pass must resolve by prepared HLC, not dictionary order.");
        });
    }
}
