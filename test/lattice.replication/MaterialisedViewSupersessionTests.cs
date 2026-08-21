using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression tests for the atomic-batch-vs-ordinary-write last-writer-wins
/// supersession fix and the rebuild-drops-in-flight-prepares fix.
/// <para>
/// A committed prepared Set installs at the source under its <i>prepare-time</i>
/// HLC, so a higher-HLC ordinary write to the same source key LWW-dominates it at
/// the source. The view maintainer applies a completed atomic batch <i>after</i>
/// the ordinary survivors in a drain, and the view tree re-stamps a fresh HLC at
/// apply time, so apply order alone would let the lower-HLC atomic value resurrect
/// over the higher-HLC ordinary value - a permanent lost write. The maintainer now
/// tracks, per still-staged source key, the highest HLC of any ordinary write seen
/// while it is staged, and drops a staged prepare that a higher-HLC ordinary write
/// supersedes. These tests drive the precise prepared / ordinary / committed WAL
/// interleavings - within one drain, across drains, and through a rebuild - and
/// assert the source last-writer wins in the view.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewSupersessionTests
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

    private static LatticePredicateNode AdultFilter() =>
        LatticePredicateTranslator.Translate<ViewPerson>(p => p.Age >= 18);

    private sealed record ScoreRecord(string Team, double Score);

    private static byte[] Record(string team, double score) =>
        JsonLatticeSerializer<ScoreRecord>.Default.Serialize(new ScoreRecord(team, score));

    private static string Team(byte[] value) =>
        JsonLatticeSerializer<ScoreRecord>.Default.Deserialize(value)!.Team;

    private static double Score(byte[] value) =>
        JsonLatticeSerializer<ScoreRecord>.Default.Deserialize(value)!.Score;

    private static double? SumValue(byte[]? bytes) =>
        bytes is null ? null : LatticeAggregationValue.DecodeDouble(bytes);

    private static int ShardOf(string key) =>
        LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount);

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private IReplicationApplyGrain Apply(string tree) =>
        _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);

    private ILatticeView CreateAdultView(string sourceTreeId, string viewName)
    {
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(sourceTreeId);
        var projection = new PredicateLatticeViewProjection(AdultFilter());
        return factory.Create(source, viewName, new LatticeViewDefinition(viewName, projection));
    }

    private ILatticeView CreateSumView(string sourceTreeId, string viewName)
    {
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(sourceTreeId);
        var projection = new AggregationLatticeViewProjection(AggregationKind.Sum, Team, "v1", valueSelector: Score);
        var definition = new LatticeViewDefinition(viewName, projection);
        return factory.Create(
            source,
            viewName,
            MaterialisedViewRuntimeProjectionProvider.DescriptorFor(definition));
    }

    private async Task<IViewMaintainerGrain> MaintainerAsync(string viewName)
    {
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
        return maintainer;
    }

    private Task<ILattice> ViewTreeAsync(string viewName) => _fixture.ActiveViewTreeAsync(viewName);

    private async Task StagePreparedAsync(
        string tree, string key, byte[] value, Guid txId, int batchSize, int batchIndex, long ticks)
        => await Apply(tree).ApplyPreparedSetAsync(
            key, value, Hlc(ticks), Origin,
            sourceVectorClock: null, expiresAtTicks: 0,
            txId, atomicBatchSize: batchSize, atomicBatchIndex: batchIndex);

    /// <summary>Appends an ordinary (non-prepared) source write stamped with an explicit HLC.</summary>
    private async Task OrdinarySetAsync(string tree, string key, byte[] value, long ticks)
        => await Apply(tree).ApplySetAsync(
            key, value, Hlc(ticks), Origin, sourceVectorClock: null, expiresAtTicks: 0);

    private async Task CommitShardAsync(string tree, Guid txId, int shardIndex, long ticks, int shardCount)
        => await Apply(tree).ApplyTxTerminalAsync(
            txId, committed: true, shardIndex, Hlc(ticks), Origin, atomicShardCount: shardCount);

    private static async Task DrainAsync(IViewMaintainerGrain maintainer, int times)
    {
        for (var i = 0; i < times; i++)
        {
            await maintainer.DrainAsync();
        }
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
    public async Task Ordinary_write_with_higher_hlc_supersedes_a_committed_atomic_batch_same_pass()
    {
        // White-box reads of a view's backing tree run under an authorised
        // ViewReadContext scope (as the maintainer and ILatticeView handle do);
        // the public read-guard otherwise rejects direct view-tree reads.
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-supersede-same-src";
        const string view = "mv-supersede-same-view";
        _ = CreateAdultView(tree, view);
        var maintainer = await MaintainerAsync(view);
        var txId = Guid.NewGuid();

        // A committed atomic batch writes k at a LOW prepare HLC; an ordinary write
        // writes k at a HIGHER HLC (the true source last-writer). All three WAL
        // entries (prepare, ordinary, terminal) land on k's single partition in
        // append order, so one drain pass sees them in offset (HLC) order.
        await StagePreparedAsync(tree, "k", Person(30, "atomic"), txId, batchSize: 1, batchIndex: 0, ticks: 100);
        await OrdinarySetAsync(tree, "k", Person(30, "ordinary"), ticks: 200);
        await CommitShardAsync(tree, txId, ShardOf("k"), ticks: 300, shardCount: 1);

        await DrainToZeroAsync(maintainer);

        var viewTree = await ViewTreeAsync(view);
        Assert.That(
            await viewTree.GetAsync("k"),
            Is.EqualTo(Person(30, "ordinary")),
            "The higher-HLC ordinary write is the source last-writer and must win; the atomic value must not resurrect it.");
    }

    [Test]
    public async Task Ordinary_write_with_higher_hlc_supersedes_a_committed_atomic_batch_across_passes()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-supersede-cross-src";
        const string view = "mv-supersede-cross-view";
        _ = CreateAdultView(tree, view);
        var maintainer = await MaintainerAsync(view);
        var txId = Guid.NewGuid();

        // Stage the prepare and the higher-HLC ordinary write, then drain BEFORE the
        // terminal: the prepare stays staged (checkpoint held back) and the ordinary
        // write is applied and recorded as a superseding write in this earlier pass.
        await StagePreparedAsync(tree, "k", Person(30, "atomic"), txId, batchSize: 1, batchIndex: 0, ticks: 100);
        await OrdinarySetAsync(tree, "k", Person(30, "ordinary"), ticks: 200);

        await DrainAsync(maintainer, times: 1);

        var viewTree = await ViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await viewTree.GetAsync("k"), Is.EqualTo(Person(30, "ordinary")), "The ordinary write applies in the earlier pass.");
            Assert.That(await maintainer.GetLagAsync(), Is.GreaterThan(0), "The uncommitted batch holds the checkpoint back.");
        });

        // Now complete the batch in a later pass: the recorded supersession must
        // survive across passes so the prepare is dropped at flush time.
        await CommitShardAsync(tree, txId, ShardOf("k"), ticks: 300, shardCount: 1);
        await DrainToZeroAsync(maintainer);

        viewTree = await ViewTreeAsync(view);
        Assert.That(
            await viewTree.GetAsync("k"),
            Is.EqualTo(Person(30, "ordinary")),
            "The batch completing in a later pass must not resurrect the lower-HLC atomic value.");
    }

    [Test]
    public async Task Aggregation_group_reflects_the_higher_hlc_ordinary_write_not_the_atomic_value()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-supersede-agg-src";
        const string view = "mv-supersede-agg-view";
        _ = CreateSumView(tree, view);
        var maintainer = await MaintainerAsync(view);
        var txId = Guid.NewGuid();

        // Source key k contributes to group "red". A committed atomic batch
        // contributes Score=4 at a LOW HLC; an ordinary write contributes Score=100
        // at a HIGHER HLC. The source last-writer is the ordinary write, so the
        // group sum must reflect 100, not the superseded atomic 4.
        await StagePreparedAsync(tree, "k", Record("red", 4), txId, batchSize: 1, batchIndex: 0, ticks: 100);
        await OrdinarySetAsync(tree, "k", Record("red", 100), ticks: 200);
        await CommitShardAsync(tree, txId, ShardOf("k"), ticks: 300, shardCount: 1);

        await DrainToZeroAsync(maintainer);

        var viewTree = await ViewTreeAsync(view);
        Assert.That(
            SumValue(await viewTree.GetAsync("red")),
            Is.EqualTo(100),
            "The group must fold the higher-HLC ordinary contribution, not the superseded atomic contribution.");
    }

    [Test]
    public async Task Rebuild_with_an_in_flight_prepare_does_not_lose_the_committed_batch()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-rebuild-inflight-src";
        const string view = "mv-rebuild-inflight-view";
        _ = CreateAdultView(tree, view);
        var maintainer = await MaintainerAsync(view);
        var txId = Guid.NewGuid();

        // A committed ordinary write the rebuild reproduces from source state.
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        await source.SetAsync("base-adult", Person(50, "base"));

        // Stage an atomic batch's prepares (offsets below head) but do NOT commit.
        await StagePreparedAsync(tree, "rb-a", Person(30, "a1"), txId, batchSize: 2, batchIndex: 0, ticks: 100);
        await StagePreparedAsync(tree, "rb-b", Person(40, "b1"), txId, batchSize: 2, batchIndex: 1, ticks: 101);

        // Drain once so the prepares are staged in the maintainer (so the rebuild's
        // floor hold-back can see them), but the batch is still in-flight.
        await DrainAsync(maintainer, times: 1);

        var viewTree = await ViewTreeAsync(view);
        Assert.That(await viewTree.GetAsync("rb-a"), Is.Null, "Pre-commit the in-flight batch must be invisible.");

        // Trigger a rebuild while the prepares are in flight. The rebuild reconverges
        // from current committed source state (which excludes the prepares) but must
        // hold each partition's resume floor back below the in-flight prepares so the
        // resumed tail re-reads and re-stages them when they commit.
        await maintainer.RebuildAsync();

        viewTree = await ViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await viewTree.GetAsync("base-adult"), Is.EqualTo(Person(50, "base")), "The rebuild reproduces committed source state.");
            Assert.That(await viewTree.GetAsync("rb-a"), Is.Null, "The uncommitted prepares are not in the rebuilt view yet.");
        });

        // Now commit the batch and drain: with the floor held back, the resumed tail
        // re-stages and flushes the committed batch WITHOUT any manual reconcile.
        await CommitShardAsync(tree, txId, ShardOf("rb-a"), ticks: 1000, shardCount: 1);
        await CommitShardAsync(tree, txId, ShardOf("rb-b"), ticks: 1001, shardCount: 1);
        await DrainToZeroAsync(maintainer);

        viewTree = await ViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await viewTree.GetAsync("rb-a"), Is.EqualTo(Person(30, "a1")), "The committed batch must appear after the rebuild without a reconcile.");
            Assert.That(await viewTree.GetAsync("rb-b"), Is.EqualTo(Person(40, "b1")), "The committed batch must appear after the rebuild without a reconcile.");
            Assert.That(await viewTree.GetAsync("base-adult"), Is.EqualTo(Person(50, "base")));
        });
    }
}
