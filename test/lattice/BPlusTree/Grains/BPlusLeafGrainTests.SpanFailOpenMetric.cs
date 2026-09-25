using System.Diagnostics.Metrics;
using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    // --- (#2125) Every fail-open local commit of an out-of-span key is counted ---
    //
    // Declared-span admission forwards an out-of-span key to the neighbouring
    // leaf and falls open to a local commit when no neighbour resolves. That
    // fall-back used to be silent. One test per caller of
    // TryResolveSpanForwardTarget pins that its own increment exists and
    // carries the right reason and origin, so removing any single site's
    // increment reddens exactly the test named after it.

    private static readonly GrainId SelfLeafId = GrainId.Create("leaf", "test-leaf");

    /// <summary>
    /// A leaf declaring <c>[null, "m")</c> whose successor pointer is
    /// <paramref name="next"/> (null for a torn chain). Keys at or above
    /// <c>"m"</c> are out of span.
    /// </summary>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, string TreeId) CreateSealedFailOpenLeaf(
        GrainId? next, IBPlusLeafGrain? sibling = null)
    {
        var treeId = $"span-fail-open-{Guid.NewGuid():N}";
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = treeId;
        var (writer, _) = CreateBatchRecordingWriter();
        writer.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
        var grain = CreateGrain(state, siblingStub: sibling, commitLog: writer);
        state.State.LowKeyInclusive = null;
        state.State.HighKeyExclusive = "m";
        state.State.NextSibling = next;
        state.State.SplitSiblingId = null;
        state.State.PrevSibling = null;
        return (grain, state, treeId);
    }

    private static async Task<List<(long Value, Dictionary<string, object?> Tags)>> RecordFailOpensAsync(
        string treeId, Func<Task> body)
    {
        var all = await RecordMeasurementsAsync(LatticeMetrics.LeafSpanFailOpenCommits, body);
        return all.Where(m => Equals(m.Tags.GetValueOrDefault(LatticeMetrics.TagTree), treeId)).ToList();
    }

    private static void AssertSingleFailOpen(
        List<(long Value, Dictionary<string, object?> Tags)> measurements, string reason, string origin, string site)
    {
        Assert.That(measurements, Has.Count.EqualTo(1),
            $"{site} committed an out-of-span key locally because no neighbour resolved, and every such "
            + "fail-open must advance orleans.lattice.leaf.span_fail_open_commits exactly once (issue #2125).");
        var (value, tags) = measurements[0];
        Assert.Multiple(() =>
        {
            Assert.That(value, Is.EqualTo(1));
            Assert.That(tags.GetValueOrDefault(LatticeMetrics.TagReason), Is.EqualTo(reason));
            Assert.That(tags.GetValueOrDefault(LatticeMetrics.TagOrigin), Is.EqualTo(origin));
            Assert.That(tags.ContainsKey(LatticeTenantLabel.TagTenant), Is.True,
                "every leaf-level instrument carries the derived tenant tag");
        });
    }

    private static Dictionary<string, LwwValue<byte[]>> Lww(params string[] keys)
    {
        var clock = HybridLogicalClock.Zero;
        var result = new Dictionary<string, LwwValue<byte[]>>(keys.Length);
        foreach (var key in keys)
        {
            clock = HybridLogicalClock.Tick(clock);
            result[key] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes(key), clock);
        }

        return result;
    }

    [Test]
    public async Task SpanFailOpen_set_with_no_successor_counts_no_sibling_client_write()
    {
        var (grain, _, treeId) = CreateSealedFailOpenLeaf(next: null);

        var measurements = await RecordFailOpensAsync(treeId, () => grain.SetAsync("z", Encoding.UTF8.GetBytes("v")));

        AssertSingleFailOpen(measurements, "no_sibling", "client_write", "SetCoreAsync");
        Assert.That(grain.EntriesForTest.Keys, Does.Contain("z"),
            "observability only: the fail-open still commits locally, exactly as before");
    }

    [Test]
    public async Task SpanFailOpen_set_with_a_self_referencing_successor_counts_self_reference()
    {
        var (grain, _, treeId) = CreateSealedFailOpenLeaf(next: SelfLeafId);

        var measurements = await RecordFailOpensAsync(treeId, () => grain.SetAsync("z", Encoding.UTF8.GetBytes("v")));

        AssertSingleFailOpen(measurements, "self_reference", "client_write", "SetCoreAsync");
    }

    [Test]
    public async Task SpanFailOpen_set_below_the_low_bound_with_no_predecessor_counts_no_sibling()
    {
        var (grain, state, treeId) = CreateSealedFailOpenLeaf(next: GrainId.Create("leaf", "successor"));
        state.State.LowKeyInclusive = "f";

        var measurements = await RecordFailOpensAsync(treeId, () => grain.SetAsync("a", Encoding.UTF8.GetBytes("v")));

        AssertSingleFailOpen(measurements, "no_sibling", "client_write", "SetCoreAsync (leftward)");
    }

    [Test]
    public async Task SpanFailOpen_delete_with_no_successor_counts_no_sibling_client_write()
    {
        var (grain, _, treeId) = CreateSealedFailOpenLeaf(next: null);

        var measurements = await RecordFailOpensAsync(treeId, () => grain.DeleteAsync("z"));

        AssertSingleFailOpen(measurements, "no_sibling", "client_write", "DeleteAsync");
    }

    [Test]
    public async Task SpanFailOpen_merge_with_no_successor_counts_merge_origin()
    {
        var (grain, _, treeId) = CreateSealedFailOpenLeaf(next: null);

        var measurements = await RecordFailOpensAsync(treeId, () => grain.MergeManyAsync(Lww("a", "z")));

        AssertSingleFailOpen(measurements, "no_sibling", "merge", "ForwardOutOfSpanMergeAsync");
        Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "a", "z" }));
    }

    [Test]
    public async Task SpanFailOpen_cross_shard_migration_merge_counts_migration_origin()
    {
        var (grain, _, treeId) = CreateSealedFailOpenLeaf(next: SelfLeafId);

        var measurements = await RecordFailOpensAsync(
            treeId, () => grain.MergeManyAsync(Lww("z"), isCrossShardMigration: true));

        AssertSingleFailOpen(measurements, "self_reference", "cross_shard_migration", "ForwardOutOfSpanMergeAsync (migration)");
    }

    [Test]
    public async Task SpanFailOpen_conditional_set_many_with_no_successor_counts_client_write()
    {
        var (grain, _, treeId) = CreateSealedFailOpenLeaf(next: null);

        var measurements = await RecordFailOpensAsync(
            treeId,
            () => grain.SetManyWherePredicateAsync([Kv("a", 1), Kv("z", 1)], ScoreAtLeast(0)));

        AssertSingleFailOpen(measurements, "no_sibling", "client_write", "ForwardOutOfSpanConditionalSetManyAsync");
    }

    [Test]
    public async Task SpanFailOpen_set_many_with_no_successor_counts_every_out_of_span_key()
    {
        var (grain, _, treeId) = CreateSealedFailOpenLeaf(next: null);

        var measurements = await RecordFailOpensAsync(treeId, () => grain.SetManyAsync(Batch("a", "z")));

        AssertSingleFailOpen(measurements, "no_sibling", "client_write", "SetManyAdmittingSpanAsync");
        Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "a", "z" }));
    }

    [Test]
    public async Task SpanFailOpen_set_many_counts_one_per_fallen_open_key()
    {
        var (grain, _, treeId) = CreateSealedFailOpenLeaf(next: null);

        var measurements = await RecordFailOpensAsync(treeId, () => grain.SetManyAsync(Batch("n", "a", "z")));

        Assert.That(measurements.Sum(m => m.Value), Is.EqualTo(2),
            "the counter is in keys: two out-of-span keys fell open, the in-span one did not");
    }

    [Test]
    public async Task SpanFailOpen_leaf_with_no_declared_span_never_counts_on_any_write_path()
    {
        var (grain, state, treeId) = CreateSealedFailOpenLeaf(next: null);
        state.State.LowKeyInclusive = null;
        state.State.HighKeyExclusive = null;

        var measurements = await RecordFailOpensAsync(treeId, async () =>
        {
            await grain.SetAsync("z", Encoding.UTF8.GetBytes("v"));
            await grain.SetManyAsync(Batch("a", "zz"));
            await grain.SetManyWherePredicateAsync([Kv("zzz", 1)], ScoreAtLeast(0));
            await grain.MergeManyAsync(Lww("zzzz"));
            await grain.MergeManyAsync(Lww("zzzzz"), isCrossShardMigration: true);
            await grain.DeleteAsync("z");
        });

        Assert.That(measurements, Is.Empty,
            "a leaf with both bounds null (single-leaf tree, bulk-loaded leaf) owns every key, so "
            + "nothing it commits is out of span and it must never advance the fail-open counter");
    }

    [Test]
    public async Task SpanFailOpen_in_span_write_on_a_bounded_leaf_does_not_count()
    {
        var (grain, _, treeId) = CreateSealedFailOpenLeaf(next: null);

        var measurements = await RecordFailOpensAsync(treeId, async () =>
        {
            await grain.SetAsync("a", Encoding.UTF8.GetBytes("v"));
            await grain.MergeManyAsync(Lww("b"));
            await grain.DeleteAsync("a");
        });

        Assert.That(measurements, Is.Empty, "an in-span key is never a fail-open, even with no neighbour");
    }

    [Test]
    public async Task SpanFailOpen_out_of_span_write_that_forwards_does_not_count()
    {
        var sibling = Substitute.For<IBPlusLeafGrain>();
        var (grain, _, treeId) = CreateSealedFailOpenLeaf(next: GrainId.Create("leaf", "successor"), sibling);

        var measurements = await RecordFailOpensAsync(treeId, async () =>
        {
            await grain.SetAsync("z", Encoding.UTF8.GetBytes("v"));
            await grain.MergeManyAsync(Lww("y"));
        });

        Assert.That(measurements, Is.Empty, "a forwarded key was not committed locally, so it is not a fail-open");
        await sibling.Received(1).SetAsync("z", Arg.Any<byte[]>(), Arg.Any<long>());
    }
}
