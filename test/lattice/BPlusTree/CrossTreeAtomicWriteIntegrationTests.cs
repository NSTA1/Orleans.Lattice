using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration coverage for cross-tree atomic writes
/// (<see cref="LatticeCrossTreeAtomicWriteExtensions"/>) against a live in-memory
/// cluster: multi-tree commit, guarded abort, idempotent re-attach, and the
/// no-partial-cross-tree-view reader invariant.
/// </summary>
[TestFixture]
[Category("Integration")]
public class CrossTreeAtomicWriteIntegrationTests
{
    private ClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static List<KeyValuePair<string, byte[]>> Entries(params (string k, string v)[] pairs) =>
        pairs.Select(p => new KeyValuePair<string, byte[]>(p.k, Bytes(p.v))).ToList();

    [Test]
    public async Task SetManyAtomicAcrossTreesAsync_commits_every_tree()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var t1 = $"orders-{suffix}";
        var t2 = $"inventory-{suffix}";
        var tree1 = _cluster.GrainFactory.GetGrain<ILattice>(t1);
        var tree2 = _cluster.GrainFactory.GetGrain<ILattice>(t2);

        var outcome = await _cluster.GrainFactory.SetManyAtomicAcrossTreesAsync(
            [
                new LatticeTreeBatch(t1, Entries(("order:1", "A"), ("order:2", "B"))),
                new LatticeTreeBatch(t2, Entries(("sku:1", "X"))),
            ],
            operationId: $"xop-{suffix}");

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        Assert.That(await tree1.GetAsync("order:1"), Is.EqualTo(Bytes("A")));
        Assert.That(await tree1.GetAsync("order:2"), Is.EqualTo(Bytes("B")));
        Assert.That(await tree2.GetAsync("sku:1"), Is.EqualTo(Bytes("X")));
    }

    [Test]
    public async Task SetManyAtomicAcrossTreesAsync_commits_three_trees()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var trees = Enumerable.Range(0, 3).Select(i => $"t{i}-{suffix}").ToArray();

        var outcome = await _cluster.GrainFactory.SetManyAtomicAcrossTreesAsync(
            trees.Select(t => new LatticeTreeBatch(t, Entries(($"{t}:k", "v")))).ToList(),
            operationId: $"xop3-{suffix}");

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        foreach (var t in trees)
        {
            Assert.That(await _cluster.GrainFactory.GetGrain<ILattice>(t).GetAsync($"{t}:k"),
                Is.EqualTo(Bytes("v")));
        }
    }

    private sealed record Doc(string Name, int Score);

    [Test]
    public async Task SetManyAtomicAcrossTreesAsync_guard_miss_commits_nothing_in_any_tree()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var t1 = $"g1-{suffix}";
        var t2 = $"g2-{suffix}";
        var tree1 = _cluster.GrainFactory.GetGrain<ILattice>(t1);
        var tree2 = _cluster.GrainFactory.GetGrain<ILattice>(t2);

        // Seed t2's guarded key with a value that fails the guard. Serialize with
        // the same JSON serializer the guard predicate is compiled against.
        var serializer = JsonLatticeSerializer<Doc>.Default;
        var seeded = serializer.Serialize(new Doc("old", 1));
        await tree2.SetAsync("guarded", seeded);

        var outcome = await _cluster.GrainFactory.BeginAtomicWrite($"xopg-{suffix}")
            .ForTree(t1).Set("order:1", Bytes("A"))
            .ForTree(t2).SetWhere("guarded", new Doc("new", 2), d => d.Score >= 100)
            .CommitAsync();

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.PreconditionFailed));
        Assert.That(await tree1.GetAsync("order:1"), Is.Null, "tree1 must not commit when tree2's guard fails");
        Assert.That(await tree2.GetAsync("guarded"), Is.EqualTo(seeded), "tree2 guarded key unchanged");
    }

    [Test]
    public async Task SetManyAtomicAcrossTreesAsync_is_idempotent_by_operationId()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var t1 = $"i1-{suffix}";
        var opId = $"xopi-{suffix}";
        var tree1 = _cluster.GrainFactory.GetGrain<ILattice>(t1);

        var first = await _cluster.GrainFactory.SetManyAtomicAcrossTreesAsync(
            [new LatticeTreeBatch(t1, Entries(("k", "v1")))], opId);
        var second = await _cluster.GrainFactory.SetManyAtomicAcrossTreesAsync(
            [new LatticeTreeBatch(t1, Entries(("k", "v1")))], opId);

        Assert.That(first, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        Assert.That(second, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        Assert.That(await tree1.GetAsync("k"), Is.EqualTo(Bytes("v1")));
    }

    [Test]
    public void SetManyAtomicAcrossTreesAsync_requires_operationId()
    {
        Assert.ThrowsAsync<ArgumentException>(() =>
            _cluster.GrainFactory.SetManyAtomicAcrossTreesAsync(
                [new LatticeTreeBatch("t", Entries(("k", "v")))], operationId: ""));
    }

    [Test]
    public async Task Builder_commits_across_trees()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var t1 = $"b1-{suffix}";
        var t2 = $"b2-{suffix}";

        var outcome = await _cluster.GrainFactory.BeginAtomicWrite($"xopb-{suffix}")
            .ForTree(t1).Set("order:1", Bytes("A"))
            .ForTree(t2).Set("sku:1", Bytes("X"))
            .CommitAsync();

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        Assert.That(await _cluster.GrainFactory.GetGrain<ILattice>(t1).GetAsync("order:1"), Is.EqualTo(Bytes("A")));
        Assert.That(await _cluster.GrainFactory.GetGrain<ILattice>(t2).GetAsync("sku:1"), Is.EqualTo(Bytes("X")));
    }

    [Test]
    public async Task Concurrent_reader_never_observes_a_partial_cross_tree_view()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var t1 = $"v1-{suffix}";
        var t2 = $"v2-{suffix}";
        var tree1 = _cluster.GrainFactory.GetGrain<ILattice>(t1);
        var tree2 = _cluster.GrainFactory.GetGrain<ILattice>(t2);

        const int generations = 25;

        // Defensive per-commit budget. A healthy cross-tree commit completes in
        // well under a second; this bound turns a stalled commit (a regression
        // that would otherwise hang this test - and the whole CI job -
        // indefinitely, since a grain call cannot be cancelled mid-flight) into
        // a deterministic failure instead.
        var perCommitBudget = TimeSpan.FromSeconds(30);

        var stop = false;
        var violations = 0;
        using var readerCts = new CancellationTokenSource();
        var readerToken = readerCts.Token;
        var reader = Task.Run(async () =>
        {
            while (!Volatile.Read(ref stop) && !readerToken.IsCancellationRequested)
            {
                // Read tree1 first, then tree2: the tree2 sample is taken no
                // earlier than the tree1 sample. Each cross-tree commit makes
                // a new generation visible in BOTH trees atomically (the
                // coordinator's single decision write), so the globally-visible
                // generation is monotonic in time and identical across the two
                // trees at any single instant. Therefore the generation observed
                // in tree2 (read later) must be >= the one observed in tree1
                // (read earlier). A tree2 read that LAGS tree1 means tree2 never
                // received a generation that tree1 already exposed - a genuine
                // partial cross-tree commit. This invariant is sound: it cannot
                // be tripped by the benign read-skew of sampling two trees at two
                // instants while a flip lands between them.
                var ga = GenerationOf(await tree1.GetAsync("k"));
                var gb = GenerationOf(await tree2.GetAsync("k"));
                if (gb < ga)
                {
                    Interlocked.Increment(ref violations);
                }
            }
        });

        try
        {
            for (var i = 0; i < generations; i++)
            {
                var commit = _cluster.GrainFactory.SetManyAtomicAcrossTreesAsync(
                    [
                        new LatticeTreeBatch(t1, Entries(("k", $"{i}"))),
                        new LatticeTreeBatch(t2, Entries(("k", $"{i}"))),
                    ],
                    operationId: $"xopv-{suffix}-{i}");

                if (await Task.WhenAny(commit, Task.Delay(perCommitBudget)) != commit)
                {
                    Assert.Fail(
                        $"cross-tree commit for generation {i} did not complete within " +
                        $"{perCommitBudget.TotalSeconds:0}s (stall/hang)");
                }

                await commit;
            }
        }
        finally
        {
            Volatile.Write(ref stop, true);
            await readerCts.CancelAsync();
            await reader;
        }

        Assert.That(violations, Is.EqualTo(0),
            "a concurrent reader observed a partial cross-tree view (the later-read tree lagged the earlier-read tree)");

        // After every commit has settled, both trees must hold the final
        // generation: the all-or-nothing commit guarantee at the terminal state.
        Assert.That(GenerationOf(await tree1.GetAsync("k")), Is.EqualTo(generations - 1));
        Assert.That(GenerationOf(await tree2.GetAsync("k")), Is.EqualTo(generations - 1));
    }

    /// <summary>
    /// Maps a stored value to its generation index: absent (null) is the
    /// pre-commit generation (-1), otherwise the integer payload written by the
    /// workload.
    /// </summary>
    private static int GenerationOf(byte[]? value) =>
        value is null ? -1 : int.Parse(Encoding.UTF8.GetString(value));
}
