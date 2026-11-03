using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit and allocation tests for <see cref="ReplicatedWriteJudgement"/>, the
/// strict-ingest judgement of plain replicated writes (#5684), and for the
/// <see cref="ILatticeWriteInterceptor.JudgesReplicatedWritesAsync"/> seam.
/// </summary>
[TestFixture]
public class ReplicatedWriteJudgementTests
{
    private const string Tree = "orders";
    private const string RefusedKey = "refused";

    private static ApplyMergeItem Row(string key, byte[]? value = null, bool tombstone = false, long expiresAtTicks = 0) => new()
    {
        Key = key,
        Value = tombstone ? null : value ?? [1],
        SourceHlc = new HybridLogicalClock { WallClockTicks = 1_000, Counter = 0 },
        OriginClusterId = "peer",
        ExpiresAtTicks = expiresAtTicks,
        IsTombstone = tombstone,
    };

    /// <summary>
    /// Opts into system-origin writes, judges every tree, accepts every row from a
    /// cached result and diverts <see cref="RefusedKey"/>.
    /// </summary>
    private sealed class StrictStage(LatticeWriteDecision? decision = null, bool judges = true) : ILatticeWriteInterceptor
    {
        private static readonly ValueTask<LatticeWriteDecision> Accepted = new(LatticeWriteDecision.Accept());
        private static readonly ValueTask<LatticeWriteDecision> Diverted = new(LatticeWriteDecision.DeadLetter("refused"));

        public int Calls { get; private set; }

        public LatticeOperation LastOperation { get; private set; }

        public string? LastTree { get; private set; }

        public bool InterceptsSystemOrigin => true;

        public ValueTask<bool> JudgesReplicatedWritesAsync(string treeId, CancellationToken cancellationToken = default) =>
            new(judges);

        public ValueTask<LatticeWriteDecision> OnWriteAsync(in LatticeWriteRequest request, CancellationToken cancellationToken = default)
        {
            Calls++;
            LastOperation = request.Operation;
            LastTree = request.TreeId;
            if (decision is { } fixedDecision)
            {
                return new ValueTask<LatticeWriteDecision>(fixedDecision);
            }

            return request.Key == RefusedKey ? Diverted : Accepted;
        }
    }

    /// <summary>An interceptor that relies on every default member.</summary>
    private sealed class DefaultsStage : ILatticeWriteInterceptor
    {
        public ValueTask<LatticeWriteDecision> OnWriteAsync(in LatticeWriteRequest request, CancellationToken cancellationToken = default) =>
            new(LatticeWriteDecision.Accept());
    }

    [Test]
    public async Task JudgesReplicatedWritesAsync_defaults_to_true_so_an_existing_interceptor_fails_closed()
    {
        ILatticeWriteInterceptor interceptor = new DefaultsStage();

        Assert.That(await interceptor.JudgesReplicatedWritesAsync(Tree), Is.True);
    }

    [Test]
    public async Task JudgesReplicatedWritesAsync_is_false_for_the_null_interceptor()
    {
        ILatticeWriteInterceptor interceptor = new NullLatticeWriteInterceptor();

        Assert.That(await interceptor.JudgesReplicatedWritesAsync(Tree), Is.False);
    }

    [Test]
    public async Task AppliesAsync_is_false_for_the_null_interceptor()
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            Assert.That(await ReplicatedWriteJudgement.AppliesAsync(new NullLatticeWriteInterceptor(), Tree, CancellationToken.None), Is.False);
        }
    }

    [Test]
    public async Task AppliesAsync_is_false_when_the_interceptor_does_not_opt_into_system_origin_writes()
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            Assert.That(await ReplicatedWriteJudgement.AppliesAsync(new DefaultsStage(), Tree, CancellationToken.None), Is.False);
        }
    }

    [Test]
    public async Task AppliesAsync_asks_the_interceptor_when_it_opts_into_system_origin_writes()
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            Assert.Multiple(async () =>
            {
                Assert.That(await ReplicatedWriteJudgement.AppliesAsync(new StrictStage(judges: true), Tree, CancellationToken.None), Is.True);
                Assert.That(await ReplicatedWriteJudgement.AppliesAsync(new StrictStage(judges: false), Tree, CancellationToken.None), Is.False);
            });
        }
    }

    [Test]
    public async Task AdmitsAsync_admits_an_accepted_or_transformed_value_and_diverts_a_dead_letter()
    {
        var accepted = new StrictStage(LatticeWriteDecision.Accept());
        var transformed = new StrictStage(LatticeWriteDecision.AcceptTransformed([9]));
        var diverted = new StrictStage(LatticeWriteDecision.DeadLetter("no"));

        Assert.Multiple(async () =>
        {
            Assert.That(await ReplicatedWriteJudgement.AdmitsAsync(accepted, Tree, "k", [1], null, CancellationToken.None), Is.True);
            Assert.That(await ReplicatedWriteJudgement.AdmitsAsync(transformed, Tree, "k", [1], null, CancellationToken.None), Is.True);
            Assert.That(await ReplicatedWriteJudgement.AdmitsAsync(diverted, Tree, "k", [1], null, CancellationToken.None), Is.False);
            Assert.That(accepted.LastOperation, Is.EqualTo(LatticeOperation.Write | LatticeOperation.Replication));
            Assert.That(accepted.LastTree, Is.EqualTo(Tree));
        });
    }

    [Test]
    public void AdmitsAsync_throws_on_a_rejection()
    {
        var rejecting = new StrictStage(LatticeWriteDecision.Reject("no"));

        Assert.That(
            async () => await ReplicatedWriteJudgement.AdmitsAsync(rejecting, Tree, "k", [1], null, CancellationToken.None),
            Throws.TypeOf<LatticeWriteRejectedException>());
    }

    [Test]
    public void RemainingTtl_is_null_without_an_expiry_and_the_time_left_otherwise()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ReplicatedWriteJudgement.RemainingTtl(0, 100), Is.Null);
            Assert.That(ReplicatedWriteJudgement.RemainingTtl(150, 100), Is.EqualTo(TimeSpan.FromTicks(50)));
            Assert.That(ReplicatedWriteJudgement.RemainingTtl(50, 100), Is.EqualTo(TimeSpan.FromTicks(-50)));
        });
    }

    [Test]
    public async Task FilterAsync_returns_the_same_list_when_nothing_is_diverted()
    {
        var stage = new StrictStage();
        IReadOnlyList<ApplyMergeItem> items = [Row("a"), Row("b")];

        var kept = await ReplicatedWriteJudgement.FilterAsync(stage, Tree, items, 0, CancellationToken.None);

        Assert.That(kept, Is.SameAs(items));
    }

    [Test]
    public async Task FilterAsync_drops_a_diverted_row_and_keeps_the_rest_in_order()
    {
        var stage = new StrictStage();
        IReadOnlyList<ApplyMergeItem> items = [Row("a"), Row(RefusedKey), Row("b")];

        var kept = await ReplicatedWriteJudgement.FilterAsync(stage, Tree, items, 0, CancellationToken.None);

        Assert.That(kept.Select(item => item.Key), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task FilterAsync_does_not_judge_a_tombstone_or_an_expired_value()
    {
        var stage = new StrictStage(LatticeWriteDecision.DeadLetter("no"));
        IReadOnlyList<ApplyMergeItem> items = [Row(RefusedKey, tombstone: true), Row("expired", expiresAtTicks: 50)];

        var kept = await ReplicatedWriteJudgement.FilterAsync(stage, Tree, items, nowTicks: 100, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(kept, Is.SameAs(items));
            Assert.That(stage.Calls, Is.Zero);
        });
    }

    [Test]
    public void AppliesAsync_allocates_nothing_for_a_tree_that_is_not_judged()
    {
        ILatticeWriteInterceptor[] interceptors = [new NullLatticeWriteInterceptor(), new DefaultsStage(), new StrictStage(judges: false)];
        foreach (var interceptor in interceptors)
        {
            using (LatticeAccessGateContext.EnterSystemOrigin())
            {
                var growth = AllocationProbe.Growth(
                    _ => interceptor,
                    static (stage, size) =>
                    {
                        for (var i = 0; i < size; i++)
                        {
                            AllocationProbe.ScalarSink += ReplicatedWriteJudgement.AppliesAsync(stage, Tree, CancellationToken.None).Result ? 1 : 0;
                        }
                    },
                    smallSize: 100,
                    largeSize: 10_000);

                Assert.That(growth, Is.Zero, $"the per-batch gate must not allocate for {interceptor.GetType().Name}");
            }
        }
    }

    [Test]
    public void FilterAsync_allocates_nothing_per_row_when_a_strict_tree_accepts_every_row()
    {
        var stage = new StrictStage();
        var growth = AllocationProbe.Growth(
            static size =>
            {
                var items = new ApplyMergeItem[size];
                for (var i = 0; i < size; i++)
                {
                    items[i] = Row("k" + i);
                }

                return (IReadOnlyList<ApplyMergeItem>)items;
            },
            (items, _) => AllocationProbe.ScalarSink +=
                ReplicatedWriteJudgement.FilterAsync(stage, Tree, items, 0, CancellationToken.None).Result.Count,
            smallSize: 100,
            largeSize: 10_000);

        Assert.That(growth, Is.Zero, "judging an accepted row must not allocate");
    }

    [Test]
    public void FilterAsync_allocates_one_list_when_a_row_is_diverted()
    {
        var stage = new StrictStage();
        var growth = AllocationProbe.Growth(
            static size =>
            {
                var items = new ApplyMergeItem[size];
                items[0] = Row(RefusedKey);
                for (var i = 1; i < size; i++)
                {
                    items[i] = Row("k" + i);
                }

                return (IReadOnlyList<ApplyMergeItem>)items;
            },
            (items, _) => AllocationProbe.ScalarSink +=
                ReplicatedWriteJudgement.FilterAsync(stage, Tree, items, 0, CancellationToken.None).Result.Count,
            smallSize: 100,
            largeSize: 10_000);

        // One list sized to the batch: its backing array grows with the batch and
        // nothing else does.
        var listArrayGrowth = (10_000 - 100) * System.Runtime.CompilerServices.Unsafe.SizeOf<ApplyMergeItem>();
        Assert.That(growth, Is.LessThanOrEqualTo(listArrayGrowth + 1_024),
            "a diverted row costs one list for the batch, never an allocation per row");
    }
}
