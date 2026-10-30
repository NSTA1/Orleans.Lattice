using NSubstitute;
using Orleans.Concurrency;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>Authoritative held-source probes retain causal verdicts with bounded RPCs.</summary>
[TestFixture]
public sealed class CausalFrontierProbeBatchingTests
{
    private const string Origin = "probe-origin";
    private const string Tree = "probe-tree";

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    private sealed class Harness
    {
        public IGrainFactory Factory { get; } = Substitute.For<IGrainFactory>();
        public ICausalApplyBufferGrain Buffer { get; } = Substitute.For<ICausalApplyBufferGrain>();
        public IReplicationDeadLetterGrain DeadLetters { get; } = Substitute.For<IReplicationDeadLetterGrain>();
        public IReplicationHighWaterMarkGrain Hwm { get; } = Substitute.For<IReplicationHighWaterMarkGrain>();
        public FakePersistentState<ReplicationOriginFrontierState> State { get; } = new();
        public List<HybridLogicalClock[]> Requests { get; } = [];
        public Func<IReadOnlyList<HybridLogicalClock>, Task<bool[]>> Confirm { get; set; } =
            timestamps => Task.FromResult(timestamps.Select(t => t.WallClockTicks % 2 == 0).ToArray());
        public ReplicationOriginFrontierGrain Grain { get; }

        public Harness(string source)
        {
            Factory.GetGrain<ICausalApplyBufferGrain>(Tree, Arg.Any<string?>()).Returns(Buffer);
            Factory.GetGrain<IReplicationDeadLetterGrain>(Tree, Arg.Any<string?>()).Returns(DeadLetters);
            Factory.GetGrain<IReplicationHighWaterMarkGrain>(Tree, Arg.Any<string?>()).Returns(Hwm);
            Buffer.IsHoldingManyAsync(Origin, Arg.Any<IReadOnlyList<HybridLogicalClock>>())
                .Returns(call => ReadAsync((IReadOnlyList<HybridLogicalClock>)call[1]));
            DeadLetters.IsHoldingManyAsync(Origin, Arg.Any<IReadOnlyList<HybridLogicalClock>>(), Arg.Any<CancellationToken>())
                .Returns(call => ReadAsync((IReadOnlyList<HybridLogicalClock>)call[1]));
            Hwm.HasAppliedManyAsync(Origin, Arg.Any<IReadOnlyList<HybridLogicalClock>>())
                .Returns(async call => (await ReadAsync((IReadOnlyList<HybridLogicalClock>)call[1])).Select(held => !held).ToArray());
            Grain = HighWaterMarkTestGrains.Frontier(Origin, Factory, State);
            State.State.HeldBySource[source + Tree] = [];
            State.State.AggregateLowWatermark = Hlc(1000);
        }

        private Task<bool[]> ReadAsync(IReadOnlyList<HybridLogicalClock> timestamps)
        {
            Requests.Add(timestamps.ToArray());
            return Confirm(timestamps);
        }
    }

    [TestCase("b|", 1)]
    [TestCase("b|", 128)]
    [TestCase("b|", 257)]
    [TestCase("d|", 1)]
    [TestCase("d|", 128)]
    [TestCase("d|", 257)]
    [TestCase("x|", 1)]
    [TestCase("x|", 128)]
    [TestCase("x|", 257)]
    public async Task CheckAsync_batches_each_source_and_preserves_order_and_stale_cleanup(string source, int count)
    {
        var h = new Harness(source);
        var required = Enumerable.Range(1, count).Select(i => Hlc(i)).Reverse().ToArray();
        h.State.State.HeldBySource[source + Tree] = required.ToHashSet();

        var result = await h.Grain.CheckAsync(required, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo(required.Select(t => t.WallClockTicks % 2 == 0
                ? CausalDependencyVerdict.Unmet : CausalDependencyVerdict.Met)));
            Assert.That(h.Requests, Has.Count.EqualTo((count + 127) / 128));
            Assert.That(h.Requests.SelectMany(batch => batch), Is.EqualTo(required));
            Assert.That(h.Requests.All(batch => batch.Length <= 128), Is.True);
            Assert.That(h.State.State.HeldBySource.GetValueOrDefault(source + Tree) ?? [],
                Is.EquivalentTo(required.Where(t => t.WallClockTicks % 2 == 0)));
            Assert.That(h.State.WriteCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task CheckAsync_keeps_duplicates_lost_priority_strict_watermark_and_source_short_circuit()
    {
        var h = new Harness("b|");
        h.State.State.HeldBySource["b|" + Tree] = [Hlc(2), Hlc(3), Hlc(4), Hlc(1000)];
        h.State.State.HeldBySource["d|" + Tree] = [Hlc(2), Hlc(3), Hlc(4), Hlc(1000)];
        h.State.State.Lost.Add(Hlc(4));
        var required = new[] { Hlc(2), Hlc(3), Hlc(2), Hlc(4), Hlc(1000), Hlc(1001), Hlc(5) };

        var result = await h.Grain.CheckAsync(required, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo(new[] {
                CausalDependencyVerdict.Unmet, CausalDependencyVerdict.Met, CausalDependencyVerdict.Unmet,
                CausalDependencyVerdict.Lost, CausalDependencyVerdict.Unmet, CausalDependencyVerdict.Unmet,
                CausalDependencyVerdict.Met }));
            Assert.That(h.Requests, Has.Count.EqualTo(2));
            Assert.That(h.Requests[0], Is.EqualTo(new[] { Hlc(2), Hlc(3), Hlc(2) }));
            Assert.That(h.Requests[1], Is.EqualTo(new[] { Hlc(3) }), "a source-confirmed hold skips later sources");
        });
    }

    [TestCase("b|")]
    [TestCase("d|")]
    [TestCase("x|")]
    public void CheckAsync_failed_batch_propagates_without_dropping_any_stale_listing(string source)
    {
        var h = new Harness(source);
        var required = Enumerable.Range(1, 129).Select(i => Hlc(i)).ToArray();
        h.State.State.HeldBySource[source + Tree] = required.ToHashSet();
        h.Confirm = timestamps => h.Requests.Count == 1
            ? Task.FromResult(new bool[timestamps.Count])
            : Task.FromException<bool[]>(new InvalidOperationException("source down"));

        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.CheckAsync(required, CancellationToken.None));
        Assert.That(h.State.State.HeldBySource[source + Tree], Is.EquivalentTo(required));
        Assert.That(h.State.WriteCount, Is.Zero);
    }

    [TestCase("b|")]
    [TestCase("d|")]
    [TestCase("x|")]
    public void CheckAsync_incomplete_response_fails_closed(string source)
    {
        var h = new Harness(source);
        h.State.State.HeldBySource[source + Tree] = [Hlc(3)];
        h.Confirm = _ => Task.FromResult(Array.Empty<bool>());

        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.CheckAsync([Hlc(3)], CancellationToken.None));
        Assert.That(h.State.State.HeldBySource[source + Tree], Has.Count.EqualTo(1));
    }

    [Test]
    public async Task CheckAsync_unknown_source_stays_held_and_empty_or_unlisted_reads_issue_no_calls()
    {
        var h = new Harness("unknown|");
        h.State.State.HeldBySource["unknown|" + Tree] = [Hlc(3)];
        Assert.That(await h.Grain.CheckAsync([Hlc(3)], CancellationToken.None),
            Is.EqualTo(new[] { CausalDependencyVerdict.Unmet }));
        Assert.That(await h.Grain.CheckAsync([], CancellationToken.None), Is.Empty);
        Assert.That(await h.Grain.CheckAsync([Hlc(5)], CancellationToken.None),
            Is.EqualTo(new[] { CausalDependencyVerdict.Met }));
        Assert.That(h.Requests, Is.Empty);
    }

    [Test]
    public void CheckAsync_cancellation_between_windows_keeps_all_listings()
    {
        var h = new Harness("b|");
        var required = Enumerable.Range(1, 129).Select(i => Hlc(i)).ToArray();
        h.State.State.HeldBySource["b|" + Tree] = required.ToHashSet();
        using var cts = new CancellationTokenSource();
        h.Confirm = timestamps =>
        {
            cts.Cancel();
            return Task.FromResult(new bool[timestamps.Count]);
        };

        Assert.ThrowsAsync<OperationCanceledException>(() => h.Grain.CheckAsync(required, cts.Token));
        Assert.That(h.Requests, Has.Count.EqualTo(1));
        Assert.That(h.State.State.HeldBySource["b|" + Tree], Is.EquivalentTo(required));
    }

    [TestCase(typeof(ICausalApplyBufferGrain), nameof(ICausalApplyBufferGrain.IsHoldingManyAsync))]
    [TestCase(typeof(IReplicationDeadLetterGrain), nameof(IReplicationDeadLetterGrain.IsHoldingManyAsync))]
    [TestCase(typeof(IReplicationHighWaterMarkGrain), nameof(IReplicationHighWaterMarkGrain.HasAppliedManyAsync))]
    public void Batch_probes_always_interleave_to_avoid_source_frontier_cycles(Type contract, string method)
    {
        Assert.That(contract.GetMethod(method), Is.Not.Null);
        Assert.That(Attribute.IsDefined(contract.GetMethod(method)!, typeof(AlwaysInterleaveAttribute)), Is.True);
    }
}
