using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using System.Diagnostics.Metrics;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A leaf division suspended inside <c>CompleteSplitAsync</c> is visible on the
/// in-flight signal while it carries neither <c>divided</c> nor <c>faulted</c>
/// (issue #2967).
/// <para>
/// The division outcome counter <see cref="LatticeMetrics.LeafSplitAttempts"/>
/// is a partition of <em>terminated</em> divisions: <c>divided</c>,
/// <c>faulted</c>, and the two declining arms. A division still executing the
/// completion body at scrape time has terminated on none of them, so before
/// this gauge existed it read as though nothing was happening - the exact
/// blind spot that let a division wedged forever inside the completion present
/// as healthy concurrency. <see cref="LatticeMetrics.LeafSplitCompletionsInFlight"/>
/// and <see cref="LatticeMetrics.LeafSplitCompletionOldestAge"/> name that
/// un-terminated state.
/// </para>
/// <para>
/// <b>The two arms carry the issue and neither means anything alone.</b> The
/// suspension arm shows the completion is counted in flight while it is
/// genuinely mid-body, and the throw arm shows the scope is released
/// inescapably on the exception path - together they establish that a
/// <em>persisting</em> in-flight entry is a division that has terminated on
/// nothing, which is precisely the "no outcome means in flight, not thrown"
/// inference the run-14 acceptance predicate now rests on (its Limb D).
/// </para>
/// <para>
/// <b>Non-vacuity.</b> Every arm proves the division was genuinely begun by
/// reading <see cref="LatticeMetrics.LeafSplits"/>, which is incremented by
/// different code in <c>SplitAsync</c> immediately before <c>CompleteSplitAsync</c>
/// is entered. Without that witness an arm could pass against code where the
/// write short-circuited before ever reaching the completion, which is the one
/// way a green fixture here would be lying.
/// </para>
/// </summary>
public sealed class LeafSplitCompletionInFlightTests
{
    private sealed record AttemptMeasurement(long Value, string Outcome);

    private const string TreeId = "tree-split-inflight";

    /// <summary>
    /// Listens to <see cref="LatticeMetrics.LeafSplits"/> - the independent
    /// witness, written by different code in <c>SplitAsync</c> after the split
    /// intent is persisted and before <c>CompleteSplitAsync</c> is entered -
    /// so a non-zero count proves the division reached the completion body
    /// rather than short-circuiting earlier.
    /// </summary>
    private static MeterListener ListenForSplits(List<long> sink)
        => MeterListening.StartForInstrument(
            LatticeMetrics.LeafSplits,
            l => l.SetMeasurementEventCallback<long>((_, value, _, _) =>
            {
                lock (sink) sink.Add(value);
            }));

    /// <summary>
    /// Listens to <see cref="LatticeMetrics.LeafSplitAttempts"/> so the suspension
    /// arm can assert the in-flight division is on none of the terminal outcome
    /// arms while it is suspended - the discriminator that makes in flight a
    /// distinct state rather than a synonym for busy.
    /// </summary>
    private static MeterListener ListenForAttempts(List<AttemptMeasurement> sink)
        => MeterListening.StartForInstrument(
            LatticeMetrics.LeafSplitAttempts,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                var outcome = string.Empty;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagOutcome)
                    {
                        outcome = tag.Value?.ToString() ?? string.Empty;
                    }
                }

                lock (sink) sink.Add(new AttemptMeasurement(value, outcome));
            }));

    /// <summary>
    /// Forces one observation of <see cref="LatticeMetrics.LeafSplitCompletionsInFlight"/>
    /// and returns the count reported for <see cref="TreeId"/> (zero when the
    /// tree has no series, which is the healthy absence rather than a measured
    /// zero).
    /// </summary>
    private static long SampleInFlightCount()
    {
        long observed = 0;
        var matchedTree = false;
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.LeafSplitCompletionsInFlight,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree && (tag.Value as string) == TreeId)
                    {
                        observed = value;
                        matchedTree = true;
                    }
                }
            }));
        listener.RecordObservableInstruments();
        return matchedTree ? observed : 0;
    }

    /// <summary>
    /// Forces one observation of <see cref="LatticeMetrics.LeafSplitCompletionOldestAge"/>
    /// and returns the age in seconds reported for <see cref="TreeId"/>, or a
    /// negative sentinel when the tree has no series.
    /// </summary>
    private static double SampleOldestAge()
    {
        var observed = -1.0;
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.LeafSplitCompletionOldestAge,
            l => l.SetMeasurementEventCallback<double>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree && (tag.Value as string) == TreeId)
                    {
                        observed = value;
                    }
                }
            }));
        listener.RecordObservableInstruments();
        return observed;
    }

    private static string Key(int i) => $"k{i:D5}";

    private static LeafSnapshotRow[] Corpus(int rowCount)
    {
        var rows = new LeafSnapshotRow[rowCount];
        for (var i = 0; i < rowCount; i++)
        {
            rows[i] = new LeafSnapshotRow(
                Key(i),
                new LwwValue<byte[]>
                {
                    Value = new byte[64],
                    Timestamp = new HybridLogicalClock { WallClockTicks = 100L + i, Counter = i },
                });
        }

        return rows;
    }

    /// <summary>
    /// A leaf online from a snapshot whose sibling's <c>InitializeSiblingAsync</c>
    /// - the first cross-grain await inside <c>CompleteSplitAsync</c>, strictly
    /// after the split intent is persisted and <see cref="LatticeMetrics.LeafSplits"/>
    /// incremented - is controllable. <paramref name="siblingInit"/> returns the
    /// Task the completion awaits there, so a test can hold the completion
    /// suspended (a pending Task) or fail it (a faulted Task) at the exact point
    /// a real division becomes durable and can no longer be abandoned cleanly.
    /// </summary>
    private static async Task<BPlusLeafGrain> RehydratedLeafAsync(
        int rowCount,
        int maxLeafKeys,
        Func<Task> siblingInit)
    {
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(new LeafSnapshotBlob
            {
                SnapshotOffset = 25L,
                EncodedRows = LeafSnapshotCodec.Encode(Corpus(rowCount)),
                SnapshotOffsetsByPartition = [25L],
            }));

        var sibling = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        var siblingContext = Substitute.For<IGrainContext>();
        siblingContext.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString()));
        ((IGrainBase)sibling).GrainContext.Returns(siblingContext);
        sibling.MergeEntriesAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));
        sibling.SetCheckpointOffsetHintsAsync(Arg.Any<long[]>()).Returns(Task.CompletedTask);
        sibling.InitializeSiblingAsync(Arg.Any<SiblingInitialization>())
            .Returns(_ => siblingInit());

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(sibling);
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(sibling);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = TreeId;
        state.State.ShardIndex = 0;

        var grain = new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            TestOptionsResolver.Create(
                baseOptions: new LatticeOptions
                {
                    WalPartitions = 1,
                    LeafPartialHydrationEnabled = true,
                    LeafHydrationResidentBytes = 4L * 1024,
                },
                maxLeafKeys: maxLeafKeys,
                shardCount: 1,
                factory: grainFactory),
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        Assert.That(
            await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None),
            Is.True,
            "the leaf must come online from its snapshot");
        return grain;
    }

    [Test]
    public async Task A_division_suspended_inside_completion_is_visible_in_flight_then_clears_on_completion()
    {
        // Before the completion runs, the tree has nothing in flight and so no
        // series - an absence, not a measured zero.
        Assert.That(SampleInFlightCount(), Is.Zero, "precondition: nothing in flight before the division");
        Assert.That(SampleOldestAge(), Is.LessThan(0), "precondition: no age series before the division");

        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var grain = await RehydratedLeafAsync(3, maxLeafKeys: 3, siblingInit: () =>
        {
            entered.TrySetResult();
            return release.Task;
        });

        var splits = new List<long>();
        var attempts = new List<AttemptMeasurement>();

        using var splitsListener = ListenForSplits(splits);
        using var attemptsListener = ListenForAttempts(attempts);

        // Drive the division. Execution runs synchronously through the fakes up
        // to the sibling await, where it suspends inside CompleteSplitAsync
        // holding its in-flight scope; the returned Task stays pending.
        var setTask = grain.SetAsync("zzz-over", Encoding.UTF8.GetBytes("v"));
        await entered.Task;

        // Non-vacuity, from different code than the gauge under test: the split
        // intent was persisted and counted, so control is genuinely inside the
        // completion body rather than short-circuited before it.
        Assert.That(splits.Sum(), Is.EqualTo(1),
            "precondition: the division must have been begun and reached the completion body");

        // The signal fires while the division is mid-completion.
        Assert.That(SampleInFlightCount(), Is.EqualTo(1),
            "a completion suspended inside CompleteSplitAsync must be counted in flight");
        Assert.That(SampleOldestAge(), Is.GreaterThanOrEqualTo(0),
            "and its age must be observable, not absent, while it is suspended");

        // The discriminator: an in-flight completion has terminated on nothing,
        // so it is on none of the outcome arms. This is what makes in flight a
        // distinct state rather than a synonym for a division in progress that
        // the outcome counters already cover.
        Assert.That(attempts.Where(m => m.Outcome == "divided").Sum(m => m.Value), Is.Zero,
            "a suspended completion has not yet divided");
        Assert.That(attempts.Where(m => m.Outcome == "faulted").Sum(m => m.Value), Is.Zero,
            "and a suspended completion has not faulted");

        // Let the completion finish and confirm the signal clears.
        release.TrySetResult();
        var split = await setTask;

        Assert.That(split, Is.Not.Null, "the released division must run to completion");
        Assert.That(attempts.Where(m => m.Outcome == "divided").Sum(m => m.Value), Is.EqualTo(1),
            "the completed division is now counted as divided");
        Assert.That(SampleInFlightCount(), Is.Zero,
            "and the completion is no longer in flight once it has returned");
    }

    [Test]
    public async Task A_completion_that_throws_is_deregistered_from_in_flight()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fault = new InvalidOperationException("sibling refused initialisation");

        var grain = await RehydratedLeafAsync(3, maxLeafKeys: 3, siblingInit: () =>
        {
            entered.TrySetResult();
            return release.Task;
        });

        var splits = new List<long>();
        using var splitsListener = ListenForSplits(splits);

        var setTask = grain.SetAsync("zzz-over", Encoding.UTF8.GetBytes("v"));
        await entered.Task;

        Assert.That(splits.Sum(), Is.EqualTo(1),
            "precondition: the division must have been begun and reached the completion body");
        Assert.That(SampleInFlightCount(), Is.EqualTo(1),
            "precondition: the suspended completion is in flight before it throws");

        // Fail the completion at the durable point. The split path rethrows
        // unchanged, so the caller sees the original exception - proof the
        // throw happened, established without the gauge under test.
        release.TrySetException(fault);
        var surfaced = Assert.CatchAsync(async () => await setTask);

        Assert.That(surfaced, Is.SameAs(fault),
            "precondition: the completion must have thrown, and must rethrow unchanged");

        // The load-bearing fact for the acceptance predicate's Limb D: the scope
        // is released on the exception path too, so a throw is inescapably
        // deregistered from the in-flight signal. A completion that stays in
        // flight has therefore terminated on nothing - it did not throw - which
        // is what licenses reading a persisting in-flight entry as genuinely
        // suspended rather than as a swallowed fault.
        Assert.That(SampleInFlightCount(), Is.Zero,
            "a completion that threw must not leak an in-flight registration");
    }
}
