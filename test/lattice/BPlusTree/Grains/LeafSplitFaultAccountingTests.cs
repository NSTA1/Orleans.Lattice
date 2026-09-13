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
/// A division that begins and throws must be visible as one (issue #2845).
/// <para>
/// <see cref="LatticeMetrics.LeafSplitAttempts"/> exists so that "no division
/// was ever sought" is a positive reading rather than an absence. Its three
/// original outcome arms all describe a division that declined to <em>start</em>
/// - the gate was held, or the leaf was already back under capacity, or the
/// division ran to completion - so a division that started and failed landed on
/// none of them. The observable it produced was therefore byte-identical to a
/// tree nothing had ever tried to divide, which is the exact collapse the
/// counter was introduced to prevent.
/// </para>
/// <para>
/// <b>That is not a hypothetical.</b> It was the measured state of a production
/// tree: fifteen increments on <see cref="LatticeMetrics.LeafSplits"/>, zero on
/// every arm of <see cref="LatticeMetrics.LeafSplitAttempts"/>, held unchanging
/// for fourteen minutes on a quiescent process. Every division begun had
/// thrown, and the instrument read as though none had been sought.
/// </para>
/// <para>
/// <b>Why the negative arm is the one that matters.</b> Asserting that
/// <c>faulted</c> increments is worth nothing unless the fault is established
/// independently of the counter under test, because the defect being fixed is
/// precisely that a fault produces no measurement. So each arm here proves the
/// throw happened by catching it at the public seam - the split path rethrows
/// unchanged, so the exception surfacing to the caller is evidence the counter
/// cannot fake - and separately proves the division was really begun by reading
/// <see cref="LatticeMetrics.LeafSplits"/>, which is incremented by different
/// code in a different method.
/// </para>
/// </summary>
public sealed class LeafSplitFaultAccountingTests
{
    private sealed record Measurement(long Value, string Outcome, string FailureClass);

    private static MeterListener ListenForAttempts(List<Measurement> sink)
        => MeterListening.StartForInstrument(
            LatticeMetrics.LeafSplitAttempts,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                var outcome = string.Empty;
                var failureClass = string.Empty;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagOutcome)
                    {
                        outcome = tag.Value?.ToString() ?? string.Empty;
                    }
                    else if (tag.Key == LatticeMetrics.TagFailureClass)
                    {
                        failureClass = tag.Value?.ToString() ?? string.Empty;
                    }
                }

                lock (sink) sink.Add(new Measurement(value, outcome, failureClass));
            }));

    /// <summary>
    /// Listens to <see cref="LatticeMetrics.LeafSplits"/>, which is incremented
    /// inside <c>SplitAsync</c> immediately after the split intent is
    /// persisted. It is the independent witness that a division was genuinely
    /// begun, written by different code in a different method from the attempt
    /// accounting under test, so the two cannot corroborate each other by
    /// accident.
    /// </summary>
    private static MeterListener ListenForSplits(List<long> sink)
        => MeterListening.StartForInstrument(
            LatticeMetrics.LeafSplits,
            l => l.SetMeasurementEventCallback<long>((_, value, _, _) =>
            {
                lock (sink) sink.Add(value);
            }));

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
    /// A leaf online from a snapshot whose sibling optionally refuses to be
    /// initialised.
    /// <para>
    /// <paramref name="siblingFault"/> is injected at
    /// <c>InitializeSiblingAsync</c>, which is the first cross-grain await
    /// inside <c>CompleteSplitAsync</c> and therefore sits strictly
    /// <em>after</em> the split intent has been persisted and
    /// <see cref="LatticeMetrics.LeafSplits"/> incremented. That placement is
    /// the whole point: it reproduces the real failure window - a division that
    /// is already durable and can no longer be abandoned cleanly - rather than
    /// one that fails before committing to anything.
    /// </para>
    /// </summary>
    private static async Task<BPlusLeafGrain> RehydratedLeafAsync(
        int rowCount,
        int maxLeafKeys = 1_000_000,
        Exception? siblingFault = null)
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

        if (siblingFault is null)
        {
            sibling.InitializeSiblingAsync(Arg.Any<SiblingInitialization>()).Returns(Task.CompletedTask);
        }
        else
        {
            // A faulted Task rather than a synchronous throw, so the failure
            // arrives the way a real cross-grain call failure does: at the
            // await, after the caller has already committed to the division.
            sibling.InitializeSiblingAsync(Arg.Any<SiblingInitialization>())
                .Returns(_ => Task.FromException(siblingFault));
        }

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(sibling);
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(sibling);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "tree-split-faults";
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

    // ---------------------------------------------------------------
    // The pair that carries the issue. Neither half means anything alone:
    // the positive arm shows `divided` is still recorded (so the catch did
    // not swallow the success path), and the negative arm shows a throw is
    // now recorded at all, which is the state that previously produced no
    // measurement whatsoever.
    // ---------------------------------------------------------------

    [Test]
    public async Task A_division_that_completes_is_counted_as_divided_and_never_as_faulted()
    {
        var grain = await RehydratedLeafAsync(3, maxLeafKeys: 3);

        var measurements = new List<Measurement>();
        SplitResult? split;
        using (ListenForAttempts(measurements))
        {
            split = await grain.SetAsync("zzz-over", Encoding.UTF8.GetBytes("v"));
        }

        // Established without the counter: a division actually completed.
        Assert.That(split, Is.Not.Null, "precondition: the write must have driven a real division");

        Assert.Multiple(() =>
        {
            Assert.That(
                measurements.Where(m => m.Outcome == "divided").Sum(m => m.Value),
                Is.EqualTo(1),
                "a completed division must still be counted exactly once");
            Assert.That(
                measurements.Where(m => m.Outcome == "faulted").Sum(m => m.Value),
                Is.Zero,
                "and a division that completed must never register as a fault");
        });
    }

    [Test]
    public async Task A_division_whose_completion_throws_is_counted_as_faulted_and_not_as_divided()
    {
        // The sibling refuses initialisation, which is the first cross-grain
        // await after the split intent is durable. This is the shape the
        // production rig was in: the division is committed to storage and then
        // cannot finish.
        var fault = new InvalidOperationException("sibling refused initialisation");
        var grain = await RehydratedLeafAsync(3, maxLeafKeys: 3, siblingFault: fault);

        var measurements = new List<Measurement>();
        var splits = new List<long>();
        Exception? surfaced = null;

        using (ListenForSplits(splits))
        using (ListenForAttempts(measurements))
        {
            try
            {
                await grain.SetAsync("zzz-over", Encoding.UTF8.GetBytes("v"));
            }
            catch (Exception ex)
            {
                surfaced = ex;
            }
        }

        // Precondition one, established WITHOUT the counter under test: the
        // division really did throw. The split path rethrows unchanged, so the
        // caller seeing the original exception is proof the fault occurred and
        // proof the fix changed no behaviour.
        Assert.That(
            surfaced, Is.SameAs(fault),
            "precondition: the division must have thrown, and must rethrow unchanged");

        // Precondition two, from a different instrument written by different
        // code: the division was genuinely BEGUN. Without this the arm below
        // could pass on a division that never started, which is the very state
        // it exists to distinguish.
        Assert.That(
            splits.Sum(), Is.EqualTo(1),
            "precondition: the split intent must have been persisted and counted");

        Assert.Multiple(() =>
        {
            // The defect, in one assertion. Before the faulted arm existed this
            // was zero, and a zero here is indistinguishable from a tree on
            // which no division was ever sought.
            Assert.That(
                measurements.Where(m => m.Outcome == "faulted").Sum(m => m.Value),
                Is.EqualTo(1),
                "a division that began and threw must be counted as faulted");

            Assert.That(
                measurements.Where(m => m.Outcome == "divided").Sum(m => m.Value),
                Is.Zero,
                "and must never be counted as a completed division");

            // The counter must not have gone silent in some other way either.
            Assert.That(
                measurements.Where(m => m.Outcome == "gate_contended" && m.Value != 0),
                Is.Empty,
                "a fault is not a refusal to evaluate");
        });
    }

    // ---------------------------------------------------------------
    // The failure class. The two named classes have opposite remedies, so
    // a fault that cannot be told apart from the other kind routes the
    // operator to the wrong one.
    // ---------------------------------------------------------------

    [Test]
    public async Task A_division_that_cannot_be_paid_for_in_memory_is_classified_unaffordable()
    {
        var fault = new LeafSnapshotUnaffordableException("tree-split-faults", 1024L, 512L, null);
        var grain = await RehydratedLeafAsync(3, maxLeafKeys: 3, siblingFault: fault);

        var measurements = new List<Measurement>();
        using (ListenForAttempts(measurements))
        {
            Assert.That(
                async () => await grain.SetAsync("zzz-over", Encoding.UTF8.GetBytes("v")),
                Throws.InstanceOf<LeafSnapshotUnaffordableException>(),
                "precondition: the division must have thrown the memory fault");
        }

        Assert.That(
            measurements.Where(m => m.Outcome == "faulted" && m.Value != 0).Select(m => m.FailureClass),
            Is.EquivalentTo(new[] { "unaffordable" }),
            "a memory shortage must be separable from a deadline, because the remedies differ");
    }

    [Test]
    public async Task A_division_that_runs_out_of_time_is_classified_timeout()
    {
        // Matched on the base TimeoutException rather than on the individual
        // typed deadlines, because this library's own deadline types derive
        // from it deliberately and Orleans surfaces a response deadline as the
        // base type itself.
        var fault = new ShardActivationTimeoutException("the sibling did not activate in time");
        var grain = await RehydratedLeafAsync(3, maxLeafKeys: 3, siblingFault: fault);

        var measurements = new List<Measurement>();
        using (ListenForAttempts(measurements))
        {
            Assert.That(
                async () => await grain.SetAsync("zzz-over", Encoding.UTF8.GetBytes("v")),
                Throws.InstanceOf<ShardActivationTimeoutException>(),
                "precondition: the division must have thrown the deadline fault");
        }

        Assert.That(
            measurements.Where(m => m.Outcome == "faulted" && m.Value != 0).Select(m => m.FailureClass),
            Is.EquivalentTo(new[] { "timeout" }),
            "a derived deadline type must classify as timeout, not as other");
    }

    [Test]
    public async Task An_unrecognised_failure_is_still_counted_as_a_fault()
    {
        var grain = await RehydratedLeafAsync(
            3, maxLeafKeys: 3, siblingFault: new InvalidOperationException("something else entirely"));

        var measurements = new List<Measurement>();
        using (ListenForAttempts(measurements))
        {
            Assert.That(
                async () => await grain.SetAsync("zzz-over", Encoding.UTF8.GetBytes("v")),
                Throws.InstanceOf<InvalidOperationException>());
        }

        // The point of the `other` arm: classification failing must never
        // downgrade a fault to silence, which would reintroduce the defect for
        // every exception type nobody thought of.
        Assert.That(
            measurements.Where(m => m.Outcome == "faulted" && m.Value != 0).Select(m => m.FailureClass),
            Is.EquivalentTo(new[] { "other" }),
            "an unclassifiable fault must still be visible as a fault");
    }

    // ---------------------------------------------------------------
    // Priming. The fault arm is only readable at zero if every class of it
    // is minted, because the class tag is part of the series identity.
    // ---------------------------------------------------------------

    [Test]
    public async Task The_capture_seam_mints_every_failure_class_at_zero()
    {
        var grain = await RehydratedLeafAsync(512);

        await grain.SetAsync("zzz-live", Encoding.UTF8.GetBytes("v"));

        var measurements = new List<Measurement>();
        var declines = new List<string>();
        using (MeterListening.StartForInstrument(
            LatticeMetrics.LeafSnapshotCaptureDeclines,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagReason)
                    {
                        lock (declines) declines.Add(tag.Value?.ToString() ?? string.Empty);
                    }
                }
            })))
        using (ListenForAttempts(measurements))
        {
            await grain.CaptureSnapshotAsync();
        }

        // Established independently of the counter: the capture reached the
        // priming site at all. Without this an empty list would be ambiguous
        // between "the prime is missing" and "the seam was never reached".
        Assert.That(
            declines, Is.Empty,
            "the capture must actually proceed, or this arm measures nothing");

        Assert.Multiple(() =>
        {
            Assert.That(
                measurements
                    .Where(m => m.Outcome == "faulted")
                    .Select(m => m.FailureClass)
                    .Distinct()
                    .OrderBy(c => c, StringComparer.Ordinal),
                Is.EquivalentTo(new[] { "other", "timeout", "unaffordable" }),
                "every failure class must be minted, or the absent one is unreadable at zero");

            Assert.That(
                measurements.Where(m => m.Value != 0),
                Is.Empty,
                "priming must not fabricate faults that did not happen");
        });
    }
}
