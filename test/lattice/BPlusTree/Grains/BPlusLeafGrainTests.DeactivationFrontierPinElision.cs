using NSubstitute;
using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3643: the <c>frontier_pin</c> deactivation barrier elides its
/// pin-store call when the pin it resolves was already acknowledged by the
/// teardown persist tail in THIS deactivation, and counts the elision.
/// </summary>
/// <remarks>
/// <para>
/// <b>The rule under test.</b> The barrier resolves its batch exactly as it
/// always has, then skips the call only when every partition is dominated on
/// both axes (frontier HLC and offset) by what the tail's awaited flush was
/// acknowledged for. Any other shape publishes the full batch: a capture that
/// raised coverage, a write that advanced the clock, no pending advance (so no
/// tail ran), or a tail publish that faulted, was cancelled, or came back
/// unacknowledged.
/// </para>
/// <para>
/// <b>The observable.</b> The tail's own publishes all happen BEFORE the
/// tail's cursor report, and the barrier runs after it, so a batched pin
/// recorded after the first <c>cursor</c> call is the barrier's publish. Each
/// test states both halves: whether that publish happened and whether the
/// elision counter moved.
/// </para>
/// <para>
/// Built on the #3393 final-advance harness, whose leaf holds its advance
/// PENDING under coalescing options so a graceful deactivation runs the
/// teardown persist tail for real.
/// </para>
/// </remarks>
public partial class BPlusLeafGrainTests
{
    /// <summary>Records the elision counter's increments for <see cref="FinalAdvanceTreeId"/>.</summary>
    private static IDisposable ListenForFrontierPinElisions(List<string?> reasons) =>
        MeterListening.StartForInstrument(
            LatticeMetrics.LeafDeactivationBarrierElided,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                string? reason = null;
                string? tree = null;
                foreach (var t in tags)
                {
                    if (t.Key == LatticeMetrics.TagReason && t.Value is string r)
                    {
                        reason = r;
                    }
                    else if (t.Key == LatticeMetrics.TagTree && t.Value is string tr)
                    {
                        tree = tr;
                    }
                }

                if (tree != FinalAdvanceTreeId)
                {
                    return;
                }

                lock (reasons)
                {
                    for (var i = 0; i < value; i++)
                    {
                        reasons.Add(reason);
                    }
                }
            }));

    /// <summary>The batched pins published after the tail's cursor report: the barrier's.</summary>
    private static List<string> PinsAfterTailCursor(FinalAdvanceLeaf leaf)
    {
        var cursor = leaf.Calls.IndexOf("cursor");
        Assert.That(cursor, Is.GreaterThanOrEqualTo(0),
            "control: the teardown persist tail never reached its cursor report, so it did not run.");
        return leaf.Calls.Skip(cursor + 1).Where(c => c.StartsWith("pin:", StringComparison.Ordinal)).ToList();
    }

    /// <summary>
    /// A leaf whose pending advance is 3 and whose durable coverage was already
    /// restamped to 3, so the tail publishes min(persisted 3, coverage 3) and
    /// the capture that follows cannot raise it: the clean deactivation shape.
    /// </summary>
    private static async Task<FinalAdvanceLeaf> CreateCleanlyDeactivatableLeafAsync(
        ILeafCursorReporter? reporterOverride = null)
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var leaf = CreateFinalAdvanceLeaf(wal.Coordinator, digestCoalescingWindowMs: 0, reporterOverride: reporterOverride);
        await ActivateAsync(leaf.Grain);
        await leaf.Grain.OnCoverageLagTimerTickAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(leaf.Grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(3L),
                "precondition: the pending advance is 3, so the teardown persist tail runs.");
            Assert.That(leaf.State.State.ProjectionCheckpointOffset, Is.EqualTo(0L),
                "precondition: the advance is still PENDING.");
            Assert.That(leaf.Grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(3L),
                "precondition: coverage is already 3, so no later capture can raise the pin.");
        });

        leaf.Published.Clear();
        leaf.BatchedFrontiers.Clear();
        leaf.Calls.Clear();
        return leaf;
    }

    /// <summary>
    /// The clean deactivation: the tail's pin was acknowledged and nothing
    /// landed after it, so the barrier makes ZERO pin-store calls, is counted
    /// once under <c>frontier_pin</c>, and still records its duration.
    /// Pre-#3643 the barrier republished the identical pin.
    /// </summary>
    [Test]
    public async Task OnDeactivateAsync_elides_the_frontier_pin_barrier_after_an_acknowledged_clean_tail()
    {
        var leaf = await CreateCleanlyDeactivatableLeafAsync();
        var elisions = new List<string?>();
        var durations = new List<(string Reason, string? Tree, double Milliseconds)>();
        var failures = new List<string>();

        using (ListenForFrontierPinElisions(elisions))
        using (ListenForBarrierDurations(durations))
        using (ListenForBarrierFailures(failures))
        {
            await DeactivateFinalAdvanceLeafAsync(leaf, CancellationToken.None);
        }

        var frontierPin = LatticeMetrics.DeactivationBarrierFrontierPin.Value as string;
        Assert.Multiple(() =>
        {
            Assert.That(leaf.State.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
                "control: the teardown persist committed the final advance.");
            Assert.That(leaf.Calls.TakeWhile(c => c != "cursor"), Does.Contain("pin:3"),
                "control: the tail published and had acknowledged min(persisted 3, coverage 3).");
            Assert.That(PinsAfterTailCursor(leaf), Is.Empty,
                "THE assertion: the barrier's pin is dominated by the tail's acknowledgement, so it must "
                + "make no pin-store call at all.");
            Assert.That(elisions, Is.EqualTo(new[] { frontierPin }),
                "the elision is counted exactly once, under reason=frontier_pin.");
            Assert.That(
                durations.Count(d => d.Reason == frontierPin && d.Tree == FinalAdvanceTreeId),
                Is.EqualTo(1),
                "an elided barrier still records its frontier_pin duration.");
            Assert.That(failures, Does.Not.Contain(frontierPin),
                "an elision is not a barrier failure or skip.");
        });
    }

    /// <summary>
    /// The deactivation capture raises coverage above what the tail published,
    /// so the barrier's pin is not dominated on the OFFSET axis: it publishes
    /// and nothing is counted.
    /// </summary>
    [Test]
    public async Task OnDeactivateAsync_publishes_the_frontier_pin_barrier_when_a_capture_raised_coverage_after_the_tail()
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var leaf = CreateFinalAdvanceLeaf(wal.Coordinator, digestCoalescingWindowMs: 0);
        await ActivateAsync(leaf.Grain);
        Assert.That(leaf.Grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(0L),
            "precondition: coverage is the rehydrated 0, below the pending advance 3.");
        leaf.Published.Clear();
        leaf.Calls.Clear();

        var elisions = new List<string?>();
        using (ListenForFrontierPinElisions(elisions))
        {
            await DeactivateFinalAdvanceLeafAsync(leaf, CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(leaf.Calls.TakeWhile(c => c != "cursor"), Does.Contain("pin:0"),
                "control: the tail published the pre-capture coverage 0.");
            Assert.That(PinsAfterTailCursor(leaf), Is.EqualTo(new[] { "pin:3" }),
                "THE assertion: the capture raised coverage to 3, so the barrier must publish 3.");
            Assert.That(elisions, Is.Empty, "a publishing barrier must never be counted as elided.");
        });
    }

    /// <summary>
    /// A write lands after the tail's acknowledged publish and advances the
    /// leaf's clock, so the barrier's pin is not dominated on the FRONTIER
    /// axis even though its offset is unchanged: it publishes.
    /// </summary>
    [Test]
    public async Task OnDeactivateAsync_publishes_the_frontier_pin_barrier_when_the_frontier_advanced_after_the_tail()
    {
        var leaf = await CreateCleanlyDeactivatableLeafAsync();
        var clockAtTail = leaf.State.State.Clock;
        var applied = false;
        leaf.OnCursorReport = () =>
        {
            if (applied)
            {
                return;
            }

            applied = true;
            AsProjection(leaf.Grain).Apply(BuildSet(
                "late-write", "v"u8.ToArray(), hlcPhysical: clockAtTail.WallClockTicks + 1_000_000,
                treeId: FinalAdvanceTreeId));
        };

        var elisions = new List<string?>();
        using (ListenForFrontierPinElisions(elisions))
        {
            await DeactivateFinalAdvanceLeafAsync(leaf, CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(leaf.State.State.Clock, Is.GreaterThan(clockAtTail),
                "control: the late write advanced the leaf's clock.");
            Assert.That(PinsAfterTailCursor(leaf), Is.Not.Empty,
                "THE assertion: the barrier's frontier exceeds the acknowledged one, so it must publish.");
            Assert.That(leaf.BatchedFrontiers[^1], Is.GreaterThan(leaf.BatchedFrontiers[0]),
                "the barrier published the advanced frontier, not the acknowledged one.");
            Assert.That(elisions, Is.Empty, "a publishing barrier must never be counted as elided.");
        });
    }

    /// <summary>
    /// Clarification B: only an acknowledgement obtained in THIS deactivation
    /// counts. With no pending advance the tail does not run, so although an
    /// ordinary flush earlier in the activation's life was acknowledged for
    /// the very same pin, the barrier must still publish - it is the last
    /// healing publish a dormant leaf gets.
    /// </summary>
    [Test]
    public async Task OnDeactivateAsync_publishes_the_frontier_pin_barrier_when_no_tail_ran_despite_an_earlier_acknowledgement()
    {
        var leaf = await CreateCleanlyDeactivatableLeafAsync();
        await AsProjection(leaf.Grain).FlushCheckpointAsync();
        await leaf.Grain.FlushDurableMaterialiserFrontierAsync();

        Assert.Multiple(() =>
        {
            Assert.That(leaf.State.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
                "precondition: the advance is persisted, so no pending advance is left for a tail.");
            Assert.That(leaf.Batched.Select(p => p.PublishedOffset), Does.Contain(3L),
                "precondition: an ordinary flush in this activation was acknowledged for pin 3.");
        });

        leaf.Published.Clear();
        leaf.Calls.Clear();
        var elisions = new List<string?>();
        using (ListenForFrontierPinElisions(elisions))
        {
            await DeactivateFinalAdvanceLeafAsync(leaf, CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(leaf.Batched.Select(p => p.PublishedOffset), Is.EqualTo(new[] { 3L }),
                "THE assertion: with no in-deactivation acknowledgement the barrier publishes, exactly once.");
            Assert.That(elisions, Is.Empty, "a publishing barrier must never be counted as elided.");
        });
    }

    /// <summary>
    /// Clarification A at the grain: a tail publish the reporter reports as
    /// NOT acknowledged (its shard write faulted and was swallowed) records
    /// nothing, so the barrier publishes.
    /// </summary>
    [Test]
    public async Task OnDeactivateAsync_publishes_the_frontier_pin_barrier_when_the_tail_was_not_acknowledged()
    {
        var leaf = await CreateCleanlyDeactivatableLeafAsync();
        leaf.AcknowledgePinFlush = false;

        var elisions = new List<string?>();
        using (ListenForFrontierPinElisions(elisions))
        {
            await DeactivateFinalAdvanceLeafAsync(leaf, CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(leaf.Calls.TakeWhile(c => c != "cursor"), Does.Contain("pin:3"),
                "control: the tail attempted its publish.");
            Assert.That(PinsAfterTailCursor(leaf), Is.EqualTo(new[] { "pin:3" }),
                "THE assertion: an unacknowledged tail publish must not let the barrier elide.");
            Assert.That(elisions, Is.Empty, "a publishing barrier must never be counted as elided.");
        });
    }

    /// <summary>
    /// A tail publish that throws - a fault or a cancellation escaping the
    /// reporter - is contained by the tail and records nothing, so the barrier
    /// publishes.
    /// </summary>
    [TestCase(false)]
    [TestCase(true)]
    public async Task OnDeactivateAsync_publishes_the_frontier_pin_barrier_when_the_tail_publish_threw(bool cancelled)
    {
        var leaf = await CreateCleanlyDeactivatableLeafAsync();
        var thrown = false;
        leaf.OnPinFlush = () =>
        {
            if (thrown)
            {
                return;
            }

            thrown = true;
            throw cancelled
                ? new OperationCanceledException("tail publish cancelled")
                : new InvalidOperationException("tail publish faulted");
        };

        var elisions = new List<string?>();
        using (ListenForFrontierPinElisions(elisions))
        {
            await DeactivateFinalAdvanceLeafAsync(leaf, CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(thrown, Is.True, "control: the tail's publish threw.");
            Assert.That(PinsAfterTailCursor(leaf), Is.EqualTo(new[] { "pin:3" }),
                "THE assertion: a tail publish that threw must not let the barrier elide.");
            Assert.That(elisions, Is.Empty, "a publishing barrier must never be counted as elided.");
        });
    }

    /// <summary>
    /// Two clean deactivations, each counted exactly once: the counter moves
    /// once per elided barrier and never runs ahead of them.
    /// </summary>
    [Test]
    public async Task OnDeactivateAsync_counts_one_elision_per_elided_barrier()
    {
        var first = await CreateCleanlyDeactivatableLeafAsync();
        var second = await CreateCleanlyDeactivatableLeafAsync();

        var elisions = new List<string?>();
        using (ListenForFrontierPinElisions(elisions))
        {
            await DeactivateFinalAdvanceLeafAsync(first, CancellationToken.None);
            await DeactivateFinalAdvanceLeafAsync(second, CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(PinsAfterTailCursor(first), Is.Empty, "control: the first barrier elided.");
            Assert.That(PinsAfterTailCursor(second), Is.Empty, "control: the second barrier elided.");
            Assert.That(elisions, Has.Count.EqualTo(2), "one increment per elided barrier, never more.");
        });
    }
}
