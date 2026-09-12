using System.Collections.Concurrent;
using System.Text;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the leaf-snapshot capture observation added for issue #2696.
/// <para>
/// Before this, the capture path carried <b>no instrument at all</b>. The write
/// half was untimed and uncounted, and
/// <c>TryCaptureSnapshotForAdvisoryAsync</c> caught every exception and only
/// logged it. The single capture-derived series on the metrics endpoint,
/// <c>orleans.lattice.storage.snapshot_bytes</c>, is fed only <b>after</b> a
/// successful save, so it reads <c>0</c> both when every capture is failing and
/// when no capture has ever been attempted.
/// </para>
/// <para>
/// Those are opposite operational states - a broken storage provider versus a
/// correctly idle deployment - and nothing exported could tell them apart. That
/// is what these tests pin, and it is why the central fixture below runs
/// <b>two arms</b> and asserts they produce <em>different</em> readings. A
/// fixture that only asserted "a failed capture increments the failure counter"
/// would pass just as well against an instrument that incremented on every
/// activation, which would not have fixed the defect at all.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static string UniqueSnapshotCaptureTree() => $"snapshot-capture-{Guid.NewGuid():N}";

    /// <summary>
    /// Returns the outcome tag of a measurement if - and only if - it carries
    /// this test's own tree tag, so a concurrently-running fixture sharing
    /// these process-wide instruments can neither satisfy nor weaken an
    /// assertion here. Returns <c>null</c> for a measurement belonging to any
    /// other tree.
    /// </summary>
    private static string? OutcomeForTree(
        ReadOnlySpan<KeyValuePair<string, object?>> tags,
        string treeId)
    {
        string? outcome = null;
        var matchedTree = false;

        foreach (var tag in tags)
        {
            if (tag.Key == LatticeMetrics.TagTree && (tag.Value as string) == treeId)
            {
                matchedTree = true;
            }
            else if (tag.Key == LatticeMetrics.TagOutcome)
            {
                outcome = tag.Value as string;
            }
        }

        return matchedTree ? outcome : null;
    }

    /// <summary>
    /// Starts a listener over both capture instruments at once, filtered to
    /// <paramref name="treeId"/>. Both are collected by the same listener
    /// because the claim under test is that they share a population: an
    /// attempt that is counted must also be timed, and a decline that is not
    /// counted must also not be timed.
    /// <para>
    /// The instruments are selected by their <c>const</c> name fields rather
    /// than by dereferencing the static instrument fields, and the meter is
    /// read at the call site, which is what forces
    /// <see cref="LatticeMetrics"/>'s type initialiser to complete before the
    /// listener starts. A listener that triggered that initialiser re-entrantly
    /// would silently capture nothing and read as a product defect.
    /// </para>
    /// </summary>
    private static (ConcurrentBag<string> Outcomes, ConcurrentBag<double> Durations)
        CaptureSnapshotCaptureObservations(string treeId, out IDisposable listener)
    {
        var outcomes = new ConcurrentBag<string>();
        var durations = new ConcurrentBag<double>();

        listener = MeterListening.StartForMeter(
            LatticeMetrics.Meter,
            new[]
            {
                LatticeMetrics.LeafSnapshotCapturesName,
                LatticeMetrics.LeafSnapshotCaptureDurationName,
            },
            l =>
            {
                l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
                {
                    if (OutcomeForTree(tags, treeId) is { } outcome)
                    {
                        outcomes.Add(outcome);
                    }
                });
                l.SetMeasurementEventCallback<double>((_, value, tags, _) =>
                {
                    if (OutcomeForTree(tags, treeId) is not null)
                    {
                        durations.Add(value);
                    }
                });
            });

        return (outcomes, durations);
    }

    private static void SeedOneCaptureRow(BPlusLeafGrain grain) =>
        grain.EntriesForTest["k"] = new LwwValue<byte[]>
        {
            Value = Encoding.UTF8.GetBytes("v"),
            Timestamp = HybridLogicalClock.Zero,
        };

    /// <summary>
    /// The central fixture, and the one that pins the actual defect. Two arms
    /// run the same activation path and differ only in whether a capture is
    /// attempted; the assertion is that the exported readings <b>differ</b>.
    /// <para>
    /// Arm A attempts a capture whose storage call throws - the failure is
    /// swallowed by the advisory handler exactly as before, so the grain still
    /// activates. Arm B never attempts one. On the pre-fix code both arms
    /// exported nothing whatsoever and were indistinguishable; that is the
    /// state this test fails against.
    /// </para>
    /// <para>
    /// The two NSubstitute controls are load-bearing rather than decorative:
    /// they establish that the arms really did differ in what they attempted,
    /// so a failure of the metric assertions cannot be explained away by the
    /// arms having accidentally behaved identically.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_failed_capture_is_distinguishable_from_a_capture_that_never_ran()
    {
        // ---- Arm A: a capture is attempted and the storage call throws. ----
        var failedTree = UniqueSnapshotCaptureTree();
        var (failedGrain, failedState, failedStub, _) = CreateGrainForProactiveCapture(
            activationDecision: FallOffLogDecision.SnapshotPending,
            persistedCheckpoint: 12,
            walHead: 12);
        failedState.State.TreeId = failedTree;
        failedStub
            .SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("simulated snapshot storage failure"));
        SeedOneCaptureRow(failedGrain);

        var (failedOutcomes, failedDurations) =
            CaptureSnapshotCaptureObservations(failedTree, out var failedListener);
        using (failedListener)
        {
            // Must not throw: the advisory handler swallows the failure so a
            // broken snapshot store cannot block a leaf coming online. This
            // fix makes the failure observable, it does not make it fatal.
            await ((IGrainBase)failedGrain).OnActivateAsync(CancellationToken.None);
        }

        // ---- Arm B: no capture is attempted at all. ----
        var idleTree = UniqueSnapshotCaptureTree();
        var (idleGrain, idleState, idleStub, _) = CreateGrainForProactiveCapture(
            activationDecision: FallOffLogDecision.TailReplay,
            persistedCheckpoint: 5,
            walHead: 5);
        idleState.State.TreeId = idleTree;
        SeedOneCaptureRow(idleGrain);

        var (idleOutcomes, idleDurations) =
            CaptureSnapshotCaptureObservations(idleTree, out var idleListener);
        using (idleListener)
        {
            await ((IGrainBase)idleGrain).OnActivateAsync(CancellationToken.None);
        }

        // Controls: the arms genuinely differed in what they attempted.
        await failedStub.Received(1).SaveAsync(
            Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());
        await idleStub.DidNotReceive().SaveAsync(
            Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());

        Assert.Multiple(() =>
        {
            Assert.That(failedOutcomes, Is.EquivalentTo(new[] { "failed" }),
                "a capture attempt whose storage call throws must be counted exactly once, "
                + "as failed. The advisory handler logs it and previously incremented "
                + "nothing, so a deployment in which every capture failed exported total "
                + "silence.");
            Assert.That(failedDurations, Has.Count.EqualTo(1),
                "a failed attempt must still be timed: how long a capture took before "
                + "failing separates a fast rejection from a provider timeout, and those "
                + "call for different operator responses.");

            Assert.That(idleOutcomes, Is.Empty,
                "a leaf that never attempts a capture must contribute no sample, so that "
                + "the absence of samples positively means 'never attempted' rather than "
                + "merely 'nothing recorded'.");
            Assert.That(idleDurations, Is.Empty,
                "and must not be timed either.");

            // The claim this fixture exists for.
            Assert.That(failedOutcomes.Count, Is.Not.EqualTo(idleOutcomes.Count),
                "THE discrimination: 'every capture failed' and 'capture never ran' must "
                + "produce different readings. Before issue #2696 both exported nothing "
                + "and an operator could not tell a broken storage provider from a "
                + "correctly idle deployment.");
        });
    }

    /// <summary>
    /// The success arm of the same discrimination. Without this, a counter that
    /// only ever incremented on failure would satisfy the fixture above while
    /// still leaving "captures are working" unreadable - and "attempted" is the
    /// sum across outcomes, so the succeeded tag is what makes that sum mean
    /// anything.
    /// </summary>
    [Test]
    public async Task A_successful_capture_is_counted_as_succeeded_and_timed()
    {
        var treeId = UniqueSnapshotCaptureTree();
        var (grain, state, snapshotStub, _) = CreateGrainForProactiveCapture(
            activationDecision: FallOffLogDecision.SnapshotPending,
            persistedCheckpoint: 12,
            walHead: 12);
        state.State.TreeId = treeId;
        SeedOneCaptureRow(grain);

        var (outcomes, durations) = CaptureSnapshotCaptureObservations(treeId, out var listener);
        using (listener)
        {
            await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        }

        await snapshotStub.Received(1).SaveAsync(
            Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());

        Assert.Multiple(() =>
        {
            Assert.That(outcomes, Is.EquivalentTo(new[] { "succeeded" }),
                "a capture that reaches the store and advances durable coverage must be "
                + "counted as succeeded.");
            Assert.That(durations, Has.Count.EqualTo(1),
                "the counter and the duration share a population: every counted attempt "
                + "is also timed.");
            Assert.That(durations.Single(), Is.GreaterThanOrEqualTo(0d),
                "a duration sample must be a real elapsed measurement.");
        });
    }

    /// <summary>
    /// Pins the <b>timing boundary</b>, which is a separate claim from the
    /// discrimination above and cannot be demonstrated by it.
    /// <para>
    /// Here the capture method is genuinely entered - unlike Arm B above, which
    /// never called it - and declines at the eligibility gate because the leaf
    /// holds no checkpoint and no live rows. Instrumenting from method entry
    /// rather than from the single-flight boundary would record a near-zero
    /// sample here. That matters because the gates are taken far more often
    /// than a capture runs, so those no-ops would dominate the sample count and
    /// drag the interval mean toward zero: an instrument reporting "captures
    /// are fast" precisely when none are happening.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_capture_that_declines_at_the_eligibility_gate_contributes_no_sample()
    {
        var treeId = UniqueSnapshotCaptureTree();
        var (grain, state, snapshotStub, _) = CreateGrainForProactiveCapture(
            activationDecision: FallOffLogDecision.TailReplay,
            persistedCheckpoint: -1,
            walHead: -1);
        state.State.TreeId = treeId;
        state.State.ProjectionCheckpointOffset = -1;

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        // Deliberately seed NO rows, so the leaf has neither a checkpoint nor
        // live data and the gate declines. Call the public capture seam
        // directly, so the method really is entered.
        var (outcomes, durations) = CaptureSnapshotCaptureObservations(treeId, out var listener);
        using (listener)
        {
            await ((IBPlusLeafGrain)grain).CaptureSnapshotAsync();
        }

        await snapshotStub.DidNotReceive().SaveAsync(
            Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());

        Assert.Multiple(() =>
        {
            Assert.That(outcomes, Is.Empty,
                "a decline is not an attempt and must not be counted as one, or the "
                + "attempt total would be dominated by leaves that did nothing.");
            Assert.That(durations, Is.Empty,
                "and must not contribute a near-zero duration sample, which would drag "
                + "the interval mean toward zero and hide a real capture slowdown.");
        });
    }

    /// <summary>
    /// A capture dropped on a deactivation deadline is <c>abandoned</c>, not
    /// <c>failed</c>. Without this split, a fleet-wide shutdown - thousands of
    /// leaves going idle together, each abandoning its capture by design
    /// (issue #1965) - would present as a mass storage-provider outage, which
    /// is the single most misleading reading this counter could produce.
    /// <para>
    /// The token is cancelled from <em>inside</em> the store call rather than
    /// beforehand: an already-cancelled token short-circuits at the earlier
    /// checkpoint flush, so the capture would never be reached and the test
    /// would pass vacuously.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_capture_abandoned_on_a_cancelled_token_is_counted_as_abandoned_not_failed()
    {
        var treeId = UniqueSnapshotCaptureTree();
        using var deadline = new CancellationTokenSource();

        var stub = Substitute.For<ILeafSnapshotStorageGrain>();
        stub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(null));
        stub.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                // Orleans' deactivation deadline fires while the blob write is
                // in flight - the shape the real overrun takes.
                deadline.Cancel();
                return Task.FromException(new OperationCanceledException(deadline.Token));
            });

        var (leaf, _, _, _, _) =
            CreateLeafWithDurablePinAndSnapshotStore(treeId: treeId, snapshotStub: stub);

        // Latch the deactivation capture gate, or no capture runs and the
        // assertions below would pass trivially.
        await CheckpointLeafAsync(leaf, "k1", hlcPhysical: 100, offset: 1);

        var (outcomes, durations) = CaptureSnapshotCaptureObservations(treeId, out var listener);
        using (listener)
        {
            await ((IGrainBase)leaf).OnDeactivateAsync(
                new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
                deadline.Token);
        }

        Assert.Multiple(() =>
        {
            Assert.That(outcomes, Does.Contain("abandoned"),
                "a capture dropped because the caller's deactivation token was cancelled "
                + "must be counted as abandoned.");
            Assert.That(outcomes, Does.Not.Contain("failed"),
                "and must NOT be counted as failed: a graceful shutdown and a broken "
                + "storage provider would otherwise be the same reading, which is the "
                + "conflation this tag split exists to prevent.");
            Assert.That(durations, Is.Not.Empty,
                "an abandoned attempt is still an attempt and is still timed.");
        });
    }
}
