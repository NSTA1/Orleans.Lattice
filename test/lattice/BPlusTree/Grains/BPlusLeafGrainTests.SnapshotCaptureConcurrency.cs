using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Cross-leaf snapshot-capture concurrency (issue #2696).
/// <para>
/// The distinction these tests exist to hold is that <b>the per-leaf
/// single-flight guard is not the quantity in question</b>. That guard is an
/// instance field on one activation, so it stops a leaf capturing twice at once
/// and is blind by construction to how many <i>different</i> leaves are
/// capturing against the one shared snapshot storage provider. The fixture
/// therefore drives two independent grains rather than one, because a
/// single-grain test cannot reach a depth above one: the second capture would be
/// turned away by the guard and counted as a decline, never crossing the attempt
/// boundary at all.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Builds a leaf that is eligible to capture and whose snapshot save can be
    /// suspended, so several leaves can be held inside the attempt boundary at
    /// the same instant.
    /// </summary>
    private static (BPlusLeafGrain Grain, TaskCompletionSource Entered, TaskCompletionSource Release)
        CreateLeafWithSuspendableCapture(string treeId)
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var (grain, state, snapshotStub, _) = CreateGrainForProactiveCapture(
            activationDecision: FallOffLogDecision.SnapshotPending,
            persistedCheckpoint: 12,
            walHead: 12);
        state.State.TreeId = treeId;
        SeedOneCaptureRow(grain);

        snapshotStub
            .SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(async _ =>
            {
                entered.TrySetResult();
                await release.Task;
                return LeafSnapshotSaveOutcome.Kept;
            });

        return (grain, entered, release);
    }

    /// <summary>
    /// Collects every value the concurrent-entries counter records for one tree.
    /// </summary>
    private static ConcurrentBag<long> CaptureConcurrentEntryObservations(
        string treeId,
        out MeterListener listener)
    {
        var values = new ConcurrentBag<long>();

        listener = MeterListening.StartForMeter(
            LatticeMetrics.Meter,
            new[] { LatticeMetrics.LeafSnapshotCaptureConcurrentEntriesName },
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree && (tag.Value as string) == treeId)
                    {
                        values.Add(value);
                        return;
                    }
                }
            }));

        return values;
    }

    /// <summary>
    /// Reads the peak gauge by forcing an observation, returning every
    /// measurement with its tenant tag.
    /// </summary>
    private static List<(int Value, string? Tenant)> ReadConcurrencyPeakGauge()
    {
        var observations = new List<(int Value, string? Tenant)>();

        using var listener = MeterListening.StartForMeter(
            LatticeMetrics.Meter,
            new[] { LatticeMetrics.LeafSnapshotCaptureConcurrencyPeakGaugeName },
            l => l.SetMeasurementEventCallback<int>((_, value, tags, _) =>
            {
                string? tenant = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeTenantLabel.TagTenant)
                    {
                        tenant = tag.Value as string;
                    }
                }

                observations.Add((value, tenant));
            }));

        listener.RecordObservableInstruments();
        return observations;
    }

    /// <summary>
    /// Registers the peak gauge the way a real host does, through
    /// <c>AddLattice</c>, rather than by calling the internal registrar.
    /// <para>
    /// This is deliberate and is what makes the gauge assertions in this fixture
    /// falsifiable. Calling <c>EnsureGaugeRegistered</c> directly would test the
    /// census in isolation and stay green against a host that had stopped
    /// registering the gauge - which is the defect worth catching, because a
    /// gauge nothing registers publishes no series, and a missing series would
    /// then mean "no detector" exactly where the operator needs it to mean
    /// "measured none". Routing through <c>AddLattice</c> means deleting that one
    /// wiring line leaves nothing in the process registering the gauge, so these
    /// tests redden.
    /// </para>
    /// </summary>
    private static void RegisterConcurrencyPeakGaugeThroughHostBuild()
        => Substitute.For<ISiloBuilder>().AddLattice((_, _) => { });

    /// <summary>
    /// The measurement issue #2696 asks for: two <b>different</b> leaves inside
    /// the capture attempt boundary at the same instant.
    /// <para>
    /// The peak is read <em>after</em> both captures have completed, which makes
    /// one assertion carry two claims. It proves the spike was recorded at all,
    /// and it proves the value did not decay when the captures drained - the
    /// monotonicity that is the entire reason this is a high-water mark rather
    /// than an instantaneous gauge. An instantaneous gauge read at this point
    /// would report zero, which is precisely the miss PR #2723 objected to when
    /// it deferred the metric.
    /// </para>
    /// </summary>
    [Test]
    public async Task Two_leaves_capturing_at_once_raise_the_cross_leaf_concurrency_peak()
    {
        LeafSnapshotCaptureConcurrencyCensus.Shared.ResetForTesting();
        RegisterConcurrencyPeakGaugeThroughHostBuild();

        var (first, firstEntered, firstRelease) =
            CreateLeafWithSuspendableCapture(UniqueSnapshotCaptureTree());
        var (second, secondEntered, secondRelease) =
            CreateLeafWithSuspendableCapture(UniqueSnapshotCaptureTree());

        var firstActivation = LeafActivationHarness.ActivateAsync(first, CancellationToken.None);
        await firstEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var secondActivation = LeafActivationHarness.ActivateAsync(second, CancellationToken.None);
        await secondEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Non-vacuity precondition. If either save had already returned, the two
        // captures would not overlap and a peak of 2 could never be produced, so
        // a later green would prove nothing about concurrency.
        Assert.Multiple(() =>
        {
            Assert.That(firstActivation.IsCompleted, Is.False,
                "precondition: the first capture must still be inside the attempt boundary, "
                + "otherwise the two captures never overlap and this test cannot observe a peak above one.");
            Assert.That(secondActivation.IsCompleted, Is.False,
                "precondition: the second capture must still be inside the attempt boundary.");
            Assert.That(LeafSnapshotCaptureConcurrencyCensus.Shared.InFlight, Is.EqualTo(2),
                "precondition: both leaves must be counted in flight at once. A value of 1 here "
                + "would mean the second capture was turned away rather than admitted, which is "
                + "the per-leaf behaviour this instrument exists to see past.");
        });

        firstRelease.TrySetResult();
        secondRelease.TrySetResult();
        await firstActivation;
        await secondActivation;

        var observations = ReadConcurrencyPeakGauge();

        Assert.Multiple(() =>
        {
            Assert.That(LeafSnapshotCaptureConcurrencyCensus.Shared.InFlight, Is.EqualTo(0),
                "every capture released its slot, so the live depth must return to zero. A "
                + "non-zero value here means a leaked slot, which would ratchet the peak on "
                + "captures that are no longer running.");
            Assert.That(observations, Has.Count.EqualTo(1),
                "the peak is a single silo-wide series, so exactly one measurement is expected.");
            Assert.That(observations[0].Value, Is.EqualTo(2),
                "the gauge is read after both captures drained, so a value of 2 proves both that "
                + "the cross-leaf spike was recorded and that it did not decay. Zero here is the "
                + "instantaneous-gauge failure mode: correct at the instant of reading and useless.");
        });
    }

    /// <summary>
    /// The gauge must exist and report before any capture has ever run, so that
    /// a scraped zero means "measured none" rather than "no detector".
    /// <para>
    /// This epic has three times read an absent series as evidence of absent
    /// behaviour. An instrument that only materialises on first use cannot
    /// support that reading at all, so the priming is asserted rather than
    /// assumed.
    /// </para>
    /// </summary>
    [Test]
    public void The_concurrency_peak_gauge_is_zero_primed_before_any_capture()
    {
        LeafSnapshotCaptureConcurrencyCensus.Shared.ResetForTesting();
        RegisterConcurrencyPeakGaugeThroughHostBuild();

        var observations = ReadConcurrencyPeakGauge();

        Assert.Multiple(() =>
        {
            Assert.That(observations, Has.Count.EqualTo(1),
                "a silo that has never captured must still publish the series, otherwise its "
                + "absence is indistinguishable from an instrument that was never registered. "
                + "The gauge here was registered only by AddLattice, so this also holds the "
                + "host-build wiring: delete that call and nothing registers the gauge.");
            Assert.That(observations[0].Value, Is.EqualTo(0),
                "and it must report zero rather than nothing.");
            Assert.That(observations[0].Tenant, Is.EqualTo(LatticeTenantLabel.PlatformTenant),
                "the peak is a maximum across every leaf on the silo, so it spans tenants and is "
                + "emitted with the platform sentinel. A derived tenant here would be a claim the "
                + "measurement cannot support.");
        });
    }

    /// <summary>
    /// A capture that runs alone is recorded as an uncontended entry, which is
    /// what gives the counter a zero-primed series per tree.
    /// </summary>
    [Test]
    public async Task A_capture_that_runs_alone_is_counted_as_an_uncontended_entry()
    {
        LeafSnapshotCaptureConcurrencyCensus.Shared.ResetForTesting();

        var treeId = UniqueSnapshotCaptureTree();
        var (grain, _, release) = CreateLeafWithSuspendableCapture(treeId);
        release.TrySetResult();

        var values = CaptureConcurrentEntryObservations(treeId, out var listener);
        using (listener)
        {
            await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(values, Is.Not.Empty,
                "a capture that entered alone must still record on this counter. Without the "
                + "zero-add the tree would have no series at all, and a reader could not tell "
                + "'measured, never contended' from 'never reached'.");
            Assert.That(values, Is.All.EqualTo(0L),
                "and it must record zero, because nothing else was in flight.");
        });
    }

    /// <summary>
    /// A capture admitted while another leaf is already capturing is recorded as
    /// a contended entry. This is the half a peak alone cannot supply: a peak of
    /// two is produced both by one transient burst and by sustained two-deep
    /// pressure, and the counter is what separates them.
    /// </summary>
    [Test]
    public async Task A_capture_admitted_while_another_leaf_captures_is_counted_as_contended()
    {
        LeafSnapshotCaptureConcurrencyCensus.Shared.ResetForTesting();

        var holderTree = UniqueSnapshotCaptureTree();
        var contenderTree = UniqueSnapshotCaptureTree();

        var (holder, holderEntered, holderRelease) = CreateLeafWithSuspendableCapture(holderTree);
        var (contender, _, contenderRelease) = CreateLeafWithSuspendableCapture(contenderTree);
        contenderRelease.TrySetResult();

        var values = CaptureConcurrentEntryObservations(contenderTree, out var listener);
        using (listener)
        {
            var holderActivation = LeafActivationHarness.ActivateAsync(holder, CancellationToken.None);
            await holderEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.That(holderActivation.IsCompleted, Is.False,
                "precondition: the holding capture must still be in flight, otherwise the second "
                + "leaf enters an empty silo and the contended case is never exercised.");

            await LeafActivationHarness.ActivateAsync(contender, CancellationToken.None);

            holderRelease.TrySetResult();
            await holderActivation;
        }

        Assert.That(values, Does.Contain(1L),
            "a capture that crossed the attempt boundary while another leaf was capturing must be "
            + "counted as contended. Recording only zero here would leave the frequency of "
            + "contention unmeasured, so a peak could not be read as transient or sustained.");
    }

    /// <summary>
    /// The concurrency slot is released on the throwing path too.
    /// <para>
    /// This matters more than an ordinary cleanup assertion because the failure
    /// is silent and cumulative: a leaked slot does not throw, it inflates every
    /// subsequent depth reading, and because the peak never falls the inflation
    /// is permanent. The instrument would then report contention that never
    /// happened, which is worse than reporting none.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_capture_that_throws_still_releases_its_concurrency_slot()
    {
        LeafSnapshotCaptureConcurrencyCensus.Shared.ResetForTesting();

        var (grain, state, snapshotStub, _) = CreateGrainForProactiveCapture(
            activationDecision: FallOffLogDecision.SnapshotPending,
            persistedCheckpoint: 12,
            walHead: 12);
        state.State.TreeId = UniqueSnapshotCaptureTree();
        SeedOneCaptureRow(grain);

        snapshotStub
            .SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("snapshot store unavailable"));

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(LeafSnapshotCaptureConcurrencyCensus.Shared.InFlight, Is.EqualTo(0),
                "a capture that threw must still release its slot, or the live depth drifts "
                + "upward permanently and the peak ratchets on captures that are not running.");
            Assert.That(LeafSnapshotCaptureConcurrencyCensus.Shared.Peak, Is.EqualTo(1),
                "and the failed capture is still a capture that ran, so it must have raised the "
                + "peak to one on its way through.");
        });
    }
}
