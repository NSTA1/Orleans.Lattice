using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #3921: the WAL replay permit gate could not
/// tell a CPU-bound silo (permits scarce, raising the ceiling helps) from a
/// store-bound one (permits held too long, raising the ceiling makes it worse).
/// <para>
/// Three defects, each pinned end to end through a real activation or
/// starvation drive rather than through the helpers in isolation:
/// </para>
/// <list type="bullet">
///   <item>no permit hold time or service rate was exported;</item>
///   <item>a <c>replay_permit_admission</c> refusal carried no attribution to
///   the arm of the predicate that fired;</item>
///   <item>a refusal from the no-progress arm reported a <c>0 ms</c> smoothed
///   wait as evidence of "not draining" and pointed at the queue-depth option,
///   which does nothing for a hold-time condition.</item>
/// </list>
/// <para>
/// Instruments are resolved by their published names and tags by their literal
/// keys and values, so the fixture compiles against a build without the fix and
/// fails there, rather than failing to build. Every test perturbs the
/// process-wide gate, so each is <see cref="NonParallelizableAttribute"/> and
/// restores what it seeded.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string PermitHoldInstrumentName = "orleans.lattice.wal.replay.permit_hold";
    private const string PermitsServedInstrumentName = "orleans.lattice.wal.replay.permits_served";

    /// <summary>
    /// Records every saturation refusal for <paramref name="treeId"/>, with its tags.
    /// </summary>
    private static MeterListener ListenForSaturationRefusals(
        string treeId, ConcurrentBag<KeyValuePair<string, object?>[]> sink)
        => MeterListening.StartForInstrument(
            LatticeMetrics.SaturationRefusals,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                var copy = tags.ToArray();
                if (copy.Any(t => t.Key == LatticeMetrics.TagTree && (string?)t.Value == treeId))
                    sink.Add(copy);
            }));

    /// <summary>
    /// Records every permit-hold sample and every served-permit increment, by
    /// instrument name, while the listener is open.
    /// </summary>
    private static MeterListener ListenForPermitHoldAndService(
        ConcurrentBag<(string Instrument, double Value, KeyValuePair<string, object?>[] Tags)> sink)
        => MeterListening.StartForMeter(
            LatticeMetrics.Meter,
            [PermitHoldInstrumentName, PermitsServedInstrumentName],
            l =>
            {
                l.SetMeasurementEventCallback<double>(
                    (instrument, value, tags, _) => sink.Add((instrument.Name, value, tags.ToArray())));
                l.SetMeasurementEventCallback<long>(
                    (instrument, value, tags, _) => sink.Add((instrument.Name, value, tags.ToArray())));
            });

    private static string? ArmOf(KeyValuePair<string, object?>[] tags)
        => tags.Where(t => t.Key == "arm").Select(t => (string?)t.Value).FirstOrDefault();

    /// <summary>
    /// Seeds a queue past the interactive depth bound whose permits have not come
    /// back: the smoothed wait is zero, and nothing has acquired for longer than
    /// the bound. That is the measured shape of issue #3921 - 227 of 249 refusals
    /// reported a 0 ms smoothed wait.
    /// </summary>
    private static async Task SeedNoProgressQueueAsync()
    {
        var options = new LatticeOptions();
        await SeedAdmittedWaitersAsync(c => c * options.WalReplayPermitQueueDepthPerPermit);
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
            TimeSpan.Zero,
            sinceLastProgress: options.WalReplayPermitMaxQueueWait + TimeSpan.FromSeconds(2));
    }

    [Test]
    [NonParallelizable]
    public async Task A_no_progress_refusal_names_the_stalled_release_and_not_the_queue_depth_option()
    {
        await SeedNoProgressQueueAsync();

        LatticeSaturatedException? refusal;
        try
        {
            var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
                preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0);
            state.State.TreeId = UniqueReplayPermitTree();

            refusal = Assert.ThrowsAsync<LatticeSaturatedException>(
                async () => await LeafActivationHarness.ActivateAsync(
                    (IGrainBase)grain, CancellationToken.None),
                "a queue past its bound whose permits are not coming back must still be refused");
        }
        finally
        {
            ClearSeededAdmittedWaiters();
        }

        var message = refusal!.Message;
        Assert.Multiple(() =>
        {
            Assert.That(message, Does.Match(@"no permit has been released to the queue in \d+ s"),
                "the refusal must name the condition the no-progress arm observed: permits already "
                + "issued are not coming back");

            Assert.That(message, Does.Not.Contain("smoothed queue wait is 0 ms"),
                "a 0 ms smoothed wait is evidence the queue is NOT slow, so quoting it as the reason "
                + "for 'not draining' contradicts itself");

            Assert.That(message, Does.Not.Contain(nameof(LatticeOptions.WalReplayPermitQueueDepthPerPermit)),
                "a queue-depth option does nothing for a hold-time condition; pointing at it sent the "
                + "issue #3921 investigation the wrong way twice");

            Assert.That(message, Does.Contain(nameof(LatticeOptions.WalMaterialiserMaxConcurrentReplays)),
                "the message must warn that raising the ceiling on a store-bound silo makes it worse");

            Assert.That(message, Does.Contain("store"),
                "the message must point at replay and store throughput");
        });
    }

    [TestCase("no_progress")]
    [TestCase("wait_exceeded")]
    [NonParallelizable]
    public async Task A_replay_admission_refusal_is_tagged_with_the_arm_that_fired(string expectedArm)
    {
        var options = new LatticeOptions();
        if (expectedArm == "no_progress")
        {
            await SeedNoProgressQueueAsync();
        }
        else
        {
            // SeedAdmittedWaitersAsync seeds a fresh mean equal to the bound, with
            // progress just now, so only the smoothed-wait arm can fire.
            await SeedAdmittedWaitersAsync(c => c * options.WalReplayPermitQueueDepthPerPermit);
        }

        var treeId = UniqueReplayPermitTree();
        var refusals = new ConcurrentBag<KeyValuePair<string, object?>[]>();
        try
        {
            var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
                preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0);
            state.State.TreeId = treeId;

            using (ListenForSaturationRefusals(treeId, refusals))
            {
                Assert.ThrowsAsync<LatticeSaturatedException>(
                    async () => await LeafActivationHarness.ActivateAsync(
                        (IGrainBase)grain, CancellationToken.None));
            }
        }
        finally
        {
            ClearSeededAdmittedWaiters();
        }

        Assert.That(refusals, Has.Count.EqualTo(1),
            "instrument validation: exactly one refusal must have been recorded for this tree");

        var tags = refusals.Single();
        Assert.Multiple(() =>
        {
            Assert.That(
                tags.Any(t => t.Key == LatticeMetrics.TagSaturationSource
                    && (string?)t.Value == "replay_permit_admission"),
                Is.True,
                "the refusal must still be attributed to its source");

            Assert.That(ArmOf(tags), Is.EqualTo(expectedArm),
                "the refusal must name the arm that fired: the two arms have opposite remedies, and "
                + "without the tag the only way to separate them was to parse exception text");
        });
    }

    [TestCase("no_progress")]
    [TestCase("wait_exceeded")]
    [NonParallelizable]
    public async Task The_refusal_summary_line_names_the_arm_of_the_most_recent_refusal(string expectedArm)
    {
        var options = new LatticeOptions();
        if (expectedArm == "no_progress")
            await SeedNoProgressQueueAsync();
        else
            await SeedAdmittedWaitersAsync(c => c * options.WalReplayPermitQueueDepthPerPermit);
        BPlusLeafGrain.ResetReplayAdmissionRefusalLogForTest();

        var logs = new Orleans.Lattice.Tests.Fakes.RecordingLoggerFactory();
        try
        {
            var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
                preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0, loggerFactory: logs);
            state.State.TreeId = UniqueReplayPermitTree();

            Assert.ThrowsAsync<LatticeSaturatedException>(
                async () => await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None));
        }
        finally
        {
            ClearSeededAdmittedWaiters();
            BPlusLeafGrain.ResetReplayAdmissionRefusalLogForTest();
        }

        var summaries = logs.Warnings
            .Where(e => e.Message.StartsWith("WAL replay admission refused", StringComparison.Ordinal))
            .ToArray();
        Assert.That(summaries, Has.Length.EqualTo(1), "instrument validation: one summary line");
        Assert.That(summaries[0].Value("Arm"), Is.EqualTo(expectedArm),
            "the aggregated line replaced the per-refusal exception text, so it must carry the arm the "
            + "exception text no longer delivers - the two arms have opposite remedies");
    }

    [Test]
    [NonParallelizable]
    public async Task A_refused_starvation_drive_is_tagged_with_the_gc_share_arm()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        var wal = new GrowingWal();
        var treeId = UniqueStarvationDriveTree();
        var (grain, _, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator,
            treeId: treeId,
            persistedCheckpoint: -1,
            starvationDriveBudget: TestStarvationDriveBudget,
            maxConcurrentReplays: 1);
        await ActivateAsync(grain);
        wal.GrowTo(3);

        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
        Assert.That(gate.Wait(0), Is.True, "instrument validation: the test must hold the only permit");

        var refusals = new ConcurrentBag<KeyValuePair<string, object?>[]>();
        try
        {
            using (ListenForSaturationRefusals(treeId, refusals))
            {
                var verdict = await grain.DriveStarvedCheckpointAsync();
                Assert.That(verdict, Is.EqualTo(LeafStarvationDriveOutcome.AdmissionRefused),
                    "instrument validation: the drive must have been refused");
            }
        }
        finally
        {
            gate.Release();
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }

        Assert.That(refusals, Has.Count.EqualTo(1));
        Assert.That(ArmOf(refusals.Single()), Is.EqualTo("gc_share"),
            "a starvation drive refused for want of a GC slot is a bounded background drive told to "
            + "try later, and must not be read as a foreground admission refusal");
    }

    [Test]
    [NonParallelizable]
    public async Task A_clean_activation_replay_records_its_permit_hold_and_one_served_permit()
    {
        var gate = await QuiescentReplayGateAsync();
        var samples = new ConcurrentBag<(string Instrument, double Value, KeyValuePair<string, object?>[] Tags)>();

        var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0);
        var treeId = UniqueReplayPermitTree();
        state.State.TreeId = treeId;

        using (ListenForPermitHoldAndService(samples))
        {
            await LeafActivationHarness.ActivateAsync((IGrainBase)grain, CancellationToken.None);
        }

        var holds = samples
            .Where(s => s.Instrument == PermitHoldInstrumentName
                && s.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (string?)t.Value == treeId))
            .ToArray();
        var served = samples.Where(s => s.Instrument == PermitsServedInstrumentName).Sum(s => s.Value);

        Assert.Multiple(() =>
        {
            Assert.That(holds, Has.Length.EqualTo(1),
                "an admitted replay must record exactly one permit-hold sample for its tree; without "
                + "it nothing separates a CPU-bound gate from a store-bound one");
            Assert.That(holds.Select(h => h.Value), Has.All.GreaterThanOrEqualTo(0d));
            Assert.That(
                holds.All(h => h.Tags.Any(t => t.Key == LatticeTenantLabel.TagTenant)),
                Is.True,
                "the derived tenant dimension must be present on every emission site");
            Assert.That(served, Is.GreaterThanOrEqualTo(1d),
                "the end of the hold must be counted on the gate's service count, whose rate is the "
                + "service rate");
            Assert.That(gate.CurrentCount, Is.EqualTo(BPlusLeafGrain.ReplayConcurrencyCeilingForTest),
                "recording the hold must not have kept the permit");
        });
    }

    [Test]
    [NonParallelizable]
    public async Task A_starvation_drive_records_its_permit_hold()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        var wal = new GrowingWal();
        var treeId = UniqueStarvationDriveTree();
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator,
            treeId: treeId,
            persistedCheckpoint: -1,
            starvationDriveBudget: TimeSpan.FromSeconds(10),
            maxConcurrentReplays: 2);
        await ActivateAsync(grain);
        wal.GrowTo(3);

        var samples = new ConcurrentBag<(string Instrument, double Value, KeyValuePair<string, object?>[] Tags)>();
        try
        {
            using (ListenForPermitHoldAndService(samples))
            {
                await grain.DriveStarvedCheckpointAsync();
            }

            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(3),
                "instrument validation: the drive must actually have replayed under its permit");
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }

        var holds = samples
            .Where(s => s.Instrument == PermitHoldInstrumentName
                && s.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (string?)t.Value == treeId))
            .ToArray();

        Assert.That(holds, Has.Length.EqualTo(1),
            "a starvation drive takes the same permit for the same work as an activation replay, so "
            + "its hold is part of the gate's service and must be recorded");
    }

    [Test]
    [NonParallelizable]
    public async Task Sizing_the_replay_gate_zero_primes_the_served_permit_count()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        var samples = new ConcurrentBag<(string Instrument, double Value, KeyValuePair<string, object?>[] Tags)>();

        using (ListenForPermitHoldAndService(samples))
        {
            await ActivateWithCleanReplayAsync();
        }

        var served = samples.Where(s => s.Instrument == PermitsServedInstrumentName).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(BPlusLeafGrain.ReplayConcurrencyCeilingForTest, Is.GreaterThan(0),
                "instrument validation: the activation must have sized the gate");
            Assert.That(served.Select(s => s.Value), Does.Contain(0d),
                "sizing the gate must mint the served series at zero, so a gate that has stopped "
                + "serving reads as a measured flat line rather than as a build without the instrument");
            Assert.That(
                served.All(s => s.Tags.Any(t => t.Key == LatticeTenantLabel.TagTenant)
                    && s.Tags.All(t => t.Key != LatticeMetrics.TagTree)),
                Is.True,
                "the service count is a property of the process-wide gate: platform tenant only, no tree");
        });
    }
}
