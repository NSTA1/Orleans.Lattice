using System.Diagnostics.Metrics;
using System.Text;
using Orleans.Lattice;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Metric-emission coverage for the <c>orleans.lattice.backup.*</c> instruments
/// published on the dedicated <see cref="BackupMetrics.Meter"/>: a successful capture
/// records its duration histogram, advances the entries / captures counters, and
/// is reflected by the inventory count gauge; a denied capture and a denied
/// restore each increment their failure counter tagged with the phase and reason
/// the fault surfaced in.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupObservabilityTests
{
    private const string Source = "orders";

    private CaptureClusterFixture _capture = null!;
    private RestoreClusterFixture _restore = null!;

    [SetUp]
    public void SetUp()
    {
        BackupInventoryRegistry.Instance.Reset();
        _capture = new CaptureClusterFixture();
        _restore = new RestoreClusterFixture();
    }

    [TearDown]
    public async Task TearDown()
    {
        await _capture.DisposeAsync();
        await _restore.DisposeAsync();
    }

    // ---- Success-path instruments ---------------------------------------

    [Test]
    public async Task CaptureAsync_records_success_path_instruments()
    {
        await _capture.InitializeAsync();
        var source = _capture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        await source.SetAsync("k2", Bytes("v2"));
        await source.SetAsync("k3", Bytes("v3"));

        long captures = 0;
        long entriesProcessed = 0;
        var entriesHistogramRecorded = false;
        var durationRecorded = false;
        long inventoryCount = 0;

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (ReferenceEquals(instrument.Meter, BackupMetrics.Meter))
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, measurement, _, _) =>
        {
            switch (instrument.Name)
            {
                case "orleans.lattice.backup.captures":
                    Interlocked.Add(ref captures, measurement);
                    break;
                case "orleans.lattice.backup.entries_processed":
                    Interlocked.Add(ref entriesProcessed, measurement);
                    break;
                case "orleans.lattice.backup.entries":
                    entriesHistogramRecorded = true;
                    break;
                case "orleans.lattice.backup.inventory.count":
                    Interlocked.Exchange(ref inventoryCount, measurement);
                    break;
            }
        });
        listener.SetMeasurementEventCallback<double>((instrument, _, _, _) =>
        {
            if (instrument.Name == "orleans.lattice.backup.capture.duration")
            {
                durationRecorded = true;
            }
        });
        listener.Start();

        await _capture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("metered", BackupScopeSelector.WholeTree(Source)));

        // Sample the observable inventory gauge after the capture registered the
        // backup.
        listener.RecordObservableInstruments();
        listener.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(captures, Is.EqualTo(1), "captures counter advanced once");
            Assert.That(entriesProcessed, Is.EqualTo(3), "entries_processed counter advanced by the entry count");
            Assert.That(entriesHistogramRecorded, Is.True, "entries histogram recorded");
            Assert.That(durationRecorded, Is.True, "capture duration histogram recorded");
            Assert.That(inventoryCount, Is.GreaterThanOrEqualTo(1), "inventory count gauge reflects the new backup");
        });
    }

    // ---- Capture failure counter ----------------------------------------

    [Test]
    public async Task CaptureAsync_denied_permission_increments_capture_failure_counter_tagged_phase_and_reason()
    {
        await _capture.InitializeAsync();
        var source = _capture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));

        long failures = 0;
        string? phase = null;
        string? reason = null;

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (ReferenceEquals(instrument.Meter, BackupMetrics.Meter)
                && instrument.Name == "orleans.lattice.backup.capture.failures")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
        {
            Interlocked.Add(ref failures, measurement);
            foreach (var tag in tags)
            {
                if (tag.Key == LatticeBackupMetrics.TagPhase)
                {
                    phase = tag.Value as string;
                }
                else if (tag.Key == LatticeBackupMetrics.TagReason)
                {
                    reason = tag.Value as string;
                }
            }
        });
        listener.Start();

        var denying = new BackupAccessAuthorizer(
            new DenyingAccessGate("no backup grant"), membership: null);
        var gatedCapture = _capture.CreateCaptureServiceWith(denying);

        Assert.That(
            async () => await gatedCapture.CaptureAsync(
                new LatticeBackupCaptureRequest("denied", BackupScopeSelector.WholeTree(Source))),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());

        listener.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.EqualTo(1), "capture failure counter incremented");
            Assert.That(phase, Is.EqualTo(LatticeBackupMetrics.PhaseSnapshotOpen));
            Assert.That(reason, Is.EqualTo(LatticeBackupMetrics.ReasonPermissionDenied));
        });
    }

    // ---- Restore failure counter ----------------------------------------

    [Test]
    public async Task RestoreAsync_denied_permission_increments_restore_failure_counter_tagged_phase_and_reason()
    {
        await _restore.InitializeAsync();
        var source = _restore.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        var backup = await _restore.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("to-deny", BackupScopeSelector.WholeTree(Source)));

        long failures = 0;
        string? phase = null;
        string? reason = null;

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (ReferenceEquals(instrument.Meter, BackupMetrics.Meter)
                && instrument.Name == "orleans.lattice.backup.restore.failures")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
        {
            Interlocked.Add(ref failures, measurement);
            foreach (var tag in tags)
            {
                if (tag.Key == LatticeBackupMetrics.TagPhase)
                {
                    phase = tag.Value as string;
                }
                else if (tag.Key == LatticeBackupMetrics.TagReason)
                {
                    reason = tag.Value as string;
                }
            }
        });
        listener.Start();

        var denying = new BackupAccessAuthorizer(
            new DenyingAccessGate("no restore grant"), membership: null);
        var restore = _restore.CreateRestoreServiceWith(denying);

        Assert.That(
            async () => await restore.RestoreAsync(
                new LatticeRestoreRequest(backup.BackupId, "orders-denied")),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());

        listener.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.EqualTo(1), "restore failure counter incremented");
            Assert.That(phase, Is.EqualTo(LatticeBackupMetrics.PhaseRead));
            Assert.That(reason, Is.EqualTo(LatticeBackupMetrics.ReasonPermissionDenied));
        });
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>A minimal access gate that denies every request, driving the fail-closed path.</summary>
    private sealed class DenyingAccessGate(string reason) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Deny(reason));
    }
}
