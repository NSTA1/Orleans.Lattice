namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit coverage for the cross-cluster backup-sink sharing model: the
/// <see cref="BackupSinkSharingReport"/> value type and its guards, the inert
/// default <see cref="NoBackupSinkSharingProbe"/> a single-cluster host resolves,
/// the new <see cref="LatticeBackupOptions"/> knobs and their validation, and the
/// backwards-compatible defaults on <see cref="BackupHealthReport"/>.
/// </summary>
[TestFixture]
public sealed class BackupSinkSharingTests
{
    private static BackupSinkSharingReport Report(
        BackupSinkSharingStatus status = BackupSinkSharingStatus.Shared,
        params string[] unconfirmed) =>
        new(status, "region-a", unconfirmed.Length, unconfirmed, DateTimeOffset.UnixEpoch, "why.");

    [Test]
    public void Constructor_null_cluster_id_throws() =>
        Assert.That(
            () => new BackupSinkSharingReport(
                BackupSinkSharingStatus.Shared, null!, 0, [], DateTimeOffset.UnixEpoch, "why."),
            Throws.ArgumentNullException);

    [Test]
    public void Constructor_null_unconfirmed_peers_throws() =>
        Assert.That(
            () => new BackupSinkSharingReport(
                BackupSinkSharingStatus.Shared, "region-a", 0, null!, DateTimeOffset.UnixEpoch, "why."),
            Throws.ArgumentNullException);

    [Test]
    public void Constructor_null_explanation_throws() =>
        Assert.That(
            () => new BackupSinkSharingReport(
                BackupSinkSharingStatus.Shared, "region-a", 0, [], DateTimeOffset.UnixEpoch, null!),
            Throws.ArgumentNullException);

    [Test]
    public void Constructor_negative_peer_count_throws() =>
        Assert.That(
            () => new BackupSinkSharingReport(
                BackupSinkSharingStatus.Shared, "region-a", -1, [], DateTimeOffset.UnixEpoch, "why."),
            Throws.InstanceOf<ArgumentOutOfRangeException>());

    [Test]
    public void Constructor_round_trips_every_field()
    {
        var report = new BackupSinkSharingReport(
            BackupSinkSharingStatus.NotShared,
            "region-a",
            peerCount: 2,
            unconfirmedPeerClusterIds: ["region-b"],
            probedAtUtc: DateTimeOffset.UnixEpoch,
            explanation: "why.");

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupSinkSharingStatus.NotShared));
            Assert.That(report.ClusterId, Is.EqualTo("region-a"));
            Assert.That(report.PeerCount, Is.EqualTo(2));
            Assert.That(report.UnconfirmedPeerClusterIds, Is.EqualTo(new[] { "region-b" }));
            Assert.That(report.ProbedAtUtc, Is.EqualTo(DateTimeOffset.UnixEpoch));
            Assert.That(report.Explanation, Is.EqualTo("why."));
        });
    }

    [Test]
    public void IsRefuted_is_true_only_for_a_positively_refuted_sink() =>
        Assert.Multiple(() =>
        {
            Assert.That(Report(BackupSinkSharingStatus.NotShared, "region-b").IsRefuted, Is.True);
            Assert.That(Report(BackupSinkSharingStatus.Shared).IsRefuted, Is.False);
            Assert.That(Report(BackupSinkSharingStatus.Unverified, "region-b").IsRefuted, Is.False);
            Assert.That(Report(BackupSinkSharingStatus.NotApplicable).IsRefuted, Is.False);
        });

    [Test]
    public async Task NoBackupSinkSharingProbe_never_probes_and_reports_not_applicable()
    {
        var probe = new NoBackupSinkSharingProbe();

        var report = await probe.ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(probe.LastReport, Is.Null, "The inert default records nothing to annotate health with.");
            Assert.That(report.Status, Is.EqualTo(BackupSinkSharingStatus.NotApplicable));
            Assert.That(report.PeerCount, Is.Zero);
            Assert.That(report.UnconfirmedPeerClusterIds, Is.Empty);
            Assert.That(report.Explanation, Is.Not.Empty);
        });
    }

    [Test]
    public void Options_default_to_warn_so_an_existing_deployment_is_never_bricked()
    {
        var options = new LatticeBackupOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.SinkSharingEnforcement, Is.EqualTo(BackupSinkSharingEnforcement.Warn));
            Assert.That(options.SinkSharingProbeTimeout, Is.EqualTo(TimeSpan.FromSeconds(15)));
        });
    }

    [Test]
    public void Validate_accepts_the_defaults() =>
        Assert.That(
            new LatticeBackupOptionsValidator().Validate(null, new LatticeBackupOptions()).Succeeded,
            Is.True);

    [Test]
    public void Validate_rejects_an_undefined_enforcement_mode()
    {
        var options = new LatticeBackupOptions { SinkSharingEnforcement = (BackupSinkSharingEnforcement)99 };

        var result = new LatticeBackupOptionsValidator().Validate(null, options);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeBackupOptions.SinkSharingEnforcement)));
        });
    }

    [Test]
    public void Validate_rejects_a_non_positive_probe_timeout()
    {
        var options = new LatticeBackupOptions { SinkSharingProbeTimeout = TimeSpan.Zero };

        var result = new LatticeBackupOptionsValidator().Validate(null, options);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeBackupOptions.SinkSharingProbeTimeout)));
        });
    }

    [Test]
    public void BackupHealthReport_defaults_peer_visibility_to_not_applicable()
    {
        // The new fields are trailing and defaulted so every pre-existing call site
        // (and every report persisted before the probe existed) still means "no
        // cross-cluster claim made".
        var report = new BackupHealthReport(
            "b1",
            BackupHealthStatus.Healthy,
            manifestPresent: true,
            missingArtifactIds: [],
            hashMismatchArtifactIds: [],
            DateTimeOffset.UnixEpoch,
            "fine.");

        Assert.Multiple(() =>
        {
            Assert.That(report.PeerVisibility, Is.EqualTo(BackupSinkSharingStatus.NotApplicable));
            Assert.That(report.PeerUnconfirmedClusterIds, Is.Empty);
        });
    }

    [Test]
    public void BackupHealthReport_null_peer_list_is_normalised_to_empty()
    {
        var report = new BackupHealthReport(
            "b1",
            BackupHealthStatus.Healthy,
            manifestPresent: true,
            missingArtifactIds: [],
            hashMismatchArtifactIds: [],
            DateTimeOffset.UnixEpoch,
            "fine.",
            BackupSinkSharingStatus.Shared,
            peerUnconfirmedClusterIds: null);

        Assert.That(report.PeerUnconfirmedClusterIds, Is.Empty);
    }
}
