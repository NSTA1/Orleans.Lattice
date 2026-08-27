using System.Runtime.CompilerServices;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit coverage for the smaller, self-contained seams of the backup add-on that
/// the integration suites do not reach: every rejection rule in
/// <see cref="LatticeBackupOptionsValidator"/>, the serializable exception types
/// the capture and restore paths raise, and the health service's
/// "manifest disappeared between the probe and the read" race.
/// </summary>
[TestFixture]
public sealed class BackupSeamUnitTests
{
    // ---- LatticeBackupOptionsValidator ----------------------------------

    private static ValidateOptionsResult Validate(LatticeBackupOptions options) =>
        new LatticeBackupOptionsValidator().Validate(name: null, options);

    [Test]
    public void Default_backup_options_are_valid()
    {
        Assert.That(Validate(new LatticeBackupOptions()).Succeeded, Is.True);
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void A_non_positive_history_retention_window_is_rejected(int seconds)
    {
        var result = Validate(new LatticeBackupOptions
        {
            HistoryRetentionWindow = TimeSpan.FromSeconds(seconds),
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeBackupOptions.HistoryRetentionWindow)));
        });
    }

    [Test]
    public void An_undefined_history_retention_mode_is_rejected()
    {
        var result = Validate(new LatticeBackupOptions
        {
            HistoryRetentionMode = (HistoryRetentionMode)9999,
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeBackupOptions.HistoryRetentionMode)));
        });
    }

    [Test]
    public void A_non_positive_cross_tree_fence_drain_timeout_is_rejected()
    {
        var result = Validate(new LatticeBackupOptions { CrossTreeFenceDrainTimeout = TimeSpan.Zero });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeBackupOptions.CrossTreeFenceDrainTimeout)));
        });
    }

    [Test]
    public void A_non_positive_cross_tree_fence_poll_interval_is_rejected()
    {
        var result = Validate(new LatticeBackupOptions { CrossTreeFencePollInterval = TimeSpan.Zero });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeBackupOptions.CrossTreeFencePollInterval)));
        });
    }

    [TestCase(0)]
    [TestCase(-2)]
    public void A_fence_attempt_budget_below_one_is_rejected(int attempts)
    {
        var result = Validate(new LatticeBackupOptions { MaxCrossTreeFenceAttempts = attempts });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeBackupOptions.MaxCrossTreeFenceAttempts)));
        });
    }

    [Test]
    public void Every_broken_backup_option_rule_is_reported_together()
    {
        var result = Validate(new LatticeBackupOptions
        {
            HistoryRetentionWindow = TimeSpan.Zero,
            HistoryRetentionMode = (HistoryRetentionMode)9999,
            CrossTreeFenceDrainTimeout = TimeSpan.Zero,
            CrossTreeFencePollInterval = TimeSpan.Zero,
            MaxCrossTreeFenceAttempts = 0,
        });

        Assert.That(result.Failures?.Count(), Is.EqualTo(5));
    }

    [Test]
    public void The_backup_options_validator_rejects_null_options()
    {
        Assert.That(
            () => new LatticeBackupOptionsValidator().Validate(name: null, options: null!),
            Throws.ArgumentNullException);
    }

    // ---- Serializable exception types ------------------------------------

    [Test]
    public void The_cross_tree_fence_exception_carries_its_message_and_inner_cause()
    {
        var inner = new InvalidOperationException("drain stalled");

        var withMessage = new LatticeBackupCrossTreeFenceException("fence failed");
        var withInner = new LatticeBackupCrossTreeFenceException("fence failed", inner);

        Assert.Multiple(() =>
        {
            Assert.That(withMessage.Message, Is.EqualTo("fence failed"));
            Assert.That(withMessage.InnerException, Is.Null);
            Assert.That(withInner.Message, Is.EqualTo("fence failed"));
            Assert.That(withInner.InnerException, Is.SameAs(inner));
        });
    }

    [Test]
    public void The_restore_validation_exception_carries_its_message_and_inner_cause()
    {
        var inner = new InvalidOperationException("digest mismatch");

        var withMessage = new LatticeRestoreValidationException("artifact rejected");
        var withInner = new LatticeRestoreValidationException("artifact rejected", inner);

        Assert.Multiple(() =>
        {
            Assert.That(withMessage.Message, Is.EqualTo("artifact rejected"));
            Assert.That(withMessage.InnerException, Is.Null);
            Assert.That(withInner.Message, Is.EqualTo("artifact rejected"));
            Assert.That(withInner.InnerException, Is.SameAs(inner));
        });
    }

    // ---- Health service concurrent-delete race ---------------------------

    [Test]
    public async Task VerifyAsync_reports_missing_when_the_manifest_vanishes_after_the_probe()
    {
        // The probe says the manifest is present, but a concurrent delete lands
        // before the read: the service must degrade to Missing rather than crash.
        var service = new LatticeBackupHealthService(new VanishingManifestSink());

        var report = await service.VerifyAsync("racy");

        Assert.Multiple(() =>
        {
            Assert.That(report.Status, Is.EqualTo(BackupHealthStatus.Missing));
            Assert.That(report.ManifestPresent, Is.False);
            Assert.That(report.MissingArtifactIds, Is.Empty);
            Assert.That(report.HashMismatchArtifactIds, Is.Empty);
            Assert.That(report.Explanation, Does.Contain("disappeared"));
            Assert.That(report.IsHealthy, Is.False);
        });
    }

    /// <summary>
    /// A sink whose presence probe succeeds but whose manifest read returns
    /// <see langword="null"/>, modelling a delete that races the verification.
    /// </summary>
    private sealed class VanishingManifestSink : ILatticeBackupSink
    {
        public bool IsDurable => true;

        public Task<BackupSinkResolution> ProbeAsync(string backupId, CancellationToken cancellationToken = default) =>
            Task.FromResult(new BackupSinkResolution(backupId, manifestPresent: true, Array.Empty<string>()));

        public Task<BackupManifest?> ReadManifestAsync(string backupId, CancellationToken cancellationToken = default) =>
            Task.FromResult<BackupManifest?>(null);

        public Task<bool> ManifestExistsAsync(string backupId, CancellationToken cancellationToken = default) =>
            Task.FromResult(true);

        public async IAsyncEnumerable<ReadOnlyMemory<byte>> ReadArtifactAsync(
            string artifactId, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            yield break;
        }

        public Task WriteArtifactAsync(string artifactId, IAsyncEnumerable<ReadOnlyMemory<byte>> content, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<bool> DeleteArtifactAsync(string artifactId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public IAsyncEnumerable<string> ListArtifactIdsAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task WriteManifestAsync(BackupManifest manifest, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public IAsyncEnumerable<BackupManifest> ListManifestsAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<bool> DeleteManifestAsync(string backupId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
}
