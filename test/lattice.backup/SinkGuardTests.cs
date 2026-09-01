using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit coverage for the startup sink guard
/// (<see cref="LatticeBackupReplicatedSinkStartupValidator"/>). Two faults are
/// distinguished: the locally provable one (a replicated tree backed by the
/// default in-cluster sink always fails fast) and the deployment fact (an external
/// sink is put to an actual cross-cluster test through the
/// <see cref="IBackupSinkSharingProbe"/> seam rather than assumed shared). Also
/// pins the inertness contract: nothing replicated means no probe at all, so a
/// single-cluster deployment gains no new failure mode.
/// </summary>
[TestFixture]
public sealed class SinkGuardTests
{
    private static LatticeBackupReplicatedSinkStartupValidator CreateValidator(
        ILatticeBackupSink sink,
        IReplicatedTreeMembership membership,
        IBackupSinkSharingProbe? probe = null,
        BackupSinkSharingEnforcement enforcement = BackupSinkSharingEnforcement.Warn)
    {
        var options = Substitute.For<IOptionsMonitor<LatticeBackupOptions>>();
        options.CurrentValue.Returns(new LatticeBackupOptions { SinkSharingEnforcement = enforcement });
        return new LatticeBackupReplicatedSinkStartupValidator(
            sink,
            membership,
            probe ?? new NoBackupSinkSharingProbe(),
            options,
            NullLogger<LatticeBackupReplicatedSinkStartupValidator>.Instance);
    }

    private static BackupSinkSharingReport Report(
        BackupSinkSharingStatus status,
        params string[] unconfirmed) =>
        new(status, "region-a", unconfirmed.Length, unconfirmed, DateTimeOffset.UtcNow, $"probe says {status}.");

    [Test]
    public void StartAsync_replicated_tree_with_in_cluster_sink_fails_fast()
    {
        var sink = new InClusterLatticeBackupSink(Substitute.For<IGrainFactory>());
        var validator = CreateValidator(sink, new FakeReplicatedTreeMembership("orders"));

        Assert.That(
            async () => await validator.StartAsync(CancellationToken.None),
            Throws.InvalidOperationException
                .With.Message.Contains("orders")
                .And.Message.Contains("shared external sink"));
    }

    [Test]
    public void StartAsync_in_cluster_sink_fails_fast_even_when_the_probe_is_disabled()
    {
        // The in-cluster sink is provably per-cluster, so it is rejected without any
        // peer consultation - disabling the probe must not open the blind spot back up.
        var sink = new InClusterLatticeBackupSink(Substitute.For<IGrainFactory>());
        var validator = CreateValidator(
            sink,
            new FakeReplicatedTreeMembership("orders"),
            enforcement: BackupSinkSharingEnforcement.Disabled);

        Assert.That(
            async () => await validator.StartAsync(CancellationToken.None),
            Throws.InvalidOperationException.With.Message.Contains("orders"));
    }

    [Test]
    public async Task StartAsync_replicated_tree_with_shared_external_sink_passes()
    {
        var probe = new FakeSharingProbe(Report(BackupSinkSharingStatus.Shared));
        var validator = CreateValidator(
            Substitute.For<ILatticeBackupSink>(),
            new FakeReplicatedTreeMembership("orders"),
            probe);

        await validator.StartAsync(CancellationToken.None);

        Assert.That(probe.Calls, Is.EqualTo(1), "A replicated tree on an external sink must be probed.");
    }

    [Test]
    public async Task StartAsync_nothing_replicated_never_probes()
    {
        // Explicit acceptance criterion: a single-cluster / unreplicated deployment
        // must not run any cross-cluster probe at all.
        var probe = new FakeSharingProbe(Report(BackupSinkSharingStatus.NotShared, "region-b"));
        var validator = CreateValidator(
            Substitute.For<ILatticeBackupSink>(),
            new NoReplicatedTreeMembership(),
            probe);

        await validator.StartAsync(CancellationToken.None);

        Assert.That(probe.Calls, Is.Zero, "Nothing replicated must mean no probe.");
    }

    [Test]
    public async Task StartAsync_single_cluster_no_op_seam_with_in_cluster_sink_passes()
    {
        var sink = new InClusterLatticeBackupSink(Substitute.For<IGrainFactory>());
        var validator = CreateValidator(sink, new NoReplicatedTreeMembership());

        await validator.StartAsync(CancellationToken.None);

        Assert.Pass("A single-cluster deployment (nothing replicated) accepts the in-cluster sink.");
    }

    [Test]
    public async Task StartAsync_disabled_enforcement_skips_the_probe_for_an_external_sink()
    {
        var probe = new FakeSharingProbe(Report(BackupSinkSharingStatus.NotShared, "region-b"));
        var validator = CreateValidator(
            Substitute.For<ILatticeBackupSink>(),
            new FakeReplicatedTreeMembership("orders"),
            probe,
            BackupSinkSharingEnforcement.Disabled);

        await validator.StartAsync(CancellationToken.None);

        Assert.That(probe.Calls, Is.Zero, "Disabled enforcement must not probe.");
    }

    [Test]
    public async Task StartAsync_not_shared_sink_warns_but_starts_under_the_default_enforcement()
    {
        // The shipped default must not brick a deployment that is merely
        // transiently misreported; detection is surfaced through the log and the
        // backup health column instead.
        var probe = new FakeSharingProbe(Report(BackupSinkSharingStatus.NotShared, "region-b"));
        var validator = CreateValidator(
            Substitute.For<ILatticeBackupSink>(),
            new FakeReplicatedTreeMembership("orders"),
            probe);

        await validator.StartAsync(CancellationToken.None);

        Assert.That(probe.Calls, Is.EqualTo(1));
    }

    [Test]
    public void StartAsync_not_shared_sink_fails_fast_under_strict_enforcement()
    {
        var probe = new FakeSharingProbe(Report(BackupSinkSharingStatus.NotShared, "region-b"));
        var validator = CreateValidator(
            Substitute.For<ILatticeBackupSink>(),
            new FakeReplicatedTreeMembership("orders"),
            probe,
            BackupSinkSharingEnforcement.FailFast);

        Assert.That(
            async () => await validator.StartAsync(CancellationToken.None),
            Throws.InvalidOperationException
                .With.Message.Contains(nameof(BackupSinkSharingEnforcement.FailFast)));
    }

    [Test]
    public async Task StartAsync_unverified_sink_never_fails_fast_even_under_strict_enforcement()
    {
        // A peer that is merely offline must never block a start: only a positively
        // refuted sink is an accusation.
        var probe = new FakeSharingProbe(Report(BackupSinkSharingStatus.Unverified, "region-b"));
        var validator = CreateValidator(
            Substitute.For<ILatticeBackupSink>(),
            new FakeReplicatedTreeMembership("orders"),
            probe,
            BackupSinkSharingEnforcement.FailFast);

        await validator.StartAsync(CancellationToken.None);

        Assert.That(probe.Calls, Is.EqualTo(1));
    }

    [Test]
    public async Task StartAsync_probe_failure_degrades_to_unverified_and_does_not_block_startup()
    {
        var probe = new ThrowingSharingProbe();
        var validator = CreateValidator(
            Substitute.For<ILatticeBackupSink>(),
            new FakeReplicatedTreeMembership("orders"),
            probe,
            BackupSinkSharingEnforcement.FailFast);

        await validator.StartAsync(CancellationToken.None);

        Assert.That(probe.Calls, Is.EqualTo(1), "A probe fault is not evidence the sink is unshared.");
    }

    [Test]
    public async Task StopAsync_is_a_no_op()
    {
        var validator = CreateValidator(Substitute.For<ILatticeBackupSink>(), new NoReplicatedTreeMembership());

        await validator.StopAsync(CancellationToken.None);

        Assert.Pass("The guard holds no resources to release.");
    }

    [Test]
    public void NoReplicatedTreeMembership_reports_nothing_replicated()
    {
        var membership = new NoReplicatedTreeMembership();

        Assert.Multiple(() =>
        {
            Assert.That(membership.ReplicatedTrees, Is.Empty);
            Assert.That(membership.IsReplicated("orders"), Is.False);
        });
    }

    /// <summary>A test-double membership seam that reports a fixed set of trees as replicated.</summary>
    private sealed class FakeReplicatedTreeMembership(params string[] trees) : IReplicatedTreeMembership
    {
        private readonly HashSet<string> _trees = new(trees, StringComparer.Ordinal);

        public IReadOnlyCollection<string> ReplicatedTrees => _trees;

        public bool IsReplicated(string treeId)
        {
            ArgumentNullException.ThrowIfNull(treeId);
            return _trees.Contains(treeId);
        }
    }

    /// <summary>A probe that returns a canned verdict and counts how often it ran.</summary>
    private sealed class FakeSharingProbe(BackupSinkSharingReport report) : IBackupSinkSharingProbe
    {
        public int Calls { get; private set; }

        public BackupSinkSharingReport? LastReport => Calls > 0 ? report : null;

        public Task<BackupSinkSharingReport> ProbeAsync(CancellationToken cancellationToken = default)
        {
            Calls++;
            return Task.FromResult(report);
        }
    }

    /// <summary>A probe whose I/O always faults, standing in for an unreachable sink.</summary>
    private sealed class ThrowingSharingProbe : IBackupSinkSharingProbe
    {
        public int Calls { get; private set; }

        public BackupSinkSharingReport? LastReport => null;

        public Task<BackupSinkSharingReport> ProbeAsync(CancellationToken cancellationToken = default)
        {
            Calls++;
            throw new InvalidOperationException("sink unreachable");
        }
    }
}
