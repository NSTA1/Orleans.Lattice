using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// The operator delete's artifact phase in the publication gate (issue #5639):
/// before each artifact delete the gate renews the delete's lease synchronously,
/// and stops - keeping the sink manifest, so a retry resumes - when the scheduler
/// refuses the renewal (the lease ran out, or the activation holding it was lost)
/// or cannot be reached; and a delete whose end keeps the manifest because a row
/// lists the backup again reports that it did not delete it. Driven against a
/// substituted scheduler grain, so the lease's loss lands before the first
/// artifact, which no cluster hook reaches.
/// </summary>
[TestFixture]
public sealed class SchedulerBackupPublicationGateDeleteTests
{
    private readonly ILatticeBackupSchedulerGrain _scheduler = Substitute.For<ILatticeBackupSchedulerGrain>();
    private readonly ILatticeBackupCatalogStore _catalog = Substitute.For<ILatticeBackupCatalogStore>();
    private readonly ILatticeBackupSink _sink = Substitute.For<ILatticeBackupSink>();
    private readonly BackupManifest _manifest = BackupManifestModelTests.Sample(id: "x");

    [SetUp]
    public void SetUp()
    {
        _catalog.ListAsync(Arg.Any<CancellationToken>()).Returns(_ => Empty());
        _scheduler.BeginDeleteAsync("x", Arg.Any<Guid>())
            .Returns(new BackupDeleteStart(BackupRetireOutcome.Removed, _manifest));
    }

    [Test]
    public async Task A_delete_whose_lease_was_lost_before_its_first_artifact_deletes_no_artifact()
    {
        // The scheduler's activation was lost during the delete's catalog walk,
        // and its leases with it: the first renewal is refused.
        _scheduler.RenewDeleteAsync("x", Arg.Any<Guid>()).Returns(false);

        await Assert.ThatAsync(() => Gate().RetireAsync(_manifest, BackupRetireKind.Delete, CancellationToken.None), Throws.TypeOf<TimeoutException>());

        await _sink.DidNotReceiveWithAnyArgs().DeleteArtifactAsync(default!, default);
        await _scheduler.Received(1).EndDeleteAsync("x", Arg.Any<Guid>(), false);
        await _scheduler.DidNotReceive().EndDeleteAsync("x", Arg.Any<Guid>(), true);
    }

    [Test]
    public async Task A_delete_that_cannot_confirm_its_lease_deletes_no_artifact()
    {
        // The scheduler cannot be reached: the delete must not go on unconfirmed.
        _scheduler.RenewDeleteAsync("x", Arg.Any<Guid>()).ThrowsAsync(new TimeoutException("grain response timeout"));

        await Assert.ThatAsync(() => Gate().RetireAsync(_manifest, BackupRetireKind.Delete, CancellationToken.None), Throws.TypeOf<TimeoutException>());

        await _sink.DidNotReceiveWithAnyArgs().DeleteArtifactAsync(default!, default);
        await _scheduler.DidNotReceive().EndDeleteAsync("x", Arg.Any<Guid>(), true);
    }

    [Test]
    public async Task A_delete_renews_its_lease_before_each_artifact_it_deletes()
    {
        _scheduler.RenewDeleteAsync("x", Arg.Any<Guid>()).Returns(true);
        _scheduler.EndDeleteAsync("x", Arg.Any<Guid>(), true).Returns(BackupRetireOutcome.Removed);

        var deleted = await Gate().RetireAsync(_manifest, BackupRetireKind.Delete, CancellationToken.None);

        Assert.That(deleted, Is.True);
        Received.InOrder(() =>
        {
            _scheduler.RenewDeleteAsync("x", Arg.Any<Guid>());
            _sink.DeleteArtifactAsync("artifact-1", Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public async Task A_delete_whose_end_keeps_the_manifest_of_a_listed_backup_reports_it_not_deleted()
    {
        // The delete's lease ran out and a capture, or a rebuild, listed the
        // backup again: the end keeps its manifest, and the backup is not deleted.
        _scheduler.RenewDeleteAsync("x", Arg.Any<Guid>()).Returns(true);
        _scheduler.EndDeleteAsync("x", Arg.Any<Guid>(), true).Returns(BackupRetireOutcome.Unchanged);
        _catalog.GetAsync("x", Arg.Any<CancellationToken>()).Returns(_manifest);

        var deleted = await Gate().RetireAsync(_manifest, BackupRetireKind.Delete, CancellationToken.None);

        Assert.That(deleted, Is.False, "a backup that is listed again was not deleted");
    }

    private SchedulerBackupPublicationGate Gate()
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeBackupSchedulerGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(_scheduler);
        var options = Options.Create(new LatticeBackupOptions
        {
            PublishHoldTimeout = TimeSpan.FromSeconds(5),
            PublishHoldRetryDelay = TimeSpan.FromMilliseconds(1),
        });
        return new SchedulerBackupPublicationGate(factory, options, new BackupPublisher(_catalog, _sink));
    }

    private static async IAsyncEnumerable<BackupManifest> Empty()
    {
        await Task.CompletedTask;
        yield break;
    }
}
