using System.Runtime.CompilerServices;
using NSubstitute;
using Orleans.Lattice.Testing;
using Orleans.Timers;

namespace Orleans.Lattice.Backup.Tests;

public sealed partial class BackupSchedulerGrainAccessGateTests
{
    [TestCase(false)]
    [TestCase(true)]
    public void TriggerAsync_denied_caller_cannot_persist_a_scope(bool incremental)
    {
        var state = new FakePersistentState<BackupSchedulerState>();
        var grain = CreateGrain(new DenyByDefaultAccessGate(), state, out var capture);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(async () =>
        {
            if (incremental)
            {
                await grain.TriggerIncrementalAsync(TestScope);
            }
            else
            {
                await grain.TriggerFullAsync(TestScope);
            }
        });

        Assert.That(state.State.Scope, Is.Null);
        Assert.That(state.WriteCount, Is.Zero);
        Assert.That(capture.Captures, Is.Zero);
    }

    [Test]
    public void TriggerAsync_rejects_a_scope_that_does_not_match_the_scheduler_key()
    {
        var gate = new DenyByDefaultAccessGate();
        gate.Allowed.Add(LatticeOperation.Backup);
        var state = new FakePersistentState<BackupSchedulerState>();
        var grain = CreateGrain(gate, state, out var capture);

        Assert.ThrowsAsync<ArgumentException>(async () =>
            await grain.TriggerFullAsync(BackupScopeSelector.WholeTree("other-tree")));

        Assert.That(state.State.Scope, Is.Null);
        Assert.That(state.WriteCount, Is.Zero);
        Assert.That(capture.Captures, Is.Zero);
    }

    [Test]
    public async Task CancelScheduleAsync_missing_scope_is_a_no_op()
    {
        var state = new FakePersistentState<BackupSchedulerState>();
        var reminders = Substitute.For<IReminderRegistry>();
        var grain = CreateGrain(new DenyByDefaultAccessGate(), state, out _, reminders);

        await grain.CancelScheduleAsync(incremental: false);

        Assert.That(state.WriteCount, Is.Zero);
        Assert.That(reminders.ReceivedCalls(), Is.Empty);
    }

    [TestCase(false)]
    [TestCase(true)]
    public void PruneAsync_denied_caller_cannot_read_or_delete_backups(bool retentionEnabled)
    {
        var gate = new DenyByDefaultAccessGate();
        var catalog = Substitute.For<ILatticeBackupCatalogStore>();
        catalog.ListAsync(Arg.Any<CancellationToken>()).Returns(_ => RetentionManifests());
        catalog.ClearReceivedCalls();
        var sink = Substitute.For<ILatticeBackupSink>();
        var grain = CreateGrain(gate, ScopedState(), out _, catalog: catalog,
            sink: sink, retentionEnabled: retentionEnabled);

        Assert.That(async () => await grain.PruneAsync(TestScope),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());
        Assert.Multiple(() =>
        {
            Assert.That(gate.Consultations, Is.GreaterThan(0));
            Assert.That(catalog.ReceivedCalls(), Is.Empty);
            Assert.That(sink.ReceivedCalls(), Is.Empty);
        });
    }

    [Test]
    public async Task PruneAsync_authorized_caller_can_delete_only_the_expired_backup()
    {
        var gate = new DenyByDefaultAccessGate();
        gate.Allowed.Add(LatticeOperation.Backup);
        var catalog = Substitute.For<ILatticeBackupCatalogStore>();
        catalog.ListAsync(Arg.Any<CancellationToken>()).Returns(_ => RetentionManifests());
        var sink = Substitute.For<ILatticeBackupSink>();
        var grain = CreateGrain(gate, ScopedState(), out _, catalog: catalog,
            sink: sink, retentionEnabled: true);

        var report = await grain.PruneAsync(TestScope);

        Assert.That(report.PrunedBackupIds, Is.EqualTo(new[] { "old" }));
        await sink.Received(1).DeleteManifestAsync("old", Arg.Any<CancellationToken>());
        await sink.DidNotReceive().DeleteManifestAsync("new", Arg.Any<CancellationToken>());
        await catalog.Received(1).RemoveAsync("old", Arg.Any<CancellationToken>());
    }

    private static async IAsyncEnumerable<BackupManifest> RetentionManifests(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var (id, date) in new[] { ("old", DateTimeOffset.UnixEpoch), ("new", DateTimeOffset.UtcNow) })
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return new BackupManifest(id, id, date, BackupKind.Full, TestScope,
                new BackupConsistencyCut(1, 1), new BackupTopologySnapshot(1, 4096, ["d0"]),
                "digest-root", [], [], []);
        }
        await Task.CompletedTask;
    }

    [TestCase(false)]
    [TestCase(true)]
    public void CancelScheduleAsync_denied_caller_cannot_remove_reminders(bool incremental)
    {
        var state = ScopedState();
        state.State.RuntimeFullBackupInterval = TimeSpan.FromHours(1);
        state.State.RuntimeIncrementalBackupInterval = TimeSpan.FromMinutes(10);
        var reminders = Substitute.For<IReminderRegistry>();
        var grain = CreateGrain(new DenyByDefaultAccessGate(), state, out _, reminders: reminders);

        Assert.That(async () => await grain.CancelScheduleAsync(incremental),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());
        Assert.Multiple(() =>
        {
            Assert.That(reminders.ReceivedCalls(), Is.Empty);
            Assert.That(state.State.RuntimeFullBackupInterval, Is.EqualTo(TimeSpan.FromHours(1)));
            Assert.That(state.State.RuntimeIncrementalBackupInterval, Is.EqualTo(TimeSpan.FromMinutes(10)));
            Assert.That(state.WriteCount, Is.Zero);
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task CancelScheduleAsync_authorized_caller_can_remove_reminders(bool incremental)
    {
        var gate = new DenyByDefaultAccessGate();
        gate.Allowed.Add(LatticeOperation.Backup);
        var state = ScopedState();
        state.State.RuntimeFullBackupInterval = TimeSpan.FromHours(1);
        state.State.RuntimeIncrementalBackupInterval = TimeSpan.FromMinutes(10);
        var grain = CreateGrain(gate, state, out _);

        await grain.CancelScheduleAsync(incremental);

        Assert.That(incremental ? state.State.RuntimeIncrementalBackupInterval
            : state.State.RuntimeFullBackupInterval, Is.Null);
        Assert.That(gate.Consultations, Is.GreaterThan(0));
    }
}
