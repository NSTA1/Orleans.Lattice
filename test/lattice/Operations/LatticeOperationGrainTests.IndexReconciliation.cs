using NSubstitute;
using Orleans.Lattice.Operations;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.Operations;

public sealed partial class LatticeOperationGrainTests
{
    [Test]
    public async Task Failed_acknowledgement_write_retains_the_outbox_for_an_autonomous_retry()
    {
        var grain = CreateGrain();
        await grain.BeginAsync(Begin());
        _index.ReconcileAsync(Arg.Any<LatticeOperationRecord>(), false).Returns(_ =>
        {
            _state.ThrowOnWrite = new IOException("acknowledgement write failed");
            return Task.CompletedTask;
        });
        Assert.That(async () => await grain.CompleteAsync(LatticeOperationCompletion.Succeeded()), Throws.TypeOf<IOException>());
        Assert.That(_state.State.PendingIndexRecord!.IsTerminal, Is.True);

        _index.ReconcileAsync(Arg.Any<LatticeOperationRecord>(), false).Returns(Task.CompletedTask);
        await grain.ReceiveReminder("operation-index", default);

        Assert.That(_state.State.PendingIndexRecord, Is.Null);
    }

    [Test]
    public async Task Activation_upgrades_a_legacy_record_to_a_persisted_outbox()
    {
        await CreateGrain().BeginAsync(Begin());
        _state.State.IndexOutboxInitialized = false;
        var writes = _state.WriteCount;
        var grain = CreateGrain();
        await grain.OnActivateAsync(CancellationToken.None);
        Assert.That(_state.State.PendingIndexRecord, Is.Not.Null);
        Assert.That(_state.WriteCount, Is.EqualTo(writes + 1));
        await grain.ReceiveReminder("operation-index", default);
        Assert.That(_state.State.PendingIndexRecord, Is.Null);
    }

    [Test]
    public async Task Expired_reconciled_operations_unregister_recovery_and_dispose_the_timer()
    {
        var reminder = Substitute.For<IGrainReminder>();
        var reminders = Substitute.For<IReminderRegistry>();
        reminders.GetReminder(Arg.Any<GrainId>(), "operation-index").Returns(reminder);
        var timer = Substitute.For<IGrainTimer>();
        var grain = CreateGrain(reminders: reminders);
        grain.TimerFactory = _ => timer;
        await grain.BeginAsync(Begin());
        await grain.CompleteAsync(LatticeOperationCompletion.Succeeded());
        _clock.Advance(_options.Retention);

        await grain.ReceiveReminder("operation-index", default);

        Assert.That(_state.State.Record, Is.Null);
        Assert.That(_state.State.PendingIndexRecord, Is.Null);
        await reminders.Received(1).UnregisterReminder(Arg.Any<GrainId>(), reminder);
        timer.Received(1).Dispose();
    }

    [Test]
    public async Task Reminder_recovers_a_durable_outbox_after_reactivation_without_a_caller_retry()
    {
        var grain = CreateGrain();
        _index.ReconcileAsync(Arg.Any<LatticeOperationRecord>(), false)
            .Returns(Task.FromException(new TimeoutException("index unavailable")));
        await grain.BeginAsync(Begin());
        Assert.That(_state.State.PendingIndexRecord, Is.Not.Null);

        _index.ReconcileAsync(Arg.Any<LatticeOperationRecord>(), false).Returns(Task.CompletedTask);
        var reactivated = CreateGrain();
        await reactivated.OnActivateAsync(CancellationToken.None);
        await reactivated.ReceiveReminder("operation-index", default);

        Assert.That(_state.State.PendingIndexRecord, Is.Null);
        await _index.Received(2).ReconcileAsync(Arg.Any<LatticeOperationRecord>(), false);
    }

    [Test]
    public async Task Registered_timer_recovers_a_failed_terminal_update_without_a_poll()
    {
        Func<CancellationToken, Task>? tick = null;
        var grain = CreateGrain();
        grain.TimerFactory = callback =>
        {
            tick = callback;
            return Substitute.For<IGrainTimer>();
        };
        await grain.BeginAsync(Begin());
        _index.ReconcileAsync(Arg.Any<LatticeOperationRecord>(), false)
            .Returns(Task.FromException(new TimeoutException("index unavailable")));
        await grain.CompleteAsync(LatticeOperationCompletion.Succeeded());
        Assert.That(_state.State.PendingIndexRecord!.IsTerminal, Is.True);

        _index.ReconcileAsync(Arg.Any<LatticeOperationRecord>(), false).Returns(Task.CompletedTask);
        Assert.That(tick, Is.Not.Null, "Begin must wire the production timer.");
        await tick!(CancellationToken.None);

        Assert.That(_state.State.PendingIndexRecord, Is.Null);
    }

    [Test]
    public async Task An_outstanding_index_request_is_not_multiplied_by_polls_or_ticks()
    {
        var grain = CreateGrain();
        var blocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _index.ReconcileAsync(Arg.Any<LatticeOperationRecord>(), false).Returns(blocked.Task);
        try
        {
            await grain.BeginAsync(Begin());
            _liveness.IsDead(RunnerSilo).Returns(true);
            for (var i = 0; i < 10; i++)
            {
                Assert.That((await grain.GetAsync())!.IsTerminal, Is.True);
                await grain.ReceiveReminder("operation-index", default);
            }
            await _index.Received(1).ReconcileAsync(Arg.Any<LatticeOperationRecord>(), false);
        }
        finally
        {
            blocked.TrySetResult();
        }
        _index.ReconcileAsync(Arg.Any<LatticeOperationRecord>(), false).Returns(Task.CompletedTask);
        await grain.ReceiveReminder("operation-index", default);
        Assert.That(_state.State.PendingIndexRecord, Is.Null, "The queued acknowledgement must not discard the newer terminal snapshot.");
        await _index.Received(1).ReconcileAsync(Arg.Is<LatticeOperationRecord>(r => r.IsTerminal), false);
    }

    [Test]
    public async Task Expiry_removal_survives_reactivation_and_retries_without_another_read()
    {
        var grain = CreateGrain();
        await grain.BeginAsync(Begin());
        await grain.CompleteAsync(LatticeOperationCompletion.Succeeded());
        _clock.Advance(_options.Retention);
        _index.ReconcileAsync(Arg.Any<LatticeOperationRecord>(), true)
            .Returns(Task.FromException(new TimeoutException("index unavailable")));
        Assert.That(await grain.GetAsync(), Is.Null);
        Assert.That(_state.State.PendingIndexRemoval, Is.True);

        _index.ReconcileAsync(Arg.Any<LatticeOperationRecord>(), true).Returns(Task.CompletedTask);
        var reactivated = CreateGrain();
        await reactivated.OnActivateAsync(CancellationToken.None);
        await reactivated.ReceiveReminder("operation-index", default);

        Assert.That(_state.State.PendingIndexRecord, Is.Null);
        Assert.That(_state.State.Record, Is.Null);
        await _index.Received(2).ReconcileAsync(Arg.Any<LatticeOperationRecord>(), true);
    }

    [Test]
    public async Task Begin_registers_recovery_before_persisting_acceptance()
    {
        var reminders = Substitute.For<IReminderRegistry>();
        var registered = false;
        reminders.RegisterOrUpdateReminder(Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
            .Returns(_ =>
            {
                Assert.That(_state.State.Record, Is.Null);
                registered = true;
                return Task.FromResult(Substitute.For<IGrainReminder>());
            });
        await CreateGrain(reminders: reminders).BeginAsync(Begin());
        Assert.That(registered, Is.True);
    }

    [Test]
    public async Task Get_persists_runner_loss_without_waiting_for_an_unresponsive_index()
    {
        var grain = CreateGrain();
        await grain.BeginAsync(Begin());
        var blocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _index.ReconcileAsync(Arg.Any<LatticeOperationRecord>(), false).Returns(blocked.Task);
        _liveness.IsDead(RunnerSilo).Returns(true);

        try
        {
            var record = await grain.GetAsync().WaitAsync(TimeSpan.FromSeconds(2));
            Assert.That(record!.State, Is.EqualTo(LatticeOperationState.Failed));
            Assert.That(_state.State.Record!.State, Is.EqualTo(LatticeOperationState.Failed));
        }
        finally
        {
            blocked.TrySetResult();
        }
    }

    [Test]
    public async Task Begin_persists_acceptance_without_waiting_for_an_unresponsive_index()
    {
        var grain = CreateGrain();
        var blocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _index.ReconcileAsync(Arg.Any<LatticeOperationRecord>(), false).Returns(blocked.Task);
        try
        {
            var result = await grain.BeginAsync(Begin()).WaitAsync(TimeSpan.FromSeconds(2));
            Assert.That(result.Created, Is.True);
            Assert.That(_state.State.Record, Is.Not.Null);
        }
        finally
        {
            blocked.TrySetResult();
        }
    }

    [Test]
    public async Task Get_expires_a_record_without_waiting_for_an_unresponsive_index()
    {
        var grain = CreateGrain();
        await grain.BeginAsync(Begin());
        await grain.CompleteAsync(LatticeOperationCompletion.Succeeded());
        _clock.Advance(_options.Retention);
        var blocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _index.ReconcileAsync(Arg.Any<LatticeOperationRecord>(), true).Returns(blocked.Task);
        try
        {
            Assert.That(await grain.GetAsync().WaitAsync(TimeSpan.FromSeconds(2)), Is.Null);
            Assert.That(_state.State.Record, Is.Null);
        }
        finally
        {
            blocked.TrySetResult();
        }
    }
}
