using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Tests.Operations;

public sealed partial class LatticeOperationIndexGrainTests
{
    private LatticeOperationRecord Snapshot(int minute, bool finished = false) => new()
    {
        OperationId = "op",
        TenantId = Tenant,
        Kind = "k",
        Phase = "queued",
        StartedAtUtc = At(minute),
        State = finished ? LatticeOperationState.Succeeded : LatticeOperationState.Queued,
        FinishedAtUtc = finished ? At(minute + 1) : null,
    };

    [Test]
    public async Task Reconcile_adds_and_finishes_a_record_whose_initial_add_was_lost()
    {
        var grain = CreateGrain();
        await grain.ReconcileAsync(Snapshot(0, finished: true), false);
        Assert.That(_state.State.Entries.Single().FinishedAtUtc, Is.EqualTo(At(1)));
    }

    [Test]
    public async Task Reconcile_ignores_delayed_updates_and_removal_of_a_reused_id()
    {
        var grain = CreateGrain();
        await grain.ReconcileAsync(Snapshot(2), false);
        await grain.ReconcileAsync(Snapshot(0, finished: true), false);
        await grain.ReconcileAsync(Snapshot(0, finished: true), true);
        Assert.That(_state.State.Entries.Single().StartedAtUtc, Is.EqualTo(At(2)));
        Assert.That(_state.State.Entries.Single().FinishedAtUtc, Is.Null);
    }

    [Test]
    public async Task Reconcile_never_resurrects_an_expired_terminal_snapshot()
    {
        var grain = CreateGrain();
        var record = Snapshot(0, finished: true);
        await grain.ReconcileAsync(record, false);
        _clock.Advance(_options.Retention + TimeSpan.FromMinutes(1));
        await grain.ReconcileAsync(record, true);
        await grain.ReconcileAsync(record, false);
        Assert.That(_state.State.Entries, Is.Empty);
    }

    [Test]
    public async Task Reconcile_does_not_undo_a_finish_with_a_delayed_queued_snapshot()
    {
        var grain = CreateGrain();
        await grain.ReconcileAsync(Snapshot(0, finished: true), false);
        await grain.ReconcileAsync(Snapshot(0), false);
        Assert.That(_state.State.Entries.Single().FinishedAtUtc, Is.EqualTo(At(1)));
    }
}
