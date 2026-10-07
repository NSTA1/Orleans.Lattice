using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit coverage for <see cref="CopyReceiveFenceGrain"/> (issue #4593): the
/// durable receive fence a coordinated restore closes on its restored copy before
/// the alias swap and opens when the saga's fence lifts.
/// </summary>
[TestFixture]
public sealed class CopyReceiveFenceGrainTests
{
    private const string SagaId = "restore-saga-1";

    private static int _copySequence;

    private static (CopyReceiveFenceGrain Grain, FakePersistentState<CopyReceiveFenceState> State, string Copy) CreateGrain(
        FakePersistentState<CopyReceiveFenceState>? state = null, string? copy = null)
    {
        copy ??= $"orders-shadow-{Interlocked.Increment(ref _copySequence)}";
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("copy-receive-fence", copy));
        state ??= new FakePersistentState<CopyReceiveFenceState>();
        return (new CopyReceiveFenceGrain(context, state, NullLogger<CopyReceiveFenceGrain>.Instance), state, copy);
    }

    [Test]
    public async Task A_copy_no_restore_closed_reads_open()
    {
        var (grain, state, _) = CreateGrain();

        Assert.That((await grain.GetStatusAsync()).Closed, Is.False);
        Assert.That(state.WriteCount, Is.Zero);
    }

    [Test]
    public async Task Close_persists_the_owning_saga_and_reads_closed()
    {
        var (grain, state, copy) = CreateGrain();

        await grain.CloseAsync(SagaId, 3);

        Assert.Multiple(async () =>
        {
            Assert.That((await grain.GetStatusAsync()).Closed, Is.True);
            Assert.That(state.State.ClosedBySagaId, Is.EqualTo(SagaId));
            Assert.That(state.State.ClosedAtTicks, Is.GreaterThan(0));
            Assert.That(CopyReceiveFenceCensus.IsEnrolled(copy), Is.True);
        });
    }

    [Test]
    public async Task Close_is_idempotent_for_the_same_saga()
    {
        var (grain, state, _) = CreateGrain();
        await grain.CloseAsync(SagaId, 3);
        var closedAt = state.State.ClosedAtTicks;

        await grain.CloseAsync(SagaId, 3);

        Assert.That(state.WriteCount, Is.EqualTo(1));
        Assert.That(state.State.ClosedAtTicks, Is.EqualTo(closedAt));
    }

    [Test]
    public async Task Open_by_the_owning_saga_clears_the_state_and_withdraws_the_census()
    {
        var (grain, state, copy) = CreateGrain();
        await grain.CloseAsync(SagaId, 3);

        await grain.OpenAsync(SagaId);

        Assert.Multiple(async () =>
        {
            Assert.That((await grain.GetStatusAsync()).Closed, Is.False);
            Assert.That(state.State.ClosedBySagaId, Is.Null);
            Assert.That((await grain.GetStatusAsync()).MinAdmissionEpoch, Is.EqualTo(3), "the open keeps the minimum admission epoch");
            Assert.That(CopyReceiveFenceCensus.IsEnrolled(copy), Is.False);
        });
    }

    [Test]
    public async Task Open_by_another_saga_leaves_the_copy_closed()
    {
        var (grain, _, _) = CreateGrain();
        await grain.CloseAsync(SagaId, 3);

        await grain.OpenAsync("some-other-saga");

        Assert.That((await grain.GetStatusAsync()).Closed, Is.True);
    }

    [Test]
    public async Task A_close_by_a_newer_saga_takes_ownership_and_keeps_the_first_close_time()
    {
        var (grain, state, _) = CreateGrain();
        await grain.CloseAsync(SagaId, 3);
        var closedAt = state.State.ClosedAtTicks;

        await grain.CloseAsync("newer-saga", 2);
        await grain.OpenAsync(SagaId);

        Assert.Multiple(async () =>
        {
            Assert.That((await grain.GetStatusAsync()).Closed, Is.True, "only the owning saga opens the copy");
            Assert.That(state.State.ClosedBySagaId, Is.EqualTo("newer-saga"));
            Assert.That(state.State.ClosedAtTicks, Is.EqualTo(closedAt), "the age measures the whole closed span");
            Assert.That(state.State.MinAdmissionEpoch, Is.EqualTo(3), "the minimum admission epoch only rises");
        });
    }

    [Test]
    public void A_close_whose_write_fails_leaves_the_copy_open_so_the_retry_writes()
    {
        var (grain, state, _) = CreateGrain();
        state.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.CloseAsync(SagaId, 3));

        Assert.That(state.State.ClosedBySagaId, Is.Null);
    }

    [Test]
    public async Task A_reactivated_closed_copy_re_enrols_and_a_deactivation_withdraws_it()
    {
        var (first, state, copy) = CreateGrain();
        await first.CloseAsync(SagaId, 3);
        await first.OnDeactivateAsync(new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"), default);
        Assert.That(CopyReceiveFenceCensus.IsEnrolled(copy), Is.False);

        var (second, _, _) = CreateGrain(state, copy);
        await second.OnActivateAsync(default);

        Assert.Multiple(async () =>
        {
            Assert.That((await second.GetStatusAsync()).Closed, Is.True, "the close is durable");
            Assert.That(CopyReceiveFenceCensus.IsEnrolled(copy), Is.True);
        });
        await second.OpenAsync(SagaId);
    }

    [Test]
    public void Close_and_open_reject_an_empty_saga_id()
    {
        var (grain, _, _) = CreateGrain();

        Assert.ThrowsAsync<ArgumentException>(() => grain.CloseAsync(string.Empty, 1));
        Assert.ThrowsAsync<ArgumentException>(() => grain.OpenAsync(string.Empty));
    }

    [Test]
    public async Task A_copy_no_restore_closed_reports_open_with_a_zero_floor()
    {
        var (grain, _, _) = CreateGrain();

        var status = await grain.GetStatusAsync();

        Assert.That(status, Is.EqualTo(new CopyReceiveFenceStatus { Closed = false, MinAdmissionEpoch = 0 }));
    }

    [Test]
    public async Task A_re_close_by_the_owning_saga_with_a_higher_epoch_raises_the_floor()
    {
        var (grain, state, _) = CreateGrain();
        await grain.CloseAsync(SagaId, 3);

        await grain.CloseAsync(SagaId, 5);

        Assert.That(state.State.MinAdmissionEpoch, Is.EqualTo(5));
        Assert.That(state.WriteCount, Is.EqualTo(2));
    }

    [Test]
    public async Task An_open_whose_write_fails_leaves_the_copy_closed_for_the_retry()
    {
        var (grain, state, _) = CreateGrain();
        await grain.CloseAsync(SagaId, 3);
        state.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.OpenAsync(SagaId));

        Assert.That(state.State.ClosedBySagaId, Is.EqualTo(SagaId));
        await grain.OpenAsync(SagaId);
        Assert.That((await grain.GetStatusAsync()).Closed, Is.False);
    }

    [Test]
    public void Close_rejects_a_negative_epoch()
    {
        var (grain, _, _) = CreateGrain();

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => grain.CloseAsync(SagaId, -1));
    }
}