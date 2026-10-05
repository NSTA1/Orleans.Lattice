using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4632. A saga's participant row in its own tree's registry is a
/// precondition of every prepare dispatch: a destination leaf refuses a forwarded
/// prepare whose saga the registry reports undecided and holds no row for, as a
/// prepare delivered after the saga was forgotten, so a live saga must never be
/// without one. The prepare phase's bulk registration is best-effort, so the
/// execute phase re-asserts the row before it dispatches.
/// </summary>
public partial class AtomicWriteGrainTests
{
    private static (AtomicWriteGrain grain, FakePersistentState<AtomicWriteState> state, ILattice lattice, ITxRegistryGrain registry)
        CreateParticipantRowGrain(Func<int, Task> registerParticipants)
    {
        var registry = Substitute.For<ITxRegistryGrain>();
        var calls = 0;
        registry.RegisterParticipantsAsync(Arg.Any<Guid>(), Arg.Any<IReadOnlyList<int>>())
            .Returns(_ => registerParticipants(++calls));
        var (grain, state, _, lattice, shard) = CreateGrain(
            configureFactory: f => f.GetGrain<ITxRegistryGrain>(TreeId).Returns(registry));
        shard.GetSplitForwardTargetsAsync().Returns(Task.FromResult(new List<int>()));
        return (grain, state, lattice, registry);
    }

    [Test]
    public async Task ExecuteAsync_re_asserts_the_participant_row_before_dispatching_when_the_bulk_registration_failed()
    {
        var (grain, state, lattice, registry) = CreateParticipantRowGrain(
            call => call == 1 ? Task.FromException(new TimeoutException("registry blip")) : Task.CompletedTask);

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]), ("b", [2])));

        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        Received.InOrder(() =>
        {
            registry.RegisterParticipantsAsync(Arg.Any<Guid>(), Arg.Any<IReadOnlyList<int>>());
            registry.RegisterParticipantsAsync(Arg.Any<Guid>(), Arg.Any<IReadOnlyList<int>>());
            lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public async Task ExecuteAsync_does_not_register_the_participant_row_again_when_the_bulk_registration_landed()
    {
        var (grain, state, _, registry) = CreateParticipantRowGrain(_ => Task.CompletedTask);

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1])));

        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed));
        await registry.Received(1).RegisterParticipantsAsync(state.State.TransactionId, Arg.Any<IReadOnlyList<int>>());
    }

    [Test]
    public void ExecuteAsync_dispatches_no_prepare_while_the_participant_row_cannot_be_registered()
    {
        var (grain, _, lattice, _) = CreateParticipantRowGrain(
            _ => Task.FromException(new TimeoutException("registry down")));

        Assert.CatchAsync(() => grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]))));

        lattice.DidNotReceive().SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<CancellationToken>());
    }
}
