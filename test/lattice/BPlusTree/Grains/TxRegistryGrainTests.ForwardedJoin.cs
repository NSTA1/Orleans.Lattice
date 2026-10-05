using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4632. A forwarded prepare's participant registration joins the row its
/// saga's coordinator holds but never creates one, so a forward delivered after
/// the saga was forgotten cannot make it read as live to the destination leaf's
/// refusal.
/// </summary>
public partial class TxRegistryGrainTests
{
    private const string ForwardedJoinTreeId = "tree-x";

    private static async Task RegisterForwardedAsync(TxRegistryGrain grain, Guid txid, int shardIndex, string registryTreeId = ForwardedJoinTreeId)
    {
        using (LatticeForwardedPrepareContext.BeginScope(registryTreeId))
        {
            await grain.RegisterParticipantAsync(txid, shardIndex);
        }
    }

    [Test]
    public async Task A_forwarded_registration_of_a_forgotten_saga_does_not_recreate_its_participant_row()
    {
        var (grain, state) = CreateGrain(treeId: ForwardedJoinTreeId);
        var txid = Guid.NewGuid();
        await grain.RegisterParticipantsAsync(txid, [0]);
        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);
        var writes = state.WriteCount;

        await RegisterForwardedAsync(grain, txid, 1);

        Assert.Multiple(async () =>
        {
            Assert.That(await grain.GetParticipantsAsync(txid), Is.Empty);
            Assert.That(state.State.Participants.ContainsKey(txid), Is.False);
            Assert.That(state.WriteCount, Is.EqualTo(writes), "a refused join writes nothing");
        });
    }

    [Test]
    public async Task A_forwarded_registration_of_a_live_saga_joins_its_participant_row()
    {
        var (grain, _) = CreateGrain(treeId: ForwardedJoinTreeId);
        var txid = Guid.NewGuid();
        await grain.RegisterParticipantsAsync(txid, [0]);

        await RegisterForwardedAsync(grain, txid, 1);

        Assert.That(await grain.GetParticipantsAsync(txid), Is.EqualTo(new[] { 0, 1 }));
    }

    [Test]
    public async Task A_forwarded_registration_naming_another_registry_still_creates_the_row()
    {
        // The marker names the registry that records the saga's decision. A
        // registration landing on another one (a resized tree's physical copy)
        // is not the row the refusal reads, so it keeps the original behaviour.
        var (grain, _) = CreateGrain(treeId: ForwardedJoinTreeId);
        var txid = Guid.NewGuid();

        await RegisterForwardedAsync(grain, txid, 1, registryTreeId: "logical-elsewhere");

        Assert.That(await grain.GetParticipantsAsync(txid), Is.EqualTo(new[] { 1 }));
    }

    [Test]
    public async Task A_forwarded_registration_of_a_replicated_saga_still_creates_the_row()
    {
        var (grain, _) = CreateGrain(treeId: ForwardedJoinTreeId);
        var txid = Guid.NewGuid();

        using (LatticeOriginContext.With("peer-cluster"))
        {
            await RegisterForwardedAsync(grain, txid, 1);
        }

        Assert.That(await grain.GetParticipantsAsync(txid), Is.EqualTo(new[] { 1 }));
    }

    [Test]
    public async Task A_coordinator_registration_still_creates_the_row()
    {
        var (grain, _) = CreateGrain(treeId: ForwardedJoinTreeId);
        var txid = Guid.NewGuid();

        await grain.RegisterParticipantAsync(txid, 2);

        Assert.That(await grain.GetParticipantsAsync(txid), Is.EqualTo(new[] { 2 }));
    }
}
