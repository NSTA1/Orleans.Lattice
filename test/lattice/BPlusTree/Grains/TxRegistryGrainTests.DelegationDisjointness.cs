using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the disjointness premise the registry's two
/// cross-tree delegation maps rest on: a transaction id may be delegated to an
/// authoring coordinator or to a receiver coordinator, never to both.
/// <para>
/// Five sites across two packages read the premise as given and none of them
/// establishes it - each defers, correctly, to the next, and the chain
/// terminates in a subsumption rather than an enforcer. These tests are the
/// artefact that terminates it. They check the consequence (two rows coexisting)
/// rather than the cause (a terminal arriving with a local origin), because the
/// consequence is the only formulation expressible in operands the core holds.
/// </para>
/// </summary>
public partial class TxRegistryGrainTests
{
    [Test]
    public async Task RegisterReceiverDecisionAuthorityAsync_rejects_a_txid_already_delegated_outbound()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "authoring-coordinator");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.RegisterReceiverDecisionAuthorityAsync(txid, "receiver-coordinator"));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain(txid.ToString()));
            Assert.That(state.State.ReceiverDecisionAuthorities, Is.Empty,
                "The guard must run before any mutation, so a rejected "
                + "registration needs no unwind.");
            Assert.That(state.State.ExternalAuthorities[txid], Is.EqualTo("authoring-coordinator"),
                "The existing delegation must survive the rejection unchanged.");
        });
    }

    [Test]
    public async Task RegisterExternalDecisionAuthorityAsync_rejects_a_txid_already_delegated_inbound()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "receiver-coordinator");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.RegisterExternalDecisionAuthorityAsync(txid, "authoring-coordinator"));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain(txid.ToString()));
            Assert.That(state.State.ExternalAuthorities, Is.Empty);
            Assert.That(state.State.ReceiverDecisionAuthorities[txid],
                Is.EqualTo("receiver-coordinator"));
        });
    }

    [Test]
    public async Task Rejecting_a_coexisting_registration_does_not_advance_the_registration_epoch()
    {
        // The epoch is the backup fence's evidence that a saga both registered
        // and completed inside a capture window. A rejected registration is not
        // a registration, so bumping it would make the fence re-observe a
        // window it had already certified.
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "authoring-coordinator");
        var epoch = state.State.CrossTreeRegistrationEpoch;
        var writes = state.WriteCount;

        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.RegisterReceiverDecisionAuthorityAsync(txid, "receiver-coordinator"));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.CrossTreeRegistrationEpoch, Is.EqualTo(epoch));
            Assert.That(state.WriteCount, Is.EqualTo(writes),
                "A rejected registration must not persist anything.");
        });
    }

    [Test]
    public async Task Registering_both_sides_for_distinct_txids_is_permitted()
    {
        // The premise is per transaction id, not per registry. A tree that both
        // authors one cross-tree saga and receives another is ordinary.
        var (grain, state) = CreateGrain();
        var authored = Guid.NewGuid();
        var received = Guid.NewGuid();

        await grain.RegisterExternalDecisionAuthorityAsync(authored, "authoring-coordinator");
        await grain.RegisterReceiverDecisionAuthorityAsync(received, "receiver-coordinator");

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ExternalAuthorities.Keys, Is.EqualTo(new[] { authored }));
            Assert.That(state.State.ReceiverDecisionAuthorities.Keys, Is.EqualTo(new[] { received }));
        });
    }

    [Test]
    public async Task Re_registering_the_same_side_after_a_terminal_dropped_it_is_permitted()
    {
        // MarkCommittedAsync drops BOTH delegation rows, so the guard must not
        // treat a completed saga's history as an occupancy that blocks the
        // other side. This is the case a naive "has this txid ever been seen"
        // check would wrongly reject.
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "authoring-coordinator");
        await grain.MarkCommittedAsync(txid);
        state.State.Decisions.Remove(txid);

        Assert.DoesNotThrowAsync(
            () => grain.RegisterReceiverDecisionAuthorityAsync(txid, "receiver-coordinator"));
        Assert.That(state.State.ReceiverDecisionAuthorities[txid],
            Is.EqualTo("receiver-coordinator"));
    }

    [Test]
    public async Task An_idempotent_repeat_is_still_a_no_op_and_not_a_coexistence_rejection()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "authoring-coordinator");
        var writes = state.WriteCount;

        Assert.DoesNotThrowAsync(
            () => grain.RegisterExternalDecisionAuthorityAsync(txid, "authoring-coordinator"));
        Assert.That(state.WriteCount, Is.EqualTo(writes));
    }

    [Test]
    public async Task A_local_decision_still_supersedes_a_registration_before_the_guard_runs()
    {
        // The "a recorded terminal supersedes any delegation" early return sits
        // above the guard, so a registration that arrives after the saga
        // finalized is ignored rather than rejected. Ordering matters: swapping
        // them would turn a benign late registration into a throw.
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RegisterReceiverDecisionAuthorityAsync(txid, "receiver-coordinator");
        state.State.Decisions[txid] = TxStatus.Committed;

        Assert.DoesNotThrowAsync(
            () => grain.RegisterExternalDecisionAuthorityAsync(txid, "authoring-coordinator"));
        Assert.That(state.State.ExternalAuthorities, Is.Empty);
    }

    [Test]
    public void The_guard_reports_both_maps_by_name_so_the_violated_invariant_is_identifiable()
    {
        var (grain, _) = CreateGrain();
        var txid = Guid.NewGuid();

        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await grain.RegisterExternalDecisionAuthorityAsync(txid, "authoring-coordinator");
            await grain.RegisterReceiverDecisionAuthorityAsync(txid, "receiver-coordinator");
        });

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message,
                Does.Contain(nameof(TxRegistryState.ExternalAuthorities)));
            Assert.That(ex.Message,
                Does.Contain(nameof(TxRegistryState.ReceiverDecisionAuthorities)));
        });
    }
}
