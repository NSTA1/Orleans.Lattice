using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Convergence unit tests for <see cref="TenantGrantSlot"/>, the LWW-element-map
/// element behind a tenant's cross-tenant grants. Both the grant payload and its
/// presence bit converge deterministically under the shared stamp order.
/// </summary>
public sealed class TenantGrantSlotTests
{
    private static readonly CrossTenantGrant Read =
        CrossTenantGrant.Create("sub-a", TenantGranteeKind.Subject, "tree-x", TenantGrantOperations.Read);

    private static readonly CrossTenantGrant ReadWrite =
        CrossTenantGrant.Create("sub-a", TenantGranteeKind.Subject, "tree-x", TenantGrantOperations.ReadWrite);

    private static TenantGrantSlot Slot(CrossTenantGrant grant, bool present, long ticks, string? writer) =>
        new() { Grant = grant, Present = present, Clock = Clock(ticks), WriterId = writer };

    private static TenantGrantSlot Slot(
        CrossTenantGrant grant, bool present, long ticks, string? writer, long generation) =>
        new() { Grant = grant, Present = present, Clock = Clock(ticks), WriterId = writer, Generation = generation };

    private static CrossTenantGrant In(CrossTenantGrant grant, TenantGrantState state) =>
        grant with { State = state };

    [Test]
    public void Merge_keeps_the_higher_clock_payload_and_presence()
    {
        var issued = Slot(Read, present: true, 10, "w1");
        var updated = Slot(ReadWrite, present: true, 20, "w1");

        var merged = TenantGrantSlot.Merge(issued, updated);

        Assert.Multiple(() =>
        {
            Assert.That(merged.Present, Is.True);
            Assert.That(merged.Grant.Operations, Is.EqualTo(TenantGrantOperations.ReadWrite));
        });
    }

    [Test]
    public void Merge_revoke_wins_over_a_lower_stamp_issue()
    {
        var issued = Slot(Read, present: true, 10, "w1");
        var revoked = Slot(Read, present: false, 20, "w1");

        Assert.That(TenantGrantSlot.Merge(issued, revoked).Present, Is.False);
    }

    [Test]
    public void Merge_is_commutative()
    {
        var issued = Slot(Read, present: true, 10, "w1");
        var revoked = Slot(Read, present: false, 20, "w2");

        Assert.That(TenantGrantSlot.Merge(issued, revoked), Is.EqualTo(TenantGrantSlot.Merge(revoked, issued)));
    }

    [Test]
    public void Merge_is_associative()
    {
        var a = Slot(Read, present: true, 10, "w1");
        var b = Slot(ReadWrite, present: true, 20, "w2");
        var c = Slot(Read, present: false, 30, "w3");

        var left = TenantGrantSlot.Merge(TenantGrantSlot.Merge(a, b), c);
        var right = TenantGrantSlot.Merge(a, TenantGrantSlot.Merge(b, c));

        Assert.That(left, Is.EqualTo(right));
        Assert.That(left.Present, Is.False);
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var slot = Slot(ReadWrite, present: true, 10, "w1");

        Assert.That(TenantGrantSlot.Merge(slot, slot), Is.EqualTo(slot));
    }

    [Test]
    public void Merge_breaks_a_clock_tie_by_ordinal_writer_id()
    {
        var loser = Slot(Read, present: true, 10, "w1");
        var winner = Slot(Read, present: false, 10, "w2");

        Assert.That(TenantGrantSlot.Merge(loser, winner).Present, Is.False);
    }

    // ---- the lifecycle state does not ride the stamp -----------------------

    [Test]
    public void Merge_keeps_a_revoke_that_lost_the_stamp_race_to_a_concurrent_approve()
    {
        // The two parties write from different replicas, so the stamp order says
        // nothing about which intent should survive. A revoke stamped behind an
        // approve must still win, or a party that walked away silently keeps
        // exposing its data.
        var revoked = Slot(In(Read, TenantGrantState.Revoked), present: true, 10, "granter");
        var approved = Slot(In(Read, TenantGrantState.Active), present: true, 20, "grantee");

        var merged = TenantGrantSlot.Merge(revoked, approved);

        Assert.That(merged.Grant.State, Is.EqualTo(TenantGrantState.Revoked));
    }

    [Test]
    public void Merge_returns_a_slot_verbatim_rather_than_grafting_a_state_onto_other_terms()
    {
        // Publishing one slot's state on the other's terms would create a
        // (terms, state) pair no writer ever wrote. The winner is the slot the
        // state order picks, returned whole.
        var revoked = Slot(In(Read, TenantGrantState.Revoked), present: true, 10, "granter");
        var approved = Slot(In(ReadWrite, TenantGrantState.Active), present: true, 20, "grantee");

        var merged = TenantGrantSlot.Merge(revoked, approved);

        Assert.Multiple(() =>
        {
            Assert.That(merged, Is.EqualTo(revoked));
            Assert.That(merged.Grant.State, Is.EqualTo(TenantGrantState.Revoked));
            Assert.That(merged.Grant.Operations, Is.EqualTo(TenantGrantOperations.Read));
        });
    }

    /// <summary>
    /// The escalation the verbatim rule exists to stop. A granting tenant amends
    /// an offer to wider terms while the grant is already approved on a converged
    /// replica at the same generation - which happens whenever the amender's read
    /// predates the slot. Publishing the pending offer's terms under the approved
    /// slot's state would bind the grantee to terms it never approved.
    /// </summary>
    [Test]
    public void Merge_never_publishes_widened_pending_terms_under_a_concurrent_active_state()
    {
        var approved = Slot(In(Read, TenantGrantState.Active), present: true, 10, "grantee");
        var widerOffer = Slot(In(ReadWrite, TenantGrantState.Pending), present: true, 20, "granter");

        Assert.Multiple(() =>
        {
            foreach (var merged in new[]
            {
                TenantGrantSlot.Merge(approved, widerOffer),
                TenantGrantSlot.Merge(widerOffer, approved),
            })
            {
                Assert.That(
                    merged.Grant.State is TenantGrantState.Active
                        && merged.Grant.Operations == TenantGrantOperations.ReadWrite,
                    Is.False,
                    "an unapproved widening was published as an active grant");
                Assert.That(merged, Is.EqualTo(approved));
            }
        });
    }

    /// <summary>
    /// The same hole reached through the tombstone path: a blind remove of a grant
    /// this replica has not seen carries a default payload whose state is the zero
    /// value, and grafting that state onto a concurrent first offer would publish
    /// it as a live, never-approved grant.
    /// </summary>
    [Test]
    public void Merge_of_a_blind_tombstone_and_a_concurrent_offer_never_yields_a_live_grant()
    {
        var blindTombstone = Slot(default, present: false, 20, "other-replica");
        var firstOffer = Slot(In(Read, TenantGrantState.Pending), present: true, 10, "granter");

        Assert.Multiple(() =>
        {
            foreach (var merged in new[]
            {
                TenantGrantSlot.Merge(blindTombstone, firstOffer),
                TenantGrantSlot.Merge(firstOffer, blindTombstone),
            })
            {
                Assert.That(
                    merged.Present && TenantGrantLifecycle.Authorizes(merged.Grant.State),
                    Is.False,
                    "a tombstone's synthesized state was published as a live grant");
            }
        });
    }

    [Test]
    public void Merge_lets_an_approval_beat_a_stale_pending_regardless_of_argument_order()
    {
        var pending = Slot(In(Read, TenantGrantState.Pending), present: true, 30, "granter");
        var approved = Slot(In(Read, TenantGrantState.Active), present: true, 10, "grantee");

        Assert.Multiple(() =>
        {
            Assert.That(
                TenantGrantSlot.Merge(pending, approved).Grant.State, Is.EqualTo(TenantGrantState.Active));
            Assert.That(
                TenantGrantSlot.Merge(approved, pending).Grant.State, Is.EqualTo(TenantGrantState.Active));
        });
    }

    [Test]
    public void Merge_is_commutative_across_a_concurrent_approve_and_revoke()
    {
        var approved = Slot(In(ReadWrite, TenantGrantState.Active), present: true, 20, "grantee");
        var revoked = Slot(In(Read, TenantGrantState.Revoked), present: true, 10, "granter");

        Assert.That(
            TenantGrantSlot.Merge(approved, revoked), Is.EqualTo(TenantGrantSlot.Merge(revoked, approved)));
    }

    [Test]
    public void Merge_is_associative_across_pending_approve_and_revoke()
    {
        var pending = Slot(In(Read, TenantGrantState.Pending), present: true, 10, "granter");
        var approved = Slot(In(Read, TenantGrantState.Active), present: true, 30, "grantee");
        var revoked = Slot(In(Read, TenantGrantState.Revoked), present: true, 20, "granter");

        var left = TenantGrantSlot.Merge(TenantGrantSlot.Merge(pending, approved), revoked);
        var right = TenantGrantSlot.Merge(pending, TenantGrantSlot.Merge(approved, revoked));

        Assert.Multiple(() =>
        {
            Assert.That(left, Is.EqualTo(right));
            Assert.That(left.Grant.State, Is.EqualTo(TenantGrantState.Revoked));
        });
    }

    // ---- generations -------------------------------------------------------

    [Test]
    public void Merge_lets_a_later_generation_win_outright_over_a_terminal_predecessor()
    {
        // Without this a revoked grant could never be offered again: the two share
        // a grant id, so they share a slot, and the terminal state is sticky.
        var closed = Slot(In(Read, TenantGrantState.Revoked), present: true, 90, "granter", generation: 0);
        var reoffered = Slot(In(Read, TenantGrantState.Pending), present: true, 10, "granter", generation: 1);

        Assert.Multiple(() =>
        {
            Assert.That(
                TenantGrantSlot.Merge(closed, reoffered).Grant.State, Is.EqualTo(TenantGrantState.Pending));
            Assert.That(
                TenantGrantSlot.Merge(reoffered, closed).Grant.State, Is.EqualTo(TenantGrantState.Pending));
        });
    }

    [Test]
    public void Merge_discards_an_approval_of_superseded_terms()
    {
        // A new offer states new terms, so an approval of the previous terms must
        // not attach to them - otherwise the granting tenant could widen a grant
        // by amending an offer the grantee is in the middle of approving.
        var approvedOldTerms = Slot(In(Read, TenantGrantState.Active), present: true, 90, "grantee", generation: 0);
        var newTerms = Slot(In(ReadWrite, TenantGrantState.Pending), present: true, 10, "granter", generation: 1);

        var merged = TenantGrantSlot.Merge(approvedOldTerms, newTerms);

        Assert.Multiple(() =>
        {
            Assert.That(merged.Grant.State, Is.EqualTo(TenantGrantState.Pending));
            Assert.That(merged.Grant.Operations, Is.EqualTo(TenantGrantOperations.ReadWrite));
            Assert.That(merged.Generation, Is.EqualTo(1));
        });
    }

    [Test]
    public void Merge_is_associative_across_generations()
    {
        var a = Slot(In(Read, TenantGrantState.Revoked), present: true, 10, "w1", generation: 0);
        var b = Slot(In(Read, TenantGrantState.Pending), present: true, 20, "w2", generation: 1);
        var c = Slot(In(Read, TenantGrantState.Active), present: true, 30, "w3", generation: 0);

        Assert.That(
            TenantGrantSlot.Merge(TenantGrantSlot.Merge(a, b), c),
            Is.EqualTo(TenantGrantSlot.Merge(a, TenantGrantSlot.Merge(b, c))));
    }

    [Test]
    public void Merge_within_one_generation_still_joins_restrictively()
    {
        var approved = Slot(In(Read, TenantGrantState.Active), present: true, 30, "grantee", generation: 2);
        var revoked = Slot(In(Read, TenantGrantState.Revoked), present: true, 10, "granter", generation: 2);

        Assert.That(
            TenantGrantSlot.Merge(approved, revoked).Grant.State, Is.EqualTo(TenantGrantState.Revoked));
    }
}
