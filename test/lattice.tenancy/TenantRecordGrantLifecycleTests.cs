using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for the cross-tenant grant lifecycle on <see cref="TenantRecord"/>:
/// the offer and transition writers, the legality guards they enforce, the
/// agreement generation that lets a terminally closed grant be re-opened, and the
/// convergence of concurrent transitions written by the two parties.
/// </summary>
/// <remarks>
/// Every case is driven by hand-built stamps and explicit merges, so nothing here
/// depends on threads, delays, or the wall clock. Concurrency is modelled the way
/// the CRDT actually resolves it - two divergent records merged - rather than by
/// racing tasks.
/// </remarks>
public sealed class TenantRecordGrantLifecycleTests
{
    private const string Granter = "acme";
    private const string Grantee = "beta";
    private const string Scope = "orders";

    private static TenantRecord NewRecord(string tenantId = Granter) =>
        TenantRecord.Create(
            TenantId.Parse(tenantId),
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            Clock(1),
            "test");

    private static CrossTenantGrant Offer(
        TenantGrantOperations operations = TenantGrantOperations.Read, string scope = Scope) =>
        CrossTenantGrant.Create(Grantee, TenantGranteeKind.Tenant, scope, operations);

    private static string GrantIdFor(string scope = Scope) => Offer(scope: scope).GrantId;

    private static TenantGrantState StateOf(TenantRecord record, string scope = Scope) =>
        record.TryGetGrant(GrantIdFor(scope), out var grant)
            ? grant.State
            : throw new InvalidOperationException("the grant is not live on the record");

    // ---- offer -------------------------------------------------------------

    [Test]
    public void OfferGrant_creates_the_grant_pending_so_it_authorizes_nothing()
    {
        var record = NewRecord();

        record.OfferGrant(Offer(), Clock(10), "granter");

        Assert.Multiple(() =>
        {
            Assert.That(StateOf(record), Is.EqualTo(TenantGrantState.Pending));
            Assert.That(TenantGrantLifecycle.Authorizes(StateOf(record)), Is.False);
        });
    }

    [Test]
    public void OfferGrant_replaces_the_supplied_state_with_pending()
    {
        var record = NewRecord();

        // A caller cannot smuggle an active grant in through the offer path.
        record.OfferGrant(
            Offer() with { State = TenantGrantState.Active }, Clock(10), "granter");

        Assert.That(StateOf(record), Is.EqualTo(TenantGrantState.Pending));
    }

    [Test]
    public void OfferGrant_amending_unanswered_terms_keeps_the_grant_pending()
    {
        var record = NewRecord();
        record.OfferGrant(Offer(TenantGrantOperations.Read), Clock(10), "granter");

        record.OfferGrant(Offer(TenantGrantOperations.ReadWrite), Clock(20), "granter");

        record.TryGetGrant(GrantIdFor(), out var grant);
        Assert.Multiple(() =>
        {
            Assert.That(grant.State, Is.EqualTo(TenantGrantState.Pending));
            Assert.That(grant.Operations, Is.EqualTo(TenantGrantOperations.ReadWrite));
        });
    }

    [Test]
    public void OfferGrant_over_an_active_grant_is_refused()
    {
        var record = NewRecord();
        record.OfferGrant(Offer(), Clock(10), "granter");
        record.TransitionGrant(GrantIdFor(), TenantGrantState.Active, Clock(20), "grantee");

        Assert.That(
            () => record.OfferGrant(Offer(TenantGrantOperations.ReadWrite), Clock(30), "granter"),
            Throws.InvalidOperationException);
    }

    [Test]
    public void OfferGrant_re_opens_a_revoked_agreement()
    {
        var record = NewRecord();
        record.OfferGrant(Offer(), Clock(10), "granter");
        record.TransitionGrant(GrantIdFor(), TenantGrantState.Active, Clock(20), "grantee");
        record.TransitionGrant(GrantIdFor(), TenantGrantState.Revoked, Clock(30), "granter");

        record.OfferGrant(Offer(), Clock(40), "granter");

        Assert.That(
            StateOf(record),
            Is.EqualTo(TenantGrantState.Pending),
            "a terminal state must not permanently poison the grantee/scope pair");
    }

    [Test]
    public void OfferGrant_re_opens_a_rejected_agreement()
    {
        var record = NewRecord();
        record.OfferGrant(Offer(), Clock(10), "granter");
        record.TransitionGrant(GrantIdFor(), TenantGrantState.Rejected, Clock(20), "grantee");

        record.OfferGrant(Offer(), Clock(30), "granter");

        Assert.That(StateOf(record), Is.EqualTo(TenantGrantState.Pending));
    }

    [Test]
    public void OfferGrant_rejects_a_grant_with_no_identity()
    {
        var record = NewRecord();

        Assert.That(
            () => record.OfferGrant(default, Clock(10), "granter"),
            Throws.ArgumentException);
    }

    // ---- transitions -------------------------------------------------------

    [Test]
    public void TransitionGrant_approves_a_pending_grant()
    {
        var record = NewRecord();
        record.OfferGrant(Offer(), Clock(10), "granter");

        record.TransitionGrant(GrantIdFor(), TenantGrantState.Active, Clock(20), "grantee");

        Assert.That(StateOf(record), Is.EqualTo(TenantGrantState.Active));
    }

    [Test]
    public void TransitionGrant_rejects_a_pending_grant()
    {
        var record = NewRecord();
        record.OfferGrant(Offer(), Clock(10), "granter");

        record.TransitionGrant(GrantIdFor(), TenantGrantState.Rejected, Clock(20), "grantee");

        Assert.That(StateOf(record), Is.EqualTo(TenantGrantState.Rejected));
    }

    [Test]
    public void TransitionGrant_revokes_an_active_grant()
    {
        var record = NewRecord();
        record.OfferGrant(Offer(), Clock(10), "granter");
        record.TransitionGrant(GrantIdFor(), TenantGrantState.Active, Clock(20), "grantee");

        record.TransitionGrant(GrantIdFor(), TenantGrantState.Revoked, Clock(30), "granter");

        Assert.That(StateOf(record), Is.EqualTo(TenantGrantState.Revoked));
    }

    [Test]
    public void TransitionGrant_preserves_the_grants_terms()
    {
        var record = NewRecord();
        record.OfferGrant(Offer(TenantGrantOperations.ReadWrite), Clock(10), "granter");

        record.TransitionGrant(GrantIdFor(), TenantGrantState.Active, Clock(20), "grantee");

        record.TryGetGrant(GrantIdFor(), out var grant);
        Assert.That(grant.Operations, Is.EqualTo(TenantGrantOperations.ReadWrite));
    }

    [Test]
    public void TransitionGrant_refuses_revoking_a_pending_offer()
    {
        var record = NewRecord();
        record.OfferGrant(Offer(), Clock(10), "granter");

        Assert.That(
            () => record.TransitionGrant(GrantIdFor(), TenantGrantState.Revoked, Clock(20), "granter"),
            Throws.InvalidOperationException);
    }

    [Test]
    public void TransitionGrant_refuses_approving_a_revoked_grant()
    {
        var record = NewRecord();
        record.OfferGrant(Offer(), Clock(10), "granter");
        record.TransitionGrant(GrantIdFor(), TenantGrantState.Active, Clock(20), "grantee");
        record.TransitionGrant(GrantIdFor(), TenantGrantState.Revoked, Clock(30), "granter");

        Assert.That(
            () => record.TransitionGrant(GrantIdFor(), TenantGrantState.Active, Clock(40), "grantee"),
            Throws.InvalidOperationException);
    }

    [Test]
    public void TransitionGrant_refuses_approving_a_rejected_grant()
    {
        var record = NewRecord();
        record.OfferGrant(Offer(), Clock(10), "granter");
        record.TransitionGrant(GrantIdFor(), TenantGrantState.Rejected, Clock(20), "grantee");

        Assert.That(
            () => record.TransitionGrant(GrantIdFor(), TenantGrantState.Active, Clock(30), "grantee"),
            Throws.InvalidOperationException);
    }

    [Test]
    public void TransitionGrant_refuses_rejecting_an_active_grant()
    {
        var record = NewRecord();
        record.OfferGrant(Offer(), Clock(10), "granter");
        record.TransitionGrant(GrantIdFor(), TenantGrantState.Active, Clock(20), "grantee");

        Assert.That(
            () => record.TransitionGrant(GrantIdFor(), TenantGrantState.Rejected, Clock(30), "grantee"),
            Throws.InvalidOperationException);
    }

    [Test]
    public void TransitionGrant_refuses_a_grant_that_does_not_exist()
    {
        var record = NewRecord();

        Assert.That(
            () => record.TransitionGrant(GrantIdFor(), TenantGrantState.Active, Clock(10), "grantee"),
            Throws.InvalidOperationException);
    }

    [Test]
    public void TransitionGrant_refuses_a_hard_removed_grant()
    {
        var record = NewRecord();
        record.OfferGrant(Offer(), Clock(10), "granter");
        record.RemoveGrant(GrantIdFor(), Clock(20), "granter");

        Assert.That(
            () => record.TransitionGrant(GrantIdFor(), TenantGrantState.Active, Clock(30), "grantee"),
            Throws.InvalidOperationException);
    }

    [Test]
    public void TransitionGrant_null_grant_id_throws()
    {
        var record = NewRecord();

        Assert.That(
            () => record.TransitionGrant(null!, TenantGrantState.Active, Clock(10), "grantee"),
            Throws.ArgumentNullException);
    }

    // ---- convergence of concurrent two-party transitions -------------------

    /// <summary>
    /// The defining race of the two-step design: the grantee approves on its
    /// replica while the granting tenant revokes on its own. Both writes land, and
    /// the merge must converge on the terminal, denying outcome - even though the
    /// approve carries the higher stamp and would win a plain last-writer-wins
    /// merge.
    /// </summary>
    [Test]
    public void Concurrent_approve_and_revoke_converge_to_revoked()
    {
        var granteeReplica = NewRecord();
        granteeReplica.OfferGrant(Offer(), Clock(10), "granter");
        granteeReplica.TransitionGrant(GrantIdFor(), TenantGrantState.Active, Clock(20), "grantee");

        var granterReplica = NewRecord();
        granterReplica.OfferGrant(Offer(), Clock(10), "granter");
        granterReplica.TransitionGrant(GrantIdFor(), TenantGrantState.Active, Clock(11), "grantee");
        granterReplica.TransitionGrant(GrantIdFor(), TenantGrantState.Revoked, Clock(12), "granter");

        // The approve is stamped ahead of the revoke, so a plain LWW merge would
        // reinstate access. Both merge directions must still deny.
        var granteeView = granteeReplica.Clone().MergeFrom(granterReplica);
        var granterView = granterReplica.Clone().MergeFrom(granteeReplica);

        Assert.Multiple(() =>
        {
            Assert.That(StateOf(granteeView), Is.EqualTo(TenantGrantState.Revoked));
            Assert.That(StateOf(granterView), Is.EqualTo(TenantGrantState.Revoked));
            Assert.That(TenantGrantLifecycle.Authorizes(StateOf(granteeView)), Is.False);
        });
    }

    [Test]
    public void Concurrent_approve_and_reject_converge_to_rejected()
    {
        var approver = NewRecord();
        approver.OfferGrant(Offer(), Clock(10), "granter");
        approver.TransitionGrant(GrantIdFor(), TenantGrantState.Active, Clock(30), "grantee-a");

        var rejecter = NewRecord();
        rejecter.OfferGrant(Offer(), Clock(10), "granter");
        rejecter.TransitionGrant(GrantIdFor(), TenantGrantState.Rejected, Clock(20), "grantee-b");

        Assert.Multiple(() =>
        {
            Assert.That(
                StateOf(approver.Clone().MergeFrom(rejecter)), Is.EqualTo(TenantGrantState.Rejected));
            Assert.That(
                StateOf(rejecter.Clone().MergeFrom(approver)), Is.EqualTo(TenantGrantState.Rejected));
        });
    }

    /// <summary>
    /// The other widening route the generation closes: the granting tenant amends
    /// its offer while the grantee is approving the terms it can currently see. The
    /// approval must not attach to the new terms.
    /// </summary>
    [Test]
    public void Concurrent_amended_offer_and_approve_converge_to_pending_on_the_new_terms()
    {
        var granteeReplica = NewRecord();
        granteeReplica.OfferGrant(Offer(TenantGrantOperations.Read), Clock(10), "granter");
        granteeReplica.TransitionGrant(GrantIdFor(), TenantGrantState.Active, Clock(90), "grantee");

        var granterReplica = NewRecord();
        granterReplica.OfferGrant(Offer(TenantGrantOperations.Read), Clock(10), "granter");
        granterReplica.OfferGrant(Offer(TenantGrantOperations.ReadWrite), Clock(20), "granter");

        var merged = granteeReplica.Clone().MergeFrom(granterReplica);
        merged.TryGetGrant(GrantIdFor(), out var grant);

        Assert.Multiple(() =>
        {
            Assert.That(
                grant.State,
                Is.EqualTo(TenantGrantState.Pending),
                "an approval of superseded terms must not activate the amended ones");
            Assert.That(grant.Operations, Is.EqualTo(TenantGrantOperations.ReadWrite));
        });
    }

    [Test]
    public void Concurrent_re_offer_and_stale_approve_of_a_closed_agreement_converge_to_pending()
    {
        var stale = NewRecord();
        stale.OfferGrant(Offer(), Clock(10), "granter");
        stale.TransitionGrant(GrantIdFor(), TenantGrantState.Active, Clock(99), "grantee");

        var reopened = NewRecord();
        reopened.OfferGrant(Offer(), Clock(10), "granter");
        reopened.TransitionGrant(GrantIdFor(), TenantGrantState.Rejected, Clock(20), "grantee");
        reopened.OfferGrant(Offer(), Clock(30), "granter");

        Assert.That(
            StateOf(reopened.Clone().MergeFrom(stale)), Is.EqualTo(TenantGrantState.Pending));
    }

    [Test]
    public void Merging_grant_state_is_commutative_across_replicas()
    {
        var first = NewRecord();
        first.OfferGrant(Offer(), Clock(10), "granter");
        first.TransitionGrant(GrantIdFor(), TenantGrantState.Active, Clock(40), "grantee");

        var second = NewRecord();
        second.OfferGrant(Offer(), Clock(10), "granter");
        second.TransitionGrant(GrantIdFor(), TenantGrantState.Active, Clock(20), "grantee");
        second.TransitionGrant(GrantIdFor(), TenantGrantState.Revoked, Clock(30), "granter");

        Assert.That(
            StateOf(first.Clone().MergeFrom(second)),
            Is.EqualTo(StateOf(second.Clone().MergeFrom(first))));
    }

    // ---- projections -------------------------------------------------------

    [Test]
    public void Grants_includes_a_pending_grant_so_the_grantee_can_see_its_inbox()
    {
        var record = NewRecord();
        record.OfferGrant(Offer(), Clock(10), "granter");

        Assert.That(record.Grants, Has.Count.EqualTo(1));
    }

    [Test]
    public void Grants_includes_a_terminally_closed_grant()
    {
        var record = NewRecord();
        record.OfferGrant(Offer(), Clock(10), "granter");
        record.TransitionGrant(GrantIdFor(), TenantGrantState.Rejected, Clock(20), "grantee");

        Assert.That(record.Grants, Has.Count.EqualTo(1));
    }

    [Test]
    public void GrantCount_counts_only_live_grants()
    {
        var record = NewRecord();

        Assert.That(record.GrantCount, Is.Zero);

        record.OfferGrant(Offer(scope: "orders"), Clock(10), "granter");
        record.OfferGrant(Offer(scope: "invoices"), Clock(20), "granter");

        Assert.That(record.GrantCount, Is.EqualTo(2));

        record.RemoveGrant(GrantIdFor("invoices"), Clock(30), "granter");

        Assert.That(record.GrantCount, Is.EqualTo(1));
    }

    [Test]
    public void GrantCount_agrees_with_the_materialised_projection()
    {
        var record = NewRecord();
        record.OfferGrant(Offer(scope: "orders"), Clock(10), "granter");
        record.OfferGrant(Offer(scope: "invoices"), Clock(20), "granter");
        record.RemoveGrant(GrantIdFor("orders"), Clock(30), "granter");

        Assert.That(record.GrantCount, Is.EqualTo(record.Grants.Count));
    }

    // ---- the pre-existing single-step path is unchanged --------------------

    [Test]
    public void AddGrant_still_issues_a_grant_that_authorizes_immediately()
    {
        var record = NewRecord();

        record.AddGrant(Offer(), Clock(10), "host");

        Assert.That(
            StateOf(record),
            Is.EqualTo(TenantGrantState.Active),
            "the shipped in-process issue path must keep its pre-lifecycle meaning");
    }

    [Test]
    public void AddGrant_still_updates_the_terms_of_an_existing_grant_in_place()
    {
        var record = NewRecord();
        record.AddGrant(Offer(TenantGrantOperations.Read), Clock(10), "host");

        record.AddGrant(Offer(TenantGrantOperations.ReadWrite), Clock(20), "host");

        record.TryGetGrant(GrantIdFor(), out var grant);
        Assert.Multiple(() =>
        {
            Assert.That(grant.Operations, Is.EqualTo(TenantGrantOperations.ReadWrite));
            Assert.That(grant.State, Is.EqualTo(TenantGrantState.Active));
        });
    }

    [Test]
    public void AddGrant_re_opens_a_revoked_agreement_so_it_is_never_permanently_blocked()
    {
        var record = NewRecord();
        record.OfferGrant(Offer(), Clock(10), "granter");
        record.TransitionGrant(GrantIdFor(), TenantGrantState.Active, Clock(20), "grantee");
        record.TransitionGrant(GrantIdFor(), TenantGrantState.Revoked, Clock(30), "granter");

        record.AddGrant(Offer(), Clock(40), "host");

        Assert.That(StateOf(record), Is.EqualTo(TenantGrantState.Active));
    }

    [Test]
    public void RemoveGrant_still_tombstones_the_slot()
    {
        var record = NewRecord();
        record.AddGrant(Offer(), Clock(10), "host");

        record.RemoveGrant(GrantIdFor(), Clock(20), "host");

        Assert.That(record.TryGetGrant(GrantIdFor(), out _), Is.False);
    }

    [Test]
    public void A_grant_hard_removed_after_it_closed_can_still_be_issued_again()
    {
        // A terminal state is sticky under the merge join whether or not the slot
        // is present, so re-issuing over a hard-removed closed grant must start a
        // new agreement generation - otherwise the grant would come back present
        // but permanently inert.
        var record = NewRecord();
        record.OfferGrant(Offer(), Clock(10), "granter");
        record.TransitionGrant(GrantIdFor(), TenantGrantState.Active, Clock(20), "grantee");
        record.TransitionGrant(GrantIdFor(), TenantGrantState.Revoked, Clock(30), "granter");
        record.RemoveGrant(GrantIdFor(), Clock(40), "host");

        record.AddGrant(Offer(), Clock(50), "host");

        Assert.That(StateOf(record), Is.EqualTo(TenantGrantState.Active));
    }

    [Test]
    public void A_grant_hard_removed_after_it_closed_can_still_be_offered_again()
    {
        var record = NewRecord();
        record.OfferGrant(Offer(), Clock(10), "granter");
        record.TransitionGrant(GrantIdFor(), TenantGrantState.Rejected, Clock(20), "grantee");
        record.RemoveGrant(GrantIdFor(), Clock(30), "granter");

        record.OfferGrant(Offer(), Clock(40), "granter");

        Assert.That(StateOf(record), Is.EqualTo(TenantGrantState.Pending));
    }
}
