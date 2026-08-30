using Orleans.Lattice.Explorer.Plugins.MyTenant;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The tenant-isolation guard: which half of a two-step cross-tenant agreement
/// the active tenant may drive, and - more importantly - which halves it may
/// not.
/// <para>
/// This is where "an admin of tenant A can never mutate tenant B through this
/// plugin" is a decision rather than a hope. The workspace routes every
/// transition through this type before a call leaves the process, so a refusal
/// here is a call that never happens.
/// </para>
/// </summary>
[TestFixture]
public sealed class TenantGrantScopeTests
{
    private const string A = MyTenantSample.TenantId;
    private const string B = MyTenantSample.OtherTenantId;
    private const string C = MyTenantSample.ThirdTenantId;

    [Test]
    public void A_grant_this_tenant_issued_is_outbound() =>
        Assert.That(
            TenantGrantScope.Direction(A, MyTenantSample.Grant(granter: A, grantee: B)),
            Is.EqualTo(TenantGrantDirection.Outbound));

    [Test]
    public void A_grant_offered_to_this_tenant_is_inbound() =>
        Assert.That(
            TenantGrantScope.Direction(A, MyTenantSample.Grant(granter: B, grantee: A)),
            Is.EqualTo(TenantGrantDirection.Inbound));

    [Test]
    public void A_grant_between_two_other_tenants_is_unrelated() =>
        Assert.That(
            TenantGrantScope.Direction(A, MyTenantSample.Grant(granter: B, grantee: C)),
            Is.EqualTo(TenantGrantDirection.Unrelated));

    [Test]
    public void A_caller_with_no_active_tenant_has_no_side()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                TenantGrantScope.Direction(null, MyTenantSample.Grant(granter: A, grantee: B)),
                Is.EqualTo(TenantGrantDirection.Unrelated));
            Assert.That(
                TenantGrantScope.Direction(string.Empty, MyTenantSample.Grant(granter: A, grantee: B)),
                Is.EqualTo(TenantGrantDirection.Unrelated));
        });
    }

    [Test]
    public void Tenant_matching_is_ordinal_so_a_case_variant_is_a_different_tenant() =>
        Assert.That(
            TenantGrantScope.Direction("ACME", MyTenantSample.Grant(granter: A, grantee: B)),
            Is.EqualTo(TenantGrantDirection.Unrelated));

    [Test]
    public void A_self_grant_resolves_to_outbound_rather_than_offering_self_approval()
    {
        var grant = MyTenantSample.Grant(granter: A, grantee: A);

        Assert.Multiple(() =>
        {
            Assert.That(TenantGrantScope.Direction(A, grant), Is.EqualTo(TenantGrantDirection.Outbound));
            Assert.That(
                TenantGrantScope.Allows(A, grant, TenantGrantActions.Approve),
                Is.False,
                "a tenant naming itself on both sides must not become able to approve its own offer");
        });
    }

    [Test]
    public void The_grantee_may_approve_or_reject_a_pending_offer()
    {
        var grant = MyTenantSample.Grant(granter: B, grantee: A, state: ExplorerTenantGrantState.Pending);

        Assert.That(
            TenantGrantScope.Available(A, grant),
            Is.EqualTo(TenantGrantActions.Approve | TenantGrantActions.Reject));
    }

    [Test]
    public void The_granter_may_not_approve_its_own_pending_offer()
    {
        var grant = MyTenantSample.Grant(granter: A, grantee: B, state: ExplorerTenantGrantState.Pending);

        Assert.Multiple(() =>
        {
            Assert.That(TenantGrantScope.Available(A, grant), Is.EqualTo(TenantGrantActions.None));
            Assert.That(TenantGrantScope.Allows(A, grant, TenantGrantActions.Approve), Is.False);
            Assert.That(TenantGrantScope.Allows(A, grant, TenantGrantActions.Reject), Is.False);
        });
    }

    [Test]
    public void An_admin_of_a_third_tenant_may_do_nothing_to_the_grant()
    {
        var grant = MyTenantSample.Grant(granter: A, grantee: B, state: ExplorerTenantGrantState.Pending);

        Assert.Multiple(() =>
        {
            Assert.That(TenantGrantScope.Available(C, grant), Is.EqualTo(TenantGrantActions.None));
            Assert.That(TenantGrantScope.Allows(C, grant, TenantGrantActions.Approve), Is.False);
            Assert.That(TenantGrantScope.Allows(C, grant, TenantGrantActions.Reject), Is.False);
            Assert.That(TenantGrantScope.Allows(C, grant, TenantGrantActions.Revoke), Is.False);
        });
    }

    [Test]
    public void Either_party_may_withdraw_a_live_grant()
    {
        var grant = MyTenantSample.Grant(granter: A, grantee: B, state: ExplorerTenantGrantState.Active);

        Assert.Multiple(() =>
        {
            Assert.That(TenantGrantScope.Available(A, grant), Is.EqualTo(TenantGrantActions.Revoke));
            Assert.That(TenantGrantScope.Available(B, grant), Is.EqualTo(TenantGrantActions.Revoke));
            Assert.That(TenantGrantScope.Available(C, grant), Is.EqualTo(TenantGrantActions.None));
        });
    }

    [Test]
    public void A_pending_grant_may_not_be_withdrawn()
    {
        var grant = MyTenantSample.Grant(granter: A, grantee: B, state: ExplorerTenantGrantState.Pending);

        Assert.That(TenantGrantScope.Allows(A, grant, TenantGrantActions.Revoke), Is.False);
    }

    [Test]
    public void A_closed_grant_admits_nothing_from_either_party()
    {
        foreach (var state in new[] { ExplorerTenantGrantState.Rejected, ExplorerTenantGrantState.Revoked })
        {
            var grant = MyTenantSample.Grant(granter: B, grantee: A, state: state);

            Assert.Multiple(() =>
            {
                Assert.That(TenantGrantScope.Available(A, grant), Is.EqualTo(TenantGrantActions.None), state.ToString());
                Assert.That(TenantGrantScope.Available(B, grant), Is.EqualTo(TenantGrantActions.None), state.ToString());
            });
        }
    }

    [Test]
    public void A_combination_of_flags_is_not_a_single_action()
    {
        var grant = MyTenantSample.Grant(granter: B, grantee: A, state: ExplorerTenantGrantState.Pending);

        Assert.Multiple(() =>
        {
            Assert.That(
                TenantGrantScope.Allows(A, grant, TenantGrantActions.Approve | TenantGrantActions.Reject),
                Is.False,
                "an action is one transition, so a combined flag must not be honoured");
            Assert.That(TenantGrantScope.Allows(A, grant, TenantGrantActions.None), Is.False);
        });
    }

    [Test]
    public void Authorize_names_the_reason_a_granter_cannot_approve()
    {
        var grant = MyTenantSample.Grant(granter: A, grantee: B, state: ExplorerTenantGrantState.Pending);

        var authorized = TenantGrantScope.Authorize(A, grant, TenantGrantActions.Approve, out var refusal);

        Assert.Multiple(() =>
        {
            Assert.That(authorized, Is.False);
            Assert.That(refusal, Is.EqualTo(TenantGrantScope.NotGranteeMessage));
        });
    }

    [Test]
    public void Authorize_names_the_reason_a_stranger_cannot_act()
    {
        var grant = MyTenantSample.Grant(granter: B, grantee: C, state: ExplorerTenantGrantState.Pending);

        var authorized = TenantGrantScope.Authorize(A, grant, TenantGrantActions.Approve, out var refusal);

        Assert.Multiple(() =>
        {
            Assert.That(authorized, Is.False);
            Assert.That(refusal, Is.EqualTo(TenantGrantScope.UnrelatedGrantMessage));
        });
    }

    [Test]
    public void Authorize_names_a_state_that_does_not_admit_the_transition()
    {
        var grant = MyTenantSample.Grant(granter: B, grantee: A, state: ExplorerTenantGrantState.Revoked);

        var authorized = TenantGrantScope.Authorize(A, grant, TenantGrantActions.Approve, out var refusal);

        Assert.Multiple(() =>
        {
            Assert.That(authorized, Is.False);
            Assert.That(refusal, Is.EqualTo(TenantGrantScope.WrongStateMessage));
        });
    }

    [Test]
    public void Authorize_reports_no_refusal_when_it_permits_the_transition()
    {
        var grant = MyTenantSample.Grant(granter: B, grantee: A, state: ExplorerTenantGrantState.Pending);

        var authorized = TenantGrantScope.Authorize(A, grant, TenantGrantActions.Approve, out var refusal);

        Assert.Multiple(() =>
        {
            Assert.That(authorized, Is.True);
            Assert.That(refusal, Is.Null);
        });
    }

    [Test]
    public void Only_a_tenants_own_data_may_be_offered()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantGrantScope.AllowsOffer(A, A), Is.True);
            Assert.That(TenantGrantScope.AllowsOffer(A, B), Is.False, "A cannot offer a grant from B");
            Assert.That(TenantGrantScope.AllowsOffer(null, A), Is.False);
            Assert.That(TenantGrantScope.AllowsOffer(A, null), Is.False);
            Assert.That(TenantGrantScope.AllowsOffer(string.Empty, string.Empty), Is.False);
        });
    }

    [Test]
    public void Every_refusal_message_is_distinct_so_the_surface_can_say_which_one_it_is() =>
        Assert.That(
            new[]
            {
                TenantGrantScope.UnrelatedGrantMessage,
                TenantGrantScope.NotGranteeMessage,
                TenantGrantScope.NotGranterMessage,
                TenantGrantScope.WrongStateMessage,
            },
            Is.Unique);
}
