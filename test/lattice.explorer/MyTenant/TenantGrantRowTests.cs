using Orleans.Lattice.Explorer.Plugins.MyTenant;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The grant row's projection and its display vocabulary: state is load-bearing
/// and travels with every row, and only an active grant reads as live access.
/// </summary>
[TestFixture]
public sealed class TenantGrantRowTests
{
    private const string A = MyTenantSample.TenantId;
    private const string B = MyTenantSample.OtherTenantId;
    private const string C = MyTenantSample.ThirdTenantId;

    [Test]
    public void A_pending_inbound_offer_is_the_row_needing_a_decision()
    {
        var row = TenantGrantRow.For(
            A,
            MyTenantSample.Grant(granter: B, grantee: A, state: ExplorerTenantGrantState.Pending));

        Assert.Multiple(() =>
        {
            Assert.That(row.Direction, Is.EqualTo(TenantGrantDirection.Inbound));
            Assert.That(row.NeedsDecision, Is.True);
            Assert.That(row.CanApprove, Is.True);
            Assert.That(row.CanReject, Is.True);
            Assert.That(row.CanRevoke, Is.False);
            Assert.That(row.AuthorizesAccess, Is.False, "a pending grant authorizes nothing");
            Assert.That(row.IsAwaitingApproval, Is.True);
        });
    }

    [Test]
    public void A_pending_outbound_offer_needs_no_decision_from_this_tenant()
    {
        var row = TenantGrantRow.For(
            A,
            MyTenantSample.Grant(granter: A, grantee: B, state: ExplorerTenantGrantState.Pending));

        Assert.Multiple(() =>
        {
            Assert.That(row.Direction, Is.EqualTo(TenantGrantDirection.Outbound));
            Assert.That(row.NeedsDecision, Is.False);
            Assert.That(row.CanApprove, Is.False);
            Assert.That(row.Actions, Is.EqualTo(TenantGrantActions.None));
        });
    }

    [Test]
    public void Only_an_active_grant_authorizes_access()
    {
        Assert.Multiple(() =>
        {
            foreach (var state in Enum.GetValues<ExplorerTenantGrantState>())
            {
                var row = TenantGrantRow.For(A, MyTenantSample.Grant(granter: B, grantee: A, state: state));
                Assert.That(
                    row.AuthorizesAccess,
                    Is.EqualTo(state == ExplorerTenantGrantState.Active),
                    state.ToString());
            }
        });
    }

    [Test]
    public void An_active_grant_offers_only_a_withdrawal()
    {
        var row = TenantGrantRow.For(
            A,
            MyTenantSample.Grant(granter: B, grantee: A, state: ExplorerTenantGrantState.Active));

        Assert.Multiple(() =>
        {
            Assert.That(row.CanRevoke, Is.True);
            Assert.That(row.CanApprove, Is.False);
            Assert.That(row.NeedsDecision, Is.False, "an already-live grant is not awaiting a decision");
        });
    }

    [Test]
    public void An_unrelated_grant_offers_nothing()
    {
        var row = TenantGrantRow.For(
            A,
            MyTenantSample.Grant(granter: B, grantee: C, state: ExplorerTenantGrantState.Pending));

        Assert.Multiple(() =>
        {
            Assert.That(row.Direction, Is.EqualTo(TenantGrantDirection.Unrelated));
            Assert.That(row.Actions, Is.EqualTo(TenantGrantActions.None));
            Assert.That(row.NeedsDecision, Is.False);
        });
    }

    [Test]
    public void The_counterparty_is_the_other_tenant_in_the_agreement()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                TenantGrantRow.For(A, MyTenantSample.Grant(granter: A, grantee: B)).CounterpartyTenantId,
                Is.EqualTo(B));
            Assert.That(
                TenantGrantRow.For(A, MyTenantSample.Grant(granter: B, grantee: A)).CounterpartyTenantId,
                Is.EqualTo(B));
        });
    }

    [Test]
    public void The_grant_travels_with_the_row_so_its_state_can_always_be_rendered()
    {
        var grant = MyTenantSample.Grant(granter: B, grantee: A, state: ExplorerTenantGrantState.Pending);

        Assert.That(TenantGrantRow.For(A, grant).Grant, Is.EqualTo(grant));
    }
}
