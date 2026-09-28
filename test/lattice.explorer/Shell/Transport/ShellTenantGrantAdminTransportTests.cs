using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.Tests.Shell.Transport;

/// <summary>The Shell's <see cref="ILatticeTenantGrantAdmin"/> transport adapter.</summary>
[TestFixture]
public sealed class ShellTenantGrantAdminTransportTests : ShellTransportAdapterContractTests<ILatticeTenantGrantAdmin>
{
    private const string Service = "/orleans.lattice.api.tenantadmin/";

    internal override IEnumerable<ShellTransportCall<ILatticeTenantGrantAdmin>> Calls() =>
    [
        new("ListGrantsAsync", Service + "ListCrossTenantGrants", (f, ct) => f.ListGrantsAsync("contoso", ct)),
        new("OfferGrantAsync", Service + "OfferCrossTenantGrant", (f, ct) => f.OfferGrantAsync("contoso", "fabrikam", "orders", TenantGrantAccess.Read, ct)),
        new("ApproveGrantAsync", Service + "ApproveCrossTenantGrant", (f, ct) => f.ApproveGrantAsync("contoso", "fabrikam", "orders", ct)),
        new("RejectGrantAsync", Service + "RejectCrossTenantGrant", (f, ct) => f.RejectGrantAsync("contoso", "fabrikam", "orders", ct)),
        new("RevokeGrantAsync", Service + "RevokeCrossTenantGrant", (f, ct) => f.RevokeGrantAsync("contoso", "fabrikam", "orders", ct)),
    ];

    [Test]
    public void An_unknown_grant_on_a_transition_maps_to_grant_not_found_with_its_key()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeTenantGrantAdmin>();
        circuit.Peer.AnswerWith(StatusCode.NotFound, "no such grant");

        Assert.Multiple(() =>
        {
            foreach (var transition in new Func<Task>[]
            {
                () => admin.ApproveGrantAsync("contoso", "fabrikam", "orders"),
                () => admin.RejectGrantAsync("contoso", "fabrikam", "orders"),
                () => admin.RevokeGrantAsync("contoso", "fabrikam", "orders"),
            })
            {
                var ex = Assert.ThrowsAsync<TenantGrantNotFoundException>(() => transition());
                Assert.That(ex!.GranterTenantId, Is.EqualTo("contoso"));
                Assert.That(ex.GranteeTenantId, Is.EqualTo("fabrikam"));
                Assert.That(ex.Scope, Is.EqualTo("orders"));
            }
        });
    }

    [Test]
    public void An_unknown_tenant_on_a_listing_or_offer_maps_to_tenant_not_found()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeTenantGrantAdmin>();
        circuit.Peer.AnswerWith(StatusCode.NotFound, "missing");

        Assert.Multiple(() =>
        {
            Assert.That(
                Assert.ThrowsAsync<TenantNotFoundException>(() => admin.ListGrantsAsync("contoso"))!.TenantId,
                Is.EqualTo("contoso"));
            Assert.That(
                Assert.ThrowsAsync<TenantNotFoundException>(() => admin.OfferGrantAsync("contoso", "fabrikam", "orders", TenantGrantAccess.Read))!.TenantId,
                Is.EqualTo("contoso"));
        });
    }

    [Test]
    public void An_illegal_transition_maps_to_invalid_operation()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeTenantGrantAdmin>();
        circuit.Peer.AnswerWith(StatusCode.FailedPrecondition, "the grant is already approved");

        Assert.That(() => admin.ApproveGrantAsync("contoso", "fabrikam", "orders"), Throws.InvalidOperationException);
    }

    [Test]
    public void Argument_guards_run_before_any_call()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeTenantGrantAdmin>();

        Assert.Multiple(() =>
        {
            Assert.That(() => admin.ListGrantsAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => admin.OfferGrantAsync("a", "b", string.Empty, TenantGrantAccess.Read), Throws.ArgumentException);
            Assert.That(() => admin.ApproveGrantAsync(string.Empty, "b", "s"), Throws.ArgumentException);
            Assert.That(() => admin.RejectGrantAsync("a", string.Empty, "s"), Throws.ArgumentException);
            Assert.That(() => admin.RevokeGrantAsync("a", "b", string.Empty), Throws.ArgumentException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }
}
