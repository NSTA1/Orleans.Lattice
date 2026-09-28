using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.Tests.Shell.Transport;

/// <summary>The Shell's <see cref="ILatticeTenantSelfService"/> transport adapter.</summary>
[TestFixture]
public sealed class ShellTenantSelfServiceTransportTests : ShellTransportAdapterContractTests<ILatticeTenantSelfService>
{
    private const string Service = "/orleans.lattice.api.tenantadmin/";

    internal override IEnumerable<ShellTransportCall<ILatticeTenantSelfService>> Calls() =>
    [
        new("GetCurrentTenantAsync", Service + "GetCurrentTenant", (f, ct) => f.GetCurrentTenantAsync(ct)),
        new("ListAccessibleTenantsAsync", Service + "ListAccessibleTenants", (f, ct) => f.ListAccessibleTenantsAsync(ct)),
        new("GetTenantAsync", Service + "GetTenant", (f, ct) => f.GetTenantAsync("contoso", ct)),
    ];

    [Test]
    public void A_tenant_outside_the_callers_authority_maps_to_tenant_not_found()
    {
        using var circuit = new ShellTransportCircuit();
        var self = circuit.Resolve<ILatticeTenantSelfService>();
        circuit.Peer.AnswerWith(StatusCode.NotFound, "missing");

        Assert.Multiple(() =>
        {
            Assert.That(Assert.ThrowsAsync<TenantNotFoundException>(() => self.GetTenantAsync("contoso"))!.TenantId, Is.EqualTo("contoso"));
            Assert.That(Assert.ThrowsAsync<TenantNotFoundException>(() => self.GetCurrentTenantAsync())!.TenantId, Is.Empty);
        });
    }

    [Test]
    public void An_empty_tenant_id_is_rejected_before_any_call()
    {
        using var circuit = new ShellTransportCircuit();
        var self = circuit.Resolve<ILatticeTenantSelfService>();

        Assert.Multiple(() =>
        {
            Assert.That(() => self.GetTenantAsync(string.Empty), Throws.ArgumentException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }
}
