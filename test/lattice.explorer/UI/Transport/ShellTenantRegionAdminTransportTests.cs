using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>The Shell's <see cref="ILatticeTenantRegionAdmin"/> transport adapter.</summary>
[TestFixture]
public sealed class ShellTenantRegionAdminTransportTests : ShellTransportAdapterContractTests<ILatticeTenantRegionAdmin>
{
    private const string Service = "/orleans.lattice.api.tenantadmin/";

    internal override IEnumerable<ShellTransportCall<ILatticeTenantRegionAdmin>> Calls() =>
    [
        new("AuthorizeAllowedRegionsAsync", Service + "AuthorizeAllowedRegions", (f, ct) => f.AuthorizeAllowedRegionsAsync("contoso", ["eu"], ct)),
        new("SetResidencyAsync", Service + "SetTenantResidency", (f, ct) => f.SetResidencyAsync("contoso", ["eu"], ct)),
        new("GetTenantRegionStatusAsync", Service + "GetTenantRegionStatus", (f, ct) => f.GetTenantRegionStatusAsync("contoso", ct)),
    ];

    [Test]
    public void An_unknown_tenant_maps_to_tenant_not_found_with_its_id()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeTenantRegionAdmin>();
        circuit.Peer.AnswerWith(StatusCode.NotFound, "missing");

        var ex = Assert.ThrowsAsync<TenantNotFoundException>(() => admin.GetTenantRegionStatusAsync("contoso"));

        Assert.That(ex!.TenantId, Is.EqualTo("contoso"));
    }

    [Test]
    public void An_unserved_residency_facade_maps_to_not_supported()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeTenantRegionAdmin>();
        circuit.Peer.AnswerWith(StatusCode.Unimplemented, "This cluster does not serve tenant region residency.");

        Assert.That(
            () => admin.SetResidencyAsync("contoso", ["eu"]),
            Throws.InstanceOf<NotSupportedException>().With.Message.EqualTo("This cluster does not serve tenant region residency."));
    }

    [Test]
    public void Argument_guards_run_before_any_call()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeTenantRegionAdmin>();

        Assert.Multiple(() =>
        {
            Assert.That(() => admin.AuthorizeAllowedRegionsAsync("contoso", null!), Throws.ArgumentNullException);
            Assert.That(() => admin.SetResidencyAsync(string.Empty, ["eu"]), Throws.ArgumentException);
            Assert.That(() => admin.SetResidencyAsync("contoso", null!), Throws.ArgumentNullException);
            Assert.That(() => admin.GetTenantRegionStatusAsync(string.Empty), Throws.ArgumentException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }
}
