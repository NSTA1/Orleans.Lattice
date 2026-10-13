using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;

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
        new("AdvanceRegionAsync", Service + "AdvanceTenantRegion", (f, ct) => f.AdvanceRegionAsync("contoso", "eu", true, ct)),
    ];

    [TestCase(true)]
    [TestCase(false)]
    public async Task AdvanceRegionAsync_forwards_every_argument_and_returns_the_status(bool acknowledgeDataInPlace)
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeTenantRegionAdmin>();
        var expected = new TenantRegionStatusReport { TenantId = "contoso", Regions = [] };
        circuit.Peer.Respond(Service + "AdvanceTenantRegion", expected);

        var result = await admin.AdvanceRegionAsync("contoso", "eu", acknowledgeDataInPlace);

        Assert.Multiple(() =>
        {
            Assert.That(circuit.Peer.Requests, Has.Count.EqualTo(1));
            Assert.That(circuit.Peer.Requests[0].Path, Is.EqualTo(Service + "AdvanceTenantRegion"));
            var request = circuit.Peer.Decode<TenantAdminRegionSetRequest>(circuit.Peer.Requests[0]);
            Assert.That(request.TenantId, Is.EqualTo("contoso"));
            Assert.That(request.RegionId, Is.EqualTo("eu"));
            Assert.That(request.AcknowledgeDataInPlace, Is.EqualTo(acknowledgeDataInPlace));
            Assert.That(result.TenantId, Is.EqualTo(expected.TenantId));
            Assert.That(result.Regions, Is.Empty);
        });
    }

    [TestCase(null, "eu")]
    [TestCase("", "eu")]
    [TestCase("contoso", null)]
    [TestCase("contoso", "")]
    public void AdvanceRegionAsync_invalid_identifier_is_rejected_before_any_rpc(string? tenant, string? region)
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeTenantRegionAdmin>();

        Assert.That(() => admin.AdvanceRegionAsync(tenant!, region!, true), Throws.InstanceOf<ArgumentException>());
        Assert.That(circuit.Peer.Requests, Is.Empty);
    }

    [Test]
    public void AdvanceRegionAsync_unknown_tenant_preserves_the_subject_in_the_fault()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeTenantRegionAdmin>();
        circuit.Peer.AnswerWith(StatusCode.NotFound, "missing");

        var exception = Assert.ThrowsAsync<TenantNotFoundException>(() => admin.AdvanceRegionAsync("contoso", "eu", true));

        Assert.That(exception!.TenantId, Is.EqualTo("contoso"));
    }

    [Test]
    public void AdvanceRegionAsync_cancelled_call_does_not_reach_the_peer()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeTenantRegionAdmin>();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.That(() => admin.AdvanceRegionAsync("contoso", "eu", true, cancellation.Token),
            Throws.InstanceOf<OperationCanceledException>());
        Assert.That(circuit.Peer.Requests, Is.Empty);
    }

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
