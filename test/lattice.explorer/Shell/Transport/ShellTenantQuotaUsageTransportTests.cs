using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.Tests.Shell.Transport;

/// <summary>The Shell's <see cref="ILatticeTenantQuotaUsage"/> transport adapter.</summary>
[TestFixture]
public sealed class ShellTenantQuotaUsageTransportTests : ShellTransportAdapterContractTests<ILatticeTenantQuotaUsage>
{
    internal override IEnumerable<ShellTransportCall<ILatticeTenantQuotaUsage>> Calls() =>
    [
        new("GetQuotaUsageAsync", "/orleans.lattice.api.tenantadmin/GetTenantQuotaUsage", (f, ct) => f.GetQuotaUsageAsync("contoso", ct)),
    ];

    [Test]
    public void An_unknown_or_unauthorized_tenant_maps_to_tenant_not_found_with_its_id()
    {
        using var circuit = new ShellTransportCircuit();
        var usage = circuit.Resolve<ILatticeTenantQuotaUsage>();
        circuit.Peer.AnswerWith(StatusCode.NotFound, "missing");

        var ex = Assert.ThrowsAsync<TenantNotFoundException>(() => usage.GetQuotaUsageAsync("contoso"));

        Assert.That(ex!.TenantId, Is.EqualTo("contoso"));
    }

    [Test]
    public void An_empty_tenant_id_is_rejected_before_any_call()
    {
        using var circuit = new ShellTransportCircuit();
        var usage = circuit.Resolve<ILatticeTenantQuotaUsage>();

        Assert.Multiple(() =>
        {
            Assert.That(() => usage.GetQuotaUsageAsync(string.Empty), Throws.ArgumentException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }
}
