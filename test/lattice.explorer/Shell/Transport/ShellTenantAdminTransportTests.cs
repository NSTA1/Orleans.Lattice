using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.Tests.Shell.Transport;

/// <summary>The Shell's <see cref="ILatticeTenantAdmin"/> transport adapter.</summary>
[TestFixture]
public sealed class ShellTenantAdminTransportTests : ShellTransportAdapterContractTests<ILatticeTenantAdmin>
{
    private const string Service = "/orleans.lattice.api.tenantadmin/";

    internal override IEnumerable<ShellTransportCall<ILatticeTenantAdmin>> Calls() =>
    [
        new("CreateTenantAsync", Service + "CreateTenant", (f, ct) => f.CreateTenantAsync("contoso", ["alice"], ct)),
        new("SuspendTenantAsync", Service + "SuspendTenant", (f, ct) => f.SuspendTenantAsync("contoso", ct)),
        new("ResumeTenantAsync", Service + "ResumeTenant", (f, ct) => f.ResumeTenantAsync("contoso", ct)),
        new("DeleteTenantAsync", Service + "DeleteTenant", (f, ct) => f.DeleteTenantAsync("contoso", ct)),
        new("SetTenantQuotasAsync", Service + "SetTenantQuotas", (f, ct) => f.SetTenantQuotasAsync("contoso", default, ct)),
    ];

    [Test]
    public void An_unknown_tenant_maps_to_tenant_not_found_with_its_id()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeTenantAdmin>();
        circuit.Peer.AnswerWith(StatusCode.NotFound, "Tenant 'contoso' is not registered.");

        var ex = Assert.ThrowsAsync<TenantNotFoundException>(() => admin.SuspendTenantAsync("contoso"));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.TenantId, Is.EqualTo("contoso"));
            Assert.That(ex.Message, Is.EqualTo("Tenant 'contoso' is not registered."));
        });
    }

    [Test]
    public void An_existing_tenant_maps_to_tenant_already_exists_with_its_id()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeTenantAdmin>();
        circuit.Peer.AnswerWith(StatusCode.AlreadyExists, "exists");

        var ex = Assert.ThrowsAsync<TenantAlreadyExistsException>(() => admin.CreateTenantAsync("contoso"));

        Assert.That(ex!.TenantId, Is.EqualTo("contoso"));
    }

    [Test]
    public void A_reserved_tenant_refusal_maps_to_invalid_operation()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeTenantAdmin>();
        circuit.Peer.AnswerWith(StatusCode.FailedPrecondition, "the default tenant cannot be deleted");

        Assert.That(() => admin.DeleteTenantAsync("default"), Throws.InvalidOperationException);
    }

    [Test]
    public void Argument_guards_run_before_any_call()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeTenantAdmin>();

        Assert.Multiple(() =>
        {
            Assert.That(() => admin.CreateTenantAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => admin.SuspendTenantAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => admin.ResumeTenantAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => admin.DeleteTenantAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => admin.SetTenantQuotasAsync(string.Empty, default), Throws.ArgumentException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }
}
