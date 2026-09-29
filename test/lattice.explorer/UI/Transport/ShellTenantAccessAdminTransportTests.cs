using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>The Shell's <see cref="ILatticeTenantAccessAdmin"/> transport adapter.</summary>
[TestFixture]
public sealed class ShellTenantAccessAdminTransportTests : ShellTransportAdapterContractTests<ILatticeTenantAccessAdmin>
{
    private const string Service = "/orleans.lattice.api.tenantadmin/";

    internal override IEnumerable<ShellTransportCall<ILatticeTenantAccessAdmin>> Calls() =>
    [
        new("ListAdminSubjectsAsync", Service + "ListTenantAdminSubjects", (f, ct) => f.ListAdminSubjectsAsync("contoso", ct)),
        new("AddAdminSubjectAsync", Service + "AddTenantAdminSubject", (f, ct) => f.AddAdminSubjectAsync("contoso", "alice", ct)),
        new("RemoveAdminSubjectAsync", Service + "RemoveTenantAdminSubject", (f, ct) => f.RemoveAdminSubjectAsync("contoso", "alice", ct)),
    ];

    [Test]
    public void An_unknown_tenant_maps_to_tenant_not_found_with_its_id()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeTenantAccessAdmin>();
        circuit.Peer.AnswerWith(StatusCode.NotFound, "missing");

        var ex = Assert.ThrowsAsync<TenantNotFoundException>(() => admin.ListAdminSubjectsAsync("contoso"));

        Assert.That(ex!.TenantId, Is.EqualTo("contoso"));
    }

    [Test]
    public void The_last_admin_subject_guard_maps_to_invalid_operation()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeTenantAccessAdmin>();
        circuit.Peer.AnswerWith(StatusCode.FailedPrecondition, "last admin subject");

        Assert.That(() => admin.RemoveAdminSubjectAsync("contoso", "alice"), Throws.InvalidOperationException);
    }

    [Test]
    public void Argument_guards_run_before_any_call()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeTenantAccessAdmin>();

        Assert.Multiple(() =>
        {
            Assert.That(() => admin.ListAdminSubjectsAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => admin.AddAdminSubjectAsync("contoso", string.Empty), Throws.ArgumentException);
            Assert.That(() => admin.RemoveAdminSubjectAsync(string.Empty, "alice"), Throws.ArgumentException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }
}
