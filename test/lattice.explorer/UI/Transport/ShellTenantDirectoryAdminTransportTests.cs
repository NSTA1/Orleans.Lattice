using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>The Shell's <see cref="ILatticeTenantDirectoryAdmin"/> transport adapter.</summary>
[TestFixture]
public sealed class ShellTenantDirectoryAdminTransportTests : ShellTransportAdapterContractTests<ILatticeTenantDirectoryAdmin>
{
    private const string Service = "/orleans.lattice.api.tenantadmin/";

    internal override IEnumerable<ShellTransportCall<ILatticeTenantDirectoryAdmin>> Calls() =>
    [
        new("ListGroupsAsync", Service + "ListTenantGroups", (f, ct) => f.ListGroupsAsync("globex", new TenantAccessPageRequest(), ct)),
        new("GetGroupAsync", Service + "GetTenantGroup", (f, ct) => f.GetGroupAsync("globex", "operators", ct)),
        new("UpsertGroupAsync", Service + "UpsertTenantGroup", (f, ct) => f.UpsertGroupAsync("globex", new TenantGroupDescriptor { Name = "operators" }, ct)),
        new("RemoveGroupAsync", Service + "RemoveTenantGroup", (f, ct) => f.RemoveGroupAsync("globex", "operators", ct)),
        new("ListGroupMembersAsync", Service + "ListTenantGroupMembers", (f, ct) => f.ListGroupMembersAsync("globex", "operators", ct)),
        new("AddGroupMemberAsync", Service + "AddTenantGroupMember", (f, ct) => f.AddGroupMemberAsync("globex", "operators", "alice", TenantSubjectKind.User, ct)),
        new("RemoveGroupMemberAsync", Service + "RemoveTenantGroupMember", (f, ct) => f.RemoveGroupMemberAsync("globex", "operators", "alice", TenantSubjectKind.User, ct)),
        new("ListMembersAsync", Service + "ListTenantMembers", (f, ct) => f.ListMembersAsync("globex", new TenantAccessPageRequest(), ct)),
        new("AddMemberAsync", Service + "AddTenantMember", (f, ct) => f.AddMemberAsync("globex", "operators", TenantSubjectKind.TenantGroup, ct)),
        new("RemoveMemberAsync", Service + "RemoveTenantMember", (f, ct) => f.RemoveMemberAsync("globex", "operators", TenantSubjectKind.TenantGroup, ct)),
        new("ResolveSubjectAsync", Service + "ResolveTenantSubject", (f, ct) => f.ResolveSubjectAsync("globex", "alice", TenantSubjectKind.User, ct)),
    ];

    [Test]
    public void The_switched_off_feature_maps_to_its_typed_refusal_naming_the_tenant()
    {
        using var circuit = new ShellTransportCircuit();
        var directory = circuit.Resolve<ILatticeTenantDirectoryAdmin>();
        circuit.Peer.AnswerWith(StatusCode.FailedPrecondition, new TenantAccessAdministrationDisabledException("globex").Message);

        var ex = Assert.ThrowsAsync<TenantAccessAdministrationDisabledException>(
            () => directory.ListGroupsAsync("globex", new TenantAccessPageRequest()));

        Assert.That(ex!.TenantId, Is.EqualTo("globex"));
    }

    [Test]
    public void The_last_admin_entry_guard_maps_to_its_typed_refusal()
    {
        using var circuit = new ShellTransportCircuit();
        var directory = circuit.Resolve<ILatticeTenantDirectoryAdmin>();
        circuit.Peer.AnswerWith(StatusCode.FailedPrecondition, new TenantLastAdminSubjectException("globex", "t/globex/admins").Message);

        var ex = Assert.ThrowsAsync<TenantLastAdminSubjectException>(() => directory.RemoveGroupAsync("globex", "admins"));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.TenantId, Is.EqualTo("globex"));
            Assert.That(ex.SubjectId, Is.EqualTo("t/globex/admins"));
        });
    }

    [Test]
    public void The_reserved_default_tenant_maps_to_its_typed_refusal()
    {
        using var circuit = new ShellTransportCircuit();
        var directory = circuit.Resolve<ILatticeTenantDirectoryAdmin>();
        circuit.Peer.AnswerWith(StatusCode.FailedPrecondition, new ReservedTenantOperationException("default", "list groups").Message);

        var ex = Assert.ThrowsAsync<ReservedTenantOperationException>(
            () => directory.ListGroupsAsync("default", new TenantAccessPageRequest()));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.TenantId, Is.EqualTo("default"));
            Assert.That(ex.Operation, Is.EqualTo("list groups"));
        });
    }

    [Test]
    public void Any_other_precondition_failure_stays_an_invalid_operation()
    {
        using var circuit = new ShellTransportCircuit();
        var directory = circuit.Resolve<ILatticeTenantDirectoryAdmin>();
        circuit.Peer.AnswerWith(StatusCode.FailedPrecondition, "The tenant is suspended.");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => directory.AddMemberAsync("globex", "alice"));

        Assert.Multiple(() =>
        {
            Assert.That(ex, Is.TypeOf<InvalidOperationException>());
            Assert.That(ex!.Message, Is.EqualTo("The tenant is suspended."));
        });
    }

    [Test]
    public void A_reached_cap_maps_to_a_quota_refusal_carrying_the_trailers()
    {
        using var circuit = new ShellTransportCircuit();
        var directory = circuit.Resolve<ILatticeTenantDirectoryAdmin>();
        circuit.Peer.AnswerWith(
            StatusCode.ResourceExhausted,
            "Tenant globex is at its cap of 500 groups.",
            new Dictionary<string, string>
            {
                ["lattice-quota-dimension"] = "groups",
                ["lattice-quota-current"] = "500",
                ["lattice-quota-limit"] = "500",
            });

        var ex = Assert.ThrowsAsync<LatticeQuotaExceededException>(
            () => directory.UpsertGroupAsync("globex", new TenantGroupDescriptor { Name = "operators" }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Is.EqualTo("Tenant globex is at its cap of 500 groups."));
            Assert.That(ex.Dimension, Is.EqualTo("groups"));
            Assert.That(ex.Current, Is.EqualTo(500));
            Assert.That(ex.Limit, Is.EqualTo(500));
            Assert.That(ex.TenantId, Is.EqualTo("globex"));
            Assert.That(ex.TreeId, Is.Empty, "the binding withholds the tree the cap protects");
        });
    }

    [Test]
    public void A_reached_cap_without_trailers_is_still_a_quota_refusal()
    {
        using var circuit = new ShellTransportCircuit();
        var directory = circuit.Resolve<ILatticeTenantDirectoryAdmin>();
        circuit.Peer.AnswerWith(StatusCode.ResourceExhausted, "over cap");

        var ex = Assert.ThrowsAsync<LatticeQuotaExceededException>(() => directory.AddGroupMemberAsync("globex", "operators", "alice"));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Is.EqualTo("over cap"));
            Assert.That(ex.Dimension, Is.Empty);
        });
    }

    [Test]
    public void A_confinement_refusal_keeps_the_facade_message_as_an_argument_failure()
    {
        using var circuit = new ShellTransportCircuit();
        var directory = circuit.Resolve<ILatticeTenantDirectoryAdmin>();
        circuit.Peer.AnswerWith(StatusCode.InvalidArgument, "'t/acme/ops' is in the reserved 't/' tenant group namespace.");

        var ex = Assert.ThrowsAsync<ArgumentException>(
            () => directory.AddGroupMemberAsync("globex", "operators", "t/acme/ops", TenantSubjectKind.ClusterGroup));

        Assert.That(ex!.Message, Does.StartWith("'t/acme/ops' is in the reserved"));
    }

    [Test]
    public void An_unknown_tenant_maps_to_tenant_not_found_with_its_id()
    {
        using var circuit = new ShellTransportCircuit();
        var directory = circuit.Resolve<ILatticeTenantDirectoryAdmin>();
        circuit.Peer.AnswerWith(StatusCode.NotFound, "missing");

        var ex = Assert.ThrowsAsync<TenantNotFoundException>(() => directory.GetGroupAsync("globex", "operators"));

        Assert.That(ex!.TenantId, Is.EqualTo("globex"));
    }

    [Test]
    public void A_cluster_that_does_not_serve_the_facade_maps_to_not_supported()
    {
        using var circuit = new ShellTransportCircuit();
        var directory = circuit.Resolve<ILatticeTenantDirectoryAdmin>();
        circuit.Peer.AnswerWith(StatusCode.Unimplemented, "not served");

        Assert.That(() => directory.ListMembersAsync("globex", new TenantAccessPageRequest()), Throws.InstanceOf<NotSupportedException>());
    }

    [Test]
    public async Task Every_call_asserts_the_circuit_tenant()
    {
        using var circuit = new ShellTransportCircuit(services => services.AddExplorerTenantView());
        circuit.Services.GetRequiredService<IExplorerTenantContext>().ActiveTenant = new ExplorerTenantId("globex");
        var directory = circuit.Resolve<ILatticeTenantDirectoryAdmin>();
        circuit.Peer.AnswerWithSuccess();

        foreach (var call in Calls())
        {
            await call.Invoke(directory, CancellationToken.None);
        }

        Assert.That(circuit.Peer.Requests.Select(request => request.Tenant), Is.All.EqualTo("globex"));
    }

    [Test]
    public void Argument_guards_run_before_any_call()
    {
        using var circuit = new ShellTransportCircuit();
        var directory = circuit.Resolve<ILatticeTenantDirectoryAdmin>();

        Assert.Multiple(() =>
        {
            Assert.That(() => directory.ListGroupsAsync(string.Empty, new TenantAccessPageRequest()), Throws.ArgumentException);
            Assert.That(() => directory.ListGroupsAsync("globex", null!), Throws.ArgumentNullException);
            Assert.That(() => directory.GetGroupAsync("globex", string.Empty), Throws.ArgumentException);
            Assert.That(() => directory.UpsertGroupAsync("globex", null!), Throws.ArgumentNullException);
            Assert.That(() => directory.RemoveGroupAsync(string.Empty, "operators"), Throws.ArgumentException);
            Assert.That(() => directory.ListGroupMembersAsync("globex", string.Empty), Throws.ArgumentException);
            Assert.That(() => directory.AddGroupMemberAsync("globex", "operators", string.Empty), Throws.ArgumentException);
            Assert.That(() => directory.RemoveGroupMemberAsync("globex", string.Empty, "alice"), Throws.ArgumentException);
            Assert.That(() => directory.ListMembersAsync("globex", null!), Throws.ArgumentNullException);
            Assert.That(() => directory.AddMemberAsync("globex", string.Empty), Throws.ArgumentException);
            Assert.That(() => directory.RemoveMemberAsync(string.Empty, "alice"), Throws.ArgumentException);
            Assert.That(() => directory.ResolveSubjectAsync("globex", string.Empty), Throws.ArgumentException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }
}
