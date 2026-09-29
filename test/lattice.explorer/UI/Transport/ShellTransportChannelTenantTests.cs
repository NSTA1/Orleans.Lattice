using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>
/// Every call a circuit makes over the Shell transport asserts that circuit's
/// active tenant through the cluster's header: read as the call starts, absent
/// with tenancy off, with no tenant or for the reserved default tenant, switched
/// by the very next call after a tenant switch, and never carried into another
/// circuit's calls.
/// </summary>
[TestFixture]
public sealed class ShellTransportChannelTenantTests
{
    [Test]
    public async Task With_tenancy_off_no_call_carries_a_tenant()
    {
        using var circuit = new ShellTransportCircuit();
        var self = circuit.Resolve<ILatticeTenantSelfService>();
        circuit.Peer.AnswerWithSuccess();

        await self.GetCurrentTenantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(circuit.Peer.Requests.Single().Tenant, Is.Null);
            Assert.That(circuit.Peer.Requests.Single().Headers.Keys, Has.None.EqualTo(LatticeActiveTenantAssertion.DefaultHeaderName));
        });
    }

    [Test]
    public async Task A_call_asserts_the_circuit_tenant()
    {
        using var circuit = TenantCircuit();
        Scope(circuit.Services, "acme");
        var self = circuit.Resolve<ILatticeTenantSelfService>();
        circuit.Peer.AnswerWithSuccess();

        await self.GetCurrentTenantAsync();

        Assert.That(circuit.Peer.Requests.Single().Tenant, Is.EqualTo("acme"));
    }

    [Test]
    public async Task No_tenant_and_the_default_tenant_assert_nothing()
    {
        using var circuit = TenantCircuit();
        var self = circuit.Resolve<ILatticeTenantSelfService>();
        circuit.Peer.AnswerWithSuccess();

        await self.GetCurrentTenantAsync();
        Scope(circuit.Services, ExplorerTenantTrees.DefaultTenantId);
        await self.GetCurrentTenantAsync();

        Assert.That(circuit.Peer.Requests.Select(request => request.Tenant), Is.EqualTo(new string?[] { null, null }));
    }

    [Test]
    public async Task A_tenant_switch_changes_the_very_next_call_without_a_new_channel()
    {
        using var circuit = TenantCircuit();
        Scope(circuit.Services, "acme");
        var self = circuit.Resolve<ILatticeTenantSelfService>();
        circuit.Peer.AnswerWithSuccess();

        await self.GetCurrentTenantAsync();
        Scope(circuit.Services, "globex");
        await self.GetCurrentTenantAsync();
        Scope(circuit.Services, null);
        await self.GetCurrentTenantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(circuit.Peer.Requests.Select(request => request.Tenant), Is.EqualTo(new[] { "acme", "globex", null }));
            Assert.That(circuit.ChannelFactory.Created, Has.Count.EqualTo(1), "the tenant is read per call, never built into the channel");
        });
    }

    [Test]
    public async Task One_circuits_tenant_never_reaches_another_circuits_call()
    {
        using var circuit = TenantCircuit();
        var other = circuit.CreateSibling();
        Scope(circuit.Services, "acme");
        Scope(other, "globex");
        var mine = circuit.Resolve<ILatticeTenantSelfService>();
        var theirs = circuit.Resolve<ILatticeTenantSelfService>(other);
        circuit.Peer.AnswerWithSuccess();

        await mine.GetCurrentTenantAsync();
        await theirs.GetCurrentTenantAsync();
        Scope(circuit.Services, "initech");
        await theirs.GetCurrentTenantAsync();
        await mine.GetCurrentTenantAsync();
        Scope(other, null);
        await theirs.GetCurrentTenantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                circuit.Peer.Requests.Select(request => request.Tenant),
                Is.EqualTo(new[] { "acme", "globex", "globex", "initech", null }));
            Assert.That(
                other.GetRequiredService<ShellTransportChannel>(),
                Is.Not.SameAs(circuit.Services.GetRequiredService<ShellTransportChannel>()));
        });
    }

    [Test]
    public async Task A_stream_asserts_the_circuit_tenant()
    {
        using var circuit = TenantCircuit();
        Scope(circuit.Services, "acme");
        var backups = circuit.Resolve<ILatticeBackupControl>();
        circuit.Peer.AnswerWithSuccess();

        await foreach (var _ in backups.StreamBackupsAsync())
        {
        }

        Assert.That(circuit.Peer.Requests.Single().Tenant, Is.EqualTo("acme"));
    }

    [Test]
    public async Task The_app_bridge_client_asserts_the_circuit_tenant()
    {
        using var circuit = TenantCircuit();
        Scope(circuit.Services, "acme");
        var bridge = circuit.Resolve<ILatticeAppBridge>();
        circuit.Peer.AnswerWithSuccess();

        await bridge.GetAsync(new AppBridgeTarget { AppSlug = "board", InstallRevision = 1, LogicalTree = "tasks" }, "k");

        Assert.That(circuit.Peer.Requests.Single().Tenant, Is.EqualTo("acme"));
    }

    [Test]
    public async Task A_configured_transport_header_cannot_stand_in_for_the_circuit_tenant()
    {
        using var circuit = TenantCircuit();
        circuit.Configuration = ShellTransportCircuit.PlaintextConfiguration(
            new Dictionary<string, string> { [LatticeActiveTenantAssertion.DefaultHeaderName] = "globex", ["x-azure-fdid"] = "origin" });
        Scope(circuit.Services, "acme");
        var self = circuit.Resolve<ILatticeTenantSelfService>();
        circuit.Peer.AnswerWithSuccess();

        await self.GetCurrentTenantAsync();
        Scope(circuit.Services, null);
        await self.GetCurrentTenantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(circuit.Peer.Requests.Select(request => request.Tenant), Is.EqualTo(new[] { "acme", null }));
            Assert.That(circuit.Peer.Requests.Select(request => request.Headers["x-azure-fdid"]), Is.All.EqualTo("origin"));
        });
    }

    private static ShellTransportCircuit TenantCircuit() => new(services => services.AddExplorerTenantView());

    private static void Scope(IServiceProvider circuit, string? tenant) =>
        circuit.GetRequiredService<IExplorerTenantContext>().ActiveTenant = tenant is null ? null : new ExplorerTenantId(tenant);
}
