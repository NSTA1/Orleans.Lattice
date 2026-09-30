using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Explorer.Web;

namespace Orleans.Lattice.Explorer.Tests.Web;

/// <summary>
/// A host that co-hosts the Explorer beside the cluster's own facades (a silo that
/// also serves the console, as the Explorer sample does) keeps the two apart in
/// both directions (issue #3831): the Explorer always calls the cluster through its
/// own credential-aware transport, never through a host's in-process facade that
/// carries no caller identity, and the host's own services (the gRPC services that
/// serve the cluster) always get the in-process facade, never the Explorer's
/// adapter, which would call back into them.
/// </summary>
[TestFixture]
public sealed class CoHostedFacadeIsolationTests
{
    private static readonly Type[] Facades =
    [
        typeof(ILatticeAuthAdmin),
        typeof(ILatticeBackupControl),
        typeof(ILatticeSchemaControl),
        typeof(ILatticeTenantAdmin),
        typeof(ILatticeTenantAccessAdmin),
        typeof(ILatticeTenantGrantAdmin),
        typeof(ILatticeTenantRegionAdmin),
        typeof(ILatticeTenantSelfService),
        typeof(ILatticeTenantQuotaUsage),
        typeof(ILatticeTelemetry),
        typeof(ILatticeTreeAdmin),
        typeof(ILatticeReplicationControl),
        typeof(ILatticeReplicationStatus),
        typeof(ILatticeAppsControl),
        typeof(ILatticeAppCatalog),
        typeof(ILatticeAppWorkspace),
        typeof(ILatticeAppBridge),
    ];

    // true: the in-process facades are registered after the Explorer, as UseOrleans does at Build.
    [TestCase(true)]
    [TestCase(false)]
    public async Task The_explorer_and_the_host_each_resolve_their_own_facades(bool hostRegistersLast)
    {
        var services = new ServiceCollection().AddLogging();
        var inProcess = Facades.ToDictionary(type => type, type => Substitute.For([type], []));
        if (!hostRegistersLast)
        {
            RegisterInProcess(services, inProcess);
        }

        services.AddLatticeExplorerWeb();
        if (hostRegistersLast)
        {
            RegisterInProcess(services, inProcess);
        }

        services.AddScoped<ServerSideAuthService>();

        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var circuit = scope.ServiceProvider;

        Assert.Multiple(() =>
        {
            foreach (var facade in Facades)
            {
                var explorers = circuit.GetKeyedService(facade, ShellFacades.Key);
                Assert.That(explorers, Is.Not.Null, facade.Name + ": the Explorer has its own");
                Assert.That(explorers, Is.Not.SameAs(inProcess[facade]), facade.Name + ": the Explorer never gets the host's in-process facade");
                Assert.That(circuit.GetService(facade), Is.SameAs(inProcess[facade]), facade.Name + ": the host keeps its in-process facade");
            }

            Assert.That(circuit.GetRequiredService<ServerSideAuthService>().Admin, Is.SameAs(inProcess[typeof(ILatticeAuthAdmin)]),
                "a server-side service resolving by interface never reaches the Explorer's adapter");

            var tenancy = circuit.GetRequiredService<TenancyCatalog>();
            Assert.That(tenancy.Admin, Is.InstanceOf<ShellTenantAdminTransport>(), "an area reads the Explorer's transport");
            Assert.That(tenancy.SelfService, Is.InstanceOf<ShellTenantSelfServiceTransport>());
        });
    }

    [Test]
    public void The_explorer_registers_no_facade_by_bare_interface()
    {
        var services = new ServiceCollection().AddLogging();

        services.AddLatticeExplorerWeb();

        Assert.That(
            services.Where(descriptor => Facades.Contains(descriptor.ServiceType) && !descriptor.IsKeyedService).Select(descriptor => descriptor.ServiceType.Name),
            Is.Empty,
            "a bare-interface registration would be resolved by a co-hosted cluster's own gRPC services");
    }

    private static void RegisterInProcess(IServiceCollection services, Dictionary<Type, object> inProcess)
    {
        foreach (var (type, instance) in inProcess)
        {
            services.AddSingleton(type, instance);
        }
    }

    /// <summary>Stands in for a cluster gRPC service, which resolves its facade by interface.</summary>
    /// <param name="admin">The facade it serves.</param>
    private sealed class ServerSideAuthService(ILatticeAuthAdmin admin)
    {
        public ILatticeAuthAdmin Admin { get; } = admin;
    }
}
