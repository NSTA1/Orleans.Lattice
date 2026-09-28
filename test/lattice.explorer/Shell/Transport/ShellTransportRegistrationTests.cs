using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.Replication.Grpc;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Shell;
using Orleans.Lattice.Explorer.Shell.Transport;

namespace Orleans.Lattice.Explorer.Tests.Shell.Transport;

/// <summary>
/// The transport registration and its lifetime audit: every adapter and the
/// channel are scoped per circuit, the only singletons are stateless, and no
/// singleton in the Explorer's container - Core's included - depends on anything
/// the transport scopes to a circuit.
/// </summary>
[TestFixture]
public sealed class ShellTransportRegistrationTests
{
    private static readonly Dictionary<Type, Type> Adapters = new()
    {
        [typeof(Orleans.Lattice.Api.Auth.ILatticeAuthAdmin)] = typeof(ShellAuthAdminTransport),
        [typeof(Orleans.Lattice.Api.Backup.ILatticeBackupControl)] = typeof(ShellBackupControlTransport),
        [typeof(Orleans.Lattice.Api.Schema.ILatticeSchemaControl)] = typeof(ShellSchemaControlTransport),
        [typeof(Orleans.Lattice.Api.TenantAdmin.ILatticeTenantAdmin)] = typeof(ShellTenantAdminTransport),
        [typeof(Orleans.Lattice.Api.TenantAdmin.ILatticeTenantAccessAdmin)] = typeof(ShellTenantAccessAdminTransport),
        [typeof(Orleans.Lattice.Api.TenantAdmin.ILatticeTenantGrantAdmin)] = typeof(ShellTenantGrantAdminTransport),
        [typeof(Orleans.Lattice.Api.TenantAdmin.ILatticeTenantRegionAdmin)] = typeof(ShellTenantRegionAdminTransport),
        [typeof(Orleans.Lattice.Api.TenantAdmin.ILatticeTenantSelfService)] = typeof(ShellTenantSelfServiceTransport),
        [typeof(Orleans.Lattice.Api.TenantAdmin.ILatticeTenantQuotaUsage)] = typeof(ShellTenantQuotaUsageTransport),
        [typeof(Orleans.Lattice.Api.Telemetry.ILatticeTelemetry)] = typeof(ShellTelemetryTransport),
        [typeof(Orleans.Lattice.Api.TreeAdmin.ILatticeTreeAdmin)] = typeof(ShellTreeAdminTransport),
        [typeof(ILatticeReplicationControl)] = typeof(ShellReplicationControlTransport),
        [typeof(ILatticeReplicationStatus)] = typeof(ShellReplicationStatusTransport),
        [typeof(Orleans.Lattice.Api.Apps.ILatticeAppsControl)] = typeof(ShellAppsControlTransport),
        [typeof(Orleans.Lattice.Api.Apps.ILatticeAppCatalog)] = typeof(ShellAppCatalogTransport),
        [typeof(Orleans.Lattice.Api.Apps.ILatticeAppWorkspace)] = typeof(ShellAppWorkspaceTransport),
    };

    [Test]
    public void The_shell_registers_one_scoped_adapter_per_facade()
    {
        var services = new ServiceCollection().AddLatticeExplorerShell();

        Assert.Multiple(() =>
        {
            Assert.That(ShellTransportServiceCollectionExtensions.FacadeTypes, Is.EquivalentTo(Adapters.Keys));
            foreach (var (facade, adapter) in Adapters)
            {
                var descriptor = services.Single(candidate => candidate.ServiceType == facade);
                Assert.That(descriptor.Lifetime, Is.EqualTo(ServiceLifetime.Scoped), facade.Name);
                Assert.That(descriptor.ImplementationType, Is.EqualTo(adapter), facade.Name);
            }

            Assert.That(Lifetime(services, typeof(ShellTransportChannel)), Is.EqualTo(ServiceLifetime.Scoped));
            Assert.That(Lifetime(services, typeof(IShellGrpcChannelFactory)), Is.EqualTo(ServiceLifetime.Singleton));
            Assert.That(Lifetime(services, typeof(ShellTransportSerializer)), Is.EqualTo(ServiceLifetime.Singleton));
        });
    }

    [Test]
    public void Every_adapter_is_a_scoped_adapter_type_over_the_circuit_channel()
    {
        Assert.Multiple(() =>
        {
            foreach (var adapter in Adapters.Values)
            {
                var parameters = adapter.GetConstructors().Single().GetParameters();
                Assert.That(parameters.Select(parameter => parameter.ParameterType), Is.EqualTo(new[] { typeof(ShellTransportChannel) }), adapter.Name);
                Assert.That(IsTransportAdapter(adapter), Is.True, adapter.Name);
            }
        });
    }

    [Test]
    public void The_transport_singletons_depend_on_nothing()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(ShellGrpcChannelFactory).GetConstructors().Single().GetParameters(), Is.Empty);
            Assert.That(typeof(ShellTransportSerializer).GetConstructors().Single().GetParameters(), Is.Empty);
        });
    }

    [Test]
    public async Task No_singleton_in_the_explorer_container_depends_on_a_circuit_scoped_transport()
    {
        var services = ExplorerContainer();
        var scoped = services
            .Where(descriptor => descriptor.Lifetime == ServiceLifetime.Scoped)
            .Select(descriptor => descriptor.ServiceType)
            .ToHashSet();
        var transport = new HashSet<Type>(Adapters.Keys.Concat(Adapters.Values)) { typeof(ShellTransportChannel) };

        var captives = new List<string>();
        foreach (var singleton in services.Where(descriptor => descriptor.Lifetime == ServiceLifetime.Singleton && descriptor.ImplementationType is not null))
        {
            foreach (var dependency in ConstructorDependencies(singleton.ImplementationType!))
            {
                if (transport.Contains(dependency) || scoped.Contains(dependency))
                {
                    captives.Add($"{singleton.ImplementationType!.Name} -> {dependency.Name}");
                }
            }
        }

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });

        Assert.Multiple(() =>
        {
            Assert.That(captives, Is.Empty, "a singleton captures circuit-scoped state");
            foreach (var facade in Adapters.Keys)
            {
                Assert.That(() => provider.GetRequiredService(facade), Throws.InvalidOperationException, $"{facade.Name} must not resolve from the root");
            }
        });
    }

    [Test]
    public async Task Each_circuit_gets_its_own_channel_and_adapters()
    {
        await using var provider = ExplorerContainer().BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        await using var first = provider.CreateAsyncScope();
        await using var second = provider.CreateAsyncScope();

        Assert.Multiple(() =>
        {
            Assert.That(
                first.ServiceProvider.GetRequiredService<ShellTransportChannel>(),
                Is.Not.SameAs(second.ServiceProvider.GetRequiredService<ShellTransportChannel>()));
            foreach (var facade in Adapters.Keys)
            {
                Assert.That(
                    first.ServiceProvider.GetRequiredService(facade),
                    Is.Not.SameAs(second.ServiceProvider.GetRequiredService(facade)),
                    facade.Name);
            }
        });
    }

    [Test]
    public void A_facade_the_host_registered_first_is_kept()
    {
        var own = NSubstitute.Substitute.For<ILatticeReplicationControl>();
        var services = new ServiceCollection();
        services.AddScoped(_ => own);

        services.AddShellTransport();

        Assert.That(services.Count(descriptor => descriptor.ServiceType == typeof(ILatticeReplicationControl)), Is.EqualTo(1));
    }

    [Test]
    public void Registration_rejects_a_missing_collection_or_factory()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => ((IServiceCollection)null!).AddShellTransport(), Throws.ArgumentNullException);
            Assert.That(
                () => ((IServiceCollection)null!).AddShellTransportClient<ILatticeReplicationStatus>(LatticeReplicationStatusGrpcClient.Create),
                Throws.ArgumentNullException);
            Assert.That(
                () => new ServiceCollection().AddShellTransportClient<ILatticeReplicationStatus>(null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task A_direct_client_joins_the_circuit_channel_and_its_credential()
    {
        using var circuit = new ShellTransportCircuit(services =>
            services.AddShellTransportClient<ILatticeReplicationStatus>(LatticeReplicationStatusGrpcClient.Create));
        circuit.Authentication = Orleans.Lattice.Explorer.Core.Connection.LatticeCallAuthentication.Basic("alice", "pw");

        var status = circuit.Resolve<ILatticeReplicationStatus>();
        circuit.Peer.AnswerWithSuccess();
        await status.GetPeerStatusAsync(ReplicationPeerStatusQuery.All);

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.InstanceOf<LatticeReplicationStatusGrpcClient>());
            Assert.That(status, Is.SameAs(circuit.Resolve<ILatticeReplicationStatus>()));
            Assert.That(circuit.Peer.Requests.Single().Authorization, Does.StartWith("Basic "));
        });
    }

    [Test]
    public void The_serializer_serves_codecs_until_disposed()
    {
        var serializer = new ShellTransportSerializer();

        Assert.That(serializer.Services.GetService(typeof(Orleans.Serialization.Serializer<Orleans.Lattice.Api.Auth.AuthGroup>)), Is.Not.Null);

        serializer.Dispose();

        Assert.That(
            () => serializer.Services.GetService(typeof(Orleans.Serialization.Serializer<Orleans.Lattice.Api.Auth.AuthGroup>)),
            Throws.InstanceOf<ObjectDisposedException>());
    }

    private static IServiceCollection ExplorerContainer()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddExplorerConfiguration();
        services.AddExplorerAuth();
        services.AddShellTransportTestHead();
        services.AddLatticeExplorerShell();
        return services;
    }

    private static ServiceLifetime Lifetime(IServiceCollection services, Type type) =>
        services.Single(descriptor => descriptor.ServiceType == type).Lifetime;

    private static bool IsTransportAdapter(Type type)
    {
        for (var current = type.BaseType; current is not null; current = current.BaseType)
        {
            if (current.IsGenericType && current.GetGenericTypeDefinition() == typeof(ShellTransportAdapter<>))
            {
                return true;
            }
        }

        return false;
    }

    private static IEnumerable<Type> ConstructorDependencies(Type type) =>
        type.GetConstructors(BindingFlags.Instance | BindingFlags.Public)
            .SelectMany(constructor => constructor.GetParameters())
            .Select(parameter => parameter.ParameterType);
}
