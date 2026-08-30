using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Schema;

[TestFixture]
public class ExplorerSchemaServiceCollectionExtensionsTests
{
    [Test]
    public void AddExplorerSchema_null_services_throws()
    {
        Assert.That(() => ((IServiceCollection)null!).AddExplorerSchema(), Throws.ArgumentNullException);
    }

    [Test]
    public void AddExplorerSchema_registers_navigation_store_and_schema_services()
    {
        var services = new ServiceCollection();

        services.AddExplorerSchema();

        Assert.Multiple(() =>
        {
            Assert.That(services.Any(d => d.ServiceType == typeof(IExplorerPluginAccessStore)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(ISchemaAdminClient)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(ISchemaPolicyService)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(ISchemaVersioningService)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(ISchemaComplianceService)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(ISchemaAdminCapabilityService)), Is.True);
        });
    }

    [Test]
    public async Task AddExplorerSchema_services_resolve_over_a_fake_client()
    {
        var services = new ServiceCollection();
        services.AddExplorerSchema();
        services.AddSingleton<ISchemaAdminClient, FakeSchemaAdminClient>();
        await using var provider = services.BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetRequiredService<ISchemaPolicyService>(), Is.InstanceOf<SchemaPolicyService>());
            Assert.That(provider.GetRequiredService<ISchemaVersioningService>(), Is.InstanceOf<SchemaVersioningService>());
            Assert.That(provider.GetRequiredService<ISchemaComplianceService>(), Is.InstanceOf<SchemaComplianceService>());
            Assert.That(provider.GetRequiredService<ISchemaAdminCapabilityService>(), Is.InstanceOf<SchemaAdminCapabilityService>());
        });
    }

    [Test]
    public async Task AddExplorerSchema_capability_store_resolves()
    {
        var services = new ServiceCollection();
        services.AddExplorerSchema();
        await using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<IExplorerPluginAccessStore>(), Is.InstanceOf<ExplorerPluginAccessStore>());
    }

    [Test]
    public async Task AddExplorerSchema_schema_client_owns_orleans_serializer()
    {
        // Regression: the schema-admin client must build its own Orleans serializer
        // provider. If it captured the application root provider (which has no
        // AddSerializer), resolving its per-message serializers throws
        // InvalidOperationException before any network call, and the Schema area
        // silently greys out. With a real serializer the call instead proceeds to the
        // transport and fails to reach the dead endpoint with an RpcException.
        var session = Substitute.For<IExplorerSession>();
        session.Current.Returns(new ExplorerConfiguration
        {
            Endpoint = "http://127.0.0.1:1",
            AllowUnencryptedHttp2 = true,
        });
        var auth = Substitute.For<IExplorerAuthSession>();

        var services = new ServiceCollection();
        services.AddSingleton(session);
        services.AddSingleton(auth);
        services.AddExplorerSchema();
        await using var provider = services.BuildServiceProvider();

        var client = provider.GetRequiredService<ISchemaAdminClient>();

        Assert.That(
            async () => await client.ProbeCapabilitiesAsync("t"),
            Throws.InstanceOf<Grpc.Core.RpcException>());
    }
}
