using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Security;

[TestFixture]
public class LatticeReplicationSecurityServiceCollectionExtensionsTests
{
    private sealed class StubSource : ILatticeReplicationSecretSource
    {
        public ValueTask<string?> GetOutboundSecretAsync(string peerClusterId, CancellationToken cancellationToken)
            => new("stub");
        public ValueTask<LatticeReplicationAcceptedSecrets> GetAcceptedSecretsAsync(CancellationToken cancellationToken)
            => new(LatticeReplicationAcceptedSecrets.Empty);
    }

    private sealed class FactoryStubSource : ILatticeReplicationSecretSource
    {
        public string Token { get; }
        public FactoryStubSource(string token) { Token = token; }
        public ValueTask<string?> GetOutboundSecretAsync(string peerClusterId, CancellationToken cancellationToken)
            => new(Token);
        public ValueTask<LatticeReplicationAcceptedSecrets> GetAcceptedSecretsAsync(CancellationToken cancellationToken)
            => new(LatticeReplicationAcceptedSecrets.Empty);
    }

    private static ISiloBuilder SiloBuilderWith(IServiceCollection services)
    {
        var b = Substitute.For<ISiloBuilder>();
        b.Services.Returns(services);
        return b;
    }

    [Test]
    public void AddLatticeReplicationSecrets_throws_on_null_builder_for_typed_overload()
    {
        Assert.That(
            () => LatticeReplicationSecurityServiceCollectionExtensions.AddLatticeReplicationSecrets<StubSource>(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeReplicationSecrets_throws_on_null_builder_for_factory_overload()
    {
        Assert.That(
            () => LatticeReplicationSecurityServiceCollectionExtensions.AddLatticeReplicationSecrets<FactoryStubSource>(
                null!,
                _ => new FactoryStubSource("x")),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeReplicationSecrets_throws_on_null_factory()
    {
        var services = new ServiceCollection();
        var builder = SiloBuilderWith(services);
        Assert.That(
            () => builder.AddLatticeReplicationSecrets<FactoryStubSource>(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeReplicationSecrets_typed_overload_registers_custom_source()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ILatticeReplicationSecretSource, EnvironmentVariableSecretSource>();
        var builder = SiloBuilderWith(services);

        builder.AddLatticeReplicationSecrets<StubSource>();
        using var sp = services.BuildServiceProvider();
        var resolved = sp.GetRequiredService<ILatticeReplicationSecretSource>();
        Assert.That(resolved, Is.InstanceOf<StubSource>());
    }

    [Test]
    public void AddLatticeReplicationSecrets_factory_overload_uses_supplied_factory()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ILatticeReplicationSecretSource, EnvironmentVariableSecretSource>();
        var builder = SiloBuilderWith(services);

        builder.AddLatticeReplicationSecrets<FactoryStubSource>(_ => new FactoryStubSource("factory-built"));
        using var sp = services.BuildServiceProvider();
        var resolved = (FactoryStubSource)sp.GetRequiredService<ILatticeReplicationSecretSource>();
        Assert.That(resolved.Token, Is.EqualTo("factory-built"));
    }

    [Test]
    public void AddLatticeReplicationSecretsFromConfiguration_replaces_default_source()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ILatticeReplicationSecretSource, EnvironmentVariableSecretSource>();
        var builder = SiloBuilderWith(services);

        var section = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["LatticeReplication:Secrets:Secret"] = "from-config",
            })
            .Build()
            .GetSection("LatticeReplication:Secrets");

        builder.AddLatticeReplicationSecretsFromConfiguration(section);
        using var sp = services.BuildServiceProvider();
        var resolved = sp.GetRequiredService<ILatticeReplicationSecretSource>();
        Assert.That(resolved, Is.InstanceOf<ConfigurationBindingSecretSource>());
    }

    [Test]
    public void AddLatticeReplicationSecretsFromConfiguration_throws_on_null_section()
    {
        var services = new ServiceCollection();
        var builder = SiloBuilderWith(services);
        Assert.That(
            () => builder.AddLatticeReplicationSecretsFromConfiguration(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeReplicationSecretsFromConfiguration_throws_on_null_builder()
    {
        var section = new ConfigurationBuilder().Build();
        Assert.That(
            () => LatticeReplicationSecurityServiceCollectionExtensions
                .AddLatticeReplicationSecretsFromConfiguration(null!, section),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ConfigureLatticeReplicationSecurity_applies_options()
    {
        var services = new ServiceCollection();
        services.AddOptions<LatticeReplicationSecurityOptions>();
        var builder = SiloBuilderWith(services);

        builder.ConfigureLatticeReplicationSecurity(o => o.RequireAuthentication = false);
        using var sp = services.BuildServiceProvider();
        var monitor = sp.GetRequiredService<Microsoft.Extensions.Options.IOptionsMonitor<LatticeReplicationSecurityOptions>>();
        Assert.That(monitor.CurrentValue.RequireAuthentication, Is.False);
    }

    [Test]
    public void ConfigureLatticeReplicationSecurity_throws_on_null_builder()
    {
        Assert.That(
            () => LatticeReplicationSecurityServiceCollectionExtensions
                .ConfigureLatticeReplicationSecurity(null!, _ => { }),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ConfigureLatticeReplicationSecurity_throws_on_null_configure()
    {
        var services = new ServiceCollection();
        var builder = SiloBuilderWith(services);
        Assert.That(
            () => builder.ConfigureLatticeReplicationSecurity(null!),
            Throws.ArgumentNullException);
    }
}
