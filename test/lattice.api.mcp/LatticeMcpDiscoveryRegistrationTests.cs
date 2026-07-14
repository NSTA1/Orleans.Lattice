using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Registration tests for the discovery-core wiring added to
/// <see cref="LatticeMcpServiceCollectionExtensions.AddLatticeMcp"/>: the default
/// permission resolver and the per-session configurator are registered, TryAdd
/// preserves a host override of the resolver, and both are singletons.
/// </summary>
[TestFixture]
public sealed class LatticeMcpDiscoveryRegistrationTests
{
    private static ServiceCollection SeedServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        return services;
    }

    [Test]
    public void AddLatticeMcp_registers_the_default_permission_resolver()
    {
        var services = SeedServices();
        services.AddLatticeMcp();

        using var provider = services.BuildServiceProvider();
        var resolver = provider.GetRequiredService<ILatticeApiMcpPermissionResolver>();
        Assert.That(resolver, Is.TypeOf<AuthAdminMcpPermissionResolver>());
    }

    [Test]
    public void AddLatticeMcp_preserves_a_host_supplied_permission_resolver()
    {
        var services = SeedServices();
        services.AddSingleton<ILatticeApiMcpPermissionResolver, StubResolver>();

        services.AddLatticeMcp();

        using var provider = services.BuildServiceProvider();
        var resolver = provider.GetRequiredService<ILatticeApiMcpPermissionResolver>();
        Assert.That(resolver, Is.TypeOf<StubResolver>(),
            "TryAdd must not overwrite a resolver the host registered first.");
    }

    [Test]
    public void AddLatticeMcp_registers_the_session_configurator()
    {
        var services = SeedServices();
        services.AddLatticeMcp();

        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetService<LatticeApiMcpSessionConfigurator>(), Is.Not.Null);
    }

    [Test]
    public void AddLatticeMcp_registers_the_configurator_as_a_singleton()
    {
        var services = SeedServices();
        services.AddLatticeMcp();

        using var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<LatticeApiMcpSessionConfigurator>();
        var second = provider.GetRequiredService<LatticeApiMcpSessionConfigurator>();
        Assert.That(first, Is.SameAs(second));
    }

    [Test]
    public void AddLatticeMcp_is_idempotent_for_the_permission_resolver()
    {
        var services = SeedServices();
        services.AddLatticeMcp();
        services.AddLatticeMcp();

        var registrations = services.Count(d => d.ServiceType == typeof(ILatticeApiMcpPermissionResolver));
        Assert.That(registrations, Is.EqualTo(1));
    }

    [Test]
    public void AddLatticeMcp_resolves_the_configurator_when_no_tool_groups_are_registered()
    {
        var services = SeedServices();
        services.AddLatticeMcp();

        using var provider = services.BuildServiceProvider();
        Assert.That(
            () => provider.GetRequiredService<LatticeApiMcpSessionConfigurator>(),
            Throws.Nothing,
            "The configurator must resolve with an empty tool-group set before the tool modules land.");
    }

    private sealed class StubResolver : ILatticeApiMcpPermissionResolver
    {
        public ValueTask<LatticeApiMcpAccessSet> ResolveAsync(
            LatticeCredential credential,
            CancellationToken cancellationToken)
            => new(LatticeApiMcpAccessSet.None);
    }
}
