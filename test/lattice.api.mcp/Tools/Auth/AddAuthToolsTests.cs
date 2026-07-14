using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Auth;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Registration tests for
/// <see cref="LatticeMcpServiceCollectionExtensions.AddAuthTools"/>. Proves the
/// opt-in registers exactly one <see cref="AuthToolGroup"/> serving the auth
/// group, flips the <see cref="LatticeApiMcpOptions.EnableAuthTools"/> flag,
/// gates the mutating administration verbs behind the
/// <paramref name="enableAdministration"/> argument (defaulting to introspection
/// only), is idempotent, and validates its argument.
/// </summary>
[TestFixture]
public sealed class AddAuthToolsTests
{
    private static ServiceCollection SeedServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(Substitute.For<ILatticeAuthAdmin>());
        return services;
    }

    private static LatticeApiMcpOptions ResolveOptions(IServiceProvider provider)
        => provider.GetRequiredService<IOptions<LatticeApiMcpOptions>>().Value;

    [Test]
    public void AddAuthTools_registers_the_auth_tool_group()
    {
        var services = SeedServices();
        services.AddAuthTools();

        using var provider = services.BuildServiceProvider();
        var groups = provider.GetServices<ILatticeApiMcpToolGroup>().ToList();

        Assert.Multiple(() =>
        {
            Assert.That(groups, Has.Exactly(1).TypeOf<AuthToolGroup>());
            Assert.That(groups.Single().Group, Is.EqualTo(LatticeApiMcpGroup.Auth));
        });
    }

    [Test]
    public void AddAuthTools_enables_the_auth_tools_flag()
    {
        var services = SeedServices();
        services.AddAuthTools();

        using var provider = services.BuildServiceProvider();
        Assert.That(ResolveOptions(provider).EnableAuthTools, Is.True);
    }

    [Test]
    public void AddAuthTools_leaves_administration_disabled_by_default()
    {
        var services = SeedServices();
        services.AddAuthTools();

        using var provider = services.BuildServiceProvider();
        var options = ResolveOptions(provider);
        var group = (AuthToolGroup)provider.GetServices<ILatticeApiMcpToolGroup>().Single();

        Assert.Multiple(() =>
        {
            Assert.That(options.EnableAuthAdministration, Is.False);
            Assert.That(
                group.Tools.Select(t => t.ProtocolTool.Name),
                Does.Not.Contain("auth_put_rule"),
                "Administration verbs must be absent unless administration is opted in.");
        });
    }

    [Test]
    public void AddAuthTools_enables_administration_when_requested()
    {
        var services = SeedServices();
        services.AddAuthTools(enableAdministration: true);

        using var provider = services.BuildServiceProvider();
        var options = ResolveOptions(provider);
        var group = (AuthToolGroup)provider.GetServices<ILatticeApiMcpToolGroup>().Single();

        Assert.Multiple(() =>
        {
            Assert.That(options.EnableAuthAdministration, Is.True);
            Assert.That(
                group.Tools.Select(t => t.ProtocolTool.Name),
                Does.Contain("auth_put_rule"));
        });
    }

    [Test]
    public void AddAuthTools_is_idempotent_for_the_tool_group()
    {
        var services = SeedServices();
        services.AddAuthTools();
        services.AddAuthTools();

        var registrations = services.Count(d => d.ServiceType == typeof(ILatticeApiMcpToolGroup));
        Assert.That(registrations, Is.EqualTo(1));
    }

    [Test]
    public void AddAuthTools_rejects_a_null_service_collection()
    {
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddAuthTools());
    }
}
