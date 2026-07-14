using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Registration tests for
/// <see cref="LatticeMcpStateToolsServiceCollectionExtensions.AddStateTools"/>:
/// the state tool module is registered as a single <see cref="ILatticeApiMcpToolGroup"/>,
/// the opt-in is reflected on the binding options, the call is idempotent, it
/// guards a null argument, and the registered group resolves against a provider
/// that has the state facade.
/// </summary>
[TestFixture]
public sealed class LatticeMcpStateToolsServiceCollectionExtensionsTests
{
    private static ServiceCollection SeedServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(Substitute.For<ILatticeStateQuery>());
        return services;
    }

    [Test]
    public void AddStateTools_registers_the_state_tool_group()
    {
        var services = SeedServices();
        services.AddLatticeMcp();

        services.AddStateTools();

        using var provider = services.BuildServiceProvider();
        var groups = provider.GetServices<ILatticeApiMcpToolGroup>().ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(groups, Has.Length.EqualTo(1));
            Assert.That(groups[0], Is.TypeOf<StateToolGroup>());
            Assert.That(groups[0].Group, Is.EqualTo(LatticeApiMcpGroup.State));
        });
    }

    [Test]
    public void AddStateTools_enables_the_state_tools_option()
    {
        var services = SeedServices();
        services.AddLatticeMcp();

        services.AddStateTools();

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<LatticeApiMcpOptions>>().Value;
        Assert.That(options.EnableStateTools, Is.True);
    }

    [Test]
    public void AddStateTools_is_idempotent()
    {
        var services = SeedServices();
        services.AddLatticeMcp();

        services.AddStateTools();
        services.AddStateTools();

        using var provider = services.BuildServiceProvider();
        var groups = provider.GetServices<ILatticeApiMcpToolGroup>().ToArray();
        Assert.That(groups, Has.Length.EqualTo(1),
            "TryAddEnumerable must contribute exactly one state tool group across repeated calls.");
    }

    [Test]
    public void AddStateTools_registers_the_group_as_a_singleton()
    {
        var services = SeedServices();
        services.AddLatticeMcp();
        services.AddStateTools();

        using var provider = services.BuildServiceProvider();
        var first = provider.GetServices<ILatticeApiMcpToolGroup>().Single();
        var second = provider.GetServices<ILatticeApiMcpToolGroup>().Single();
        Assert.That(first, Is.SameAs(second));
    }

    [Test]
    public void AddStateTools_returns_the_same_service_collection()
    {
        var services = SeedServices();

        var returned = services.AddStateTools();

        Assert.That(returned, Is.SameAs(services));
    }

    [Test]
    public void AddStateTools_rejects_a_null_service_collection()
    {
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddStateTools());
    }
}
