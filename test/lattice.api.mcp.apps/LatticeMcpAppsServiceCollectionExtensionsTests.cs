using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

[TestFixture]
public sealed class LatticeMcpAppsServiceCollectionExtensionsTests
{
    [Test]
    public void AddAppMcpTools_registers_one_source_shared_with_the_discovery_seam_even_when_called_twice()
    {
        var services = new ServiceCollection().AddLogging();
        services.AddAppMcpTools().AddAppMcpTools();
        using var provider = services.BuildServiceProvider();

        var sources = provider.GetServices<ILatticeApiMcpAppToolSource>().ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(sources, Has.Length.EqualTo(1));
            Assert.That(sources[0], Is.SameAs(provider.GetRequiredService<AppMcpToolSource>()));
        });
    }

    [Test]
    public void AddAppMcpTools_rejects_null_services()
        => Assert.Throws<ArgumentNullException>(() => LatticeMcpAppsServiceCollectionExtensions.AddAppMcpTools(null!));

    [Test]
    public async Task A_host_without_the_app_registry_offers_no_app_tools()
    {
        var services = new ServiceCollection().AddLogging();
        services.AddSingleton<IAppMcpToolProvider>(
            new AppMcpToolProvider(AppMcpTestData.Slug("notes"), [AppMcpTestData.Tool("search")]));
        services.AddAppMcpTools();
        using var provider = services.BuildServiceProvider();

        var tools = await provider.GetRequiredService<AppMcpToolSource>().GetPermittedToolsAsync(
            new Microsoft.AspNetCore.Http.DefaultHttpContext(), new LatticeCredential("t"), CancellationToken.None);

        Assert.That(tools, Is.Empty);
    }
}
