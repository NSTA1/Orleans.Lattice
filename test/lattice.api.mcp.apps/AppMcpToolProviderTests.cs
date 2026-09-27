using ModelContextProtocol.Server;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

[TestFixture]
public sealed class AppMcpToolProviderTests
{
    [Test]
    public void Constructor_exposes_the_slug_and_a_copy_of_the_tools()
    {
        var tools = new List<McpServerTool> { AppMcpTestData.Tool("search") };
        var provider = new AppMcpToolProvider(AppSlug.Parse("notes"), tools);
        tools.Add(AppMcpTestData.Tool("later"));

        Assert.Multiple(() =>
        {
            Assert.That(provider.Slug, Is.EqualTo(AppSlug.Parse("notes")));
            Assert.That(provider.Tools.Select(t => t.ProtocolTool.Name), Is.EqualTo(new[] { "search" }));
        });
    }

    [Test]
    public void Constructor_rejects_invalid_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentException>(() => new AppMcpToolProvider(default, []));
            Assert.Throws<ArgumentNullException>(() => new AppMcpToolProvider(AppSlug.Parse("notes"), null!));
            Assert.Throws<ArgumentException>(() => new AppMcpToolProvider(AppSlug.Parse("notes"), [null!]));
        });
    }
}
