using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// An app whose slug is the leading segment of a built-in tool namespace would advertise
/// <c>{slug}_{tool}</c> names inside that namespace (<c>lattice_state_get</c>,
/// <c>repocontext_search</c>). For a caller the built-in tool is withheld from, nothing
/// collides, so the app tool would be offered under the built-in's name. Such slugs must
/// contribute no tools at all.
/// </summary>
[TestFixture]
public sealed class AppMcpToolActivationReservedSlugTests
{
    [TestCase("lattice", "state_get")]
    [TestCase("lattice", "capabilities")]
    [TestCase("repocontext", "search")]
    public void Pair_refuses_a_slug_that_owns_a_built_in_tool_namespace(string slugText, string tool)
    {
        var slug = AppSlug.Parse(slugText);
        var manifest = AppMcpTestData.ReaderManifest(slug, AppMcpTestData.V1, tool);
        var provider = new AppMcpToolProvider(slug, [AppMcpTestData.Tool(tool)]);

        var activation = AppMcpToolActivation.Pair(manifest, [provider], new([], NullLogger<AppMcpToolSource>.Instance));

        Assert.Multiple(() =>
        {
            Assert.That(activation.Succeeded, Is.False);
            Assert.That(activation.Tools, Is.Empty);
            Assert.That(activation.Failure, Does.Contain("reserved"));
        });
    }

    [TestCase("repo-context")]
    [TestCase("lattice-crm")]
    [TestCase("notes")]
    public void Pair_accepts_a_slug_outside_the_built_in_namespaces(string slugText)
    {
        var slug = AppSlug.Parse(slugText);
        var manifest = AppMcpTestData.ReaderManifest(slug, AppMcpTestData.V1, "search");
        var provider = new AppMcpToolProvider(slug, [AppMcpTestData.Tool("search")]);

        var activation = AppMcpToolActivation.Pair(manifest, [provider], new([], NullLogger<AppMcpToolSource>.Instance));

        Assert.That(activation.Succeeded, Is.True, activation.Failure);
    }

    [TestCase("lattice", true)]
    [TestCase("repocontext", true)]
    [TestCase("repo-context", false)]
    [TestCase("lattices", false)]
    public void IsReservedSlug_names_exactly_the_built_in_namespaces(string slug, bool reserved) =>
        Assert.That(AppMcpToolName.IsReservedSlug(AppSlug.Parse(slug)), Is.EqualTo(reserved));
}
