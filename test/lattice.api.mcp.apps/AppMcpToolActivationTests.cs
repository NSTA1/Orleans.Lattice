using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Server;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

[TestFixture]
public sealed class AppMcpToolActivationTests
{
    private static readonly AppSlug Notes = AppSlug.Parse("notes");

    private static AppMcpToolSource Owner() => new([], NullLogger<AppMcpToolSource>.Instance);

    private static IAppMcpToolProvider Provider(params string[] names)
        => new AppMcpToolProvider(Notes, names.Select(n => AppMcpTestData.Tool(n)));

    [Test]
    public void Pair_namespaces_every_declared_tool_in_declaration_order()
    {
        var manifest = AppMcpTestData.ReaderManifest(Notes, AppMcpTestData.V1, "search", "read");

        var activation = AppMcpToolActivation.Pair(manifest, [Provider("read", "search")], Owner());

        Assert.Multiple(() =>
        {
            Assert.That(activation.Succeeded, Is.True);
            Assert.That(activation.Tools.Select(t => t.ProtocolTool.Name), Is.EqualTo(new[] { "notes_search", "notes_read" }));
            Assert.That(activation.Tools.Select(t => t.LocalName), Is.EqualTo(new[] { "search", "read" }));
            Assert.That(activation.TryGetTool("read", out var read) && read.RoleIndex == 0, Is.True);
            Assert.That(activation.TryGetTool("missing", out _), Is.False);
        });
    }

    [Test]
    public void Pair_unions_the_tools_of_every_provider_for_the_slug()
    {
        var manifest = AppMcpTestData.ReaderManifest(Notes, AppMcpTestData.V1, "search", "read");

        var activation = AppMcpToolActivation.Pair(manifest, [Provider("search"), Provider("read")], Owner());

        Assert.That(activation.Succeeded, Is.True);
    }

    [TestCase("search,search", TestName = "Pair_fails_on_a_local_name_implemented_twice_by_one_provider")]
    [TestCase("search", TestName = "Pair_fails_on_a_declared_tool_without_an_implementation")]
    [TestCase("search,read,extra", TestName = "Pair_fails_on_an_implementation_the_manifest_does_not_declare")]
    public void Pair_hard_fails_a_mismatch_and_contributes_no_tools(string implemented)
    {
        var manifest = AppMcpTestData.ReaderManifest(Notes, AppMcpTestData.V1, "search", "read");

        var activation = AppMcpToolActivation.Pair(manifest, [Provider(implemented.Split(','))], Owner());

        Assert.Multiple(() =>
        {
            Assert.That(activation.Succeeded, Is.False);
            Assert.That(activation.Failure, Is.Not.Null.And.Not.Empty);
            Assert.That(activation.Tools, Is.Empty);
        });
    }

    [Test]
    public void Pair_fails_on_a_local_name_implemented_by_two_providers()
    {
        var manifest = AppMcpTestData.ReaderManifest(Notes, AppMcpTestData.V1, "search");

        var activation = AppMcpToolActivation.Pair(manifest, [Provider("search"), Provider("search")], Owner());

        Assert.That(activation.Failure, Does.Contain("implemented more than once"));
    }

    [Test]
    public void Pair_fails_on_a_local_name_declared_twice()
    {
        var manifest = AppMcpTestData.ReaderManifest(Notes, AppMcpTestData.V1, "search", "search");

        var activation = AppMcpToolActivation.Pair(manifest, [Provider("search")], Owner());

        Assert.That(activation.Failure, Does.Contain("declared more than once"));
    }

    [Test]
    public void Pair_fails_on_a_declaration_naming_an_undeclared_role()
    {
        var manifest = AppMcpTestData.Manifest(
            Notes, AppMcpTestData.V1, [], [AppMcpTestData.ToolDecl("search", "ghost")]);

        var activation = AppMcpToolActivation.Pair(manifest, [Provider("search")], Owner());

        Assert.That(activation.Failure, Does.Contain("undeclared role"));
    }

    [Test]
    public void Failed_records_the_identity_and_reason()
    {
        var activation = AppMcpToolActivation.Failed(Notes, AppMcpTestData.V1, "why");

        Assert.Multiple(() =>
        {
            Assert.That(activation.Slug, Is.EqualTo(Notes));
            Assert.That(activation.Version, Is.EqualTo(AppMcpTestData.V1));
            Assert.That(activation.Manifest, Is.Null);
            Assert.That(activation.Succeeded, Is.False);
            Assert.That(activation.Failure, Is.EqualTo("why"));
        });
    }

    [Test]
    public void Pair_fails_on_an_implementation_carrying_no_name()
    {
        // A nameless implementation cannot be paired with a declaration or addressed over the
        // wire, and it must not be silently skipped: skipping it would leave the app's declared
        // tool unimplemented, which is the mismatch the pairing exists to refuse.
        var manifest = AppMcpTestData.ReaderManifest(Notes, AppMcpTestData.V1, "search");

        var activation = AppMcpToolActivation.Pair(
            manifest, [new RawToolProvider(Notes, new McpServerTool[] { null! })], Owner());

        Assert.Multiple(() =>
        {
            Assert.That(activation.Succeeded, Is.False);
            Assert.That(activation.Failure, Does.Contain("no name"));
            Assert.That(activation.Tools, Is.Empty);
        });
    }
}
