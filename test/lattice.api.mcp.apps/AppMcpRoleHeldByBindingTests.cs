using ModelContextProtocol;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// Regression coverage for #3902 on the app MCP tool surface: a tool is offered, and invocable, exactly when the
/// caller holds its declared role by binding - membership of a group the install binds to the role - never
/// because of rights the caller holds outside the app's own rules. The shared evaluator reports the same roles.
/// </summary>
[TestFixture]
public sealed class AppMcpRoleHeldByBindingTests
{
    private static readonly AppSlug Notes = AppMcpTestData.Slug("notes");
    private const LatticeOperation Everything =
        LatticeOperation.Read | LatticeOperation.RangeRead | LatticeOperation.Write | LatticeOperation.Delete;

    private static AppManifest Manifest() =>
        AppMcpTestData.Manifest(
            Notes,
            AppMcpTestData.V1,
            [
                AppMcpTestData.Role("viewer", LatticeOperation.Read, AppMcpTestData.TreeScope("notes")),
                AppMcpTestData.Role("editor", LatticeOperation.Read | LatticeOperation.Write, AppMcpTestData.TreeScope("notes")),
            ],
            [AppMcpTestData.ToolDecl("read", "viewer"), AppMcpTestData.ToolDecl("write", "editor")]);

    private static AppRegistryRecord Record(long revision = 1, string viewers = "g-viewers") =>
        AppMcpTestData.Record(
            TenantId.Default,
            Notes,
            AppMcpTestData.V1,
            bindings: [AppRoleBinding.Create("viewer", viewers), AppRoleBinding.Create("editor", "g-editors")]) with
        {
            Revision = revision,
        };

    private static AppMcpTestHost Host()
    {
        var host = new AppMcpTestHost()
            .Provide(Notes, AppMcpTestData.Tool("read"), AppMcpTestData.Tool("write"))
            .Publish(1, Record());
        host.Source.Add(Manifest());
        return host;
    }

    private static async Task<string[]> AppToolsAsync(AppMcpTestHost host) =>
        [.. (await host.AdvertisedAsync()).Where(name => name.StartsWith("notes_", StringComparison.Ordinal))];

    [Test]
    public async Task A_caller_with_broad_rights_bound_only_to_viewer_is_offered_the_viewer_tools_only()
    {
        var host = Host().Member("alice", "g-viewers");
        host.Gate.Grant("alice", "a/notes/notes", Everything);

        var evaluation = await new AppRoleGrantEvaluator(host.Projection, host.Source, host.Gate)
            .EvaluateAsync(TenantId.Default, Notes, new LatticeSubject("alice", ["g-viewers"]), CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await AppToolsAsync(host), Is.EqualTo(new[] { "notes_read" }));
            Assert.That(evaluation!.HeldRoles, Is.EqualTo(new[] { "viewer" }));
            Assert.That(await host.SessionToolAsync("notes_write"), Is.Null, "a withheld tool is not invocable either");
        });
    }

    [Test]
    public async Task A_bound_editor_is_offered_the_editor_tools()
    {
        var host = Host().Member("alice", "g-editors");

        Assert.That(await AppToolsAsync(host), Is.EqualTo(new[] { "notes_write" }));
        Assert.That((await host.InvokeAsync((await host.SessionToolAsync("notes_write"))!)).Text(), Does.Contain("ok"));
    }

    [Test]
    public async Task A_caller_bound_to_no_role_is_offered_no_app_tool_whatever_its_rights()
    {
        var host = Host().Member("alice", "g-operators");
        host.Gate.Grant("alice", "a/notes/notes", Everything);
        host.Gate.Grant("alice", LatticeScope.ClusterWideTreeId, Everything);

        Assert.That(await AppToolsAsync(host), Is.Empty);
        Assert.That(host.Gate.Requests, Is.Empty, "holding a role never consults the access gate");
    }

    [Test]
    public async Task Rebinding_a_role_moves_its_tools_on_the_next_evaluation()
    {
        var host = Host().Member("alice", "g-reviewers");
        host.Gate.Grant("alice", "a/notes/notes", Everything);
        Assert.That(await AppToolsAsync(host), Is.Empty);

        host.Publish(2, Record(revision: 2, viewers: "g-reviewers"));

        Assert.That(await AppToolsAsync(host), Is.EqualTo(new[] { "notes_read" }));
    }

    [Test]
    public async Task Leaving_the_bound_group_after_advertisement_denies_the_invocation()
    {
        var host = Host().Member("alice", "g-viewers");
        var tool = await host.SessionToolAsync("notes_read");
        Assert.That(tool, Is.Not.Null);

        host.Member("alice");

        Assert.ThrowsAsync<McpException>(() => host.InvokeAsync(tool!));
    }
}
