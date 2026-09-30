using System.Text.Json;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// The epic's hard non-goal, asserted: registering the app tool surface changes nothing the
/// discovery core already reports. With the package absent, and with it registered but no
/// app enabled (or an enabled app whose tools the caller may not use), the
/// <c>lattice_capabilities</c> payload, the advertised tool list and the server
/// instructions are byte-identical.
/// </summary>
[TestFixture]
public sealed class LatticeCapabilitiesUnchangedTests
{
    private static readonly AppSlug Notes = AppSlug.Parse("notes");

    private static readonly LatticeApiMcpAccessSet Access =
        AppTestAccessSets.Granting(LatticeApiMcpGroup.Data, LatticeApiMcpGroup.State);

    private static async Task<(string Capabilities, string Tools, string Instructions)> SnapshotAsync(AppMcpTestHost host)
    {
        var configurator = host.Configurator(
            Access,
            new FakeToolGroup(LatticeApiMcpGroup.Data, "data_read", "data_write"),
            new FakeToolGroup(LatticeApiMcpGroup.State, "state_tree"));
        var plan = await configurator.BuildSessionPlanAsync(host.Context(), CancellationToken.None);

        var capabilitiesTool = plan.Tools.Single(t => t.ProtocolTool.Name == "lattice_capabilities");
        var result = await McpToolInvocation.CallAsync(capabilitiesTool, host.Services);
        var capabilities = JsonSerializer.Serialize(result.StructuredContent) + "|" + result.Text();
        var tools = string.Join(
            "\n",
            plan.Tools.Select(t => JsonSerializer.Serialize(t.ProtocolTool)).OrderBy(s => s, StringComparer.Ordinal));
        return (capabilities, tools, plan.Instructions);
    }

    [Test]
    public async Task Registered_with_no_enabled_app_is_identical_to_absent()
    {
        var absent = await SnapshotAsync(new AppMcpTestHost(registerApps: false));

        var registered = new AppMcpTestHost();
        registered.Source.Add(AppMcpTestData.ReaderManifest(Notes, AppMcpTestData.V1, "search"));
        registered.Provide(Notes, AppMcpTestData.Tool("search"));
        registered.Publish(1, AppMcpTestData.Record(TenantId.Default, Notes, AppMcpTestData.V1, AppRegistryLifecycleState.Disabled));
        var present = await SnapshotAsync(registered);

        Assert.Multiple(() =>
        {
            Assert.That(present.Capabilities, Is.EqualTo(absent.Capabilities));
            Assert.That(present.Tools, Is.EqualTo(absent.Tools));
            Assert.That(present.Instructions, Is.EqualTo(absent.Instructions));
            Assert.That(absent.Tools, Does.Contain("data_read"), "The snapshot must cover real group tools, not an empty list.");
        });
    }

    [Test]
    public async Task Registered_with_an_empty_registry_is_identical_to_absent()
    {
        var absent = await SnapshotAsync(new AppMcpTestHost(registerApps: false));
        var registered = new AppMcpTestHost().Provide(Notes, AppMcpTestData.Tool("search"));
        registered.Publish(1);

        Assert.That(await SnapshotAsync(registered), Is.EqualTo(absent));
    }

    [Test]
    public async Task An_enabled_app_changes_only_the_tool_list_never_the_capability_report_or_instructions()
    {
        var absent = await SnapshotAsync(new AppMcpTestHost(registerApps: false));
        var registered = new AppMcpTestHost();
        registered.Source.Add(AppMcpTestData.ReaderManifest(Notes, AppMcpTestData.V1, "search"));
        registered.Provide(Notes, AppMcpTestData.Tool("search"));
        registered.Publish(1, AppMcpTestData.Record(TenantId.Default, Notes, AppMcpTestData.V1));
        registered.Bind("alice");

        var present = await SnapshotAsync(registered);

        Assert.Multiple(() =>
        {
            Assert.That(present.Capabilities, Is.EqualTo(absent.Capabilities));
            Assert.That(present.Instructions, Is.EqualTo(absent.Instructions));
            Assert.That(present.Tools, Does.Contain("notes_search"));
        });
    }
}
