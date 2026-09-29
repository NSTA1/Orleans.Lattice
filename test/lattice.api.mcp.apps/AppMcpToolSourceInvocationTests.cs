using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// Invocation stays in lock-step with advertisement: a withheld tool is absent from the
/// session collection that serves <c>tools/call</c>, and an advertised tool re-runs the same
/// decision when invoked, so a later revocation or disable takes effect mid-session.
/// </summary>
[TestFixture]
public sealed class AppMcpToolSourceInvocationTests
{
    private static readonly AppSlug Notes = AppSlug.Parse("notes");

    private static AppMcpTestHost NotesHost()
    {
        var host = new AppMcpTestHost();
        host.Source.Add(AppMcpTestData.ReaderManifest(Notes, AppMcpTestData.V1, "search"));
        host.Provide(Notes, AppMcpTestData.Tool("search", "found"));
        host.Publish(1, AppMcpTestData.Record(TenantId.Default, Notes, AppMcpTestData.V1));
        return host;
    }

    [Test]
    public async Task An_advertised_tool_invokes_the_apps_implementation()
    {
        var host = NotesHost();
        host.Member("alice", "g-readers");

        var tool = await host.SessionToolAsync("notes_search");
        var result = await host.InvokeAsync(tool!);

        Assert.That(result.Text(), Does.Contain("found"));
    }

    [Test]
    public async Task A_withheld_tool_is_absent_from_the_collection_that_serves_tools_call()
    {
        var host = NotesHost();

        Assert.That(await host.SessionToolAsync("notes_search"), Is.Null);
    }

    [Test]
    public async Task Revoking_the_membership_after_advertisement_denies_the_invocation()
    {
        var host = NotesHost();
        host.Member("alice", "g-readers");
        var tool = await host.SessionToolAsync("notes_search");

        host.Member("alice");

        Assert.ThrowsAsync<McpException>(() => host.InvokeAsync(tool!));
    }

    [Test]
    public async Task Disabling_the_app_after_advertisement_denies_the_invocation()
    {
        var host = NotesHost();
        host.Member("alice", "g-readers");
        var tool = await host.SessionToolAsync("notes_search");

        host.Publish(2, AppMcpTestData.Record(TenantId.Default, Notes, AppMcpTestData.V1, AppRegistryLifecycleState.Disabled));

        Assert.ThrowsAsync<McpException>(() => host.InvokeAsync(tool!));
    }

    [Test]
    public async Task Upgrading_the_app_after_advertisement_denies_the_stale_versions_invocation()
    {
        var host = NotesHost();
        host.Source.Add(AppMcpTestData.ReaderManifest(Notes, AppMcpTestData.V2, "search"));
        host.Member("alice", "g-readers");
        var tool = await host.SessionToolAsync("notes_search");

        host.Publish(2, AppMcpTestData.Record(TenantId.Default, Notes, AppMcpTestData.V2));

        Assert.ThrowsAsync<McpException>(() => host.InvokeAsync(tool!));
    }

    [Test]
    public async Task A_tool_advertised_to_one_tenant_is_denied_when_invoked_under_another()
    {
        var host = NotesHost();
        host.Member("alice", "g-readers");
        var tool = await host.SessionToolAsync("notes_search");

        Assert.ThrowsAsync<McpException>(() => host.InvokeAsync(tool!, TenantId.Parse("acme")));
    }

    [Test]
    public async Task The_current_region_is_an_accepted_target_but_a_peer_region_is_rejected()
    {
        var host = NotesHost();
        host.Member("alice", "g-readers");
        var tool = (await host.SessionToolAsync("notes_search"))!;
        var router = new LatticeApiMcpRegionRouter("home", [
            new LatticeApiMcpRegionDefinition { RegionId = "home", ClusterId = "c1", IsCurrent = true, Groups = new Dictionary<LatticeApiMcpGroup, string?> { [LatticeApiMcpGroup.Data] = null } },
            new LatticeApiMcpRegionDefinition { RegionId = "peer", ClusterId = "c2", IsCurrent = false, Groups = new Dictionary<LatticeApiMcpGroup, string?> { [LatticeApiMcpGroup.Data] = "https://peer" } },
        ]);
        var services = new RouterServiceProvider(host.Services, router);
        host.Services.GetRequiredService<Microsoft.AspNetCore.Http.IHttpContextAccessor>().HttpContext = host.Context();

        var home = await McpToolInvocation.CallAsync(tool, services, Args("home"));

        Assert.Multiple(() =>
        {
            Assert.That(home.Text(), Does.Contain("found"));
            var fault = Assert.ThrowsAsync<McpException>(() => McpToolInvocation.CallAsync(tool, services, Args("peer")));
            Assert.That(fault!.Message, Does.Contain("current region"));
        });
    }

    private static Dictionary<string, JsonElement> Args(string region)
        => new() { ["region"] = JsonSerializer.SerializeToElement(region) };

    private sealed class RouterServiceProvider(IServiceProvider inner, ILatticeApiMcpRegionRouter router) : IServiceProvider
    {
        public object? GetService(Type serviceType)
            => serviceType == typeof(ILatticeApiMcpRegionRouter) ? router : inner.GetService(serviceType);
    }
}
