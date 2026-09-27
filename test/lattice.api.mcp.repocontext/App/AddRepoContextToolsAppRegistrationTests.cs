using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Mcp.Apps;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.App;

/// <summary>
/// The opt-in dual registration: with the flag off (the default) the registration and the
/// wire-visible MCP surface are byte-identical to the package without the app path; with the
/// flag on the app is registered in-image and, once installed and enabled, its tools are
/// additionally advertised as <c>repo-context_{tool}</c> and invocable, while the group tools
/// and the <c>lattice_capabilities</c> report are unchanged.
/// </summary>
[TestFixture]
public sealed class AddRepoContextToolsAppRegistrationTests
{
    // The pre-epic registration shape: the group alone, exactly as AddRepoContextTools()
    // builds it with no workspace root (so no enforcing guard).
    private static RepoContextAppTestHost WithoutAppPath()
        => new(services => services.AddSingleton<ILatticeApiMcpToolGroup>(new RepoContextToolGroup(workspaceGuarded: false)));

    private static RepoContextAppTestHost FlagOff()
        => new(services => services.AddRepoContextTools());

    private static RepoContextAppTestHost FlagOn()
        => new(services => services.AddRepoContextTools(registerAsApp: true));

    private static RepoContextAppTestHost EnabledAndGranted()
    {
        var host = FlagOn();
        host.Projection.Publish(1, RepoContextAppTestHost.Record());
        host.Gate.Grant(RepoContextAppTestHost.Principal, RepoContextTrees.Memory, LatticeOperation.Read | LatticeOperation.RangeRead);
        return host;
    }

    private static string[] AppToolNames()
        => RepoContextAppManifest.AppToolNames.Select(n => "repo-context_" + n).Order(StringComparer.Ordinal).ToArray();

    [Test]
    public void Flag_defaults_off_and_registers_no_app_services()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools();

        Assert.Multiple(() =>
        {
            Assert.That(services.Any(d => d.ServiceType == typeof(IAppMcpToolProvider)), Is.False);
            Assert.That(services.Any(d => d.ServiceType == typeof(ILatticeApiMcpAppToolSource)), Is.False);
            Assert.That(services.Any(d => d.ServiceType == typeof(IConfigureOptions<InImageAppSourceOptions>)), Is.False);
        });
    }

    [Test]
    public void Flag_off_registers_the_same_service_descriptors_as_the_default()
    {
        var defaults = new ServiceCollection().AddRepoContextTools();
        var flagOff = new ServiceCollection().AddRepoContextTools(registerAsApp: false);

        Assert.That(
            flagOff.Select(d => (d.ServiceType, d.ImplementationType, d.Lifetime)),
            Is.EqualTo(defaults.Select(d => (d.ServiceType, d.ImplementationType, d.Lifetime))));
    }

    [Test]
    public async Task Flag_off_capabilities_and_advertised_tools_are_byte_identical_to_the_package_without_the_app_path()
    {
        var baseline = await WithoutAppPath().SnapshotAsync();
        var flagOff = await FlagOff().SnapshotAsync();

        Assert.Multiple(() =>
        {
            Assert.That(flagOff.Capabilities, Is.EqualTo(baseline.Capabilities));
            Assert.That(flagOff.Tools, Is.EqualTo(baseline.Tools));
            Assert.That(flagOff.Instructions, Is.EqualTo(baseline.Instructions));
            Assert.That(baseline.Tools, Does.Contain("\"repocontext_search\""), "the snapshot must cover the real group tools");
            Assert.That(baseline.Tools, Does.Not.Contain("repo-context_"));
        });
    }

    [Test]
    public async Task Flag_on_without_an_installed_app_is_byte_identical_to_flag_off()
    {
        var flagOff = await FlagOff().SnapshotAsync();
        var flagOn = await FlagOn().SnapshotAsync();

        Assert.That(flagOn, Is.EqualTo(flagOff));
    }

    [Test]
    public async Task Flag_on_with_the_app_disabled_is_byte_identical_to_flag_off()
    {
        var flagOff = await FlagOff().SnapshotAsync();
        var host = FlagOn();
        host.Projection.Publish(1, RepoContextAppTestHost.Record(AppRegistryLifecycleState.Disabled));
        host.Gate.Grant(RepoContextAppTestHost.Principal, RepoContextTrees.Memory, LatticeOperation.Read | LatticeOperation.RangeRead);

        Assert.That(await host.SnapshotAsync(), Is.EqualTo(flagOff));
    }

    [Test]
    public void Flag_on_registers_the_in_image_app_the_app_tool_surface_and_one_provider()
    {
        var services = new ServiceCollection().AddLogging().AddRepoContextTools(registerAsApp: true);
        using var provider = services.BuildServiceProvider();

        var registrations = provider.GetRequiredService<IOptions<InImageAppSourceOptions>>().Value.Registrations;

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetServices<IAppMcpToolProvider>().Single(), Is.InstanceOf<RepoContextAppMcpToolProvider>());
            Assert.That(provider.GetServices<ILatticeApiMcpAppToolSource>().Count(), Is.EqualTo(1));
            Assert.That(registrations, Has.Count.EqualTo(1));
            Assert.That(registrations[0].Slug.Value, Is.EqualTo(RepoContextAppManifest.Slug));
            Assert.That(registrations[0].ManifestResourceName, Is.EqualTo(RepoContextAppManifest.ResourceName));
            Assert.That(registrations[0].Assembly, Is.SameAs(typeof(RepoContextAppManifest).Assembly));
        });
    }

    [Test]
    public void Flag_on_is_idempotent_across_repeated_calls()
    {
        var services = new ServiceCollection()
            .AddLogging()
            .AddRepoContextTools(registerAsApp: true)
            .AddRepoContextTools(registerAsApp: true);
        using var provider = services.BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetServices<IAppMcpToolProvider>().Count(), Is.EqualTo(1));
            Assert.That(provider.GetServices<ILatticeApiMcpAppToolSource>().Count(), Is.EqualTo(1));
            Assert.That(provider.GetRequiredService<IOptions<InImageAppSourceOptions>>().Value.Registrations, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void Flag_on_provider_adapts_the_registered_group_when_a_later_call_turns_the_flag_on()
    {
        var services = new ServiceCollection()
            .AddRepoContextTools()
            .AddRepoContextTools(registerAsApp: true);
        using var provider = services.BuildServiceProvider();

        var group = (RepoContextToolGroup)provider.GetServices<ILatticeApiMcpToolGroup>().Single();
        var appProvider = provider.GetServices<IAppMcpToolProvider>().Single();
        var search = appProvider.Tools.Single(t => t.ProtocolTool.Name == "search");

        Assert.That(
            search.ProtocolTool.InputSchema.GetRawText(),
            Is.EqualTo(group.Tools.Single(t => t.ProtocolTool.Name == "repocontext_search").ProtocolTool.InputSchema.GetRawText()));
    }

    [Test]
    public async Task Flag_on_with_the_app_enabled_advertises_the_app_tools_alongside_the_unchanged_group_tools()
    {
        var flagOff = await FlagOff().SnapshotAsync();
        var groupTools = await FlagOff().AdvertisedAsync();
        var host = EnabledAndGranted();

        var advertised = await host.AdvertisedAsync();
        var snapshot = await host.SnapshotAsync();

        Assert.Multiple(() =>
        {
            Assert.That(advertised.Where(n => n.StartsWith("repo-context_", StringComparison.Ordinal)), Is.EqualTo(AppToolNames()));
            Assert.That(advertised.Where(n => !n.StartsWith("repo-context_", StringComparison.Ordinal)), Is.EqualTo(groupTools));
            Assert.That(snapshot.Capabilities, Is.EqualTo(flagOff.Capabilities));
            Assert.That(snapshot.Instructions, Is.EqualTo(flagOff.Instructions));
        });
    }

    [Test]
    public async Task Flag_on_with_the_app_enabled_offers_no_app_tools_to_a_caller_without_the_reader_role()
    {
        var host = FlagOn();
        host.Projection.Publish(1, RepoContextAppTestHost.Record());
        host.Gate.Grant(RepoContextAppTestHost.Principal, RepoContextTrees.Memory, LatticeOperation.Read);

        var advertised = await host.AdvertisedAsync();

        Assert.That(advertised.Where(n => n.StartsWith("repo-context_", StringComparison.Ordinal)), Is.Empty);
    }

    [Test]
    public async Task Flag_on_app_tool_is_invocable_through_the_app_surface()
    {
        var host = EnabledAndGranted();
        var tool = await host.SessionToolAsync("repo-context_stats");
        Assert.That(tool, Is.Not.Null);

        var result = await host.InvokeAsync(tool!);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsError, Is.Not.True);
            Assert.That(result.StructuredContent.ToString(), Does.Contain("calls"));
        });
    }

    [Test]
    public async Task Flag_on_app_tool_is_refused_once_the_caller_loses_the_reader_role()
    {
        var host = EnabledAndGranted();
        var tool = await host.SessionToolAsync("repo-context_stats");
        Assert.That(tool, Is.Not.Null);

        host.Gate.RevokeAll();

        Assert.That(async () => await host.InvokeAsync(tool!), Throws.InstanceOf<ModelContextProtocol.McpException>());
    }
}
