using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>
/// End-to-end discovery through the real session configurator: namespaced advertisement,
/// per-role gating, intra-app duplicate failing activation, cross-app name reuse, tenant
/// isolation, and epoch-driven rebuilds.
/// </summary>
[TestFixture]
public sealed class AppMcpToolSourceDiscoveryTests
{
    private static readonly AppSlug Notes = AppSlug.Parse("notes");
    private static readonly AppSlug Tasks = AppSlug.Parse("tasks");

    private static AppMcpTestHost NotesHost()
    {
        var host = new AppMcpTestHost();
        host.Source.Add(AppMcpTestData.ReaderManifest(Notes, AppMcpTestData.V1, "search"));
        host.Provide(Notes, AppMcpTestData.Tool("search"));
        host.Publish(1, AppMcpTestData.Record(TenantId.Default, Notes, AppMcpTestData.V1));
        return host;
    }

    [Test]
    public async Task An_enabled_apps_tool_is_advertised_under_its_namespaced_name_when_the_caller_holds_its_role()
    {
        var host = NotesHost();
        host.Member("alice", "g-readers");

        Assert.That(await host.AdvertisedAsync(), Is.EqualTo(new[] { "lattice_capabilities", "notes_search" }));
    }

    [Test]
    public async Task A_caller_without_the_role_is_not_offered_the_tool()
    {
        var host = NotesHost();
        host.Member("alice", "g-writers");

        Assert.That(await host.AdvertisedAsync(), Is.EqualTo(new[] { "lattice_capabilities" }));
    }

    [Test]
    public async Task The_role_is_evaluated_for_the_callers_own_credential()
    {
        var host = NotesHost().Member("bob", "g-readers");

        Assert.That(await host.AdvertisedAsync(), Does.Not.Contain("notes_search"), "bob's membership is not alice's");

        host.Member("alice", "g-readers");
        Assert.That(await host.AdvertisedAsync(), Does.Contain("notes_search"));
    }

    [Test]
    public async Task An_unauthenticated_session_is_offered_no_app_tools_and_the_source_is_not_consulted()
    {
        var host = NotesHost();
        host.Member("alice", "g-readers");
        host.Bridge.Credential = null;

        Assert.Multiple(async () =>
        {
            Assert.That(await host.AdvertisedAsync(), Is.Empty);
            Assert.That(host.Gate.Requests, Is.Empty);
        });
    }

    [Test]
    public async Task The_coarse_authorizer_still_governs_app_tools()
    {
        var host = new AppMcpTestHost(authorizer: new NameAuthorizer("lattice_capabilities"));
        host.Source.Add(AppMcpTestData.ReaderManifest(Notes, AppMcpTestData.V1, "search"));
        host.Provide(Notes, AppMcpTestData.Tool("search"));
        host.Publish(1, AppMcpTestData.Record(TenantId.Default, Notes, AppMcpTestData.V1));
        host.Member("alice", "g-readers");

        Assert.That(await host.AdvertisedAsync(), Is.EqualTo(new[] { "lattice_capabilities" }));
    }

    [Test]
    public async Task Tools_of_one_app_are_gated_by_their_own_declared_roles()
    {
        var host = new AppMcpTestHost();
        host.Source.Add(AppMcpTestData.Manifest(
            Notes,
            AppMcpTestData.V1,
            [
                AppMcpTestData.Role("reader", LatticeOperation.Read, AppMcpTestData.TreeScope("notes")),
                AppMcpTestData.Role("writer", LatticeOperation.Write, AppMcpTestData.TreeScope("notes")),
            ],
            [AppMcpTestData.ToolDecl("search", "reader"), AppMcpTestData.ToolDecl("put", "writer"), AppMcpTestData.ToolDecl("list", "reader")]));
        host.Provide(Notes, AppMcpTestData.Tool("search"), AppMcpTestData.Tool("put"), AppMcpTestData.Tool("list"));
        host.Publish(1, AppMcpTestData.Record(TenantId.Default, Notes, AppMcpTestData.V1));
        host.Member("alice", "g-readers");

        var advertised = await host.AdvertisedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(advertised, Is.EqualTo(new[] { "lattice_capabilities", "notes_list", "notes_search" }));
            Assert.That(host.Gate.Requests, Is.Empty, "A role is held by binding; the access gate is not consulted.");
        });
    }

    [Test]
    public async Task An_intra_app_duplicate_fails_that_apps_activation_while_other_apps_are_unaffected()
    {
        var host = new AppMcpTestHost();
        host.Source
            .Add(AppMcpTestData.ReaderManifest(Notes, AppMcpTestData.V1, "search"))
            .Add(AppMcpTestData.ReaderManifest(Tasks, AppMcpTestData.V1, "search"));
        host.Provide(Notes, AppMcpTestData.Tool("search", "first"));
        host.Provide(Notes, AppMcpTestData.Tool("search", "shadow"));
        host.Provide(Tasks, AppMcpTestData.Tool("search"));
        host.Publish(
            1,
            AppMcpTestData.Record(TenantId.Default, Notes, AppMcpTestData.V1),
            AppMcpTestData.Record(TenantId.Default, Tasks, AppMcpTestData.V1));
        host.Member("alice", "g-readers");

        var advertised = await host.AdvertisedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(advertised, Is.EqualTo(new[] { "lattice_capabilities", "tasks_search" }));
            Assert.That(host.ToolSource.Catalog.Failures.Select(f => f.Slug), Is.EqualTo(new[] { Notes }));
        });
    }

    [Test]
    public async Task Two_apps_reusing_a_local_name_coexist_and_each_invokes_its_own_implementation()
    {
        var host = new AppMcpTestHost();
        host.Source
            .Add(AppMcpTestData.ReaderManifest(Notes, AppMcpTestData.V1, "search"))
            .Add(AppMcpTestData.ReaderManifest(Tasks, AppMcpTestData.V1, "search"));
        host.Provide(Notes, AppMcpTestData.Tool("search", "from-notes"));
        host.Provide(Tasks, AppMcpTestData.Tool("search", "from-tasks"));
        host.Publish(
            1,
            AppMcpTestData.Record(TenantId.Default, Notes, AppMcpTestData.V1),
            AppMcpTestData.Record(TenantId.Default, Tasks, AppMcpTestData.V1));
        host.Member("alice", "g-readers");

        Assert.That(await host.AdvertisedAsync(), Is.EqualTo(new[] { "lattice_capabilities", "notes_search", "tasks_search" }));

        var notes = await host.InvokeAsync((await host.SessionToolAsync("notes_search"))!);
        var tasks = await host.InvokeAsync((await host.SessionToolAsync("tasks_search"))!);

        Assert.Multiple(() =>
        {
            Assert.That(notes.Text(), Does.Contain("from-notes"));
            Assert.That(tasks.Text(), Does.Contain("from-tasks"));
        });
    }

    [Test]
    public async Task Only_the_callers_tenant_installs_are_offered_under_tenant_composed_scopes()
    {
        var acme = TenantId.Parse("acme");
        var host = new AppMcpTestHost();
        host.Source
            .Add(AppMcpTestData.ReaderManifest(Notes, AppMcpTestData.V1, "search"))
            .Add(AppMcpTestData.ReaderManifest(Tasks, AppMcpTestData.V1, "search"));
        host.Provide(Notes, AppMcpTestData.Tool("search"));
        host.Provide(Tasks, AppMcpTestData.Tool("search"));
        host.Publish(
            1,
            AppMcpTestData.Record(acme, Notes, AppMcpTestData.V1),
            AppMcpTestData.Record(TenantId.Default, Tasks, AppMcpTestData.V1));
        host.Member("alice", "g-readers");

        Assert.Multiple(async () =>
        {
            Assert.That(await host.AdvertisedAsync(acme), Is.EqualTo(new[] { "lattice_capabilities", "notes_search" }));
            Assert.That(await host.AdvertisedAsync(), Is.EqualTo(new[] { "lattice_capabilities", "tasks_search" }));
        });
    }

    [TestCase(AppRegistryLifecycleState.Installed)]
    [TestCase(AppRegistryLifecycleState.Disabled)]
    [TestCase(AppRegistryLifecycleState.Uninstalled)]
    public async Task An_install_that_is_not_enabled_contributes_no_tools(AppRegistryLifecycleState state)
    {
        var host = NotesHost();
        host.Publish(1, AppMcpTestData.Record(TenantId.Default, Notes, AppMcpTestData.V1, state));
        host.Member("alice", "g-readers");

        Assert.That(await host.AdvertisedAsync(), Is.EqualTo(new[] { "lattice_capabilities" }));
    }

    [Test]
    public async Task An_install_whose_ceiling_is_not_pinned_to_its_version_contributes_no_tools()
    {
        var host = NotesHost();
        host.Publish(1, AppMcpTestData.Record(TenantId.Default, Notes, AppMcpTestData.V1, ceilingVersion: AppMcpTestData.V2));
        host.Member("alice", "g-readers");

        Assert.That(await host.AdvertisedAsync(), Is.EqualTo(new[] { "lattice_capabilities" }));
    }

    [Test]
    public async Task A_manifest_that_does_not_resolve_fails_activation_and_contributes_no_tools()
    {
        var host = NotesHost();
        host.Source.Override = slug => AppSourceResult.NotFound(slug);
        host.Member("alice", "g-readers");

        Assert.Multiple(async () =>
        {
            Assert.That(await host.AdvertisedAsync(), Is.EqualTo(new[] { "lattice_capabilities" }));
            Assert.That(host.ToolSource.Catalog.Failures.Single().Failure, Does.Contain("NotFound"));
        });
    }

    [Test]
    public async Task The_catalog_is_built_once_per_epoch_and_a_successful_activation_is_reused_across_epochs()
    {
        var host = NotesHost();
        host.Member("alice", "g-readers");

        await host.AdvertisedAsync();
        await host.AdvertisedAsync();
        var first = host.ToolSource.Catalog;
        host.Publish(2, AppMcpTestData.Record(TenantId.Default, Notes, AppMcpTestData.V1));
        await host.AdvertisedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(host.Source.Resolutions, Is.EqualTo(1));
            Assert.That(host.ToolSource.Catalog, Is.Not.SameAs(first));
            Assert.That(host.ToolSource.Catalog.Epoch, Is.EqualTo(2));
            Assert.That(
                host.ToolSource.Catalog.Activations.Single().Value.Tools[0],
                Is.SameAs(first.Activations.Single().Value.Tools[0]),
                "Sessions select prebuilt tool instances; nothing is re-materialised per epoch or per session.");
        });
    }

    [Test]
    public async Task A_cold_projection_is_warmed_before_the_first_catalog_build()
    {
        var host = new AppMcpTestHost();
        host.Provide(Notes, AppMcpTestData.Tool("search"));

        await host.AdvertisedAsync();

        Assert.That(host.Projection.WarmCalls, Is.EqualTo(1));
    }

    [Test]
    public void A_transient_membership_fault_surfaces_as_a_retryable_discovery_error()
    {
        var host = NotesHost();
        host.Membership.Fault = new TimeoutException("membership unreachable");

        Assert.ThrowsAsync<LatticeApiMcpDiscoveryUnavailableException>(() => host.AdvertisedAsync());
    }

    [Test]
    public async Task A_non_transient_membership_fault_offers_no_app_tools()
    {
        var host = NotesHost().Member("alice", "g-readers");
        host.Membership.Fault = new InvalidOperationException("broken");

        Assert.That(await host.AdvertisedAsync(), Is.EqualTo(new[] { "lattice_capabilities" }));
    }

    [Test]
    public async Task A_caller_whose_tenant_resolution_is_denied_is_offered_no_app_tools()
    {
        var host = NotesHost();
        host.Member("alice", "g-readers");
        var tools = await new AppMcpToolSource(
                host.Providers,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<AppMcpToolSource>.Instance,
                host.Projection,
                host.Source,
                host.Gate,
                host.Membership,
                new DenyingTenantResolver())
            .GetPermittedToolsAsync(host.Context(), new LatticeCredential("t", principalId: "alice"), CancellationToken.None);

        Assert.That(tools, Is.Empty);
    }

    [Test]
    public void Constructor_rejects_null_dependencies()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => new AppMcpToolSource(null!, Microsoft.Extensions.Logging.Abstractions.NullLogger<AppMcpToolSource>.Instance));
            Assert.Throws<ArgumentNullException>(() => new AppMcpToolSource([], null!));
        });
    }

    private sealed class NameAuthorizer(params string[] allowed) : ILatticeApiMcpAuthorizer
    {
        public Task<bool> IsAuthorizedAsync(LatticeApiMcpAuthorizationContext authorizationContext, CancellationToken cancellationToken)
            => Task.FromResult(allowed.Contains(authorizationContext.ToolName));
    }

    private sealed class DenyingTenantResolver : ITenantContextResolver
    {
        public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default)
            => throw new LatticeTenantAccessDeniedException();
    }
}
