using System.Reflection;
using NSubstitute;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;
using Orleans.Lattice.Apps.Tests;

namespace Orleans.Lattice.Api.Apps.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeAppWorkspace"/>: the role-holder gate on every verb (including the denial
/// matrix, where a denial is indistinguishable from absence), the sanitised projection, and verified icon and
/// UI asset reads from the installed version.
/// </summary>
[TestFixture]
public sealed class LatticeAppWorkspaceTests
{
    private static WorkspaceHarness Granted() => new WorkspaceHarness().Publish(WorkspaceHarness.Record()).GrantReader();

    private static async Task AssertIndistinguishableFromAbsentAsync(WorkspaceHarness harness)
    {
        var workspace = harness.Workspace;
        Assert.That(await workspace.ListMyAppsAsync(), Is.Empty);
        Assert.That(await workspace.DescribeMyAppAsync(AppsControlHarness.Slug), Is.Null);
        Assert.That(await workspace.GetIconAsync(AppsControlHarness.Slug), Is.Null);
        Assert.That(await workspace.GetUiAssetAsync(AppsControlHarness.Slug, UiTestManifests.EntryPath), Is.Null);
        Assert.That(harness.Source.AssetOpens, Is.Zero, "no asset is read for a caller without a grant");
    }

    [Test]
    public async Task A_role_holder_sees_the_app_everywhere()
    {
        var workspace = Granted().Workspace;

        var apps = await workspace.ListMyAppsAsync();

        Assert.That(apps, Has.Length.EqualTo(1));
        Assert.That(apps[0].Slug, Is.EqualTo(AppsControlHarness.Slug));
        Assert.That(apps[0].Version, Is.EqualTo(AppsControlHarness.Version));
        Assert.That(apps[0].InstallRevision, Is.EqualTo(7));
        Assert.That(apps[0].HasUi, Is.True);
        Assert.That(apps[0].Roles, Is.EqualTo(new[] { "reader" }));
        Assert.That(apps[0].Presentation!.DisplayName, Is.EqualTo("Notes <b>app</b>"));
        Assert.That(await workspace.DescribeMyAppAsync(AppsControlHarness.Slug), Is.Not.Null);
        Assert.That(await workspace.GetIconAsync(AppsControlHarness.Slug), Is.Not.Null);
        Assert.That(await workspace.GetUiAssetAsync(AppsControlHarness.Slug, UiTestManifests.EntryPath), Is.Not.Null);
    }

    [Test]
    public async Task A_caller_without_a_role_sees_nothing() =>
        await AssertIndistinguishableFromAbsentAsync(new WorkspaceHarness().Publish(WorkspaceHarness.Record()));

    [Test]
    public async Task A_role_held_only_in_another_tenant_grants_nothing_in_the_active_tenant()
    {
        var harness = new WorkspaceHarness().Publish(WorkspaceHarness.Record(tenant: AppsControlHarness.Acme)).GrantReader();
        harness.Tenants.Tenant = TenantId.Default;

        await AssertIndistinguishableFromAbsentAsync(harness);
    }

    [Test]
    public async Task A_role_in_the_active_tenant_is_honoured_for_that_tenant()
    {
        var harness = new WorkspaceHarness().Publish(WorkspaceHarness.Record(tenant: AppsControlHarness.Acme)).GrantReader();
        harness.Tenants.Tenant = AppsControlHarness.Acme;

        Assert.That((await harness.Workspace.ListMyAppsAsync()).Single().Slug, Is.EqualTo(AppsControlHarness.Slug));
    }

    [TestCase(AppRegistryLifecycleState.Installed)]
    [TestCase(AppRegistryLifecycleState.Disabled)]
    [TestCase(AppRegistryLifecycleState.Uninstalled)]
    public async Task An_install_that_is_not_enabled_grants_nothing(AppRegistryLifecycleState state) =>
        await AssertIndistinguishableFromAbsentAsync(new WorkspaceHarness().Publish(WorkspaceHarness.Record(state)).GrantReader());

    [Test]
    public async Task An_install_whose_ceiling_is_not_pinned_to_its_version_grants_nothing() =>
        await AssertIndistinguishableFromAbsentAsync(new WorkspaceHarness()
            .Publish(WorkspaceHarness.Record() with { CeilingVersion = AppsControlHarness.V("0.9.0") })
            .GrantReader());

    [Test]
    public async Task A_missing_membership_context_fails_closed()
    {
        var harness = Granted();
        harness.Membership = null;

        await AssertIndistinguishableFromAbsentAsync(harness);
    }

    [Test]
    public async Task The_anonymous_null_membership_context_fails_closed()
    {
        var harness = Granted();
        harness.Membership = (ILatticeMembershipContext)Activator.CreateInstance(
            typeof(ILatticeMembershipContext).Assembly.GetType("Orleans.Lattice.NullLatticeMembershipContext", throwOnError: true)!,
            nonPublic: true)!;

        await AssertIndistinguishableFromAbsentAsync(harness);
    }

    [Test]
    public async Task A_denied_tenant_resolution_fails_closed()
    {
        var harness = Granted();
        harness.Tenants.Tenant = default;

        await AssertIndistinguishableFromAbsentAsync(harness);
    }

    [Test]
    public async Task A_missing_collaborator_fails_closed()
    {
        var harness = Granted();
        var workspaces = new[]
        {
            new LatticeAppWorkspace(new AppRoleGrantEvaluator(null, harness.Sources, harness.Gate), harness.Sources, harness.Tenants, harness.Membership),
            new LatticeAppWorkspace(new AppRoleGrantEvaluator(harness.Projection, null, harness.Gate), harness.Sources, harness.Tenants, harness.Membership),
            new LatticeAppWorkspace(new AppRoleGrantEvaluator(harness.Projection, harness.Sources, null), harness.Sources, harness.Tenants, harness.Membership),
            new LatticeAppWorkspace(new AppRoleGrantEvaluator(harness.Projection, harness.Sources, harness.Gate), harness.Sources, null, harness.Membership),
        };

        foreach (var workspace in workspaces)
        {
            Assert.That(await workspace.ListMyAppsAsync(), Is.Empty);
            Assert.That(await workspace.DescribeMyAppAsync(AppsControlHarness.Slug), Is.Null);
        }

        var noSources = new LatticeAppWorkspace(new AppRoleGrantEvaluator(harness.Projection, harness.Sources, harness.Gate), null, harness.Tenants, harness.Membership);
        Assert.That(await noSources.GetIconAsync(AppsControlHarness.Slug), Is.Null);
    }

    /// <summary>
    /// #3902: the workspace reports the roles a caller is bound to, never the roles its other memberships or
    /// rights would amount to. A caller bound only to the reader role holds reader only, whatever else it
    /// belongs to - the roles the frame is told are the ones the bridge honours.
    /// </summary>
    [Test]
    public async Task A_caller_bound_only_to_one_role_is_reported_holding_that_role_only()
    {
        var harness = new WorkspaceHarness().Publish(TwoBindingRecord()).GrantReader();
        harness.AliceMembership.Join("cluster-admins");

        var workspace = harness.Workspace;

        Assert.Multiple(async () =>
        {
            Assert.That((await workspace.ListMyAppsAsync()).Single().Roles, Is.EqualTo(new[] { "reader" }));
            Assert.That((await workspace.DescribeMyAppAsync(AppsControlHarness.Slug))!.Roles.Select(r => r.Name), Is.EqualTo(new[] { "reader" }));
        });
    }

    /// <summary>
    /// Coordinator review of #3902: one rule on every surface - a binding grants the role, and a deny can only
    /// take it away. A bound writer with an explicit deny on the role's tree is not reported as writer, so the UI
    /// never shows it controls the bridge refuses; its undenied reader binding still stands.
    /// </summary>
    [Test]
    public async Task A_bound_member_with_an_explicit_deny_is_not_reported_holding_the_denied_role()
    {
        var harness = new WorkspaceHarness().Publish(TwoBindingRecord()).GrantReader();
        harness.AliceMembership.Join("g-writers");
        harness.Gate.Deny(WorkspaceHarness.Alice, WorkspaceHarness.AppsTreeId(TenantId.Default, "contacts"), LatticeOperation.Write);

        var workspace = harness.Workspace;

        Assert.Multiple(async () =>
        {
            Assert.That((await workspace.ListMyAppsAsync()).Single().Roles, Is.EqualTo(new[] { "reader" }));
            Assert.That((await workspace.DescribeMyAppAsync(AppsControlHarness.Slug))!.Roles.Select(r => r.Name), Is.EqualTo(new[] { "reader" }));
        });
    }

    [Test]
    public async Task A_bound_member_denied_every_role_sees_nothing()
    {
        var harness = new WorkspaceHarness().Publish(WorkspaceHarness.Record()).GrantReader();
        foreach (var tree in new[] { "a/crm/contacts", "a/billing/ledger" })
        {
            harness.Gate.Deny(WorkspaceHarness.Alice, tree, LatticeOperation.Read);
        }

        await AssertIndistinguishableFromAbsentAsync(harness);
    }

    [Test]
    public async Task A_caller_bound_to_the_writer_role_holds_writer()
    {
        var harness = new WorkspaceHarness().Publish(TwoBindingRecord());
        harness.AliceMembership.Join("g-writers");

        Assert.That((await harness.Workspace.ListMyAppsAsync()).Single().Roles, Is.EqualTo(new[] { "writer" }));
    }

    [Test]
    public async Task Re_binding_a_role_moves_it_on_the_next_evaluation()
    {
        var harness = new WorkspaceHarness().Publish(WorkspaceHarness.Record()).GrantReader();
        var workspace = harness.Workspace;
        Assert.That((await workspace.ListMyAppsAsync()).Single().Roles, Is.EqualTo(new[] { "reader" }));

        harness.Publish(WorkspaceHarness.Record() with { Revision = 8, RoleBindings = [AppRoleBinding.Create("reader", "g-new-readers")] });

        Assert.That(await workspace.ListMyAppsAsync(), Is.Empty, "the old group no longer holds the role");

        harness.AliceMembership.Join("g-new-readers");

        Assert.That((await workspace.ListMyAppsAsync()).Single().Roles, Is.EqualTo(new[] { "reader" }));
    }

    private static AppRegistryRecord TwoBindingRecord() =>
        WorkspaceHarness.Record() with
        {
            RoleBindings = [AppRoleBinding.Create("reader", WorkspaceHarness.ReadersGroup), AppRoleBinding.Create("writer", "g-writers")],
        };

    [Test]
    public void Constructor_rejects_a_null_evaluator() =>
        Assert.Throws<ArgumentNullException>(() => new LatticeAppWorkspace(null!, null, null, null));

    [Test]
    public void Malformed_input_is_a_caller_error()
    {
        var workspace = Granted().Workspace;

        Assert.ThrowsAsync<ArgumentException>(() => workspace.DescribeMyAppAsync("Not A Slug"));
        Assert.ThrowsAsync<ArgumentException>(() => workspace.GetIconAsync(""));
        Assert.ThrowsAsync<ArgumentException>(() => workspace.GetUiAssetAsync(AppsControlHarness.Slug, ""));
    }

    [Test]
    public async Task DescribeMyApp_returns_the_sanitised_projection_with_only_the_held_roles()
    {
        var descriptor = await Granted().Workspace.DescribeMyAppAsync(AppsControlHarness.Slug);

        Assert.That(descriptor, Is.Not.Null);
        Assert.That(descriptor!.Slug, Is.EqualTo(AppsControlHarness.Slug));
        Assert.That(descriptor.InstallRevision, Is.EqualTo(7));
        Assert.That(descriptor.SourceKey, Is.EqualTo(InImageAppSource.SourceKey));
        Assert.That(descriptor.State, Is.EqualTo(AppLifecycleState.Enabled));
        Assert.That(descriptor.Roles.Select(r => r.Name), Is.EqualTo(new[] { "reader" }));
        Assert.That(descriptor.Trees.Select(t => (t.Name, t.Adopted)), Is.EqualTo(new[] { ("contacts", false), ("legacy", true) }));
        Assert.That(descriptor.McpTools.Single().Name, Is.EqualTo("find"));
        Assert.That(descriptor.Subscriptions, Has.Length.EqualTo(2));
        Assert.That(descriptor.Replication.Single().Tree, Is.EqualTo("contacts"));
        Assert.That(descriptor.Ui!.Bridge.Single(), Is.EqualTo(new AppUiBridgeGrantDescriptor { Operation = AppUiBridgeOperations.DataRead }));
        Assert.That(descriptor.Presentation!.Summary, Is.EqualTo("Take notes."));
    }

    [Test]
    public async Task DescribeMyApp_reports_a_failed_activation()
    {
        var harness = Granted();
        harness.Pipeline.GetStatusAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Status(AppsControlHarness.Outcome(AppActivationOperation.Reconcile, AppRegistryLifecycleState.Enabled, AppActivationFailure.RulePersistenceFailed)));

        Assert.That((await harness.Workspace.DescribeMyAppAsync(AppsControlHarness.Slug))!.State, Is.EqualTo(AppLifecycleState.Failed));
    }

    [Test]
    public void The_workspace_projection_exposes_no_ceiling_scope_binding_or_physical_id_member()
    {
        var forbidden = new[] { "Ceiling", "ExceptionScope", "ApprovedExceptionScopes", "Binding", "RoleBindings", "GroupId", "AdoptedTreeId", "TreeId", "PhysicalTreeId", "Consent" };
        var visited = new HashSet<Type>();
        var pending = new Stack<Type>([typeof(WorkspaceAppDescriptor)]);
        while (pending.TryPop(out var type))
        {
            if (!visited.Add(type) || type.Namespace != typeof(WorkspaceAppDescriptor).Namespace)
                continue;
            foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                Assert.That(forbidden.Any(f => property.Name.Contains(f, StringComparison.Ordinal)), Is.False, $"{type.Name}.{property.Name}");
                var member = property.PropertyType;
                member = Nullable.GetUnderlyingType(member) ?? member;
                if (member.IsGenericType)
                    member.GetGenericArguments().ToList().ForEach(pending.Push);
                pending.Push(member);
            }
        }

        Assert.That(visited, Does.Contain(typeof(WorkspaceTreeDescriptor)).And.Contain(typeof(AppRoleDescriptor)).And.Contain(typeof(AppUiDescriptor)));
    }

    [Test]
    public async Task GetIcon_returns_the_installed_versions_verified_icon()
    {
        var icon = await Granted().Workspace.GetIconAsync(AppsControlHarness.Slug);

        Assert.That(icon!.Bytes.ToArray(), Is.EqualTo(UiTestManifests.IconBytes));
        Assert.That(icon.Sha256, Is.EqualTo(UiTestManifests.Sha256(UiTestManifests.IconBytes)));
    }

    [Test]
    public async Task GetUiAsset_returns_a_verified_asset_of_the_installed_version()
    {
        var asset = await Granted().Workspace.GetUiAssetAsync(AppsControlHarness.Slug, UiTestManifests.ScriptPath);

        Assert.That(asset, Is.Not.Null);
        Assert.That(asset!.Path, Is.EqualTo(UiTestManifests.ScriptPath));
        Assert.That(asset.Bytes.ToArray(), Is.EqualTo(UiTestManifests.ScriptBytes));
        Assert.That(asset.MediaType, Is.EqualTo("text/javascript"));
        Assert.That(asset.Sha256, Is.EqualTo(UiTestManifests.Sha256(UiTestManifests.ScriptBytes)));
    }

    [TestCase("missing.js")]
    [TestCase("INDEX.HTML")]
    [TestCase("../index.html")]
    public async Task GetUiAsset_returns_null_for_a_path_the_manifest_does_not_declare(string path)
    {
        var harness = Granted();

        Assert.That(await harness.Workspace.GetUiAssetAsync(AppsControlHarness.Slug, path), Is.Null);
        Assert.That(harness.Source.AssetOpens, Is.Zero);
    }

    [Test]
    public async Task GetUiAsset_returns_null_rather_than_bytes_that_fail_digest_verification()
    {
        var harness = Granted();
        harness.Source.WithAsset(UiTestManifests.ScriptPath, "export const tampered = 1;"u8.ToArray());

        Assert.That(await harness.Workspace.GetUiAssetAsync(AppsControlHarness.Slug, UiTestManifests.ScriptPath), Is.Null);
        Assert.That(harness.Source.AssetOpens, Is.EqualTo(1));
    }

    [Test]
    public async Task GetUiAsset_refuses_an_entry_fragment_that_fails_fragment_validation()
    {
        var entry = "<script>alert(1)</script>"u8.ToArray();
        var assets = new[]
        {
            new Orleans.Lattice.Apps.AppUiAsset { Path = UiTestManifests.EntryPath, MediaType = "text/html", Digest = UiTestManifests.Sha256(entry) },
        };
        var manifest = AppsControlHarness.Manifest() with
        {
            Ui = new AppUiDeclaration { Entry = UiTestManifests.EntryPath, Assets = assets, BundleDigest = AppUiBundle.ComputeBundleDigest(assets) },
        };
        var harness = new WorkspaceHarness().Publish(WorkspaceHarness.Record()).GrantReader();
        var source = new TestCatalogSource(InImageAppSource.SourceKey).Publish(manifest).WithAsset(UiTestManifests.EntryPath, entry);
        harness.Sources = new AppSourceSet([source]);

        Assert.That(await harness.Workspace.GetUiAssetAsync(AppsControlHarness.Slug, UiTestManifests.EntryPath), Is.Null);
        Assert.That(source.AssetOpens, Is.EqualTo(1));
    }

    [Test]
    public async Task GetUiAsset_serves_only_the_installed_version()
    {
        var harness = new WorkspaceHarness().Publish(WorkspaceHarness.Record(version: AppsControlHarness.OtherVersion)).GrantReader();
        var source = new TestCatalogSource(InImageAppSource.SourceKey)
            .Publish(WorkspaceHarness.Manifest(AppsControlHarness.OtherVersion))
            .Publish(WorkspaceHarness.Manifest())
            .WithUiAssets();
        harness.Sources = new AppSourceSet([source]);

        var described = await harness.Workspace.DescribeMyAppAsync(AppsControlHarness.Slug);

        Assert.That(described!.Version, Is.EqualTo(AppsControlHarness.OtherVersion));
        Assert.That(await harness.Workspace.GetUiAssetAsync(AppsControlHarness.Slug, UiTestManifests.EntryPath), Is.Not.Null);
    }

    [Test]
    public async Task The_installed_app_resolves_from_its_own_source_when_another_source_offers_the_slug()
    {
        var harness = Granted();
        var other = new TestCatalogSource("feed").Publish(WorkspaceHarness.Manifest()).WithUiAssets();
        harness.Sources = new AppSourceSet([harness.Source, other]);

        Assert.That(await harness.Workspace.ListMyAppsAsync(), Has.Length.EqualTo(1));
        Assert.That(await harness.Workspace.GetUiAssetAsync(AppsControlHarness.Slug, UiTestManifests.EntryPath), Is.Not.Null);
        Assert.That(other.Resolutions + other.AssetOpens, Is.Zero);
    }

    [Test]
    public async Task An_app_whose_manifest_no_longer_resolves_is_absent()
    {
        var harness = Granted();
        harness.Sources = new AppSourceSet([new TestCatalogSource(InImageAppSource.SourceKey)]);

        await AssertIndistinguishableFromAbsentAsync(harness);
    }
}
