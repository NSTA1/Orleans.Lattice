using NSubstitute;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Apps.Tests;

[TestFixture]
public sealed class LatticeAppsControlReadTests
{
    private AppsControlHarness _h = null!;

    [SetUp]
    public void SetUp() => _h = new AppsControlHarness();

    [Test]
    public async Task ListAsync_maps_records_with_failed_state_from_activation_evidence()
    {
        _h.RegistryLists(
            AppsControlHarness.Record(AppRegistryLifecycleState.Installed, slug: "alpha"),
            AppsControlHarness.Record(AppRegistryLifecycleState.Disabled, slug: "beta"),
            AppsControlHarness.Record(AppRegistryLifecycleState.Uninstalled, slug: "gamma"));
        _h.Pipeline.GetStatusAsync(Arg.Any<TenantId>(), AppSlug.Parse("alpha"), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Status(AppsControlHarness.Outcome(
                AppActivationOperation.Enable, AppRegistryLifecycleState.Installed, AppActivationFailure.CeilingExceeded)));
        _h.Pipeline.GetStatusAsync(Arg.Any<TenantId>(), AppSlug.Parse("beta"), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Status(AppsControlHarness.Outcome(
                AppActivationOperation.Disable, AppRegistryLifecycleState.Disabled)));

        var catalog = await _h.Control.ListAsync();

        Assert.That(catalog.Apps.Select(a => (a.Slug, a.Version, a.State)), Is.EqualTo(new[]
        {
            ("alpha", AppsControlHarness.Version, AppLifecycleState.Failed),
            ("beta", AppsControlHarness.Version, AppLifecycleState.Disabled),
            ("gamma", AppsControlHarness.Version, AppLifecycleState.Uninstalled),
        }));
        Assert.That(catalog.Apps[0].Provenance.Publisher, Is.EqualTo("first-party"));
        await _h.Pipeline.DidNotReceive().GetStatusAsync(Arg.Any<TenantId>(), AppSlug.Parse("gamma"), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ListAsync_empty_registry_returns_empty_catalog()
    {
        _h.RegistryLists();

        var catalog = await _h.Control.ListAsync();

        Assert.That(catalog.Apps, Is.Empty);
    }

    [TestCase(AppActivationFailure.NotInstalled)]
    [TestCase(AppActivationFailure.InvalidTransition)]
    public async Task ListAsync_caller_error_outcomes_are_not_failures(AppActivationFailure failure)
    {
        _h.RegistryLists(AppsControlHarness.Record(AppRegistryLifecycleState.Disabled));
        _h.StatusIs(AppsControlHarness.Status(AppsControlHarness.Outcome(AppActivationOperation.Enable, AppRegistryLifecycleState.Disabled, failure)));

        var catalog = await _h.Control.ListAsync();

        Assert.That(catalog.Apps.Single().State, Is.EqualTo(AppLifecycleState.Disabled));
    }

    [Test]
    public async Task DescribeAsync_not_installed_app_describes_source_manifest()
    {
        _h.RegistryHas(null);
        _h.SourceResolves();

        var descriptor = await _h.Control.DescribeAsync(AppsControlHarness.Slug);

        Assert.That(descriptor, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(descriptor!.Slug, Is.EqualTo(AppsControlHarness.Slug));
            Assert.That(descriptor.Version, Is.EqualTo(AppsControlHarness.Version));
            Assert.That(descriptor.State, Is.EqualTo(AppLifecycleState.NotInstalled));
            Assert.That(descriptor.Ceiling, Is.Null);
            Assert.That(descriptor.RoleBindings, Is.Empty);
            Assert.That(descriptor.Provenance.Publisher, Is.EqualTo("contoso"));
        });
        await _h.Source.Received(1).ResolveAsync(AppsControlHarness.AppSlugValue, null, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DescribeAsync_maps_every_manifest_section_with_app_local_references()
    {
        _h.RegistryHas(null);
        _h.SourceResolves();

        var d = (await _h.Control.DescribeAsync(AppsControlHarness.Slug))!;

        Assert.Multiple(() =>
        {
            Assert.That(d.Trees.Select(t => (t.Name, t.AdoptedTreeId)), Is.EqualTo(new[] { ("contacts", (string?)null), ("legacy", "legacy-contacts") }));
            Assert.That(d.Trees[0].ShardCount, Is.EqualTo(2));
            Assert.That(d.Trees[0].Rebuildable, Is.True);
            Assert.That(d.Roles.Select(r => r.Name), Is.EqualTo(new[] { "reader", "writer" }));
            Assert.That(d.Roles[0].Scopes, Is.EqualTo(new[]
            {
                new AppRoleScope { Tree = "contacts" },
                new AppRoleScope { Tree = "ledger", App = "billing" },
                new AppRoleScope { Tree = "contacts", Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "p/" },
            }));
            Assert.That(d.Subscriptions, Is.EqualTo(new[]
            {
                new AppSubscriptionDescriptor { Name = "on-contact", Tree = "contacts" },
                new AppSubscriptionDescriptor { Name = "on-invoice", Tree = "ledger", App = "billing", KeyPrefix = "inv/" },
            }));
            Assert.That(d.McpTools.Single(), Is.EqualTo(new AppMcpToolDescriptor { Name = "find", Description = "Find contacts.", Role = "reader" }));
            Assert.That(d.Replication.Single(), Is.EqualTo(new AppReplicationDescriptor { Tree = "contacts", MergeMode = LatticeMergeMode.LwwRegister }));
            Assert.That(d.Schema.Single(), Is.EqualTo(new AppSchemaDescriptor { Tree = "contacts", Family = "contact", Version = 3, StrictIngest = true }));
        });
    }

    [Test]
    public async Task DescribeAsync_installed_app_reports_state_ceiling_and_bindings_of_installed_version()
    {
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Enabled));
        _h.SourceResolves();
        _h.StatusIs(AppsControlHarness.Status(AppsControlHarness.Outcome(AppActivationOperation.Enable, AppRegistryLifecycleState.Enabled)));

        var d = (await _h.Control.DescribeAsync(AppsControlHarness.Slug))!;

        Assert.That(d.State, Is.EqualTo(AppLifecycleState.Enabled));
        Assert.That(d.Ceiling!.AllowedOperations, Is.EqualTo(LatticeOperation.Read | LatticeOperation.Write));
        Assert.That(d.RoleBindings.Single(), Is.EqualTo(new AppRoleBindingDescriptor { RoleName = "reader", GroupId = "g-readers" }));
        await _h.Source.Received(1).ResolveAsync(
            AppsControlHarness.AppSlugValue, AppsControlHarness.V(AppsControlHarness.Version), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DescribeAsync_failed_last_activation_reports_failed()
    {
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Installed));
        _h.SourceResolves();
        _h.StatusIs(AppsControlHarness.Status(AppsControlHarness.Outcome(
            AppActivationOperation.Enable, AppRegistryLifecycleState.Installed, AppActivationFailure.TreeProvisioningFailed)));

        var d = (await _h.Control.DescribeAsync(AppsControlHarness.Slug))!;

        Assert.That(d.State, Is.EqualTo(AppLifecycleState.Failed));
    }

    [Test]
    public async Task DescribeAsync_other_version_than_installed_reports_not_installed()
    {
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Enabled));
        _h.SourceResolves(AppsControlHarness.Manifest(AppsControlHarness.OtherVersion));

        var d = (await _h.Control.DescribeAsync(AppsControlHarness.Slug, AppsControlHarness.OtherVersion))!;

        Assert.That(d.State, Is.EqualTo(AppLifecycleState.NotInstalled));
        Assert.That(d.Ceiling, Is.Null);
    }

    [Test]
    public async Task DescribeAsync_uninstalled_version_reports_uninstalled_without_consent()
    {
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Uninstalled));
        _h.SourceResolves();

        var d = (await _h.Control.DescribeAsync(AppsControlHarness.Slug))!;

        Assert.That(d.State, Is.EqualTo(AppLifecycleState.Uninstalled));
        Assert.That(d.Ceiling, Is.Null);
        await _h.Pipeline.DidNotReceiveWithAnyArgs().GetStatusAsync(default, default, default);
    }

    [Test]
    public async Task DescribeAsync_unknown_app_or_version_returns_null()
    {
        _h.RegistryHas(null);
        _h.SourceReturns(AppSourceResult.NotFound(AppsControlHarness.AppSlugValue));
        Assert.That(await _h.Control.DescribeAsync(AppsControlHarness.Slug), Is.Null);

        _h.SourceReturns(AppSourceResult.VersionMismatch(
            AppsControlHarness.AppSlugValue, AppsControlHarness.V(AppsControlHarness.OtherVersion), AppsControlHarness.V(AppsControlHarness.Version)));
        Assert.That(await _h.Control.DescribeAsync(AppsControlHarness.Slug, AppsControlHarness.OtherVersion), Is.Null);
    }

    [Test]
    public void DescribeAsync_duplicate_source_registration_throws_invalid_operation()
    {
        _h.RegistryHas(null);
        _h.SourceReturns(AppSourceResult.DuplicateRegistration(AppsControlHarness.AppSlugValue));

        Assert.ThrowsAsync<InvalidOperationException>(() => _h.Control.DescribeAsync(AppsControlHarness.Slug));
    }

    [Test]
    public void DescribeAsync_rejects_invalid_slug_or_version_before_authorization()
    {
        Assert.ThrowsAsync<ArgumentException>(() => _h.Control.DescribeAsync("Bad"));
        Assert.ThrowsAsync<ArgumentException>(() => _h.Control.DescribeAsync(AppsControlHarness.Slug, "v1"));
        Assert.That(_h.Gate.Requests, Is.Empty);
        _h.AssertEngineUntouched();
    }

    [Test]
    public void GetConsentAsync_rejects_invalid_slug_before_authorization()
    {
        Assert.ThrowsAsync<ArgumentException>(() => _h.Control.GetConsentAsync(""));
        Assert.That(_h.Gate.Requests, Is.Empty);
        _h.AssertEngineUntouched();
    }

    [Test]
    public async Task GetCapabilitiesAsync_allowed_caller_reports_every_permission()
    {
        var capabilities = await _h.Control.GetCapabilitiesAsync();

        Assert.That(capabilities, Is.EqualTo(new LatticeAppsCapabilities
        {
            CanInstall = true,
            CanEnable = true,
            CanDisable = true,
            CanUninstall = true,
            CanList = true,
            CanDescribe = true,
            CanGetConsent = true,
            CanUpdateConsent = true,
        }));
        _h.AssertEngineUntouched();
    }
}
