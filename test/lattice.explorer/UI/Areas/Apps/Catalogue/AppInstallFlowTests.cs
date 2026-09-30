using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>
/// The staged install state machine (epic decision E15), driven against fakes:
/// the static path, the acquisition states a dynamic source produces, every
/// transition and failure, upgrades, re-consent, and resumability.
/// </summary>
[TestFixture]
public sealed class AppInstallFlowTests
{
    private FakeAppCatalog _catalog = null!;
    private FakeAppsControl _control = null!;
    private AppsFacades _facades = null!;
    private ServiceProvider _services = null!;

    [SetUp]
    public void SetUp()
    {
        _catalog = new FakeAppCatalog();
        _control = new FakeAppsControl();
        _services = new ServiceCollection()
            .AddKeyedSingleton<ILatticeAppCatalog>(ShellFacades.Key, _catalog)
            .AddKeyedSingleton<ILatticeAppsControl>(ShellFacades.Key, _control)
            .BuildServiceProvider();
        _facades = new AppsFacades(_services);
    }

    [TearDown]
    public void TearDown() => _services.Dispose();

    [Test]
    public async Task A_static_source_goes_from_resolving_straight_to_review()
    {
        var app = AppsTestData.TaskBoard();
        _catalog.Descriptions[("in-image", app.Slug, app.Version)] = app;
        _catalog.DescribeGate = new TaskCompletionSource();
        var flow = Flow(AppsTestData.InImage);
        var seen = Record(flow);

        var loading = flow.LoadAsync();
        Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Resolving));
        _catalog.DescribeGate.SetResult();
        await loading;

        Assert.Multiple(() =>
        {
            Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Review));
            Assert.That(seen, Does.Not.Contain(AppInstallStage.Acquiring).And.Not.Contain(AppInstallStage.Verifying));
            Assert.That(flow.Mode, Is.EqualTo(AppInstallMode.Install));
            Assert.That(flow.Draft, Is.EqualTo(AppConsentDraft.Requested(app)).Using<AppConsentDraft>(SameDraft));
            Assert.That(flow.RequiresAcquisition, Is.False);
            Assert.That(flow.IsBusy, Is.False);
        });
    }

    [Test]
    public async Task A_source_that_requires_acquisition_passes_through_acquiring_and_verifying()
    {
        var app = AppsTestData.TaskBoard(source: "nuget-contoso", withIcon: true);
        _catalog.Descriptions[("nuget-contoso", app.Slug, app.Version)] = app;
        _catalog.Icons[("nuget-contoso", app.Slug)] = AppsTestData.Icon;
        _catalog.DescribeGate = new TaskCompletionSource();
        _catalog.IconGate = new TaskCompletionSource();
        var flow = Flow(AppsTestData.Feed);

        var loading = flow.LoadAsync();
        Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Acquiring));

        _catalog.DescribeGate.SetResult();
        Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Verifying));

        _catalog.IconGate.SetResult();
        await loading;

        Assert.Multiple(() =>
        {
            Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Review));
            Assert.That(flow.Icon, Is.SameAs(AppsTestData.Icon));
            Assert.That(flow.RequiresAcquisition, Is.True);
        });
    }

    [Test]
    public async Task Verification_fails_on_a_mismatched_description_and_retries_from_acquisition()
    {
        var app = AppsTestData.TaskBoard(source: "nuget-contoso") with { Slug = "other" };
        _catalog.Descriptions[("nuget-contoso", "task-board", "1.0.0")] = app;
        var flow = Flow(AppsTestData.Feed);

        await flow.LoadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Failed));
            Assert.That(flow.Error, Does.Contain("different app"));
            Assert.That(flow.ResumeStage, Is.EqualTo(AppInstallStage.Acquiring));
        });

        _catalog.Descriptions[("nuget-contoso", "task-board", "1.0.0")] = AppsTestData.TaskBoard(source: "nuget-contoso");
        flow.Back();
        await flow.LoadAsync();

        Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Review));
    }

    [TestCase("version")]
    [TestCase("source")]
    [TestCase("bundle")]
    [TestCase("icon-digest")]
    [TestCase("icon-bytes")]
    public async Task Verification_refuses_what_does_not_match_its_manifest(string defect)
    {
        var app = AppsTestData.TaskBoard("1.0.0", source: "nuget-contoso", withIcon: defect.StartsWith("icon", StringComparison.Ordinal));
        app = defect switch
        {
            "version" => app with { Version = "9.9.9" },
            "source" => app with { SourceKey = "blob-ops" },
            "bundle" => app with { Ui = app.Ui! with { BundleDigest = "not-a-digest" } },
            "icon-digest" => app with { Presentation = app.Presentation! with { Icon = new AppIconDescriptor { Path = "i.svg", Sha256 = "XYZ" } } },
            _ => app,
        };
        _catalog.Descriptions[("nuget-contoso", "task-board", "1.0.0")] = app;
        var flow = new AppInstallFlow(_facades, new AppInstallFlowKey(null, "nuget-contoso", "task-board", "1.0.0"), AppsTestData.Feed);

        await flow.LoadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Failed));
            Assert.That(flow.Error, Does.StartWith("Could not verify task-board from nuget-contoso."));
        });
    }

    [Test]
    public async Task An_unknown_app_is_not_found_and_a_failing_source_is_a_retryable_failure()
    {
        var missing = Flow(AppsTestData.InImage);
        await missing.LoadAsync();

        _catalog.Descriptions[("in-image", "task-board", "1.0.0")] = AppsTestData.TaskBoard();
        var throwing = new AppInstallFlow(new AppsFacades(new ServiceCollection().AddKeyedSingleton<ILatticeAppCatalog>(ShellFacades.Key, new ThrowingCatalog()).BuildServiceProvider()),
            new AppInstallFlowKey(null, "in-image", "task-board", null), AppsTestData.InImage);
        await throwing.LoadAsync();

        var none = new AppInstallFlow(new AppsFacades(new ServiceCollection().BuildServiceProvider()), new AppInstallFlowKey(null, "in-image", "x", null), null);
        await none.LoadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(missing.Stage, Is.EqualTo(AppInstallStage.NotFound));
            Assert.That(throwing.Stage, Is.EqualTo(AppInstallStage.Failed));
            Assert.That(throwing.Error, Is.EqualTo("Could not read task-board. The cluster could not be reached. Try again."));
            Assert.That(throwing.ResumeStage, Is.EqualTo(AppInstallStage.Resolving));
            Assert.That(none.Error, Does.Contain("does not serve the app catalogue"));
        });
    }

    [Test]
    public async Task Install_binds_every_role_confirms_the_ceiling_installs_then_enables()
    {
        var app = AppsTestData.TaskBoard();
        _catalog.Descriptions[("in-image", app.Slug, app.Version)] = app;
        var flow = Flow(AppsTestData.InImage);
        await flow.LoadAsync();

        flow.Begin();
        Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.BindRoles));
        Assert.That(() => flow.ConfirmBindings(), Throws.InvalidOperationException, "every role must be bound");

        flow.Bind("viewer", " readers ");
        flow.Bind("editor", "writers");
        flow.Bind("editor", "");
        Assert.That(flow.UnboundRoles, Is.EqualTo(new[] { "editor" }));
        flow.Bind("editor", "writers");
        flow.ConfirmBindings();
        Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.ConfirmCeiling));

        _control.InstallGate = new TaskCompletionSource();
        var committing = flow.CommitAsync();
        Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Installing));
        Assert.That(flow.IsBusy, Is.True);
        _control.InstallGate.SetResult();
        await committing;

        Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Installed));
        var request = _control.Installs.Single();
        Assert.Multiple(() =>
        {
            Assert.That(request.SourceKey, Is.EqualTo("in-image"));
            Assert.That(request.Version, Is.EqualTo("1.0.0"));
            Assert.That(request.RoleBindings.Select(binding => (binding.RoleName, binding.GroupId)), Is.EquivalentTo(new[] { ("viewer", "readers"), ("editor", "writers") }));
            Assert.That(request.Ceiling.AllowedOperations, Is.EqualTo(AppConsentAnalysis.RequiredOperations(app)));
            Assert.That(_control.ConsentUpdates, Is.Empty, "a fresh install records the requested bridge consent itself");
        });

        await flow.EnableAsync();

        Assert.Multiple(() =>
        {
            Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Enabled));
            Assert.That(_control.Calls.Select(call => call.Verb), Is.EqualTo(new[] { "install", "enable" }));
        });
    }

    [Test]
    public async Task Withholding_a_bridge_grant_records_that_consent_after_install()
    {
        var app = AppsTestData.TaskBoard();
        _catalog.Descriptions[("in-image", app.Slug, app.Version)] = app;
        var flow = await AtCeilingAsync(app);

        var userName = app.Ui!.Bridge.Single(grant => grant.Operation == "context.user");
        flow.SetBridge(userName, consented: false);

        Assert.That(flow.Issues.Single().Kind, Is.EqualTo(AppActivationIssueKind.BridgeConsentRequired));

        await flow.CommitAsync();

        Assert.Multiple(() =>
        {
            Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Installed));
            Assert.That(_control.ConsentUpdates.Single().BridgeGrants, Does.Not.Contain(userName));
        });
    }

    [Test]
    public async Task Editing_the_ceiling_updates_the_activation_preview()
    {
        var app = AppsTestData.TaskBoard(crossApp: true);
        _catalog.Descriptions[("in-image", app.Slug, app.Version)] = app;
        var flow = await AtCeilingAsync(app);
        var scope = AppConsentAnalysis.RequiredScopes(app).Single();

        flow.SetOperation(LatticeOperation.Delete, approved: false);
        flow.SetScope(scope, approved: false);

        Assert.That(flow.Issues.Select(issue => issue.Kind), Is.EqualTo(new[] { AppActivationIssueKind.CeilingExceeded, AppActivationIssueKind.ScopeNotApproved }));

        flow.SetOperation(LatticeOperation.Delete, approved: true);
        flow.SetScope(scope, approved: true);

        Assert.That(flow.Issues, Is.Empty);
    }

    [Test]
    public async Task A_refused_install_fails_with_a_human_sentence_and_goes_back_to_the_ceiling()
    {
        var app = AppsTestData.TaskBoard();
        _catalog.Descriptions[("in-image", app.Slug, app.Version)] = app;
        var flow = await AtCeilingAsync(app);
        _control.Failures["install"] = new InvalidOperationException("Could not install app 'task-board' (TreeOwnershipConflict): tenant-acme/a/task-board/tasks");

        await flow.CommitAsync();

        Assert.Multiple(() =>
        {
            Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Failed));
            Assert.That(flow.Error, Does.StartWith("Could not install Task board.").And.Not.Contain("tenant-acme"));
        });

        flow.Back();

        Assert.Multiple(() =>
        {
            Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.ConfirmCeiling));
            Assert.That(flow.Error, Is.Null);
        });
    }

    [Test]
    public async Task A_refused_enable_returns_to_installed()
    {
        var app = AppsTestData.TaskBoard();
        _catalog.Descriptions[("in-image", app.Slug, app.Version)] = app;
        var flow = await AtCeilingAsync(app);
        await flow.CommitAsync();
        _control.Failures["enable"] = new InvalidOperationException("The enable of app 'task-board' failed (CeilingExceeded).");

        await flow.EnableAsync();
        Assert.That(flow.Error, Does.Contain("more than the approved ceiling"));
        flow.Back();

        Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Installed));
    }

    [Test]
    public async Task An_installed_other_version_makes_an_upgrade_that_reconsents_new_bridge_grants()
    {
        var installed = AppsTestData.TaskBoard("1.0.0");
        _control.Install(installed with { RoleBindings = [new AppRoleBindingDescriptor { RoleName = "viewer", GroupId = "readers" }, new AppRoleBindingDescriptor { RoleName = "editor", GroupId = "writers" }] }, AppLifecycleState.Enabled);
        var next = AppsTestData.TaskBoard("2.0.0", bridge: [new AppUiBridgeGrantDescriptor { Operation = "data.read" }, new AppUiBridgeGrantDescriptor { Operation = "ui.notify" }]);
        _catalog.Descriptions[("in-image", next.Slug, next.Version)] = next;
        var flow = new AppInstallFlow(_facades, new AppInstallFlowKey(null, "in-image", "task-board", "2.0.0"), AppsTestData.InImage);

        await flow.LoadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(flow.Mode, Is.EqualTo(AppInstallMode.Upgrade));
            Assert.That(flow.Diff!.BridgeAdded.Single().Operation, Is.EqualTo("ui.notify"));
            Assert.That(flow.Diff.RequiresReconsent, Is.True);
            Assert.That(flow.Bindings, Is.EquivalentTo(new Dictionary<string, string> { ["viewer"] = "readers", ["editor"] = "writers" }), "bindings carry over");
            Assert.That(flow.Issues, Is.Empty, "the draft starts from the consent plus what the new version asks for");
        });

        flow.Begin();
        flow.ConfirmBindings();
        await flow.CommitAsync();

        Assert.Multiple(() =>
        {
            Assert.That(_control.Installs.Single().Version, Is.EqualTo("2.0.0"));
            Assert.That(_control.ConsentUpdates.Single().BridgeGrants!.Value.Select(grant => grant.Operation), Does.Contain("ui.notify"));
            Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Installed));
        });
    }

    [Test]
    public async Task Managing_the_installed_version_is_never_blocked_by_an_ownership_conflict()
    {
        var installed = AppsTestData.TaskBoard();
        _control.Install(installed, AppLifecycleState.Enabled);
        _catalog.Descriptions[("in-image", installed.Slug, installed.Version)] = installed with
        {
            Trees = [new AppTreeDescriptor { Name = "tasks", OwnershipConflict = "owned by another install" }],
        };
        var flow = Flow(AppsTestData.InImage);
        await flow.LoadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(flow.Mode, Is.EqualTo(AppInstallMode.Reconsent));
            Assert.That(flow.IsManaging, Is.True);
            Assert.That(flow.Issues.Select(issue => issue.Kind), Does.Not.Contain(AppActivationIssueKind.TreeOwnershipConflict));
            Assert.That(flow.IsBlocked, Is.False);
        });
    }

    [Test]
    public async Task A_version_that_is_not_installed_is_not_managed_and_a_conflict_blocks_it()
    {
        var app = AppsTestData.TaskBoard() with { Trees = [new AppTreeDescriptor { Name = "tasks", OwnershipConflict = "owned by crm" }] };
        _catalog.Descriptions[("in-image", app.Slug, app.Version)] = app;
        var flow = Flow(AppsTestData.InImage);
        await flow.LoadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(flow.IsManaging, Is.False);
            Assert.That(flow.IsBlocked, Is.True);
        });
    }

    [Test]
    public async Task The_installed_version_with_drift_is_reconsented_without_rebinding_roles()
    {
        var installed = AppsTestData.TaskBoard();
        _control.Install(installed, AppLifecycleState.Failed, new AppConsentReport
        {
            Slug = installed.Slug,
            Version = installed.Version,
            Ceiling = new AppCapabilityCeilingDescriptor { AllowedOperations = LatticeOperation.Read },
            BridgeGrants = [],
        });
        _catalog.Descriptions[("in-image", installed.Slug, installed.Version)] = installed;
        var flow = Flow(AppsTestData.InImage);
        await flow.LoadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(flow.Mode, Is.EqualTo(AppInstallMode.Reconsent));
            Assert.That(flow.Drift, Is.Not.Empty);
            Assert.That(flow.Drift.Select(issue => issue.Kind), Does.Contain(AppActivationIssueKind.CeilingExceeded).And.Contain(AppActivationIssueKind.BridgeConsentRequired));
        });

        flow.Begin();
        Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.ConfirmCeiling), "re-consent skips role binding");
        flow.Back();
        Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Review));
        flow.Begin();

        await flow.CommitAsync();

        Assert.Multiple(() =>
        {
            Assert.That(_control.Installs, Is.Empty);
            Assert.That(_control.ConsentUpdates.Single().Ceiling.AllowedOperations, Is.EqualTo(AppConsentAnalysis.RequiredOperations(installed)));
            Assert.That(flow.Stage, Is.EqualTo(AppInstallStage.Installed), "a failed app is offered Enable now after re-consent");
        });
    }

    [Test]
    public async Task Without_a_control_facade_commit_and_enable_fail_closed()
    {
        var app = AppsTestData.TaskBoard();
        _catalog.Descriptions[("in-image", app.Slug, app.Version)] = app;
        var catalogOnly = new AppsFacades(new ServiceCollection().AddKeyedSingleton<ILatticeAppCatalog>(ShellFacades.Key, _catalog).BuildServiceProvider());
        var flow = new AppInstallFlow(catalogOnly, new AppInstallFlowKey(null, "in-image", "task-board", null), AppsTestData.InImage);
        await flow.LoadAsync();
        flow.Begin();
        flow.Bind("viewer", "g");
        flow.Bind("editor", "g");
        flow.ConfirmBindings();

        await flow.CommitAsync();

        Assert.That(flow.Error, Does.Contain("does not serve app management"));
    }

    [Test]
    public void Transitions_out_of_order_are_refused()
    {
        var flow = Flow(AppsTestData.InImage);

        Assert.Multiple(() =>
        {
            Assert.That(() => flow.Begin(), Throws.InvalidOperationException);
            Assert.That(() => flow.ConfirmBindings(), Throws.InvalidOperationException);
            Assert.That(() => flow.SetOperation(LatticeOperation.Read, true), Throws.InvalidOperationException);
            Assert.ThrowsAsync<InvalidOperationException>(() => flow.CommitAsync());
            Assert.ThrowsAsync<InvalidOperationException>(() => flow.EnableAsync());
            Assert.That(() => flow.Bind(" ", "g"), Throws.ArgumentException);
        });
    }

    [Test]
    public async Task The_store_resumes_a_flow_at_its_stage_and_forgets_it_on_request()
    {
        var app = AppsTestData.TaskBoard();
        _catalog.Descriptions[("in-image", app.Slug, app.Version)] = app;
        var store = new AppInstallFlowStore(_facades);
        var key = new AppInstallFlowKey("acme", "in-image", "task-board", null);

        var first = store.GetOrCreate(key, AppsTestData.InImage);
        await first.LoadAsync();
        first.Begin();

        var again = store.GetOrCreate(key, AppsTestData.InImage);
        await again.LoadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(again, Is.SameAs(first));
            Assert.That(again.Stage, Is.EqualTo(AppInstallStage.BindRoles));
            Assert.That(store.Flows, Has.Count.EqualTo(1));
            Assert.That(_catalog.Describes, Has.Count.EqualTo(1), "a resumed flow does not describe again");
            Assert.That(store.GetOrCreate(key with { Tenant = "other" }, null), Is.Not.SameAs(first), "flows are per tenant");
            Assert.That(store.Remove(key), Is.True);
            Assert.That(store.GetOrCreate(key, null), Is.Not.SameAs(first));
        });
    }

    private AppInstallFlow Flow(AppSourceSummary source) =>
        new(_facades, new AppInstallFlowKey(null, source.Key, "task-board", null), source);

    private async Task<AppInstallFlow> AtCeilingAsync(AppDescriptor app)
    {
        var flow = Flow(AppsTestData.InImage);
        await flow.LoadAsync();
        flow.Begin();
        foreach (var role in app.Roles)
        {
            flow.Bind(role.Name, role.Name + "-group");
        }

        flow.ConfirmBindings();
        return flow;
    }

    private static List<AppInstallStage> Record(AppInstallFlow flow)
    {
        var seen = new List<AppInstallStage>();
        flow.Changed += () => seen.Add(flow.Stage);
        return seen;
    }

    private static bool SameDraft(AppConsentDraft a, AppConsentDraft b) =>
        a.Operations == b.Operations && a.Scopes.SequenceEqual(b.Scopes) && a.BridgeGrants.SequenceEqual(b.BridgeGrants);

    private sealed class ThrowingCatalog : ILatticeAppCatalog
    {
        public Task<System.Collections.Immutable.ImmutableArray<AppSourceSummary>> ListSourcesAsync(CancellationToken cancellationToken = default) => throw new TimeoutException("secret");

        public Task<AvailableAppPage> ListAvailableAsync(AvailableAppQuery query, CancellationToken cancellationToken = default) => throw new TimeoutException("secret");

        public Task<AppDescriptor?> DescribeFromSourceAsync(string sourceKey, string appSlug, string? version = null, CancellationToken cancellationToken = default) => throw new TimeoutException("secret");

        public Task<AppIconAsset?> GetIconAsync(string sourceKey, string appSlug, string? version = null, CancellationToken cancellationToken = default) => throw new TimeoutException("secret");

        public Task<LatticeAppCatalogCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default) => throw new TimeoutException("secret");
    }
}
