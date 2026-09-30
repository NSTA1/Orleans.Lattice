using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Areas.Apps.App;
using static Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App.AppPageTestData;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App;

/// <summary>
/// The app page loader's fail-closed reading of the two read paths epic decision E8 gates
/// differently: a role holder's workspace, and an <c>AppInstall</c> holder's control.
/// </summary>
[TestFixture]
public sealed class AppPageLoaderTests
{
    [Test]
    public async Task A_role_holder_is_loaded_from_the_workspace_with_their_roles_and_open_right()
    {
        var workspace = new FakeAppPagesWorkspace().Grant(Workspace(), "viewer", "auditor");
        workspace.Icons[Slug] = Icon();

        var load = await new AppPageLoader(workspace, null).LoadAsync(Slug, CancellationToken.None);
        var model = load.Model!;

        Assert.Multiple(() =>
        {
            Assert.That(load.Kind, Is.EqualTo(AppPageLoadKind.Loaded));
            Assert.That(model.Slug, Is.EqualTo(Slug));
            Assert.That(model.Version, Is.EqualTo("2.1.0"));
            Assert.That(model.SourceKey, Is.EqualTo("in-image"));
            Assert.That(model.State, Is.EqualTo(AppLifecycleState.Enabled));
            Assert.That(model.DisplayName, Is.EqualTo("CRM"));
            Assert.That(model.CallerRoleNames, Is.EqualTo(new[] { "viewer", "auditor" }));
            Assert.That(model.CallerRoles.Select(role => role.Name), Is.EqualTo(new[] { "viewer" }));
            Assert.That(model.Trees.Select(tree => (tree.Name, tree.Adopted)), Is.EqualTo(new[] { ("orders", false), ("legacy", true) }));
            Assert.That(model.CanOpen, Is.True);
            Assert.That(model.IconDataUri, Does.StartWith("data:image/svg+xml;base64,"));
            Assert.That(model.IsAppInstallHolder, Is.False);
            Assert.That(model.Drift, Is.Null);
            Assert.That(model.McpTools, Has.Length.EqualTo(1));
            Assert.That(model.Subscriptions, Has.Length.EqualTo(1));
            Assert.That(model.Replication, Has.Length.EqualTo(1));
            Assert.That(model.Ui, Is.Not.Null);
        });
    }

    [Test]
    public async Task An_app_install_holder_without_a_role_is_loaded_from_the_control_with_consent_and_drift()
    {
        var control = new FakeAppPagesControl().Administer(Admin(), DriftedConsent());

        var model = (await new AppPageLoader(new FakeAppPagesWorkspace(), control).LoadAsync(Slug, CancellationToken.None)).Model!;

        Assert.Multiple(() =>
        {
            Assert.That(model.IsAppInstallHolder, Is.True);
            Assert.That(model.CallerRoleNames, Is.Empty);
            Assert.That(model.CanOpen, Is.False, "an app not in ListMyAppsAsync cannot be opened");
            Assert.That(model.Consent, Is.SameAs(control.Consents[Slug]));
            Assert.That(model.Drift!.HasDrift, Is.True);
            Assert.That(model.Trees.Select(tree => tree.Adopted), Is.EqualTo(new[] { false, true }));
            Assert.That(model.IconDataUri, Is.Null);
            Assert.That(model.Admin!.Provenance.Publisher, Is.EqualTo("Contoso"));
        });
    }

    [Test]
    public async Task An_app_install_holder_without_a_role_gets_the_installed_versions_icon_from_the_catalogue()
    {
        var control = new FakeAppPagesControl().Administer(Admin(), DriftedConsent());
        var catalog = new Catalogue.FakeAppCatalog();
        catalog.Icons[("in-image", Slug)] = Icon();

        var model = (await new AppPageLoader(new FakeAppPagesWorkspace(), control, catalog: catalog).LoadAsync(Slug, CancellationToken.None)).Model!;

        Assert.That(model.IconDataUri, Does.StartWith("data:image/svg+xml;base64,"));
    }

    [Test]
    public async Task A_catalogue_that_cannot_answer_leaves_the_admin_view_without_an_icon()
    {
        var control = new FakeAppPagesControl().Administer(Admin(), DriftedConsent());
        var catalog = new Catalogue.FakeAppCatalog { IconGate = new TaskCompletionSource() };
        catalog.IconGate.SetException(new InvalidOperationException("catalogue down"));

        var model = (await new AppPageLoader(new FakeAppPagesWorkspace(), control, catalog: catalog).LoadAsync(Slug, CancellationToken.None)).Model!;

        Assert.That(model.IconDataUri, Is.Null);
    }

    [Test]
    public async Task A_caller_with_neither_path_is_not_found_and_the_control_is_never_asked_to_describe()
    {
        var control = new FakeAppPagesControl();
        control.Descriptions[Slug] = Admin();

        var load = await new AppPageLoader(new FakeAppPagesWorkspace(), control).LoadAsync(Slug, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(load, Is.SameAs(AppPageLoad.NotFound));
            Assert.That(control.DescribeCalls, Is.Zero);
        });
    }

    [Test]
    public async Task A_host_without_either_facade_answers_not_found()
    {
        Assert.That(await new AppPageLoader(null, null).LoadAsync(Slug, CancellationToken.None), Is.SameAs(AppPageLoad.NotFound));
    }

    [TestCase("")]
    [TestCase("  ")]
    public async Task An_empty_slug_is_not_found_without_asking_anything(string slug)
    {
        var workspace = new FakeAppPagesWorkspace();

        var load = await new AppPageLoader(workspace, null).LoadAsync(slug, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(load, Is.SameAs(AppPageLoad.NotFound));
            Assert.That(workspace.Described, Is.Empty);
        });
    }

    [Test]
    public async Task A_workspace_fault_is_unavailable_only_when_nothing_else_answered()
    {
        var faulted = new FakeAppPagesWorkspace { Throw = new InvalidOperationException("down") };
        var control = new FakeAppPagesControl().Administer(Admin(), CoveringConsent());

        var alone = await new AppPageLoader(faulted, null).LoadAsync(Slug, CancellationToken.None);
        var withControl = await new AppPageLoader(faulted, control).LoadAsync(Slug, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(alone, Is.SameAs(AppPageLoad.Unavailable));
            Assert.That(withControl.Kind, Is.EqualTo(AppPageLoadKind.Loaded));
            Assert.That(withControl.Model!.IsAppInstallHolder, Is.True);
        });
    }

    [Test]
    public async Task Consent_that_cannot_be_read_leaves_drift_unjudged()
    {
        var withoutRight = new FakeAppPagesControl().Administer(Admin(), CoveringConsent());
        withoutRight.Capabilities = withoutRight.Capabilities with { CanGetConsent = false };
        var faulted = new FakeAppPagesControl().Administer(Admin(), CoveringConsent());
        faulted.ConsentThrows = new InvalidOperationException("down");

        var a = (await new AppPageLoader(null, withoutRight).LoadAsync(Slug, CancellationToken.None)).Model!;
        var b = (await new AppPageLoader(null, faulted).LoadAsync(Slug, CancellationToken.None)).Model!;

        Assert.Multiple(() =>
        {
            Assert.That(withoutRight.ConsentCalls, Is.Zero);
            Assert.That((a.Consent, a.Drift), Is.EqualTo(((AppConsentReport?)null, (AppConsentDrift?)null)));
            Assert.That((b.Consent, b.Drift), Is.EqualTo(((AppConsentReport?)null, (AppConsentDrift?)null)));
        });
    }

    [TestCase(AppLifecycleState.NotInstalled)]
    [TestCase(AppLifecycleState.Uninstalled)]
    public async Task A_description_of_an_app_that_is_not_installed_is_not_found(AppLifecycleState state)
    {
        var control = new FakeAppPagesControl().Administer(Admin(state: state), CoveringConsent());

        var load = await new AppPageLoader(null, control).LoadAsync(Slug, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(load, Is.SameAs(AppPageLoad.NotFound));
            Assert.That(control.ConsentCalls, Is.Zero);
        });
    }

    [Test]
    public async Task A_description_for_another_slug_is_never_shown()
    {
        var workspace = new FakeAppPagesWorkspace();
        workspace.Descriptions[Slug] = Workspace() with { Slug = "other" };
        var control = new FakeAppPagesControl().Administer(Admin() with { Slug = "other" }, CoveringConsent());
        control.Descriptions[Slug] = control.Descriptions["other"];

        Assert.That(await new AppPageLoader(workspace, control).LoadAsync(Slug, CancellationToken.None), Is.SameAs(AppPageLoad.NotFound));
    }

    [Test]
    public async Task A_failing_icon_is_simply_not_shown()
    {
        var workspace = new ThrowingIconWorkspace();
        workspace.Grant(Workspace());

        var model = (await new AppPageLoader(workspace, null, NullLogger<AppPageLoader>.Instance).LoadAsync(Slug, CancellationToken.None)).Model!;

        Assert.That(model.IconDataUri, Is.Null);
    }

    [Test]
    public async Task No_icon_is_fetched_when_the_presentation_declares_none()
    {
        var workspace = new FakeAppPagesWorkspace().Grant(Workspace(presentation: Presentation() with { Icon = null }));

        await new AppPageLoader(workspace, null).LoadAsync(Slug, CancellationToken.None);

        Assert.That(workspace.IconCalls, Is.Zero);
    }

    [Test]
    public void Cancellation_propagates()
    {
        var workspace = new FakeAppPagesWorkspace { Gate = new TaskCompletionSource() }.Grant(Workspace());
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.That(async () => await new AppPageLoader(workspace, null).LoadAsync(Slug, cancellation.Token), Throws.InstanceOf<OperationCanceledException>());
    }

    [TestCase("image/svg+xml", true)]
    [TestCase("IMAGE/PNG", true)]
    [TestCase("image/webp", true)]
    [TestCase("text/html", false)]
    [TestCase("application/javascript", false)]
    public void Only_an_image_becomes_a_data_uri(string mediaType, bool shown)
    {
        Assert.That(AppPageLoader.ToDataUri(Icon(mediaType)) is not null, Is.EqualTo(shown));
    }

    [Test]
    public void An_empty_or_oversized_icon_is_not_shown()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppPageLoader.ToDataUri(null), Is.Null);
            Assert.That(AppPageLoader.ToDataUri(Icon() with { Bytes = ReadOnlyMemory<byte>.Empty }), Is.Null);
            Assert.That(AppPageLoader.ToDataUri(Icon() with { Bytes = new byte[AppPageLoader.MaxIconBytes + 1] }), Is.Null);
            Assert.That(AppPageLoader.ToDataUri(Icon("image/PNG")), Does.StartWith("data:image/png;base64,"));
        });
    }

    [Test]
    public void The_loaded_outcome_needs_a_model()
    {
        Assert.That(() => AppPageLoad.Loaded(null!), Throws.ArgumentNullException);
    }

    /// <summary>A workspace whose icon read always fails.</summary>
    private sealed class ThrowingIconWorkspace : ILatticeAppWorkspace
    {
        private readonly FakeAppPagesWorkspace _inner = new();

        public void Grant(WorkspaceAppDescriptor descriptor) => _inner.Grant(descriptor);

        public Task<System.Collections.Immutable.ImmutableArray<WorkspaceAppSummary>> ListMyAppsAsync(CancellationToken cancellationToken = default) => _inner.ListMyAppsAsync(cancellationToken);

        public Task<WorkspaceAppDescriptor?> DescribeMyAppAsync(string appSlug, CancellationToken cancellationToken = default) => _inner.DescribeMyAppAsync(appSlug, cancellationToken);

        public Task<AppIconAsset?> GetIconAsync(string appSlug, CancellationToken cancellationToken = default) => throw new InvalidOperationException("icon down");

        public Task<AppUiAsset?> GetUiAssetAsync(string appSlug, string path, CancellationToken cancellationToken = default) => _inner.GetUiAssetAsync(appSlug, path, cancellationToken);
    }
}
