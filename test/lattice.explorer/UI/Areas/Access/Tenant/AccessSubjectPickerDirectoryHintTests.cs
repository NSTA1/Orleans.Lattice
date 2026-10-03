using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Groups;
using Orleans.Lattice.Explorer.UI.Areas.Access;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant;

/// <summary>
/// Issue #4345: a tenant administrator who may not read the cluster's identity
/// directory is told that searching it needs operator access - never that no
/// directory is configured - and can still enter an id as typed. A cluster with
/// no directory keeps its existing hint. The refusal is read from the one access
/// model call the page already makes; nothing further is asked of the directory.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessSubjectPickerDirectoryHintTests : TenantAccessPagesTestContext
{
    private static LatticeAuthorizationDeniedException Denied() =>
        new("_lattice_policy", LatticeOperation.Admin, "alice", "not an operator");

    [Test]
    public async Task Without_a_directory_the_hint_says_none_is_configured_and_the_id_is_used_as_typed()
    {
        var cut = RenderPicker(TenantSubjectKind.User, denied: false);

        AccessForms.Type(cut, "Subject", "u-1");
        var accepted = await cut.InvokeAsync(cut.Instance.ConfirmAsync);

        Assert.Multiple(() =>
        {
            Assert.That(HintOf(cut, "Subject"), Is.EqualTo(AccessSubjectPicker.NoDirectoryHint));
            Assert.That(accepted, Is.True);
        });
    }

    [Test]
    [TestCase(TenantSubjectKind.User, AccessSubjectPicker.DeniedUserHint)]
    [TestCase(TenantSubjectKind.ClusterGroup, AccessSubjectPicker.DeniedGroupHint)]
    public async Task A_refused_directory_says_search_needs_operator_access_and_accepts_the_typed_id(TenantSubjectKind kind, string hint)
    {
        var cut = RenderPicker(kind, denied: true);

        AccessForms.Type(cut, "Subject", "u-1");
        var accepted = await cut.InvokeAsync(cut.Instance.ConfirmAsync);

        Assert.Multiple(() =>
        {
            Assert.That(HintOf(cut, "Subject"), Is.EqualTo(hint));
            Assert.That(cut.Markup, Does.Not.Contain("No identity directory"), "a refusal never claims the directory is absent");
            Assert.That(accepted, Is.True);
            Assert.That(Admin.Calls, Does.Not.Contain(nameof(FakeAuthAdmin.SearchDirectoryAsync)), "a refused caller's typing is not sent to the directory");
            Assert.That(Admin.Calls, Does.Not.Contain(nameof(FakeAuthAdmin.ResolveDirectoryPrincipalAsync)));
        });
    }

    [Test]
    public void A_refusal_does_not_change_the_tenant_group_source()
    {
        var cut = RenderPicker(TenantSubjectKind.TenantGroup, denied: true);

        Assert.That(HintOf(cut, "Subject"), Is.EqualTo("Type part of a name to choose one of tenant acme's own groups."));
    }

    [Test]
    public void A_searchable_directory_wins_over_a_stale_refusal()
    {
        var cut = RenderPicker(TenantSubjectKind.User, denied: true, directory: true);

        Assert.That(HintOf(cut, "Subject"), Is.EqualTo("An object id."));
    }

    [Test]
    public void On_the_members_page_a_tenant_admin_refused_the_access_model_is_told_search_needs_operator_access()
    {
        TenantFacades.AsTenantAdmin();
        Admin.Fail(nameof(FakeAuthAdmin.GetAccessModelAsync), Denied());

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");

        cut.WaitUntil(() =>
        {
            Assert.That(HintOf(cut, "Member"), Is.EqualTo(AccessSubjectPicker.DeniedUserHint));
            Assert.That(cut.Markup, Does.Not.Contain("No identity directory"));
            Assert.That(Admin.Calls.Count(call => call == nameof(FakeAuthAdmin.GetAccessModelAsync)), Is.EqualTo(1));
        });
    }

    [Test]
    public void On_the_members_page_a_cluster_without_a_directory_keeps_the_existing_hint()
    {
        TenantFacades.AsTenantAdmin();

        var cut = RenderAt<AccessMembersPage>("t/acme/access/members");

        cut.WaitUntil(() => Assert.That(HintOf(cut, "Member"), Is.EqualTo(AccessSubjectPicker.NoDirectoryHint)));
    }

    [Test]
    public async Task The_catalogue_reports_a_refused_access_model_and_asks_again_next_time()
    {
        Admin.Fail(nameof(FakeAuthAdmin.GetAccessModelAsync), Denied());
        var catalog = new AccessCatalog(Admin);

        var first = await catalog.ReadAccessModelAsync(CancellationToken.None);
        var second = await catalog.ReadAccessModelAsync(CancellationToken.None);
        var model = await catalog.GetAccessModelAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(new AccessModelRead(null, Denied: true)));
            Assert.That(second, Is.EqualTo(first));
            Assert.That(model, Is.Null);
            Assert.That(Admin.Calls.Count(call => call == nameof(FakeAuthAdmin.GetAccessModelAsync)), Is.EqualTo(3), "one call per read, as before");
        });
    }

    [Test]
    public async Task The_catalogue_does_not_read_another_fault_as_a_refusal()
    {
        Admin.Fail(nameof(FakeAuthAdmin.GetAccessModelAsync), new InvalidOperationException("down"));
        var catalog = new AccessCatalog(Admin);

        Assert.That(await catalog.ReadAccessModelAsync(CancellationToken.None), Is.EqualTo(default(AccessModelRead)));
    }

    [Test]
    public async Task The_catalogue_remembers_a_read_model_and_reports_no_refusal()
    {
        var catalog = new AccessCatalog(Admin);

        var first = await catalog.ReadAccessModelAsync(CancellationToken.None);
        var second = await catalog.ReadAccessModelAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.Model, Is.SameAs(Admin.Model));
            Assert.That(first.Denied, Is.False);
            Assert.That(second.Model, Is.SameAs(Admin.Model));
            Assert.That(Admin.Calls.Count(call => call == nameof(FakeAuthAdmin.GetAccessModelAsync)), Is.EqualTo(1));
        });
    }

    [Test]
    public void The_catalogue_lets_a_cancellation_through()
    {
        Admin.Fail(nameof(FakeAuthAdmin.GetAccessModelAsync), new OperationCanceledException());
        var catalog = new AccessCatalog(Admin);

        Assert.That(async () => await catalog.ReadAccessModelAsync(CancellationToken.None), Throws.InstanceOf<OperationCanceledException>());
    }

    private static string? HintOf<TComponent>(IRenderedComponent<TComponent> cut, string label)
        where TComponent : Microsoft.AspNetCore.Components.IComponent =>
        AccessForms.Field(cut, label).Closest(".lt-field")?.QuerySelector(".lt-field__hint")?.TextContent.Trim();

    private IRenderedComponent<AccessSubjectPicker> RenderPicker(TenantSubjectKind kind, bool denied, bool directory = false) =>
        Render<AccessSubjectPicker>(parameters => parameters
            .Add(picker => picker.Label, "Subject")
            .Add(picker => picker.Tenant, "acme")
            .Add(picker => picker.SubjectKind, kind)
            .Add(picker => picker.Kind, kind == TenantSubjectKind.User ? LatticeSubjectSelectorKind.User : LatticeSubjectSelectorKind.Group)
            .Add(picker => picker.DirectoryAvailable, directory)
            .Add(picker => picker.DirectoryExplanation, directory ? "An object id." : null)
            .Add(picker => picker.DirectorySearchDenied, denied));
}
