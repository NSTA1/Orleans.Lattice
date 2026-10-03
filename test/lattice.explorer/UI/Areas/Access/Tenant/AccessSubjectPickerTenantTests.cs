using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant;

/// <summary>
/// Issue #4158: on a tenant page the shared subject picker offers three labelled
/// sources - this tenant's groups (<c>Tenant</c>), and the cluster's groups and
/// users (<c>Cluster</c>, through the identity-directory search) - and never
/// another tenant's group, whether listed by the directory or typed. A chosen
/// group's provenance is shown with its full id.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessSubjectPickerTenantTests : AccessTestContext
{
    [Test]
    public void On_a_tenant_page_the_three_sources_are_labelled()
    {
        var cut = RenderPicker(TenantSubjectKind.TenantGroup);

        var options = cut.FindAll("select option").Select(option => (option.GetAttribute("value"), option.TextContent)).ToArray();
        Assert.That(options, Is.EqualTo(new (string?, string)[]
        {
            ("tenant-group", "Tenant group"),
            ("cluster-group", "Cluster group"),
            ("user", "Cluster user"),
        }));
    }

    [Test]
    public void The_tenant_source_offers_only_this_tenants_groups_labelled_tenant()
    {
        TenantFacades.AsTenantAdmin()
            .WithGroup("acme", "ops", "Operators")
            .WithGroup("acme", "eng")
            .WithGroup("globex", "ops-globex", "Globex operators");
        var cut = RenderPicker(TenantSubjectKind.TenantGroup);

        AccessForms.Type(cut, "Subject", "op");

        cut.WaitUntil(() =>
        {
            var options = cut.FindAll("[role=option]");
            Assert.That(options.Select(option => option.QuerySelector(".lt-combobox__value")!.TextContent), Is.EqualTo(new[] { "ops" }));
            Assert.That(options[0].QuerySelector(".lt-combobox__detail")!.TextContent, Is.EqualTo("Tenant - Operators"));
            Assert.That(cut.Markup, Does.Not.Contain("globex"));
        });
    }

    [Test]
    public void The_cluster_group_source_never_offers_a_tenant_group_and_is_labelled_cluster()
    {
        Admin.WithPrincipal("t/globex/ops", "Globex operators", DirectoryPrincipalKind.Group)
            .WithPrincipal("t/acme/ops", "Acme operators", DirectoryPrincipalKind.Group)
            .WithPrincipal("eng-all", "Engineering", DirectoryPrincipalKind.Group);
        var cut = RenderPicker(TenantSubjectKind.ClusterGroup);

        AccessForms.Type(cut, "Subject", "g");

        cut.WaitUntil(() =>
        {
            var options = cut.FindAll("[role=option]");
            Assert.That(options.Select(option => option.QuerySelector(".lt-combobox__value")!.TextContent), Is.EqualTo(new[] { "eng-all" }));
            Assert.That(options[0].QuerySelector(".lt-combobox__detail")!.TextContent, Is.EqualTo("Cluster - Engineering"));
        });
    }

    [Test]
    public void The_cluster_user_source_searches_users_through_the_directory()
    {
        Admin.WithPrincipal("u-1", "Alice", DirectoryPrincipalKind.User)
            .WithPrincipal("g-1", "Ops", DirectoryPrincipalKind.Group);
        var cut = RenderPicker(TenantSubjectKind.User);

        AccessForms.Type(cut, "Subject", "1");

        cut.WaitUntil(() => Assert.That(
            cut.FindAll("[role=option]").Select(option => option.QuerySelector(".lt-combobox__detail")!.TextContent),
            Is.EqualTo(new[] { "Cluster - Alice" })));
    }

    [Test]
    [TestCase(TenantSubjectKind.ClusterGroup, true)]
    [TestCase(TenantSubjectKind.ClusterGroup, false)]
    [TestCase(TenantSubjectKind.User, false)]
    public async Task A_typed_tenant_group_id_is_refused_by_a_cluster_source(TenantSubjectKind kind, bool directory)
    {
        var cut = RenderPicker(kind, directory: directory);

        AccessForms.Type(cut, "Subject", "t/globex/ops");
        var accepted = await cut.InvokeAsync(cut.Instance.ConfirmAsync);

        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.False);
            Assert.That(AccessForms.ErrorOf(cut, "Subject"), Is.EqualTo(AccessSubjectPicker.ForeignTenantGroupMessage));
        });
    }

    [Test]
    public async Task Another_tenants_group_typed_into_the_tenant_source_is_refused()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops").WithGroup("globex", "finance");
        var cut = RenderPicker(TenantSubjectKind.TenantGroup);

        AccessForms.Type(cut, "Subject", "finance");
        var foreign = await cut.InvokeAsync(cut.Instance.ConfirmAsync);
        AccessForms.Type(cut, "Subject", "ops");
        var own = await cut.InvokeAsync(cut.Instance.ConfirmAsync);

        Assert.Multiple(() =>
        {
            Assert.That(foreign, Is.False);
            Assert.That(own, Is.True);
        });
    }

    [Test]
    public void A_chosen_tenant_group_shows_its_provenance_and_full_id_in_mono()
    {
        TenantFacades.AsTenantAdmin().WithGroup("acme", "ops", "Operators");
        DirectoryPrincipalDescriptor? chosen = null;
        var cut = RenderPicker(TenantSubjectKind.TenantGroup, onPrincipal: principal => chosen = principal);

        AccessForms.Type(cut, "Subject", "op");
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=option]"), Has.Count.EqualTo(1)));
        cut.Find("[role=option]").Click();

        cut.WaitUntil(() =>
        {
            var provenance = cut.Find(".lt-access-provenance");
            Assert.That(provenance.GetAttribute("data-lt-provenance"), Is.EqualTo("tenant"));
            Assert.That(provenance.QuerySelector(".lt-access-provenance__source")!.TextContent, Is.EqualTo("Tenant"));
            Assert.That(provenance.QuerySelector(".lt-access-provenance__id")!.TextContent, Is.EqualTo("t/acme/ops"));
            Assert.That(chosen!.Id, Is.EqualTo("ops"));
            Assert.That(chosen.DisplayName, Is.EqualTo("Operators"));
            Assert.That(chosen.Kind, Is.EqualTo(DirectoryPrincipalKind.Group));
        });
    }

    [Test]
    public void A_cluster_group_shows_cluster_provenance_and_a_user_none()
    {
        var group = RenderPicker(TenantSubjectKind.ClusterGroup, initialId: "eng-all");
        var user = RenderPicker(TenantSubjectKind.User, initialId: "u-1");

        Assert.Multiple(() =>
        {
            Assert.That(group.Find(".lt-access-provenance__source").TextContent, Is.EqualTo("Cluster"));
            Assert.That(group.Find(".lt-access-provenance__id").TextContent, Is.EqualTo("eng-all"));
            Assert.That(user.FindAll(".lt-access-provenance"), Is.Empty);
        });
    }

    [Test]
    public void Changing_the_source_clears_the_id_and_reports_both_kinds()
    {
        var subjectKinds = new List<TenantSubjectKind>();
        var kinds = new List<LatticeSubjectSelectorKind>();
        var ids = new List<string>();
        var cut = RenderPicker(TenantSubjectKind.User, initialId: "u-1", onId: ids.Add, onKind: kinds.Add, onSubjectKind: subjectKinds.Add);

        AccessForms.Choose(cut, "Subject kind", "tenant-group");
        AccessForms.Choose(cut, "Subject kind", "cluster-group");

        Assert.Multiple(() =>
        {
            Assert.That(subjectKinds, Is.EqualTo(new[] { TenantSubjectKind.TenantGroup, TenantSubjectKind.ClusterGroup }));
            Assert.That(kinds, Is.EqualTo(new[] { LatticeSubjectSelectorKind.Group }), "both group sources are a group subject");
            Assert.That(ids, Has.All.Empty);
        });
    }

    [Test]
    public void A_cluster_wide_picker_offers_no_tenant_source_and_no_provenance()
    {
        var cut = Render<AccessSubjectPicker>(parameters => parameters
            .Add(picker => picker.Label, "Subject")
            .Add(picker => picker.Kind, LatticeSubjectSelectorKind.Group)
            .Add(picker => picker.Id, "t/acme/ops")
            .Add(picker => picker.DirectoryAvailable, true));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("select option").Select(option => option.TextContent), Is.EqualTo(new[] { "User", "Group" }));
            Assert.That(cut.FindAll(".lt-access-provenance"), Is.Empty);
        });
    }

    [Test]
    [TestCase("ops", "Tenant", "ops")]
    [TestCase("ops", "Tenant - Operators", "Operators")]
    [TestCase("eng", "Cluster - Engineering", "Engineering")]
    [TestCase("u-1", null, "u-1")]
    [TestCase("u-1", "Cluster", "u-1")]
    public void A_chosen_suggestions_display_name_drops_the_provenance_label(string value, string? detail, string expected)
    {
        Assert.That(AccessSubjectPicker.DisplayNameOf(new LtSuggestion(value, detail)), Is.EqualTo(expected));
    }

    [Test]
    public void A_tenant_groups_full_id_is_composed_under_its_tenant()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AccessSubjectPicker.TenantGroupId("acme", "ops"), Is.EqualTo("t/acme/ops"));
            Assert.That(() => AccessSubjectPicker.TenantGroupId(string.Empty, "ops"), Throws.ArgumentException);
            Assert.That(() => AccessSubjectPicker.TenantGroupId("acme", null!), Throws.ArgumentNullException);
        });
    }

    private IRenderedComponent<AccessSubjectPicker> RenderPicker(
        TenantSubjectKind kind,
        bool directory = true,
        string? initialId = null,
        Action<string>? onId = null,
        Action<LatticeSubjectSelectorKind>? onKind = null,
        Action<TenantSubjectKind>? onSubjectKind = null,
        Action<DirectoryPrincipalDescriptor>? onPrincipal = null) =>
        Render<AccessSubjectPicker>(parameters => parameters
            .Add(picker => picker.Label, "Subject")
            .Add(picker => picker.Tenant, "acme")
            .Add(picker => picker.SubjectKind, kind)
            .Add(picker => picker.Kind, kind == TenantSubjectKind.User ? LatticeSubjectSelectorKind.User : LatticeSubjectSelectorKind.Group)
            .Add(picker => picker.Id, initialId)
            .Add(picker => picker.DirectoryAvailable, directory)
            .Add(picker => picker.IdChanged, value => onId?.Invoke(value))
            .Add(picker => picker.KindChanged, value => onKind?.Invoke(value))
            .Add(picker => picker.SubjectKindChanged, value => onSubjectKind?.Invoke(value))
            .Add(picker => picker.OnPrincipalSelected, principal => onPrincipal?.Invoke(principal)));
}
