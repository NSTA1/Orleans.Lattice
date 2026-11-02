using Bunit;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access;

/// <summary>
/// The cluster-wide subject picker's group sources (#5370): a group comes from the
/// identity directory or from the groups stored in Access, each labelled, so a
/// stored group can be chosen once a directory exists; a platform operator, and
/// only one, may also choose a tenant group by tenant and name, verified against
/// that tenant's stored groups; and a tenant group id is never typed into existence.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessSubjectPickerGroupSourcesTests : AccessTestContext
{
    [Test]
    public async Task With_a_directory_a_stored_group_is_offered_labelled_beside_the_directorys_and_accepted()
    {
        Admin.WithPrincipal("ops", "Operations", DirectoryPrincipalKind.Group).WithGroup("data-stewards", "Data stewards");
        string? id = null;
        DirectoryPrincipalDescriptor? chosen = null;
        var cut = RenderPicker(onId: value => id = value, onPrincipal: principal => chosen = principal);

        AccessForms.Type(cut, "Subject", "s");

        cut.WaitUntil(() => Assert.That(
            cut.FindAll("[role=option]").Select(option => option.QuerySelector(".lt-combobox__detail")!.TextContent),
            Is.EqualTo(new[] { "Directory - Operations", "Stored - Data stewards" })));
        cut.FindAll("[role=option]")[1].Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-access-provenance").GetAttribute("data-lt-provenance"), Is.EqualTo("stored")));
        Assert.Multiple(() =>
        {
            Assert.That(id, Is.EqualTo("data-stewards"));
            Assert.That(chosen!.DisplayName, Is.EqualTo("Data stewards"), "the label is not part of the name");
            Assert.That(chosen.Kind, Is.EqualTo(DirectoryPrincipalKind.Group));
            Assert.That(cut.Find(".lt-access-provenance__source").TextContent, Is.EqualTo("Stored"));
        });
        Assert.That(await cut.InvokeAsync(cut.Instance.ConfirmAsync), Is.True);
    }

    [Test]
    public async Task A_group_no_source_lists_is_refused_and_users_still_come_from_the_directory_alone()
    {
        Admin.WithPrincipal("alice", "Alice", DirectoryPrincipalKind.User).WithGroup("data-stewards");
        var cut = RenderPicker();

        AccessForms.Type(cut, "Subject", "ghost");
        var refused = await cut.InvokeAsync(cut.Instance.ConfirmAsync);
        AccessForms.Choose(cut, "Subject kind", "user");
        AccessForms.Type(cut, "Subject", "data-stewards");
        var storedAsUser = await cut.InvokeAsync(cut.Instance.ConfirmAsync);

        Assert.Multiple(() =>
        {
            Assert.That(refused, Is.False);
            Assert.That(storedAsUser, Is.False, "a stored group is never a user");
        });
    }

    [Test]
    public void Without_a_directory_stored_groups_are_still_suggested_and_the_id_is_used_as_typed()
    {
        Admin.WithGroup("data-stewards", "Data stewards");
        var cut = RenderPicker(directory: false);

        AccessForms.Type(cut, "Subject", "data");

        cut.WaitUntil(() => Assert.That(cut.Find("[role=option] .lt-combobox__detail").TextContent, Is.EqualTo("Stored - Data stewards")));
    }

    [TestCase(true, AccessSubjectPicker.TypedTenantGroupMessage)]
    [TestCase(false, AccessSubjectPicker.TenantGroupNotOfferedMessage)]
    public async Task A_typed_tenant_group_id_is_refused(bool allowTenantGroups, string message)
    {
        Admin.WithPrincipal("ops", "Operations", DirectoryPrincipalKind.Group);
        var cut = RenderPicker(allowTenantGroups: allowTenantGroups);

        AccessForms.Type(cut, "Subject", "t/acme/ops");

        Assert.That(await cut.InvokeAsync(cut.Instance.ConfirmAsync), Is.False);
        Assert.That(AccessForms.ErrorOf(cut, "Subject"), Is.EqualTo(message));
    }

    [Test]
    public void The_tenant_group_kind_is_offered_only_to_a_platform_operator_where_it_is_allowed()
    {
        var allowed = RenderPicker(allowTenantGroups: true);
        var notAllowed = RenderPicker();

        allowed.WaitUntil(() => Assert.That(Options(allowed), Is.EqualTo(new[] { "User", "Group", "Tenant group" })));
        Assert.That(Options(notAllowed), Is.EqualTo(new[] { "User", "Group" }));
    }

    [Test]
    public void A_caller_who_is_not_a_platform_operator_is_not_offered_the_tenant_group_kind()
    {
        Admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), new LatticeAuthorizationDeniedException("not an operator"));

        var cut = RenderPicker(allowTenantGroups: true);

        cut.WaitUntil(() => Assert.That(Admin.Calls, Does.Contain(nameof(FakeAuthAdmin.ListGroupsAsync))));
        Assert.That(Options(cut), Is.EqualTo(new[] { "User", "Group" }));
    }

    [Test]
    public async Task An_operator_chooses_a_tenant_group_by_tenant_and_name_and_it_is_verified()
    {
        TenantFacades.AsOperator().WithGroup("acme", "acme-analysts", "Acme analysts");
        var ids = new List<string>();
        var kinds = new List<LatticeSubjectSelectorKind>();
        var cut = RenderPicker(allowTenantGroups: true, kind: LatticeSubjectSelectorKind.User, onId: ids.Add, onKind: kinds.Add);
        cut.WaitUntil(() => Assert.That(Options(cut), Has.Member("Tenant group")));

        AccessForms.Choose(cut, "Subject kind", AccessSubjectPicker.TenantGroupValue);
        Assert.That(AccessForms.Field(cut, "Subject").HasAttribute("disabled"), Is.True, "the name waits for its tenant");
        AccessForms.Type(cut, "Subject tenant", "acme");
        AccessForms.Type(cut, "Subject", "acme-an");
        cut.WaitUntil(() => Assert.That(cut.Find("[role=option] .lt-combobox__detail").TextContent, Is.EqualTo("Tenant - Acme analysts")));
        cut.Find("[role=option]").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-access-provenance").GetAttribute("data-lt-provenance"), Is.EqualTo("tenant")));
        Assert.Multiple(() =>
        {
            Assert.That(kinds, Is.EqualTo(new[] { LatticeSubjectSelectorKind.Group }));
            Assert.That(ids.Last(), Is.EqualTo("t/acme/acme-analysts"));
            Assert.That(cut.Find(".lt-access-provenance__source").TextContent, Is.EqualTo("Tenant group"));
            Assert.That(cut.Find(".lt-access-provenance__id").TextContent, Is.EqualTo("t/acme/acme-analysts"));
        });
        Assert.That(await cut.InvokeAsync(cut.Instance.ConfirmAsync), Is.True);
    }

    [Test]
    public async Task A_forged_or_half_chosen_tenant_group_is_refused()
    {
        TenantFacades.AsOperator().WithGroup("acme", "acme-analysts");
        var cut = RenderPicker(allowTenantGroups: true, id: "t/acme/ghost");

        var forged = await cut.InvokeAsync(cut.Instance.ConfirmAsync);
        var forgedError = AccessForms.ErrorOf(cut, "Subject");
        AccessForms.Type(cut, "Subject tenant", "globex");
        var halfChosen = await cut.InvokeAsync(cut.Instance.ConfirmAsync);

        Assert.Multiple(() =>
        {
            Assert.That(forged, Is.False);
            Assert.That(forgedError, Is.EqualTo(AccessSubjectPicker.NoSuchTenantGroupMessage("acme", "ghost")));
            Assert.That(halfChosen, Is.False);
            Assert.That(AccessForms.ErrorOf(cut, "Subject"), Is.EqualTo(AccessSubjectPicker.ChooseTenantAndGroupMessage));
        });
    }

    [Test]
    public async Task An_existing_tenant_group_subject_is_shown_but_refused_for_a_caller_who_is_not_an_operator()
    {
        TenantFacades.AsOperator().WithGroup("acme", "acme-analysts");
        Admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), new LatticeAuthorizationDeniedException("not an operator"));
        var cut = RenderPicker(allowTenantGroups: true, id: "t/acme/acme-analysts");

        Assert.That(Options(cut), Has.Member("Tenant group"));
        Assert.That(await cut.InvokeAsync(cut.Instance.ConfirmAsync), Is.False);
        Assert.That(AccessForms.ErrorOf(cut, "Subject"), Is.EqualTo(AccessSubjectPicker.OperatorOnlyMessage));
    }

    [TestCase("denied", AccessSubjectPicker.OperatorOnlyMessage)]
    [TestCase("off", AccessSubjectPicker.TenantGroupsOffMessage)]
    [TestCase("unserved", "Tenant acme has no group named acme-analysts.")]
    [TestCase("fault", "Tenant group t/acme/acme-analysts could not be verified, so it cannot be chosen now.")]
    public async Task A_tenant_group_the_tenant_directory_cannot_confirm_is_refused(string failure, string message)
    {
        TenantFacades.AsOperator().WithGroup("acme", "acme-analysts");
        switch (failure)
        {
            case "denied":
                TenantFacades.Gate.Denied = true;
                break;
            case "off":
                TenantFacades.Gate.Enabled = false;
                break;
            case "unserved":
                TenantFacades.ServesDirectory = false;
                break;
            default:
                TenantFacades.Gate.NextFailure = new TimeoutException("down");
                break;
        }

        var cut = RenderPicker(allowTenantGroups: true, id: "t/acme/acme-analysts");

        Assert.That(await cut.InvokeAsync(cut.Instance.ConfirmAsync), Is.False);
        Assert.That(AccessForms.ErrorOf(cut, "Subject"), Is.EqualTo(message));
    }

    [Test]
    public void Display_names_drop_the_cluster_source_labels()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AccessSubjectPicker.DisplayNameOf(new("ops", "Directory - Operations")), Is.EqualTo("Operations"));
            Assert.That(AccessSubjectPicker.DisplayNameOf(new("ops", "Directory and stored - Operations")), Is.EqualTo("Operations"));
            Assert.That(AccessSubjectPicker.DisplayNameOf(new("ops", "Stored")), Is.EqualTo("ops"));
            Assert.That(AccessSubjectPicker.LabelOf("Stored - Data stewards"), Is.EqualTo("Stored"));
            Assert.That(AccessSubjectPicker.LabelOf("Stored-ish"), Is.Null);
            Assert.That(AccessSubjectPicker.LabelOf(null), Is.Null);
        });
    }

    private static string[] Options(IRenderedComponent<AccessSubjectPicker> cut) =>
        [.. cut.FindAll("select option").Select(option => option.TextContent)];

    private IRenderedComponent<AccessSubjectPicker> RenderPicker(
        bool directory = true,
        bool allowTenantGroups = false,
        LatticeSubjectSelectorKind kind = LatticeSubjectSelectorKind.Group,
        string? id = null,
        Action<string>? onId = null,
        Action<LatticeSubjectSelectorKind>? onKind = null,
        Action<DirectoryPrincipalDescriptor>? onPrincipal = null)
    {
        if (directory && !Admin.Model.DirectoryAvailable)
        {
            Admin.Model = Admin.Model with { DirectoryAvailable = true, DirectoryProviderId = "entra" };
        }

        return Render<AccessSubjectPicker>(parameters => parameters
            .Add(picker => picker.Label, "Subject")
            .Add(picker => picker.Kind, kind)
            .Add(picker => picker.Id, id)
            .Add(picker => picker.DirectoryAvailable, directory)
            .Add(picker => picker.AllowTenantGroups, allowTenantGroups)
            .Add(picker => picker.IdChanged, value => onId?.Invoke(value))
            .Add(picker => picker.KindChanged, value => onKind?.Invoke(value))
            .Add(picker => picker.OnPrincipalSelected, principal => onPrincipal?.Invoke(principal)));
    }
}
