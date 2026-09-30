using Bunit;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access;

/// <summary>
/// The subject picker, folded onto the shared combobox: the directory is searched
/// as the id is typed, display names are rendered as text beside the id, choosing
/// a match fills the id and reports the principal, only a listed principal is
/// accepted, a kind change clears the id, and without a directory the picker is a
/// plain id box that says the id is not validated.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessSubjectPickerTests : AccessTestContext
{
    [Test]
    public void Without_a_directory_the_picker_is_a_plain_id_box_that_says_it_is_not_validated()
    {
        var cut = RenderPicker(directory: false);

        AccessForms.Type(cut, "Subject", "anyone");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[role=listbox]"), Is.Empty);
            Assert.That(cut.Find(".lt-field__hint").TextContent, Does.Contain("not validated"));
            Assert.That(Admin.Calls, Does.Not.Contain(nameof(FakeAuthAdmin.SearchDirectoryAsync)));
        });
    }

    [Test]
    public void Typing_offers_directory_matches_by_id_with_the_display_name_as_text()
    {
        Admin.WithPrincipal("u-1", "<img src=x onerror=alert(1)>", DirectoryPrincipalKind.User)
            .WithPrincipal("g-1", "Operations", DirectoryPrincipalKind.Group);
        var cut = RenderPicker();

        AccessForms.Type(cut, "Subject", "u");

        cut.WaitUntil(() =>
        {
            var option = cut.Find("[role=option]");
            Assert.That(cut.FindAll("[role=option]"), Has.Count.EqualTo(1), "only the chosen kind is searched");
            Assert.That(option.QuerySelector(".lt-combobox__value")!.TextContent, Is.EqualTo("u-1"));
            Assert.That(option.QuerySelector(".lt-combobox__detail")!.TextContent, Is.EqualTo("<img src=x onerror=alert(1)>"));
            Assert.That(option.QuerySelectorAll("img"), Is.Empty, "a display name is never markup");
        });
    }

    [Test]
    public void Choosing_a_match_fills_the_id_and_reports_the_principal()
    {
        Admin.WithPrincipal("u-1", "Alice", DirectoryPrincipalKind.User);
        string? id = null;
        DirectoryPrincipalDescriptor? chosen = null;
        var cut = RenderPicker(onId: value => id = value, onPrincipal: principal => chosen = principal);

        AccessForms.Type(cut, "Subject", "ali");
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=option]"), Has.Count.EqualTo(1)));
        cut.Find("[role=option]").Click();

        Assert.Multiple(() =>
        {
            Assert.That(id, Is.EqualTo("u-1"));
            Assert.That(chosen!.DisplayName, Is.EqualTo("Alice"));
            Assert.That(chosen.Kind, Is.EqualTo(DirectoryPrincipalKind.User));
            Assert.That(cut.FindAll("[role=listbox]"), Is.Empty);
        });
    }

    [Test]
    public async Task A_subject_the_directory_does_not_list_is_refused()
    {
        Admin.WithPrincipal("u-1", "Alice", DirectoryPrincipalKind.User);
        var cut = RenderPicker();

        AccessForms.Type(cut, "Subject", "u-404");
        var refused = await cut.InvokeAsync(cut.Instance.ConfirmAsync);
        AccessForms.Type(cut, "Subject", "u-1");
        var accepted = await cut.InvokeAsync(cut.Instance.ConfirmAsync);

        Assert.Multiple(() =>
        {
            Assert.That(refused, Is.False);
            Assert.That(accepted, Is.True);
        });
    }

    [Test]
    public void A_refused_subject_is_named_inline()
    {
        Admin.WithPrincipal("u-1", "Alice", DirectoryPrincipalKind.User);
        var cut = RenderPicker();

        AccessForms.Type(cut, "Subject", "u-404");
        AccessForms.Field(cut, "Subject").Blur();

        cut.WaitUntil(() => Assert.That(AccessForms.ErrorOf(cut, "Subject"), Is.EqualTo("No user is named u-404. Choose one from the list.")));
    }

    [Test]
    public void Changing_the_kind_clears_the_id_and_searches_the_other_kind()
    {
        Admin.WithPrincipal("u-1", "Alice", DirectoryPrincipalKind.User).WithPrincipal("g-1", "Ops", DirectoryPrincipalKind.Group);
        var kinds = new List<LatticeSubjectSelectorKind>();
        var ids = new List<string>();
        var cut = RenderPicker(onId: ids.Add, onKind: kinds.Add, initialId: "u-1");

        AccessForms.Choose(cut, "Subject kind", "group");
        AccessForms.Type(cut, "Subject", "1");

        Assert.Multiple(() =>
        {
            Assert.That(kinds, Is.EqualTo(new[] { LatticeSubjectSelectorKind.Group }));
            Assert.That(ids.First(), Is.Empty);
        });
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=option] .lt-combobox__value").Select(value => value.TextContent), Is.EqualTo(new[] { "g-1" })));
    }

    [Test]
    public async Task A_failed_search_is_a_note_and_the_id_is_used_as_typed()
    {
        Admin.WithPrincipal("u-1", "Alice", DirectoryPrincipalKind.User);
        Admin.Fail(nameof(FakeAuthAdmin.SearchDirectoryAsync), new InvalidOperationException("down"));
        var cut = RenderPicker();

        AccessForms.Type(cut, "Subject", "someone");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-combobox__note").TextContent, Does.Contain("used as typed")));
        Assert.That(await cut.InvokeAsync(cut.Instance.ConfirmAsync), Is.True);
    }

    [Test]
    public void The_directory_explanation_is_the_hint_and_a_fixed_kind_offers_no_choice()
    {
        Admin.WithPrincipal("g", "G", DirectoryPrincipalKind.Group);
        var cut = Render<AccessSubjectPicker>(parameters => parameters
            .Add(picker => picker.Label, "Group id")
            .Add(picker => picker.Kind, LatticeSubjectSelectorKind.Group)
            .Add(picker => picker.AllowKindChange, false)
            .Add(picker => picker.DirectoryAvailable, true)
            .Add(picker => picker.DirectoryExplanation, "An object id."));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("select"), Is.Empty);
            Assert.That(cut.Find(".lt-field__hint").TextContent, Is.EqualTo("An object id."));
            Assert.That(cut.Find("input").GetAttribute("role"), Is.EqualTo("combobox"));
        });
    }

    private IRenderedComponent<AccessSubjectPicker> RenderPicker(
        bool directory = true,
        Action<string>? onId = null,
        Action<LatticeSubjectSelectorKind>? onKind = null,
        Action<DirectoryPrincipalDescriptor>? onPrincipal = null,
        string? initialId = null) =>
        Render<AccessSubjectPicker>(parameters => parameters
            .Add(picker => picker.Label, "Subject")
            .Add(picker => picker.Id, initialId)
            .Add(picker => picker.DirectoryAvailable, directory)
            .Add(picker => picker.IdChanged, value => onId?.Invoke(value))
            .Add(picker => picker.KindChanged, kind => onKind?.Invoke(kind))
            .Add(picker => picker.OnPrincipalSelected, principal => onPrincipal?.Invoke(principal)));
}
