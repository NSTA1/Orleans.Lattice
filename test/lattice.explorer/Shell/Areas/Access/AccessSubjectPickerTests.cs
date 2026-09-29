using Bunit;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Shell.Areas.Access;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Access;

/// <summary>
/// The subject picker: directory search on request, display names rendered as
/// text, choosing a match, paging, a kind change clearing the id, and the plain
/// id box when no directory is configured.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessSubjectPickerTests : AccessTestContext
{
    [Test]
    public void Without_a_directory_the_picker_is_a_plain_id_box_that_says_it_is_not_validated()
    {
        var cut = RenderPicker(directory: false);

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("button"), Is.Empty);
            Assert.That(cut.Find(".lt-field__hint").TextContent, Does.Contain("not validated"));
        });
    }

    [Test]
    public void A_search_lists_matches_by_display_name_as_text_with_the_id_beside_it()
    {
        Admin.WithPrincipal("u-1", "<img src=x onerror=alert(1)>", DirectoryPrincipalKind.User)
            .WithPrincipal("g-1", "Operations", DirectoryPrincipalKind.Group);
        var cut = RenderPicker();

        AccessForms.Button(cut, "Search the directory").Click();

        cut.WaitUntil(() =>
        {
            var option = cut.Find(".lt-access-results__option");
            Assert.That(cut.FindAll(".lt-access-results__option"), Has.Count.EqualTo(1), "only the chosen kind is searched");
            Assert.That(option.QuerySelector("span")!.TextContent, Is.EqualTo("<img src=x onerror=alert(1)>"));
            Assert.That(option.QuerySelectorAll("img"), Is.Empty, "a display name is never markup");
            Assert.That(option.QuerySelector(".lt-access-results__id")!.TextContent, Is.EqualTo("u-1"));
        });
    }

    [Test]
    public void Choosing_a_match_fills_the_id_and_reports_the_principal()
    {
        Admin.WithPrincipal("u-1", "Alice", DirectoryPrincipalKind.User);
        string? id = null;
        DirectoryPrincipalDescriptor? chosen = null;
        var cut = RenderPicker(onId: value => id = value, onPrincipal: principal => chosen = principal);

        AccessForms.Button(cut, "Search the directory").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-access-results__option"), Has.Count.EqualTo(1)));
        cut.Find(".lt-access-results__option").Click();

        Assert.Multiple(() =>
        {
            Assert.That(id, Is.EqualTo("u-1"));
            Assert.That(chosen!.DisplayName, Is.EqualTo("Alice"));
            Assert.That(cut.FindAll(".lt-access-results"), Is.Empty);
        });
    }

    [Test]
    public void No_match_says_so_and_more_matches_load_on_request()
    {
        for (var i = 0; i < 25; i++)
        {
            Admin.WithPrincipal($"u-{i:00}", $"User {i}", DirectoryPrincipalKind.User);
        }

        var cut = RenderPicker();
        AccessForms.Button(cut, "Search the directory").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-access-results__option"), Has.Count.EqualTo(20)));

        AccessForms.Button(cut, "Load more matches").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-access-results__option"), Has.Count.EqualTo(25)));

        AccessForms.Type(cut, "Subject", "nobody");
        AccessForms.Button(cut, "Search the directory").Click();
        cut.WaitUntil(() => Assert.That(cut.Find("[role=status]").TextContent, Is.EqualTo("No user in the directory matches.")));
    }

    [Test]
    public void Changing_the_kind_clears_the_id_and_the_results()
    {
        Admin.WithPrincipal("u-1", "Alice", DirectoryPrincipalKind.User);
        var kinds = new List<LatticeSubjectSelectorKind>();
        var ids = new List<string>();
        var cut = RenderPicker(onId: ids.Add, onKind: kinds.Add, initialId: "u-1");
        AccessForms.Button(cut, "Search the directory").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-access-results__option"), Has.Count.EqualTo(1)));

        AccessForms.Choose(cut, "Subject kind", "group");

        Assert.Multiple(() =>
        {
            Assert.That(kinds, Is.EqualTo(new[] { LatticeSubjectSelectorKind.Group }));
            Assert.That(ids, Is.EqualTo(new[] { string.Empty }));
            Assert.That(cut.FindAll(".lt-access-results"), Is.Empty);
        });
    }

    [Test]
    public void A_failed_search_is_reported_in_place()
    {
        Admin.WithPrincipal("u-1", "Alice", DirectoryPrincipalKind.User);
        Admin.Fail(nameof(FakeAuthAdmin.SearchDirectoryAsync), new InvalidOperationException("down"));
        var cut = RenderPicker();

        AccessForms.Button(cut, "Search the directory").Click();

        cut.WaitUntil(() => Assert.That(cut.Find("[role=alert]").TextContent, Does.Contain("could not be reached")));
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
