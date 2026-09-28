using Bunit;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Shell.Layout;

/// <summary>
/// The address line's completion fan-out as the user sees it: groups in
/// directory order however the answers race, a slow source cut off at its
/// timeout and named, a failing source named, neither holding back the rest, and
/// typing again cancelling what was pending.
/// </summary>
public sealed partial class AddressLineTests
{
    [Test]
    public void A_search_asks_every_visible_area_and_groups_the_answers_in_directory_order()
    {
        var slow = new TaskCompletionSource<IReadOnlyList<AddressCompletion>>();
        var data = Visible(new FakeArea("data", "Data", 1) { Completions = FakeCompletionSource.Gated(slow) });
        var apps = Visible(new FakeArea("apps", "Apps", 2) { Completions = FakeCompletionSource.Answering(Completion("a/crm", "apps")) });
        var cut = RenderLine(Location("/", data, apps));
        cut.Find(".lt-shell-address-line__edit").Click();

        cut.Find("input").Input("cr");

        cut.WaitUntil(() => Assert.That(GroupNames(cut), Is.EqualTo(new[] { "Apps" }), "an answer that is ready shows at once"));

        cut.InvokeAsync(() => slow.SetResult([Completion("a/crm/orders", "data")]));

        cut.WaitUntil(() =>
        {
            Assert.That(GroupNames(cut), Is.EqualTo(new[] { "Data", "Apps" }), "groups keep directory order, not arrival order");
            Assert.That(cut.FindAll("[role='option'] .lt-shell-combobox__label").Select(label => label.TextContent),
                Is.EqualTo(new[] { "a/crm/orders", "a/crm" }));
            Assert.That(cut.Find("input").GetAttribute("aria-expanded"), Is.EqualTo("true"));
            Assert.That(cut.Find("[role='status']").TextContent, Is.EqualTo("2 suggestions."));
        });
    }

    [Test]
    public void A_search_also_offers_the_areas_by_name()
    {
        var data = Visible(new FakeArea("data", "Data", 1));
        var cut = RenderLine(Location("/", data));
        cut.Find(".lt-shell-address-line__edit").Click();

        cut.Find("input").Input("dat");

        cut.WaitUntil(() =>
        {
            Assert.That(GroupNames(cut), Is.EqualTo(new[] { "Areas" }));
            Assert.That(cut.Find("[role='option'] .lt-shell-combobox__detail").TextContent, Is.EqualTo("Data"));
        });
    }

    [Test]
    public void A_source_that_does_not_answer_in_time_is_named_and_does_not_hold_back_the_rest()
    {
        var never = new TaskCompletionSource<IReadOnlyList<AddressCompletion>>();
        var telemetry = Visible(new FakeArea("telemetry", "Telemetry", 1) { Completions = FakeCompletionSource.Gated(never) });
        var data = Visible(new FakeArea("data", "Data", 2) { Completions = FakeCompletionSource.Answering(Completion("orders", "data")) });
        var cut = RenderLine(Location("/", telemetry, data));
        cut.Find(".lt-shell-address-line__edit").Click();

        cut.Find("input").Input("ord");

        cut.WaitUntil(() =>
        {
            Assert.That(GroupNames(cut), Is.EqualTo(new[] { "Data" }));
            Assert.That(cut.Find("[role='status']").TextContent, Is.EqualTo("Searching."), "a source is still pending");
        });

        cut.InvokeAsync(() => Time.Advance(new ExplorerChromeOptions().CompletionTimeout));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-shell-combobox__note").TextContent, Is.EqualTo("Telemetry did not answer in time."));
            Assert.That(cut.Find("[role='status']").TextContent, Is.EqualTo("1 suggestion. Telemetry did not answer in time."));
            Assert.That(GroupNames(cut), Is.EqualTo(new[] { "Data" }));
        });
    }

    [Test]
    public void A_source_that_fails_is_named_and_the_others_still_answer()
    {
        var schema = Visible(new FakeArea("schema", "Schema", 1) { Completions = FakeCompletionSource.Throwing() });
        var data = Visible(new FakeArea("data", "Data", 2) { Completions = FakeCompletionSource.Answering(Completion("orders", "data")) });
        var cut = RenderLine(Location("/", schema, data));
        cut.Find(".lt-shell-address-line__edit").Click();

        cut.Find("input").Input("ord");

        cut.WaitUntil(() =>
        {
            Assert.That(GroupNames(cut), Is.EqualTo(new[] { "Data" }));
            Assert.That(cut.Find(".lt-shell-combobox__note").TextContent, Is.EqualTo("Schema could not be searched."));
        });
    }

    [Test]
    public void Typing_again_cancels_the_pending_completion()
    {
        var never = new TaskCompletionSource<IReadOnlyList<AddressCompletion>>();
        var source = FakeCompletionSource.Gated(never);
        var data = Visible(new FakeArea("data", "Data") { Completions = source });
        var cut = RenderLine(Location("/", data));
        cut.Find(".lt-shell-address-line__edit").Click();

        cut.Find("input").Input("or");
        cut.Find("input").Input("ord");

        Assert.Multiple(() =>
        {
            Assert.That(source.Queries.Select(query => query.Text), Is.EqualTo(new[] { "or", "ord" }));
            Assert.That(source.Tokens[0].IsCancellationRequested, Is.True);
            Assert.That(source.Tokens[1].IsCancellationRequested, Is.False);
        });
    }

    [Test]
    public void Closing_the_input_cancels_the_pending_completion()
    {
        var never = new TaskCompletionSource<IReadOnlyList<AddressCompletion>>();
        var source = FakeCompletionSource.Gated(never);
        var cut = RenderLine(Location("/", Visible(new FakeArea("data", "Data") { Completions = source })));
        cut.Find(".lt-shell-address-line__edit").Click();
        cut.Find("input").Input("ord");

        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "Escape" });

        Assert.That(source.Tokens.Single().IsCancellationRequested, Is.True);
    }

    [Test]
    public void An_unavailable_area_is_never_asked()
    {
        var source = FakeCompletionSource.Answering(Completion("x", "backups"));
        var cut = RenderLine(Location("/", new ExplorerAreaEntry(
            new FakeArea("backups", "Backups") { Completions = source },
            AreaAvailability.Unavailable("No grant."))));
        cut.Find(".lt-shell-address-line__edit").Click();

        cut.Find("input").Input("x");

        Assert.That(source.Queries, Is.Empty);
    }

    [Test]
    public void The_app_prefix_asks_the_areas_in_app_mode_and_choosing_a_completion_navigates()
    {
        var source = FakeCompletionSource.Answering(new AddressCompletion("a/crm", ExplorerAddress.ForArea("apps", "crm"), "CRM 2.1.0"));
        var cut = RenderLine(Location("/", Visible(new FakeArea("apps", "Apps") { Completions = source })));
        cut.Find(".lt-shell-address-line__edit").Click();

        cut.Find("input").Input("a/cr");
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role='option']"), Has.Count.EqualTo(1)));
        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "ArrowDown" });
        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "Enter" });

        Assert.Multiple(() =>
        {
            Assert.That(source.Queries.Single().Mode, Is.EqualTo(AddressQueryMode.App));
            Assert.That(source.Queries.Single().Text, Is.EqualTo("cr"));
            Assert.That(Navigation.Uri, Is.EqualTo(Navigation.BaseUri + "apps/crm"));
        });
    }

    [Test]
    public void The_active_option_survives_a_later_group_arriving()
    {
        var slow = new TaskCompletionSource<IReadOnlyList<AddressCompletion>>();
        var data = Visible(new FakeArea("data", "Data", 1) { Completions = FakeCompletionSource.Gated(slow) });
        var apps = Visible(new FakeArea("apps", "Apps", 2) { Completions = FakeCompletionSource.Answering(Completion("a/crm", "apps")) });
        var cut = RenderLine(Location("/", data, apps));
        cut.Find(".lt-shell-address-line__edit").Click();
        cut.Find("input").Input("cr");
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role='option']"), Has.Count.EqualTo(1)));
        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "ArrowDown" });

        cut.InvokeAsync(() => slow.SetResult([Completion("a/crm/orders", "data")]));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("[role='option']"), Has.Count.EqualTo(2));
            Assert.That(ActiveOption(cut), Does.Contain("a/crm").And.Not.Contain("orders"), "tracked by identity, not position");
        });
    }

    private static AddressCompletion Completion(string label, string area) => new(label, ExplorerAddress.ForArea(area, "x"));

    private static IEnumerable<string> GroupNames(IRenderedComponent<Orleans.Lattice.Explorer.Shell.Layout.AddressLine> cut) =>
        cut.FindAll(".lt-shell-combobox__group-name").Select(name => name.TextContent);
}
