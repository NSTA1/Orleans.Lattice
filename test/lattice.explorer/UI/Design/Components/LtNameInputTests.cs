using Bunit;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// The name field for a new thing: a plain labelled text box with no list and no
/// arrow, which refuses (or flags) a name that is already taken as it is typed,
/// runs the page's own check when it is left and at submit, and turns a check that
/// cannot answer into a note rather than a refusal.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtNameInputTests : ShellDesignTestContext
{
    private static readonly string[] Trees = ["crm/orders", "crm/customers", "billing/invoices", "crm/orders-archive"];

    [Test]
    public void It_is_a_labelled_text_box_with_no_list_and_no_arrow()
    {
        var cut = RenderName(new FakeSuggestionSource(Trees));
        var input = cut.Find("input");

        input.Input("crm/");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("label").GetAttribute("for"), Is.EqualTo(input.Id));
            Assert.That(input.Id, Is.EqualTo(cut.Instance.InputId));
            Assert.That(input.GetAttribute("type"), Is.EqualTo("text"));
            Assert.That(input.HasAttribute("role"), Is.False);
            Assert.That(input.HasAttribute("aria-expanded") || input.HasAttribute("aria-controls") || input.HasAttribute("aria-autocomplete"), Is.False);
            Assert.That(input.ClassList, Does.Contain("lt-input--mono"));
            Assert.That(cut.FindAll("[role=listbox], [role=option], .lt-combobox__chevron"), Is.Empty, "existing names are never offered as a list");
        });
    }

    [Test]
    public void A_name_that_is_taken_is_refused_as_it_is_typed_and_announced()
    {
        var cut = RenderName(new FakeSuggestionSource(Trees));

        cut.Find("input").Input("crm/new");
        Assert.That(cut.FindAll(".lt-field__error"), Is.Empty);

        cut.Find("input").Input("crm/orders");

        var input = cut.Find("input");
        var error = cut.Find(".lt-field__error");
        Assert.Multiple(() =>
        {
            Assert.That(error.TextContent, Does.Contain("A tree named crm/orders already exists."));
            Assert.That(input.GetAttribute("aria-invalid"), Is.EqualTo("true"));
            Assert.That(input.GetAttribute("aria-describedby"), Does.Contain(error.Id));
            Assert.That(cut.Find("[role=status]").TextContent, Is.EqualTo("A tree named crm/orders already exists."));
        });
    }

    [Test]
    public void A_prefix_of_a_taken_name_is_not_refused()
    {
        var cut = RenderName(new FakeSuggestionSource(Trees));

        cut.Find("input").Input("crm/order");

        Assert.That(cut.FindAll(".lt-field__error"), Is.Empty);
    }

    [Test]
    public void The_pages_own_sentence_replaces_the_default()
    {
        var cut = RenderName(new FakeSuggestionSource(Trees), p => p.Add(x => x.ExistingMessage, "A snapshot needs a new tree."));

        cut.Find("input").Input("crm/orders");

        Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("A snapshot needs a new tree."));
    }

    [Test]
    public void Without_rejection_a_taken_name_is_flagged_not_refused()
    {
        var cut = RenderName(new FakeSuggestionSource(Trees), p => p.Add(x => x.RejectExisting, false));

        cut.Find("input").Input("crm/orders");

        var flag = cut.Find(".lt-name-input__flag");
        Assert.Multiple(() =>
        {
            Assert.That(flag.TextContent, Is.EqualTo("A tree named crm/orders already exists."));
            Assert.That(cut.Find("input").GetAttribute("aria-describedby"), Does.Contain(flag.Id));
            Assert.That(cut.Find("input").HasAttribute("aria-invalid"), Is.False, "a flag is not an error");
        });
    }

    [Test]
    public async Task Confirm_refuses_a_taken_name_and_accepts_a_new_one_or_none()
    {
        var cut = RenderName(new FakeSuggestionSource(Trees));

        var empty = await cut.InvokeAsync(cut.Instance.ConfirmAsync);
        cut.Find("input").Input("crm/orders");
        var refused = await cut.InvokeAsync(cut.Instance.ConfirmAsync);
        cut.Find("input").Input("crm/brand-new");
        var accepted = await cut.InvokeAsync(cut.Instance.ConfirmAsync);

        Assert.Multiple(() =>
        {
            Assert.That(empty, Is.True, "whether a name is required is the page's rule");
            Assert.That(refused, Is.False);
            Assert.That(accepted, Is.True);
            Assert.That(cut.FindAll(".lt-field__error"), Is.Empty);
        });
    }

    [Test]
    public async Task The_pages_check_runs_when_the_field_is_left_and_at_confirm_but_not_per_key()
    {
        var checkedNames = new List<string>();
        var cut = RenderName(new FakeSuggestionSource(Trees), p => p.Add(x => x.Validate, (name, _) =>
        {
            checkedNames.Add(name);
            return Task.FromResult<string?>(name == "ghost" ? "ghost is not in the directory." : null);
        }));

        cut.Find("input").Input("g");
        cut.Find("input").Input("gh");
        cut.Find("input").Input("ghost");
        Assert.That(checkedNames, Is.Empty);

        cut.Find("input").Blur();
        Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("ghost is not in the directory."));

        var refused = await cut.InvokeAsync(cut.Instance.ConfirmAsync);
        cut.Find("input").Input("real");
        var accepted = await cut.InvokeAsync(cut.Instance.ConfirmAsync);

        Assert.Multiple(() =>
        {
            Assert.That(refused, Is.False);
            Assert.That(accepted, Is.True);
            Assert.That(checkedNames, Is.EqualTo(new[] { "ghost", "ghost", "real" }));
        });
    }

    [Test]
    public async Task A_taken_name_is_refused_without_asking_the_pages_check()
    {
        var asked = 0;
        var cut = RenderName(new FakeSuggestionSource(Trees), p => p.Add(x => x.Validate, (_, _) =>
        {
            asked++;
            return Task.FromResult<string?>(null);
        }));

        cut.Find("input").Input("crm/orders");

        Assert.That(await cut.InvokeAsync(cut.Instance.ConfirmAsync), Is.False);
        Assert.That(asked, Is.Zero);
    }

    [Test]
    public void A_burst_of_keys_costs_one_query_for_the_latest_name()
    {
        var source = new FakeSuggestionSource(Trees) { Gated = true, IgnoresCancellation = true };
        var cut = RenderName(source);

        cut.Find("input").Input("c");
        cut.Find("input").Input("cr");
        cut.Find("input").Input("crm");
        cut.Find("input").Input("crm/orders");
        Assert.Multiple(() =>
        {
            Assert.That(source.Queries, Has.Count.EqualTo(1), "one query outstanding while the keys arrive");
            Assert.That(source.Queries[0].Token.IsCancellationRequested, Is.True);
        });

        cut.InvokeAsync(() => source.Queries[0].Gate.SetResult(source.Answer("c", 8)));

        cut.WaitUntil(() => Assert.That(source.Queries.Select(query => query.Text), Is.EqualTo(new[] { "c", "crm/orders" })));
        cut.InvokeAsync(() => source.Queries[1].Gate.SetResult(source.Answer("crm/orders", 8)));
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("A tree named crm/orders already exists.")));
        Assert.That(source.Queries, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task A_source_that_cannot_list_is_a_note_and_the_name_is_accepted()
    {
        var cut = RenderName(new FakeSuggestionSource(Trees) { Unavailable = "No catalogue can be read." });

        cut.Find("input").Input("crm/orders");

        var note = cut.Find(".lt-name-input__note");
        Assert.Multiple(() =>
        {
            Assert.That(note.TextContent, Is.EqualTo("No catalogue can be read."));
            Assert.That(cut.Find("input").GetAttribute("aria-describedby"), Does.Contain(note.Id));
        });
        Assert.That(await cut.InvokeAsync(cut.Instance.ConfirmAsync), Is.True);
    }

    [Test]
    public async Task A_check_that_throws_is_a_note_never_a_crash()
    {
        var cut = RenderName(new FakeSuggestionSource(Trees) { Throws = new InvalidOperationException("boom") }, p => p
            .Add(x => x.Validate, (_, _) => throw new InvalidOperationException("bang")));

        cut.Find("input").Input("crm/orders");
        cut.Find("input").Blur();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-name-input__note").TextContent, Is.EqualTo(LtNameInput.CheckFailedNote));
            Assert.That(cut.Markup, Does.Not.Contain("boom").And.Not.Contain("bang"));
        });
        Assert.That(await cut.InvokeAsync(cut.Instance.ConfirmAsync), Is.True, "the server checks the name again when it is saved");
    }

    [Test]
    public void The_pages_error_replaces_the_fields_own()
    {
        var cut = RenderName(new FakeSuggestionSource(Trees), p => p.Add(x => x.Error, "Enter the tree id."));

        cut.Find("input").Input("crm/orders");

        Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Enter the tree id.").And.Not.Contain("already exists"));
    }

    [Test]
    public async Task A_read_only_or_disabled_field_checks_nothing()
    {
        var source = new FakeSuggestionSource(Trees);
        var cut = RenderName(source, p => p.Add(x => x.Value, "crm/orders").Add(x => x.ReadOnly, true));

        Assert.That(await cut.InvokeAsync(cut.Instance.ConfirmAsync), Is.True);
        cut.Render(p => p.Add(x => x.ReadOnly, false).Add(x => x.Disabled, true));
        Assert.That(await cut.InvokeAsync(cut.Instance.ConfirmAsync), Is.True);
        Assert.That(source.Queries, Is.Empty);
    }

    [Test]
    public void A_new_value_from_the_page_clears_the_verdict()
    {
        var cut = RenderName(new FakeSuggestionSource(Trees));
        cut.Find("input").Input("crm/orders");
        Assert.That(cut.FindAll(".lt-field__error"), Has.Count.EqualTo(1));

        cut.Render(p => p.Add(x => x.Value, string.Empty));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-field__error"), Is.Empty);
            Assert.That(cut.Find("input").GetAttribute("value"), Is.Empty);
        });
    }

    [Test]
    public async Task Focus_is_requested_safely()
    {
        var cut = RenderName(null);

        await cut.InvokeAsync(() => cut.Instance.FocusAsync().AsTask());

        Assert.That(cut.Find("input").Id, Is.EqualTo(cut.Instance.InputId));
    }

    [Test]
    public void Leaving_the_field_abandons_its_checks()
    {
        var source = new FakeSuggestionSource(Trees) { Gated = true };
        var cut = RenderName(source);
        cut.Find("input").Input("crm/orders");

        cut.Instance.Dispose();

        Assert.That(source.Queries[0].Token.IsCancellationRequested, Is.True);
    }

    private IRenderedComponent<LtNameInput> RenderName(
        ILtSuggestionSource? source,
        Action<ComponentParameterCollectionBuilder<LtNameInput>>? configure = null) =>
        Render<LtNameInput>(parameters =>
        {
            parameters.Add(x => x.Label, "Destination tree").Add(x => x.Existing, source).Add(x => x.Noun, "tree");
            configure?.Invoke(parameters);
        });
}
