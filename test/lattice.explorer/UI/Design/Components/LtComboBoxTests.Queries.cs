using Bunit;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// Queries are debounced by input, not by the clock: at most one is outstanding,
/// typing cancels it, the keys typed meanwhile collapse into one query for the
/// latest text, and a stale answer never replaces a newer one. A new source - a
/// tenant switch re-keys the page's sources - discards every answer.
/// </summary>
public sealed partial class LtComboBoxTests
{
    [Test]
    public void Typing_cancels_the_running_query_and_the_last_query_is_for_the_latest_text()
    {
        var source = new FakeSuggestionSource(Trees) { Gated = true };
        var cut = RenderBox(source);

        cut.Find("input").Input("c");
        cut.Find("input").Input("cr");
        cut.Find("input").Input("crm/");

        Assert.That(SpinWait.SpinUntil(() => source.Queries.Count > 0 && source.Queries[^1].Text == "crm/", TimeSpan.FromSeconds(10)), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(source.Queries.Take(source.Queries.Count - 1).All(query => query.Token.IsCancellationRequested), Is.True, "every stale query is cancelled");
            Assert.That(source.Queries[^1].Token.IsCancellationRequested, Is.False);
        });

        cut.InvokeAsync(() => source.Queries[^1].Gate.SetResult(source.Answer("crm/", 8)));

        cut.WaitUntil(() => Assert.That(Options(cut), Is.EqualTo(new[] { "crm/orders", "crm/customers", "crm/orders-archive" })));
    }

    [Test]
    public void Keys_typed_while_a_query_is_still_settling_collapse_into_one_query()
    {
        var source = new FakeSuggestionSource(Trees) { Gated = true, IgnoresCancellation = true };
        var cut = RenderBox(source);

        cut.Find("input").Input("c");
        cut.Find("input").Input("cr");
        cut.Find("input").Input("crm");
        cut.Find("input").Input("crm/");

        Assert.Multiple(() =>
        {
            Assert.That(source.Queries, Has.Count.EqualTo(1), "at most one query is outstanding");
            Assert.That(source.Queries[0].Token.IsCancellationRequested, Is.True);
        });

        cut.InvokeAsync(() => source.Queries[0].Gate.SetResult(source.Answer("c", 8)));

        Assert.That(SpinWait.SpinUntil(() => source.Queries.Count == 2, TimeSpan.FromSeconds(10)), Is.True);
        Assert.That(source.Queries[1].Text, Is.EqualTo("crm/"), "the burst costs one query, for the latest text");
        cut.InvokeAsync(() => source.Queries[1].Gate.SetResult(source.Answer("crm/", 8)));
        cut.WaitUntil(() => Assert.That(Options(cut), Has.Length.EqualTo(3)));
        Assert.That(source.Queries, Has.Count.EqualTo(2));
    }
    [Test]
    public void A_stale_answer_from_a_source_that_ignores_cancellation_is_discarded()
    {
        var source = new FakeSuggestionSource(Trees) { Gated = true, IgnoresCancellation = true };
        var cut = RenderBox(source);
        cut.Find("input").Input("b");
        cut.Find("input").Input("crm/");

        // The stale query answers anyway; the latest text is queried after it.
        cut.InvokeAsync(() => source.Queries[0].Gate.SetResult(source.Answer("b", 8)));
        Assert.That(SpinWait.SpinUntil(() => source.Queries.Count == 2, TimeSpan.FromSeconds(10)), Is.True, "the next query is issued when the cancelled one settles");

        Assert.That(cut.FindAll("[role=option]"), Is.Empty, "billing/invoices was the answer for stale text");

        cut.InvokeAsync(() => source.Queries[1].Gate.SetResult(source.Answer("crm/", 8)));
        cut.WaitUntil(() => Assert.That(Options(cut), Does.Contain("crm/orders").And.Not.Contain("billing/invoices")));
    }

    [Test]
    public void Leaving_the_field_abandons_the_running_query()
    {
        var source = new FakeSuggestionSource(Trees) { Gated = true };
        var cut = RenderBox(source, p => p.Add(x => x.Mode, LtComboBoxMode.Suggest));
        cut.Find("input").Input("crm/");

        cut.Find("input").Blur();

        Assert.That(source.Queries[0].Token.IsCancellationRequested, Is.True);
    }

    [Test]
    public void A_new_source_discards_every_answer_the_old_one_gave()
    {
        var acme = new FakeSuggestionSource("acme/orders");
        var globex = new FakeSuggestionSource("globex/orders");
        var cut = RenderBox(acme);
        cut.Find("input").Input("a");
        Assert.That(Options(cut), Is.EqualTo(new[] { "acme/orders" }));

        cut.Render(p => p.Add(x => x.Source, globex));

        Assert.That(cut.FindAll("[role=option]"), Is.Empty, "acme's answer is not shown under globex");

        cut.Find("input").Input("g");
        Assert.Multiple(() =>
        {
            Assert.That(globex.Queries.Single().Text, Is.EqualTo("g"));
            Assert.That(Options(cut), Is.EqualTo(new[] { "globex/orders" }));
        });
    }

    [Test]
    public async Task A_query_running_under_the_old_source_is_cancelled_when_the_source_changes()
    {
        var acme = new FakeSuggestionSource("acme/orders") { Gated = true };
        var cut = RenderBox(acme);
        cut.Find("input").Input("a");

        cut.Render(p => p.Add(x => x.Source, new FakeSuggestionSource("globex/orders")));
        await cut.InvokeAsync(() => acme.Queries[0].Gate.TrySetResult(acme.Answer("a", 8)));

        Assert.Multiple(() =>
        {
            Assert.That(acme.Queries[0].Token.IsCancellationRequested, Is.True);
            Assert.That(cut.FindAll("[role=option]"), Is.Empty);
        });
    }

    [Test]
    public async Task Disposing_the_field_cancels_its_query()
    {
        var source = new FakeSuggestionSource(Trees) { Gated = true };
        var cut = RenderBox(source);
        cut.Find("input").Input("crm/");

        await cut.InvokeAsync(() => cut.Instance.DisposeAsync().AsTask());

        Assert.That(source.Queries[0].Token.IsCancellationRequested, Is.True);
    }
}
