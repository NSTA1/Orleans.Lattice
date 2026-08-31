using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.Plugins.Selection;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// The shared empty / error / loading block every per-selection surface renders
/// (issue #1855).
/// </summary>
/// <remarks>
/// <para>
/// The block is where the vocabulary module's copy actually reaches a reader,
/// so what is asserted here is the contract between the two: the headline, the
/// explanation and the remedy all arrive, the tone the message declares becomes
/// a class rather than a colour the call site picked, and an action is rendered
/// only when activating it will do something.
/// </para>
/// <para>
/// That last one is load-bearing rather than fussy. A remedy the reader can see
/// but not perform is the dead end this issue exists to remove, and a button
/// bound to nothing is exactly that dead end with a nicer shape.
/// </para>
/// </remarks>
[TestFixture]
public sealed class SelectionStateViewTests
{
    private static readonly ExplorerStateMessage Failed =
        ExplorerStateCopy.Failed(ExplorerSubjects.Entries, "the endpoint is unreachable");

    [Test]
    public async Task Render_with_no_message_writes_nothing()
    {
        var html = await RenderAsync(new Dictionary<string, object?>());

        Assert.That(html, Does.Not.Contain("lx-selection-message"));
    }

    [Test]
    public async Task Render_writes_the_headline_the_explanation_and_the_remedy()
    {
        var html = await RenderAsync(new Dictionary<string, object?> { ["Message"] = Failed });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain(Failed.Headline));
            Assert.That(html, Does.Contain("the endpoint is unreachable"));

            // Compared on an apostrophe-free run of the remedy: the renderer
            // HTML-encodes the apostrophe in "the cluster's health", so asserting
            // the raw string would fail on the encoding rather than on the copy.
            Assert.That(Failed.Remedy, Does.StartWith("Try again."));
            Assert.That(html, Does.Contain("Try again. If it keeps failing, check the connection settings"));
            Assert.That(html, Does.Contain(Failed.RemedyLabel));
        });
    }

    [Test]
    public async Task Render_omits_the_remedy_block_when_the_message_carries_no_remedy()
    {
        var message = ExplorerStateCopy.Loading(ExplorerSubjects.Entries);

        var html = await RenderAsync(new Dictionary<string, object?> { ["Message"] = message });

        Assert.Multiple(() =>
        {
            Assert.That(message.Remedy, Is.Null, "the loading copy offers no remedy, which is what this pins");
            Assert.That(html, Does.Not.Contain("lx-selection-message-remedy"));
        });
    }

    [Test]
    public async Task Render_is_a_polite_status_region()
    {
        var html = await RenderAsync(new Dictionary<string, object?> { ["Message"] = Failed });

        Assert.That(html, Does.Contain("role=\"status\""),
            "a surface renders this block in one slot for every state, so the region must be live "
            + "for the change between two states to be announced");
    }

    [Test]
    public async Task Render_marks_only_a_load_in_flight_as_busy()
    {
        var busy = await RenderAsync(new Dictionary<string, object?>
        {
            ["Message"] = ExplorerStateCopy.Loading(ExplorerSubjects.Entries),
        });

        var settled = await RenderAsync(new Dictionary<string, object?> { ["Message"] = Failed });

        Assert.Multiple(() =>
        {
            Assert.That(busy, Does.Contain("aria-busy=\"true\""));
            Assert.That(settled, Does.Not.Contain("aria-busy"));
        });
    }

    [Test]
    public async Task Render_writes_the_action_when_a_label_and_a_handler_are_both_present()
    {
        var html = await RenderAsync(new Dictionary<string, object?>
        {
            ["Message"] = Failed,
            ["OnAction"] = EventCallback.Empty,
        });

        Assert.Multiple(() =>
        {
            Assert.That(Failed.ActionLabel, Is.EqualTo(ExplorerVocabulary.RetryAction));
            Assert.That(html, Does.Contain("lx-selection-message-action"));
            Assert.That(html, Does.Contain(ExplorerVocabulary.RetryAction));
        });
    }

    [Test]
    public async Task Render_omits_the_action_when_no_handler_is_bound()
    {
        var html = await RenderAsync(new Dictionary<string, object?> { ["Message"] = Failed });

        Assert.Multiple(() =>
        {
            Assert.That(Failed.ActionLabel, Is.Not.Null, "the message does offer an action");
            Assert.That(html, Does.Not.Contain("lx-selection-message-action"),
                "a button wired to nothing is the dead end this block exists to remove");
        });
    }

    [Test]
    public async Task Render_omits_the_action_when_the_message_names_none()
    {
        var message = ExplorerStateCopy.Empty(ExplorerSubjects.Entries);

        var html = await RenderAsync(new Dictionary<string, object?>
        {
            ["Message"] = message,
            ["OnAction"] = EventCallback.Empty,
        });

        Assert.Multiple(() =>
        {
            Assert.That(message.ActionLabel, Is.Null);
            Assert.That(html, Does.Not.Contain("lx-selection-message-action"));
        });
    }

    [Test]
    public async Task Render_prefers_the_callers_action_label_over_the_messages()
    {
        var html = await RenderAsync(new Dictionary<string, object?>
        {
            ["Message"] = Failed,
            ["ActionLabel"] = "Reload the scan",
            ["OnAction"] = EventCallback.Empty,
        });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Reload the scan"));
            Assert.That(html, Does.Not.Contain(">" + ExplorerVocabulary.RetryAction));
        });
    }

    [TestCase(ExplorerStateKind.NotPermitted, "is-denial")]
    [TestCase(ExplorerStateKind.SignInRequired, "is-denial")]
    [TestCase(ExplorerStateKind.Failed, "is-failed")]
    [TestCase(ExplorerStateKind.Loading, "is-busy")]
    public async Task Render_carries_the_tone_as_a_class(ExplorerStateKind kind, string expected)
    {
        var html = await RenderAsync(new Dictionary<string, object?>
        {
            ["Message"] = ExplorerStateCopy.For(ExplorerSubjects.Entries, kind),
        });

        Assert.That(html, Does.Contain(expected),
            "tone must survive greyscale and forced colours, so it cannot be carried by hue alone");
    }

    [TestCase(ExplorerStateKind.Empty)]
    [TestCase(ExplorerStateKind.ScopedOut)]
    [TestCase(ExplorerStateKind.Unavailable)]
    public async Task Render_carries_no_tone_class_for_a_neutral_state(ExplorerStateKind kind)
    {
        var html = await RenderAsync(new Dictionary<string, object?>
        {
            ["Message"] = ExplorerStateCopy.For(ExplorerSubjects.Entries, kind),
        });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-selection-message"));
            Assert.That(html, Does.Not.Contain("is-denial"));
            Assert.That(html, Does.Not.Contain("is-failed"));
            Assert.That(html, Does.Not.Contain("is-busy"));
        });
    }

    [Test]
    public async Task Render_appends_the_callers_class()
    {
        var html = await RenderAsync(new Dictionary<string, object?>
        {
            ["Message"] = Failed,
            ["Class"] = "lx-data-state",
        });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-selection-message"));
            Assert.That(html, Does.Contain("lx-data-state"));
        });
    }

    [Test]
    public async Task Render_writes_the_child_content_inside_the_block()
    {
        var html = await RenderAsync(new Dictionary<string, object?>
        {
            ["Message"] = Failed,
            ["ChildContent"] = (RenderFragment)(builder => builder.AddMarkupContent(0, "<em>extra</em>")),
        });

        Assert.That(html, Does.Contain("<em>extra</em>"));
    }

    private static Task<string> RenderAsync(IReadOnlyDictionary<string, object?> parameters) =>
        SelectionViewRenderHarness.RenderComponentAsync<SelectionStateView>(parameters);
}
