using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// The dialog: a named modal that takes focus when it opens, keeps it inside,
/// closes on Escape or its Close button, and says so to its host.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtDialogTests : ShellDesignTestContext
{
    [Test]
    public void A_closed_dialog_renders_nothing()
    {
        var cut = Render<LtDialog>(p => p.Add(x => x.Title, "Resize tree"));

        Assert.That(cut.Markup.Trim(), Is.Empty);
    }

    [Test]
    public void An_open_dialog_is_a_modal_named_by_its_title_and_described_by_its_description()
    {
        var cut = RenderOpen(p => p.Add(x => x.Description, "Rebuilds the tree online."));
        var dialog = cut.Find(".lt-dialog");

        Assert.Multiple(() =>
        {
            Assert.That(dialog.GetAttribute("role"), Is.EqualTo("dialog"));
            Assert.That(dialog.GetAttribute("aria-modal"), Is.EqualTo("true"));
            Assert.That(dialog.GetAttribute("tabindex"), Is.EqualTo("-1"));
            Assert.That(dialog.GetAttribute("aria-labelledby"), Is.EqualTo(cut.Find("h2").Id));
            Assert.That(cut.Find("h2").TextContent, Is.EqualTo("Resize tree"));
            Assert.That(dialog.GetAttribute("aria-describedby"), Is.EqualTo(cut.Find(".lt-dialog__description").Id));
            Assert.That(cut.Find(".lt-dialog__body").TextContent, Is.EqualTo("Body"));
        });
    }

    [Test]
    public void An_alert_dialog_is_announced_as_one()
    {
        var cut = RenderOpen(p => p.Add(x => x.Alert, true));

        Assert.That(cut.Find(".lt-dialog").GetAttribute("role"), Is.EqualTo("alertdialog"));
    }

    [Test]
    public void Opening_moves_focus_into_the_dialog()
    {
        var cut = RenderOpen();

        var invocation = JSInterop.VerifyFocusAsyncInvoke();
        Assert.That(((ElementReference)invocation.Arguments[0]!).Id,
            Is.EqualTo(cut.Find(".lt-dialog").GetAttribute("blazor:elementreference")));
    }

    [Test]
    public void A_dialog_that_does_not_auto_focus_leaves_focus_to_its_content()
    {
        RenderOpen(p => p.Add(x => x.AutoFocus, false));

        Assert.That(JSInterop.Invocations.Where(i => i.Identifier.Contains("focus", StringComparison.OrdinalIgnoreCase)), Is.Empty);
    }

    [Test]
    public void Tabbing_past_either_end_returns_focus_to_the_dialog()
    {
        var cut = RenderOpen(p => p.Add(x => x.AutoFocus, false));
        var sentinels = cut.FindAll(".lt-dialog__sentinel");

        Assert.That(sentinels, Has.Count.EqualTo(2), "one sentinel before the dialog and one after it");
        Assert.That(sentinels.All(sentinel => sentinel.GetAttribute("tabindex") == "0"), Is.True);

        sentinels[1].Focus();

        var invocation = JSInterop.VerifyFocusAsyncInvoke();
        Assert.That(((ElementReference)invocation.Arguments[0]!).Id,
            Is.EqualTo(cut.Find(".lt-dialog").GetAttribute("blazor:elementreference")));
    }

    [Test]
    public void Escape_asks_the_host_to_close_it()
    {
        var observed = new List<bool>();
        var cut = RenderOpen(p => p.Add(x => x.OpenChanged, (bool open) => observed.Add(open)));

        cut.Find(".lt-dialog").KeyDown(new KeyboardEventArgs { Key = "Escape" });

        Assert.That(observed, Is.EqualTo(new[] { false }));
    }

    [Test]
    public void Escape_is_ignored_when_the_dialog_is_not_dismissible()
    {
        var observed = new List<bool>();
        var cut = RenderOpen(p => p
            .Add(x => x.DismissOnEscape, false)
            .Add(x => x.OpenChanged, (bool open) => observed.Add(open)));

        cut.Find(".lt-dialog").KeyDown(new KeyboardEventArgs { Key = "Escape" });
        cut.Find(".lt-dialog").KeyDown(new KeyboardEventArgs { Key = "Enter" });

        Assert.That(observed, Is.Empty);
    }

    [Test]
    public void The_close_button_asks_the_host_to_close_it()
    {
        var observed = new List<bool>();
        var cut = RenderOpen(p => p.Add(x => x.OpenChanged, (bool open) => observed.Add(open)));

        cut.Find(".lt-dialog__header button").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-dialog__header button").TextContent, Is.EqualTo("Close"));
            Assert.That(observed, Is.EqualTo(new[] { false }));
        });
    }

    [Test]
    public void The_close_button_and_actions_are_optional()
    {
        var cut = RenderOpen(p => p.Add(x => x.ShowClose, false));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-dialog__header button"), Is.Empty);
            Assert.That(cut.FindAll(".lt-dialog__actions"), Is.Empty);
        });
    }

    [Test]
    public void Actions_render_at_the_foot()
    {
        var cut = RenderOpen(p => p.Add(x => x.Actions, "<button>Resize</button>"));

        Assert.That(cut.Find(".lt-dialog__actions button").TextContent, Is.EqualTo("Resize"));
    }

    [Test]
    public void Closing_returns_focus_to_the_element_that_opened_it()
    {
        var opener = new ElementReference("opener-ref", new WebElementReferenceContext(JSInterop.JSRuntime));
        var cut = RenderOpen(p => p.Add(x => x.ReturnFocus, opener));

        cut.Render(p => p.Add(x => x.Open, false));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Markup.Trim(), Is.Empty);
            Assert.That(
                JSInterop.Invocations.Select(i => i.Arguments.FirstOrDefault()).OfType<ElementReference>().Select(e => e.Id),
                Does.Contain("opener-ref"));
        });
    }

    [Test]
    public async Task CloseAsync_asks_the_host_to_close_it()
    {
        var observed = new List<bool>();
        var cut = RenderOpen(p => p.Add(x => x.OpenChanged, (bool open) => observed.Add(open)));

        await cut.InvokeAsync(cut.Instance.CloseAsync);

        Assert.That(observed, Is.EqualTo(new[] { false }));
    }

    private IRenderedComponent<LtDialog> RenderOpen(Action<ComponentParameterCollectionBuilder<LtDialog>>? configure = null) =>
        Render<LtDialog>(p =>
        {
            p.Add(x => x.Open, true).Add(x => x.Title, "Resize tree").AddChildContent("Body");
            configure?.Invoke(p);
        });
}
