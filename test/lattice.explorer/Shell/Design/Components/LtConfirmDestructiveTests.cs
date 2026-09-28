using Bunit;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Shell.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.Shell.Design.Components;

/// <summary>
/// The destructive confirmation: an alert dialog whose destructive action is
/// enabled only once the object's exact, case-sensitive name has been typed.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtConfirmDestructiveTests : ShellDesignTestContext
{
    private const string TreeName = "crm/orders";

    [Test]
    public void It_is_an_alert_dialog_naming_the_object_to_type()
    {
        var cut = RenderConfirm();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-dialog").GetAttribute("role"), Is.EqualTo("alertdialog"));
            Assert.That(cut.Find("h2").TextContent, Is.EqualTo("Delete tree"));
            Assert.That(cut.Find(".lt-confirm__name").TextContent, Is.EqualTo(TreeName));
            Assert.That(cut.Find("label").TextContent, Is.EqualTo("Tree name"));
            Assert.That(cut.Find(".lt-confirm__consequence").TextContent, Is.EqualTo("Recoverable for seven days."));
            Assert.That(cut.Find("input").ClassList, Does.Contain("lt-input--mono"));
        });
    }

    [Test]
    public void The_destructive_action_starts_disabled()
    {
        var cut = RenderConfirm();

        Assert.Multiple(() =>
        {
            Assert.That(Confirm(cut).HasAttribute("disabled"), Is.True);
            Assert.That(Confirm(cut).GetAttribute("type"), Is.EqualTo("submit"));
            Assert.That(Confirm(cut).TextContent, Is.EqualTo("Delete tree"));
        });
    }

    [Test]
    [TestCase("crm/order")]
    [TestCase("CRM/ORDERS")]
    [TestCase(" crm/orders")]
    public void A_near_miss_leaves_it_disabled(string typed)
    {
        var cut = RenderConfirm();

        cut.Find("input").Input(typed);

        Assert.That(Confirm(cut).HasAttribute("disabled"), Is.True);
    }

    [Test]
    public void The_exact_name_enables_it()
    {
        var cut = RenderConfirm();

        cut.Find("input").Input(TreeName);

        Assert.That(Confirm(cut).HasAttribute("disabled"), Is.False);
    }

    [Test]
    public void Submitting_the_exact_name_confirms_and_closes()
    {
        var events = new List<string>();
        var cut = RenderConfirm(events);

        cut.Find("input").Input(TreeName);
        cut.Find("form").Submit();

        Assert.That(events, Is.EqualTo(new[] { "confirmed", "open:False" }), "confirm first, then close");
    }

    [Test]
    public void Submitting_anything_else_does_nothing()
    {
        var events = new List<string>();
        var cut = RenderConfirm(events);

        cut.Find("input").Input("crm");
        cut.Find("form").Submit();

        Assert.That(events, Is.Empty);
    }

    [Test]
    public void Cancel_closes_without_confirming()
    {
        var events = new List<string>();
        var cut = RenderConfirm(events);

        cut.Find("input").Input(TreeName);
        cut.FindAll(".lt-dialog__actions button")[0].Click();

        Assert.That(events, Is.EqualTo(new[] { "open:False" }));
    }

    [Test]
    public void Opening_moves_focus_to_the_name_field()
    {
        var cut = RenderConfirm();

        var invocation = JSInterop.VerifyFocusAsyncInvoke();
        Assert.That(((ElementReference)invocation.Arguments[0]!).Id,
            Is.EqualTo(cut.Find("input").GetAttribute("blazor:elementreference")));
    }

    [Test]
    public void Reopening_clears_what_was_typed()
    {
        var cut = RenderConfirm();
        cut.Find("input").Input(TreeName);

        cut.Render(p => p.Add(x => x.Open, false));
        cut.Render(p => p.Add(x => x.Open, true));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("input").GetAttribute("value"), Is.Empty.Or.Null);
            Assert.That(Confirm(cut).HasAttribute("disabled"), Is.True);
        });
    }

    private static AngleSharp.Dom.IElement Confirm(IRenderedComponent<LtConfirmDestructive> cut) =>
        cut.FindAll(".lt-dialog__actions button")[1];

    private IRenderedComponent<LtConfirmDestructive> RenderConfirm(List<string>? events = null) =>
        Render<LtConfirmDestructive>(p =>
        {
            p.Add(x => x.Open, true)
                .Add(x => x.Title, "Delete tree")
                .Add(x => x.ObjectKind, "tree")
                .Add(x => x.ObjectName, TreeName)
                .Add(x => x.ConfirmText, "Delete tree")
                .AddChildContent("Recoverable for seven days.");
            if (events is not null)
            {
                p.Add(x => x.OnConfirm, () => events.Add("confirmed"));
                p.Add(x => x.OpenChanged, (bool open) => events.Add("open:" + open));
            }
        });
}
