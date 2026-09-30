using Bunit;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// Which option the list opens on: always its first, never the one a resting
/// pointer happens to cover. The pointer shades an option only once it has moved.
/// </summary>
public sealed partial class AddressLineTests
{
    [Test]
    public void The_palette_opens_on_its_first_option()
    {
        var cut = RenderLine(Location("/"));
        cut.Find(".lt-shell-address-line__edit").Click();

        cut.Find("input").Input(">theme");

        var first = cut.FindAll("[role='option']")[0];
        Assert.Multiple(() =>
        {
            Assert.That(first.GetAttribute("aria-selected"), Is.EqualTo("true"));
            Assert.That(cut.FindAll("[aria-selected='true']"), Has.Count.EqualTo(1));
            Assert.That(cut.Find("input").GetAttribute("aria-activedescendant"), Is.EqualTo(first.Id));
        });
    }

    [Test]
    public void A_list_under_a_resting_pointer_is_not_shaded_until_the_pointer_moves()
    {
        var cut = RenderLine(Location("/"));
        cut.Find(".lt-shell-address-line__edit").Click();
        cut.Find("input").Input(">theme");

        var resting = cut.Find(".lt-shell-combobox__popup").ClassList.ToArray();
        cut.Find(".lt-shell-combobox__popup").MouseMove();
        var moved = cut.Find(".lt-shell-combobox__popup").ClassList.ToArray();

        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "Escape" });
        cut.Find(".lt-shell-address-line__edit").Click();
        cut.Find("input").Input(">theme");
        var reopened = cut.Find(".lt-shell-combobox__popup").ClassList.ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(resting, Does.Not.Contain("lt-shell-combobox__popup--pointer"));
            Assert.That(moved, Does.Contain("lt-shell-combobox__popup--pointer"));
            Assert.That(reopened, Does.Not.Contain("lt-shell-combobox__popup--pointer"), "every opening starts at rest");
            Assert.That(ActiveOption(cut), Does.Contain("system theme"), "the pointer never moves the active option");
        });
    }

    [Test]
    public void The_first_option_stays_active_as_earlier_groups_arrive()
    {
        var slow = new TaskCompletionSource<IReadOnlyList<AddressCompletion>>();
        var data = Visible(new FakeArea("data", "Data", 1) { Completions = FakeCompletionSource.Gated(slow) });
        var apps = Visible(new FakeArea("apps", "Apps", 2) { Completions = FakeCompletionSource.Answering(Completion("a/crm", "apps")) });
        var cut = RenderLine(Location("/", data, apps));
        cut.Find(".lt-shell-address-line__edit").Click();
        cut.Find("input").Input("cr");
        cut.WaitUntil(() => Assert.That(ActiveOption(cut), Does.Contain("a/crm")));

        cut.InvokeAsync(() => slow.SetResult([Completion("a/crm/orders", "data")]));

        cut.WaitUntil(() => Assert.That(ActiveOption(cut), Does.Contain("a/crm/orders"), "the list's first option, not the first to arrive"));
    }

    [Test]
    public void An_option_the_reader_chose_stays_active_as_groups_arrive()
    {
        var slow = new TaskCompletionSource<IReadOnlyList<AddressCompletion>>();
        var data = Visible(new FakeArea("data", "Data", 1) { Completions = FakeCompletionSource.Gated(slow) });
        var apps = Visible(new FakeArea("apps", "Apps", 2) { Completions = FakeCompletionSource.Answering(Completion("a/crm", "apps"), Completion("a/crm-archive", "apps")) });
        var cut = RenderLine(Location("/", data, apps));
        cut.Find(".lt-shell-address-line__edit").Click();
        cut.Find("input").Input("cr");
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role='option']"), Has.Count.EqualTo(2)));
        cut.Find("input").KeyDown(new KeyboardEventArgs { Key = "ArrowDown" });

        cut.InvokeAsync(() => slow.SetResult([Completion("a/crm/orders", "data")]));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("[role='option']"), Has.Count.EqualTo(3));
            Assert.That(ActiveOption(cut), Does.Contain("a/crm-archive"));
        });
    }
}
