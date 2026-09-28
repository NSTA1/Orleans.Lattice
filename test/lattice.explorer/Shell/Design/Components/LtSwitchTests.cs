using Bunit;
using Orleans.Lattice.Explorer.Shell.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.Shell.Design.Components;

/// <summary>
/// The switch: a button with the <c>switch</c> role, named by its label, whose
/// state is <c>aria-checked</c> and is also written as text beside the track.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtSwitchTests : ShellDesignTestContext
{
    [Test]
    [TestCase(false, "false", "Off")]
    [TestCase(true, "true", "On")]
    public void It_is_a_switch_whose_state_is_announced_and_written(bool on, string ariaChecked, string text)
    {
        var cut = Render<LtSwitch>(p => p.Add(x => x.Label, "Enabled").Add(x => x.Checked, on));
        var button = cut.Find("button");

        Assert.Multiple(() =>
        {
            Assert.That(button.GetAttribute("role"), Is.EqualTo("switch"));
            Assert.That(button.GetAttribute("type"), Is.EqualTo("button"));
            Assert.That(button.GetAttribute("aria-checked"), Is.EqualTo(ariaChecked));
            Assert.That(cut.Find(".lt-switch__state").TextContent, Is.EqualTo(text));
            Assert.That(cut.Find(".lt-switch__state").GetAttribute("aria-hidden"), Is.EqualTo("true"),
                "the state text is visual; aria-checked is what announces it");
        });
    }

    [Test]
    public void Its_accessible_name_is_its_label_alone()
    {
        var cut = Render<LtSwitch>(p => p.Add(x => x.Label, "Publish events"));

        // Everything else inside the button is aria-hidden, so the label is the
        // whole of the name computed from the button's content.
        var visible = cut.Find("button").Children.Where(child => child.GetAttribute("aria-hidden") != "true").ToArray();

        Assert.That(visible.Select(child => child.TextContent), Is.EqualTo(new[] { "Publish events" }));
    }

    [Test]
    public void Pressing_it_toggles_and_reports_the_new_state()
    {
        var observed = new List<bool>();
        var cut = Render<LtSwitch>(p => p.Add(x => x.Label, "Enabled").Add(x => x.CheckedChanged, (bool value) => observed.Add(value)));

        cut.Find("button").Click();
        cut.Find("button").Click();

        Assert.Multiple(() =>
        {
            Assert.That(observed, Is.EqualTo(new[] { true, false }));
            Assert.That(cut.Find("button").GetAttribute("aria-checked"), Is.EqualTo("false"));
        });
    }

    [Test]
    public void A_disabled_switch_does_not_toggle()
    {
        var observed = new List<bool>();
        var cut = Render<LtSwitch>(p => p
            .Add(x => x.Label, "Enabled")
            .Add(x => x.Disabled, true)
            .Add(x => x.CheckedChanged, (bool value) => observed.Add(value)));

        cut.Find("button").Click();

        Assert.Multiple(() =>
        {
            Assert.That(observed, Is.Empty);
            Assert.That(cut.Find("button").HasAttribute("disabled"), Is.True);
            Assert.That(cut.Find("button").GetAttribute("aria-checked"), Is.EqualTo("false"));
        });
    }

    [Test]
    public void Custom_state_text_and_a_hint_are_rendered()
    {
        var cut = Render<LtSwitch>(p => p
            .Add(x => x.Label, "Replication")
            .Add(x => x.Checked, true)
            .Add(x => x.OnText, "Enrolled")
            .Add(x => x.Hint, "Ships this tree's writes to every peer."));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-switch__state").TextContent, Is.EqualTo("Enrolled"));
            Assert.That(cut.Find("button").GetAttribute("aria-describedby"), Is.EqualTo(cut.Find(".lt-field__hint").Id));
        });
    }
}
