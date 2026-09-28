using Bunit;
using Orleans.Lattice.Explorer.Shell.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.Shell.Design.Components;

/// <summary>The checkbox: a native checkbox with a bound label, a hint, and a change that reports its new state.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtCheckboxTests : ShellDesignTestContext
{
    [Test]
    public void It_renders_a_native_checkbox_bound_to_its_label()
    {
        var cut = Render<LtCheckbox>(p => p.Add(x => x.Label, "Include tombstones").Add(x => x.Checked, true));
        var box = cut.Find("input");

        Assert.Multiple(() =>
        {
            Assert.That(box.GetAttribute("type"), Is.EqualTo("checkbox"));
            Assert.That(box.HasAttribute("checked"), Is.True);
            Assert.That(cut.Find("label").GetAttribute("for"), Is.EqualTo(box.Id));
            Assert.That(cut.Find("label").TextContent, Is.EqualTo("Include tombstones"));
        });
    }

    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public void Toggling_reports_the_new_state(bool next)
    {
        bool? observed = null;
        var cut = Render<LtCheckbox>(p => p
            .Add(x => x.Label, "Include tombstones")
            .Add(x => x.Checked, !next)
            .Add(x => x.CheckedChanged, (bool value) => observed = value));

        cut.Find("input").Change(next);

        Assert.That(observed, Is.EqualTo(next));
    }

    [Test]
    public void A_hint_is_announced_with_the_box()
    {
        var cut = Render<LtCheckbox>(p => p.Add(x => x.Label, "Deep").Add(x => x.Hint, "Slower; counts tombstones."));

        Assert.That(cut.Find("input").GetAttribute("aria-describedby"), Is.EqualTo(cut.Find(".lt-field__hint").Id));
    }

    [Test]
    public void A_disabled_box_is_disabled()
    {
        var cut = Render<LtCheckbox>(p => p.Add(x => x.Label, "Deep").Add(x => x.Disabled, true));

        Assert.That(cut.Find("input").HasAttribute("disabled"), Is.True);
    }
}
