using Bunit;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// The design system's button: a native button, so Enter, Space and focus are
/// the platform's; three weights; a toggle state announced through
/// <c>aria-pressed</c>; and a disabled state that raises nothing.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtButtonTests : ShellDesignTestContext
{
    [Test]
    public void It_renders_a_native_button_that_never_submits_by_default()
    {
        var button = Render<LtButton>(p => p.AddChildContent("Refresh")).Find("button");

        Assert.Multiple(() =>
        {
            Assert.That(button.GetAttribute("type"), Is.EqualTo("button"));
            Assert.That(button.ClassName, Is.EqualTo("lt-btn"));
            Assert.That(button.TextContent, Is.EqualTo("Refresh"));
            Assert.That(button.HasAttribute("aria-pressed"), Is.False, "an ordinary button is not a toggle");
            Assert.That(button.HasAttribute("disabled"), Is.False);
        });
    }

    [Test]
    [TestCase(LtButtonVariant.Outlined, "lt-btn")]
    [TestCase(LtButtonVariant.Quiet, "lt-btn lt-btn--quiet")]
    [TestCase(LtButtonVariant.Destructive, "lt-btn lt-btn--destructive")]
    public void Each_variant_has_its_own_class(LtButtonVariant variant, string expected)
    {
        var button = Render<LtButton>(p => p.Add(x => x.Variant, variant).AddChildContent("Go")).Find("button");

        Assert.That(button.ClassName, Is.EqualTo(expected));
    }

    [Test]
    public void A_submit_button_submits_its_form()
    {
        var button = Render<LtButton>(p => p.Add(x => x.Type, LtButtonType.Submit).AddChildContent("Save")).Find("button");

        Assert.That(button.GetAttribute("type"), Is.EqualTo("submit"));
    }

    [Test]
    public void Activating_it_raises_OnClick()
    {
        var clicks = 0;
        var cut = Render<LtButton>(p => p.Add(x => x.OnClick, (MouseEventArgs _) => clicks++).AddChildContent("Go"));

        cut.Find("button").Click();

        Assert.That(clicks, Is.EqualTo(1));
    }

    [Test]
    public void A_disabled_button_is_disabled_and_raises_nothing()
    {
        var clicks = 0;
        var cut = Render<LtButton>(p => p
            .Add(x => x.Disabled, true)
            .Add(x => x.OnClick, (MouseEventArgs _) => clicks++)
            .AddChildContent("Go"));

        cut.Find("button").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("button").HasAttribute("disabled"), Is.True);
            Assert.That(clicks, Is.Zero);
        });
    }

    [Test]
    [TestCase(true, "true")]
    [TestCase(false, "false")]
    public void A_toggle_announces_its_pressed_state(bool pressed, string expected)
    {
        var button = Render<LtButton>(p => p.Add(x => x.Pressed, pressed).AddChildContent("Compact")).Find("button");

        Assert.That(button.GetAttribute("aria-pressed"), Is.EqualTo(expected));
    }

    [Test]
    public void Further_attributes_pass_through_to_the_button()
    {
        var button = Render<LtButton>(p => p
            .AddUnmatched("aria-label", "Refresh the tree list")
            .AddUnmatched("aria-describedby", "hint")
            .AddChildContent("Refresh")).Find("button");

        Assert.Multiple(() =>
        {
            Assert.That(button.GetAttribute("aria-label"), Is.EqualTo("Refresh the tree list"));
            Assert.That(button.GetAttribute("aria-describedby"), Is.EqualTo("hint"));
        });
    }
}
