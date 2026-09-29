using Bunit;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// The text input: a visible label bound to the input, a hint and an error that
/// are announced with it, an error that is marked by text as well as colour, and
/// a mono variant for keys and ids.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtTextInputTests : ShellDesignTestContext
{
    [Test]
    public void The_visible_label_is_bound_to_the_input()
    {
        var cut = Render<LtTextInput>(p => p.Add(x => x.Label, "Tree id"));
        var input = cut.Find("input");
        var label = cut.Find("label");

        Assert.Multiple(() =>
        {
            Assert.That(label.TextContent, Is.EqualTo("Tree id"));
            Assert.That(label.GetAttribute("for"), Is.EqualTo(input.Id));
            Assert.That(input.Id, Is.EqualTo(cut.Instance.InputId));
            Assert.That(input.HasAttribute("aria-describedby"), Is.False);
            Assert.That(input.HasAttribute("aria-invalid"), Is.False);
        });
    }

    [Test]
    public void Editing_raises_ValueChanged_with_the_new_value()
    {
        string? observed = null;
        var cut = Render<LtTextInput>(p => p
            .Add(x => x.Label, "Tree id")
            .Add(x => x.ValueChanged, (string value) => observed = value));

        cut.Find("input").Input("crm/orders");

        Assert.That(observed, Is.EqualTo("crm/orders"));
    }

    [Test]
    public void A_hint_is_announced_with_the_input()
    {
        var cut = Render<LtTextInput>(p => p.Add(x => x.Label, "Prefix").Add(x => x.Hint, "Keys that start with this."));
        var hint = cut.Find(".lt-field__hint");

        Assert.That(cut.Find("input").GetAttribute("aria-describedby"), Is.EqualTo(hint.Id));
    }

    [Test]
    public void An_error_marks_the_input_invalid_and_is_announced_with_the_hint()
    {
        var cut = Render<LtTextInput>(p => p
            .Add(x => x.Label, "Tree id")
            .Add(x => x.Hint, "Lower case.")
            .Add(x => x.Error, "Tree ids are lower case."));
        var input = cut.Find("input");
        var error = cut.Find(".lt-field__error");

        Assert.Multiple(() =>
        {
            Assert.That(input.GetAttribute("aria-invalid"), Is.EqualTo("true"));
            Assert.That(input.GetAttribute("aria-describedby"), Is.EqualTo(cut.Find(".lt-field__hint").Id + " " + error.Id));
            Assert.That(error.TextContent, Does.Contain("Tree ids are lower case."));
            Assert.That(cut.Find(".lt-field__error-mark").GetAttribute("aria-hidden"), Is.EqualTo("true"),
                "the error's text mark is visual; the message itself is what is announced");
        });
    }

    [Test]
    public void The_mono_variant_is_set_in_cascadia_and_not_spell_checked()
    {
        var input = Render<LtTextInput>(p => p.Add(x => x.Label, "Key").Add(x => x.Mono, true)).Find("input");

        Assert.Multiple(() =>
        {
            Assert.That(input.ClassList, Does.Contain("lt-input--mono"));
            Assert.That(input.GetAttribute("spellcheck"), Is.EqualTo("false"));
        });
    }

    [Test]
    public void Disabled_and_read_only_reach_the_input()
    {
        var input = Render<LtTextInput>(p => p.Add(x => x.Label, "Key").Add(x => x.Disabled, true).Add(x => x.ReadOnly, true)).Find("input");

        Assert.Multiple(() =>
        {
            Assert.That(input.HasAttribute("disabled"), Is.True);
            Assert.That(input.HasAttribute("readonly"), Is.True);
        });
    }

    [Test]
    public async Task FocusAsync_moves_focus_to_the_input()
    {
        var cut = Render<LtTextInput>(p => p.Add(x => x.Label, "Key"));

        await cut.InvokeAsync(() => cut.Instance.FocusAsync().AsTask());

        var invocation = JSInterop.VerifyFocusAsyncInvoke();
        Assert.That(
            ((ElementReference)invocation.Arguments[0]!).Id,
            Is.EqualTo(cut.Find("input").GetAttribute("blazor:elementreference")));
    }
}
