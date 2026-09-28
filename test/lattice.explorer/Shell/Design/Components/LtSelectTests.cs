using Bunit;
using Orleans.Lattice.Explorer.Shell.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.Shell.Design.Components;

/// <summary>The select: a native, labelled select whose options, selection and hint are the platform's.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtSelectTests : ShellDesignTestContext
{
    private static readonly LtSelectOption[] Sources =
    [
        new("all", "All sources"),
        new("in-image", "In-image"),
        new("feed", "NuGet feed") { Disabled = true },
    ];

    [Test]
    public void It_renders_a_labelled_native_select_with_its_options()
    {
        var cut = Render<LtSelect>(p => p.Add(x => x.Label, "Source").Add(x => x.Options, Sources).Add(x => x.Value, "in-image"));
        var select = cut.Find("select");
        var options = cut.FindAll("option");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("label").GetAttribute("for"), Is.EqualTo(select.Id));
            Assert.That(options.Select(option => option.TextContent), Is.EqualTo(new[] { "All sources", "In-image", "NuGet feed" }));
            Assert.That(options.Select(option => option.GetAttribute("value")), Is.EqualTo(new[] { "all", "in-image", "feed" }));
            Assert.That(options[1].HasAttribute("selected"), Is.True);
            Assert.That(options[0].HasAttribute("selected"), Is.False);
            Assert.That(options[2].HasAttribute("disabled"), Is.True);
        });
    }

    [Test]
    public void Choosing_an_option_raises_ValueChanged()
    {
        string? observed = null;
        var cut = Render<LtSelect>(p => p
            .Add(x => x.Label, "Source")
            .Add(x => x.Options, Sources)
            .Add(x => x.ValueChanged, (string value) => observed = value));

        cut.Find("select").Change("in-image");

        Assert.That(observed, Is.EqualTo("in-image"));
    }

    [Test]
    public void A_hint_is_announced_with_the_select()
    {
        var cut = Render<LtSelect>(p => p.Add(x => x.Label, "Source").Add(x => x.Options, Sources).Add(x => x.Hint, "Where apps come from."));

        Assert.That(cut.Find("select").GetAttribute("aria-describedby"), Is.EqualTo(cut.Find(".lt-field__hint").Id));
    }

    [Test]
    public void A_disabled_select_is_disabled()
    {
        var cut = Render<LtSelect>(p => p.Add(x => x.Label, "Source").Add(x => x.Options, Sources).Add(x => x.Disabled, true));

        Assert.That(cut.Find("select").HasAttribute("disabled"), Is.True);
    }
}
