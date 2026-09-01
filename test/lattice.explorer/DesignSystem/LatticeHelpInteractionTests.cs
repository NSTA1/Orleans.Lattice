using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.DesignSystem.Components;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Interaction tests for the help primitive: pressing the trigger discloses the
/// explanation, pressing it again hides it, and Escape closes it.
/// </summary>
/// <remarks>
/// These dispatch real DOM events at the rendered component rather than
/// asserting on static markup, because the point of the primitive is that a
/// pointer and a keyboard both reach the same text - which static markup cannot
/// show. Every interaction is driven explicitly by the test, so nothing here
/// depends on a timer, a clock, or an ordering.
/// </remarks>
[TestFixture]
public sealed class LatticeHelpInteractionTests
{
    private static Task<DesignSystemInteractiveHarness> RenderAsync(
        LatticeHelpTone tone = LatticeHelpTone.Informational,
        EventCallback<bool> onOpenChanged = default) =>
        DesignSystemInteractiveHarness.RenderAsync<LatticeHelp>(new Dictionary<string, object?>
        {
            ["Id"] = "shard",
            ["Term"] = "shard",
            ["Tone"] = tone,
            ["Explanation"] = "A shard is one self-balancing sub-tree of the keyspace.",
            ["Remedy"] = tone == LatticeHelpTone.Denial ? "Ask an operator." : null,
            ["IsOpenChanged"] = onOpenChanged,
        });

    private static bool IsTrigger(DesignSystemInteractiveHarness.RenderedElement element) =>
        element.HasClass("lx-help-trigger");

    private static bool IsPanel(DesignSystemInteractiveHarness.RenderedElement element) =>
        element.HasClass("lx-help-panel");

    [Test]
    public async Task Pressing_the_trigger_discloses_the_explanation()
    {
        await using var harness = await RenderAsync();

        await harness.ClickAsync(IsTrigger);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Element(IsTrigger).Attribute("aria-expanded"), Is.EqualTo("true"));
            Assert.That(harness.Element(IsPanel).Attribute("hidden"), Is.Null);
        });
    }

    [Test]
    public async Task Pressing_the_trigger_twice_closes_the_disclosure_again()
    {
        await using var harness = await RenderAsync();

        await harness.ClickAsync(IsTrigger);
        await harness.ClickAsync(IsTrigger);

        Assert.That(harness.Element(IsTrigger).Attribute("aria-expanded"), Is.EqualTo("false"));
    }

    [Test]
    public async Task Escape_closes_an_open_disclosure()
    {
        await using var harness = await RenderAsync();
        await harness.ClickAsync(IsTrigger);

        await harness.KeyDownAsync(IsPanel, "Escape");

        Assert.That(harness.Element(IsTrigger).Attribute("aria-expanded"), Is.EqualTo("false"));
    }

    [Test]
    public async Task Escape_on_the_trigger_closes_an_open_disclosure()
    {
        await using var harness = await RenderAsync();
        await harness.ClickAsync(IsTrigger);

        await harness.KeyDownAsync(IsTrigger, "Escape");

        Assert.That(harness.Element(IsTrigger).Attribute("aria-expanded"), Is.EqualTo("false"));
    }

    [Test]
    public async Task Escape_on_a_closed_disclosure_changes_nothing()
    {
        var raised = 0;
        await using var harness = await RenderAsync(
            onOpenChanged: EventCallback.Factory.Create<bool>(new object(), _ => raised++));

        await harness.KeyDownAsync(IsTrigger, "Escape");

        Assert.Multiple(() =>
        {
            Assert.That(raised, Is.Zero);
            Assert.That(harness.Element(IsTrigger).Attribute("aria-expanded"), Is.EqualTo("false"));
        });
    }

    [Test]
    public async Task Another_key_leaves_the_disclosure_alone()
    {
        await using var harness = await RenderAsync();
        await harness.ClickAsync(IsTrigger);

        await harness.KeyDownAsync(IsPanel, "ArrowDown");

        Assert.That(harness.Element(IsTrigger).Attribute("aria-expanded"), Is.EqualTo("true"));
    }

    [Test]
    public async Task Toggling_tells_a_bound_host_which_way_it_went()
    {
        var observed = new List<bool>();
        await using var harness = await RenderAsync(
            onOpenChanged: EventCallback.Factory.Create<bool>(new object(), observed.Add));

        await harness.ClickAsync(IsTrigger);
        await harness.ClickAsync(IsTrigger);

        Assert.That(observed, Is.EqualTo(new[] { true, false }));
    }

    [Test]
    public async Task A_denial_discloses_its_remedy_as_well_as_its_refusal()
    {
        await using var harness = await RenderAsync(LatticeHelpTone.Denial);

        await harness.ClickAsync(IsTrigger);

        var remedy = harness.Element(element => element.HasClass("lx-help-remedy"));

        Assert.That(remedy.Text, Does.Contain("Ask an operator."));
    }
}
