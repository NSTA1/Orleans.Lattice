using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.DesignSystem.Components;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Component tests for the help primitive that replaces the Explorer's bare
/// <c>title</c> attributes.
/// </summary>
/// <remarks>
/// The defect it closes: seventeen explanations on the home surface alone were
/// carried by <c>title</c>, which a touch caller never sees, a keyboard caller
/// cannot reach, and screen readers announce inconsistently. What is asserted
/// here is therefore reachability and association, not appearance.
/// </remarks>
[TestFixture]
public sealed class LatticeHelpTests
{
    private static Task<string> RenderAsync(IDictionary<string, object?> parameters) =>
        DesignSystemRenderHarness.RenderAsync<LatticeHelp>(parameters);

    private static Task<string> RenderTermAsync(bool isOpen = false) =>
        RenderAsync(new Dictionary<string, object?>
        {
            ["Id"] = "shard",
            ["Term"] = "shard",
            ["Explanation"] = "A shard is one self-balancing sub-tree of the keyspace.",
            ["IsOpen"] = isOpen,
        });

    private static Task<string> RenderDenialAsync(bool isOpen = false) =>
        RenderAsync(new Dictionary<string, object?>
        {
            ["Id"] = "backups",
            ["Term"] = "Backups",
            ["Tone"] = LatticeHelpTone.Denial,
            ["Explanation"] = "Your account cannot read backup catalogues in this cluster.",
            ["Remedy"] = "Ask an operator for the backups.read permission.",
            ["IsOpen"] = isOpen,
        });

    // ------------------------------------------------------------ reachability

    [Test]
    public async Task Render_theTrigger_isAFocusableButtonRatherThanAHoverTarget()
    {
        var html = await RenderTermAsync();

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("<button"));
            Assert.That(html, Does.Contain("type=\"button\""));
            Assert.That(html, Does.Contain("lx-help-trigger"));
            Assert.That(html, Does.Not.Contain("title="),
                "the primitive exists precisely so an explanation is not a title attribute");
        });
    }

    [Test]
    public async Task Render_whenClosed_reportsTheDisclosureCollapsed()
    {
        var html = await RenderTermAsync();

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("aria-expanded=\"false\""));

            // Matched against the panel's own attribute run, because the glyph
            // carries aria-hidden and a bare `hidden` substring would match it.
            Assert.That(html, Does.Contain("role=\"note\" hidden"));
        });
    }

    [Test]
    public async Task Render_whenOpen_reportsTheDisclosureExpandedAndShowsThePanel()
    {
        var html = await RenderTermAsync(isOpen: true);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("aria-expanded=\"true\""));
            Assert.That(html, Does.Not.Contain("role=\"note\" hidden"));
            Assert.That(html, Does.Contain("A shard is one self-balancing sub-tree of the keyspace."));
        });
    }

    [Test]
    public async Task Render_keepsTheExplanationInTheDomEvenWhileCollapsed()
    {
        var html = await RenderTermAsync();

        Assert.Multiple(() =>
        {
            // An aria-describedby target contributes its text even when hidden,
            // so a control described by this help is described in both states.
            Assert.That(html, Does.Contain("id=\"shard-explanation\""));
            Assert.That(html, Does.Contain("A shard is one self-balancing sub-tree of the keyspace."));
        });
    }

    [Test]
    public async Task Render_theTriggerControlsTheExplanation()
    {
        var html = await RenderTermAsync();

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("aria-controls=\"shard-explanation\""));
            Assert.That(html, Does.Contain("id=\"shard-trigger\""));
            Assert.That(html, Does.Contain("role=\"note\""));
        });
    }

    [Test]
    public void ExplanationElementId_isTheIdAControlDescribesItselfWith()
    {
        Assert.That(LatticeHelp.ExplanationElementId("shard"), Is.EqualTo("shard-explanation"));
    }

    [Test]
    public void ExplanationElementId_withANullId_throws()
    {
        Assert.That(() => LatticeHelp.ExplanationElementId(null!), Throws.ArgumentNullException);
    }

    // -------------------------------------------------------- accessible names

    [Test]
    public async Task Render_namesTheTriggerAfterTheTermItExplains()
    {
        var html = await RenderTermAsync();

        Assert.That(html, Does.Contain("aria-label=\"Explain shard\""),
            "a row of bare question marks is unusable by a screen-reader caller");
    }

    [Test]
    public async Task Render_aDenial_namesTheTriggerAsARefusal()
    {
        var html = await RenderDenialAsync();

        Assert.That(html, Does.Contain("aria-label=\"Why Backups is unavailable\""));
    }

    [Test]
    public async Task Render_withNoTerm_stillNamesTheTrigger()
    {
        var html = await RenderAsync(new Dictionary<string, object?>
        {
            ["Explanation"] = "Something worth explaining.",
        });

        Assert.That(html, Does.Contain("aria-label=\"Explain this\""));
    }

    [Test]
    public async Task Render_aDenialWithNoTerm_stillNamesTheTrigger()
    {
        var html = await RenderAsync(new Dictionary<string, object?>
        {
            ["Tone"] = LatticeHelpTone.Denial,
            ["Explanation"] = "Refused.",
        });

        Assert.That(html, Does.Contain("aria-label=\"Why this is unavailable\""));
    }

    [Test]
    public async Task Render_anExplicitTriggerLabelWinsOverTheComposedOne()
    {
        var html = await RenderAsync(new Dictionary<string, object?>
        {
            ["Term"] = "shard",
            ["TriggerLabel"] = "About sharding",
            ["Explanation"] = "...",
        });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("aria-label=\"About sharding\""));
            Assert.That(html, Does.Not.Contain("Explain shard"));
        });
    }

    [Test]
    public async Task Render_withTriggerText_namesTheTriggerByItsVisibleTextAlone()
    {
        // WCAG 2.5.3 Label in Name: the accessible name must contain the visible
        // text, so a trigger that shows a phrase must not also announce a
        // different composed one. The rail's capabilities disclosure showed
        // "Why can I not see everything?" while announcing "Explain missing
        // areas", which axe reports as a serious label-content-name-mismatch and
        // which leaves a speech-input user unable to activate it by saying what
        // they can see. Emitting no aria-label lets the visible text be the name.
        var html = await RenderAsync(new Dictionary<string, object?>
        {
            ["Term"] = "missing areas",
            ["TriggerText"] = "Why can I not see everything?",
            ["Explanation"] = "...",
        });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Not.Contain("aria-label"),
                "a trigger with visible text must not carry a competing aria-label");
            Assert.That(html, Does.Contain("Why can I not see everything?"));
        });
    }

    [Test]
    public async Task Render_withTriggerText_ignoresAnExplicitTriggerLabelThatWouldMaskIt()
    {
        // An explicit TriggerLabel cannot rescue the mismatch either: whatever it
        // says, it is not the visible text, so it would reintroduce the very
        // violation. Visible text wins.
        var html = await RenderAsync(new Dictionary<string, object?>
        {
            ["Term"] = "residency",
            ["TriggerText"] = "What is this?",
            ["TriggerLabel"] = "About residency",
            ["Explanation"] = "...",
        });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Not.Contain("aria-label"));
            Assert.That(html, Does.Not.Contain("About residency"));
            Assert.That(html, Does.Contain("What is this?"));
        });
    }

    [Test]
    public async Task Render_withTriggerText_showsThePhraseBesideTheGlyph()
    {
        var html = await RenderAsync(new Dictionary<string, object?>
        {
            ["Term"] = "residency",
            ["TriggerText"] = "What is this?",
            ["Explanation"] = "...",
        });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-help-trigger-text"));
            Assert.That(html, Does.Contain("What is this?"));
        });
    }

    [Test]
    public async Task Render_withoutTriggerText_rendersTheGlyphAlone()
    {
        var html = await RenderTermAsync();

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-help-glyph"));
            Assert.That(html, Does.Not.Contain("lx-help-trigger-text"));
        });
    }

    // -------------------------------------------------------- denial and remedy

    [Test]
    public async Task Render_aDenial_carriesTheDenialTone()
    {
        var html = await RenderDenialAsync();

        Assert.That(html, Does.Contain("lx-help is-denial"));
    }

    [Test]
    public async Task Render_anInformationalExplanation_carriesNoDenialTone()
    {
        var html = await RenderTermAsync();

        Assert.That(html, Does.Not.Contain("is-denial"));
    }

    [Test]
    public async Task Render_aDenial_statesItsRemedyAndNotOnlyItsRefusal()
    {
        var html = await RenderDenialAsync(isOpen: true);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Your account cannot read backup catalogues in this cluster."));
            Assert.That(html, Does.Contain("What to do:"));
            Assert.That(html, Does.Contain("Ask an operator for the backups.read permission."));
            Assert.That(html, Does.Contain("lx-help-remedy"));
        });
    }

    [Test]
    public async Task Render_aCallerSuppliedRemedyLabel_replacesTheDefault()
    {
        var html = await RenderAsync(new Dictionary<string, object?>
        {
            ["Tone"] = LatticeHelpTone.Denial,
            ["Explanation"] = "Refused.",
            ["Remedy"] = "Sign in.",
            ["RemedyLabel"] = "Next step:",
            ["IsOpen"] = true,
        });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Next step:"));
            Assert.That(html, Does.Not.Contain("What to do:"));
        });
    }

    [Test]
    public async Task Render_withNoRemedy_rendersNoRemedyBlock()
    {
        var html = await RenderTermAsync(isOpen: true);

        Assert.That(html, Does.Not.Contain("lx-help-remedy"));
    }

    // ------------------------------------------------------------- composition

    [Test]
    public async Task Render_withChildContent_placesItInThePanel()
    {
        var html = await RenderAsync(new Dictionary<string, object?>
        {
            ["Term"] = "residency",
            ["Explanation"] = "Where the data for a tenant may live.",
            ["IsOpen"] = true,
            ["ChildContent"] = (RenderFragment)(builder =>
                builder.AddMarkupContent(0, "<a href=\"/docs\">Read more</a>")),
        });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Read more"));
            Assert.That(html, Does.Contain("Where the data for a tenant may live."));
        });
    }

    [Test]
    public async Task Render_withChildContentAlone_stillRenders()
    {
        var html = await RenderAsync(new Dictionary<string, object?>
        {
            ["ChildContent"] = (RenderFragment)(builder =>
                builder.AddMarkupContent(0, "<a href=\"/docs\">Read more</a>")),
        });

        Assert.That(html, Does.Contain("lx-help-trigger"));
    }

    [Test]
    public async Task Render_withNothingToExplain_rendersNothing()
    {
        var html = await RenderAsync(new Dictionary<string, object?> { ["Term"] = "shard" });

        Assert.That(html.Trim(), Is.Empty,
            "a caller may bind the explanation conditionally, so an empty one is a no-op "
            + "rather than an empty affordance the caller can press");
    }

    [Test]
    public async Task Render_appendsTheCallersClass()
    {
        var html = await RenderAsync(new Dictionary<string, object?>
        {
            ["Explanation"] = "...",
            ["Class"] = "lx-shell-help",
        });

        Assert.That(html, Does.Contain("lx-help lx-shell-help"));
    }

    [Test]
    public async Task Render_withNoId_stillProducesMatchingTriggerAndPanelIds()
    {
        var html = await RenderAsync(new Dictionary<string, object?> { ["Explanation"] = "..." });

        var controls = html.IndexOf("aria-controls=\"lx-help-", StringComparison.Ordinal);
        var panel = html.IndexOf("id=\"lx-help-", StringComparison.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(controls, Is.GreaterThanOrEqualTo(0));
            Assert.That(panel, Is.GreaterThanOrEqualTo(0));
            Assert.That(html, Does.Contain("-explanation\""));
        });
    }
}
