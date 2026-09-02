namespace Orleans.Lattice.Explorer.Tests;

/// <summary>
/// The design system's non-text contrast gate (issue #1846): every border, focus
/// ring and state indicator clears WCAG 2.1 SC 1.4.11's 3:1 against every
/// surface it can be drawn on, every palette keeps a real elevation ladder, and
/// a selected state is never carried by hue alone.
/// </summary>
/// <remarks>
/// <para>
/// SC 1.4.11 is a WCAG 2.1 criterion, and the Explorer's browser sweep runs axe
/// with WCAG 2.0 A/AA tags only - so non-text contrast was out of that sweep's
/// scope by construction, and stayed unmeasured until it was audited by hand.
/// What the audit found: <c>--lx-color-border</c> at 1.21:1 on the dark surface
/// and 1.28:1 on the light one, <c>--lx-color-border-strong</c> at 1.50:1 and
/// 1.62:1, an active tab separated from an inactive one by 1.54:1 of hue, and a
/// light palette whose three elevation levels were all <c>#ffffff</c>.
/// </para>
/// <para>
/// This fixture needs no browser, so unlike the sweep it runs in the required
/// build-and-test check on every pull request.
/// </para>
/// <para>
/// Two of its rules are worth explaining, because both encode a constraint that
/// is easy to mistake for a preference.
/// </para>
/// <list type="bullet">
/// <item><b>Greyscale.</b> A WCAG contrast ratio is a pure function of relative
/// luminance, and relative luminance is what a colour collapses to in
/// greyscale. Measuring the selected and rest foregrounds at 3:1 is therefore
/// not a proxy for "distinguishable without colour" - it is exactly that test.
/// <see cref="An_accent_can_never_be_the_thing_that_carries_state"/> proves the
/// corollary the token layer had to be designed around.</item>
/// <item><b>Elevation.</b> The step between adjacent surfaces is measured as a
/// contrast ratio rather than a luminance difference, because a difference is
/// not comparable between palettes: the whole dark surface family lives inside
/// the bottom hundredth of the luminance range, so any absolute threshold that
/// suits the light palette would reject every legal dark one.</item>
/// </list>
/// </remarks>
[TestFixture]
public sealed class NonTextContrastTokenHygieneTests
{
    /// <summary>
    /// Every token drawn as a boundary or an indicator, and therefore held to
    /// SC 1.4.11 rather than to the text minimum.
    /// </summary>
    private static readonly string[] NonTextTokens =
    [
        "--lx-color-border",
        "--lx-color-border-strong",
        "--lx-color-focus-ring",
        "--lx-color-state-selected-indicator",
        "--lx-color-danger-border",
    ];

    /// <summary>
    /// The colours a focus ring can be drawn adjacent to: every surface, plus
    /// the accent fills, because a focused primary button rings its own fill.
    /// </summary>
    private static readonly string[] FocusRingAdjacents =
    [
        .. DesignTokens.BackgroundTokens,
        "--lx-color-accent",
        "--lx-color-accent-hover",
    ];

    /// <summary>
    /// The minimum contrast between adjacent elevation levels. Small, because it
    /// is a floor against collapse rather than a design target: the light
    /// palette's three levels measured exactly 1.00:1 apart before #1846.
    /// </summary>
    private const double MinimumElevationStep = 1.03;

    [Test]
    [TestCase(DesignTokenPalettes.Dark)]
    [TestCase(DesignTokenPalettes.Light)]
    public void Every_non_text_token_clears_the_non_text_minimum_on_every_surface(string paletteName)
    {
        AssertEveryNonTextTokenClears(paletteName, DesignTokens.NonTextMinimum);
    }

    [Test]
    [TestCase(DesignTokenPalettes.DarkHighContrast)]
    [TestCase(DesignTokenPalettes.LightHighContrast)]
    public void Every_high_contrast_non_text_token_clears_the_raised_minimum(string paletteName)
    {
        AssertEveryNonTextTokenClears(paletteName, DesignTokens.NonTextEnhancedMinimum);
    }

    [Test]
    [TestCase(DesignTokenPalettes.Dark)]
    [TestCase(DesignTokenPalettes.Light)]
    [TestCase(DesignTokenPalettes.DarkHighContrast)]
    [TestCase(DesignTokenPalettes.LightHighContrast)]
    public void The_stronger_border_out_contrasts_the_plain_one(string paletteName)
    {
        // Raising both borders to clear 3:1 is only half a fix if they land on
        // the same value: the palette would then have two names for one weight
        // and no way to say "this boundary matters more than that one".
        var palette = DesignTokenPalettes.Resolve(paletteName);

        var plain = DesignTokens.Worst(palette, paletteName, "--lx-color-border");
        var strong = DesignTokens.Worst(palette, paletteName, "--lx-color-border-strong");

        Assert.That(strong.Ratio, Is.GreaterThan(plain.Ratio),
            $"{paletteName}: --lx-color-border-strong ({DesignTokens.Format(strong.Ratio)}) must "
            + $"out-contrast --lx-color-border ({DesignTokens.Format(plain.Ratio)}), or the palette "
            + "has two names for one boundary weight.");
    }

    [Test]
    [TestCase(DesignTokenPalettes.Dark)]
    [TestCase(DesignTokenPalettes.Light)]
    [TestCase(DesignTokenPalettes.DarkHighContrast)]
    [TestCase(DesignTokenPalettes.LightHighContrast)]
    public void The_focus_ring_pair_is_visible_against_every_colour_it_can_be_drawn_on(string paletteName)
    {
        // A single ring cannot clear 3:1 against both a near-black surface and a
        // mid-tone accent fill - the two constraints pull in opposite
        // directions. So the ring is a pair drawn as concentric rings, and the
        // guarantee is that whichever one the underlying colour swallows, the
        // other stays visible.
        var palette = DesignTokenPalettes.Resolve(paletteName);
        var minimum = paletteName.Contains("high contrast", StringComparison.Ordinal)
            ? DesignTokens.NonTextEnhancedMinimum
            : DesignTokens.NonTextMinimum;

        var ring = DesignTokens.Colour(palette, paletteName, "--lx-color-focus-ring");
        var companion = DesignTokens.Colour(palette, paletteName, "--lx-color-focus-ring-contrast");

        Assert.That(DesignTokens.ContrastRatio(ring, companion),
            Is.GreaterThanOrEqualTo(DesignTokens.NonTextMinimum),
            $"{paletteName}: the two rings must be distinguishable from each other, or the pair is "
            + "one ring drawn twice.");

        var failures = new List<string>();
        foreach (var adjacent in FocusRingAdjacents)
        {
            var against = DesignTokens.Colour(palette, paletteName, adjacent);
            var best = Math.Max(
                DesignTokens.ContrastRatio(ring, against),
                DesignTokens.ContrastRatio(companion, against));

            if (best < minimum)
            {
                failures.Add(
                    $"neither ring reaches {DesignTokens.Format(minimum)} on {adjacent} ({against}); "
                    + $"the better of the two is {DesignTokens.Format(best)}");
            }
        }

        Assert.That(failures, Is.Empty,
            $"{paletteName}: the focus indicator must be perceivable wherever it is drawn."
            + Environment.NewLine
            + string.Join(Environment.NewLine, failures));
    }

    [Test]
    [TestCase(DesignTokenPalettes.Dark)]
    [TestCase(DesignTokenPalettes.Light)]
    [TestCase(DesignTokenPalettes.DarkHighContrast)]
    [TestCase(DesignTokenPalettes.LightHighContrast)]
    public void Every_palette_keeps_three_ordered_and_separated_elevation_levels(string paletteName)
    {
        // The light palette declared surface, surface-raised and surface-sunken
        // all as #ffffff, so a raised menu, a plain panel and a sunken well were
        // one colour and elevation was carried entirely by a border measuring
        // 1.28:1. Ordering alone would not catch that; the step is what does.
        var palette = DesignTokenPalettes.Resolve(paletteName);

        var levels = DesignTokens.ElevationTokens
            .Select(token => (Token: token, Colour: DesignTokens.Colour(palette, paletteName, token)))
            .ToArray();

        Assert.Multiple(() =>
        {
            for (var i = 1; i < levels.Length; i++)
            {
                var lower = DesignTokens.RelativeLuminance(levels[i - 1].Colour);
                var upper = DesignTokens.RelativeLuminance(levels[i].Colour);

                Assert.That(upper, Is.GreaterThan(lower),
                    $"{paletteName}: {levels[i].Token} ({levels[i].Colour}) must be lighter than "
                    + $"{levels[i - 1].Token} ({levels[i - 1].Colour}). Both palettes order elevation the "
                    + "same way, so a primitive can reason about depth without knowing the theme.");

                var step = DesignTokens.ContrastRatio(levels[i - 1].Colour, levels[i].Colour);
                Assert.That(step, Is.GreaterThanOrEqualTo(MinimumElevationStep),
                    $"{paletteName}: {levels[i - 1].Token} and {levels[i].Token} are only "
                    + $"{DesignTokens.Format(step)} apart. Adjacent elevation levels must differ by a "
                    + "step a reader can actually see.");
            }
        });
    }

    [Test]
    [TestCase(DesignTokenPalettes.Dark)]
    [TestCase(DesignTokenPalettes.Light)]
    public void The_selected_and_rest_foregrounds_survive_a_greyscale_rendering(string paletteName)
    {
        // WCAG 2.1 SC 1.4.1 asks that colour never be the only way a state is
        // conveyed. Because a contrast ratio is a pure function of relative
        // luminance - which is exactly the greyscale value - holding this pair
        // to 3:1 is the greyscale test rather than a proxy for it.
        var palette = DesignTokenPalettes.Resolve(paletteName);

        var selected = DesignTokens.Colour(palette, paletteName, "--lx-color-state-selected-fg");
        var rest = DesignTokens.Colour(palette, paletteName, "--lx-color-state-rest-fg");
        var ratio = DesignTokens.ContrastRatio(selected, rest);

        Assert.That(ratio, Is.GreaterThanOrEqualTo(DesignTokens.NonTextMinimum),
            $"{paletteName}: the selected foreground ({selected}) and the rest foreground ({rest}) are "
            + $"only {DesignTokens.Format(ratio)} apart. An active tab was separated from an inactive one "
            + "by 1.54:1 of hue before issue #1846, which a greyscale reader cannot see at all.");
    }

    [Test]
    [TestCase(DesignTokenPalettes.Dark)]
    [TestCase(DesignTokenPalettes.Light)]
    [TestCase(DesignTokenPalettes.DarkHighContrast)]
    [TestCase(DesignTokenPalettes.LightHighContrast)]
    public void The_selected_state_also_carries_a_cue_that_is_not_colour(string paletteName)
    {
        // The luminance step above is necessary but not sufficient: a reader who
        // has replaced the palette entirely - forced colours - loses it. So the
        // token layer publishes a weight step and an indicator thickness, and a
        // primitive spends all three.
        var shared = DesignTokens.Block(DesignTokens.RootSelector);
        const string Where = "the shared :root block";

        Assert.Multiple(() =>
        {
            var rest = DesignTokens.Value(shared, Where, "--lx-weight-state-rest");
            var selected = DesignTokens.Value(shared, Where, "--lx-weight-state-selected");
            Assert.That(rest, Is.Not.EqualTo(selected),
                "the two weights must differ, or the weight cue conveys nothing");

            DesignTokens.Value(shared, Where, "--lx-state-indicator-thickness");

            // Forced colours drops box-shadow, so a focus ring drawn only as a
            // shadow disappears there. These let a primitive draw it as an
            // outline as well, which survives.
            DesignTokens.Value(shared, Where, "--lx-focus-outline-width");
            DesignTokens.Value(shared, Where, "--lx-focus-outline-offset");

            // The indicator itself has to resolve in every palette, because the
            // forced-colours path is the only one where it does all the work.
            var palette = DesignTokenPalettes.Resolve(paletteName);
            Assert.That(palette.ContainsKey("--lx-color-state-selected-indicator"), Is.True,
                $"{paletteName} must resolve --lx-color-state-selected-indicator");
        });
    }

    [Test]
    [TestCase(DesignTokenPalettes.Dark)]
    [TestCase(DesignTokenPalettes.Light)]
    [TestCase(DesignTokenPalettes.DarkHighContrast)]
    [TestCase(DesignTokenPalettes.LightHighContrast)]
    public void The_accent_fill_keeps_the_only_foreground_drawn_on_it_legible(string paletteName)
    {
        // The accent is measured as a foreground by the text fixture. This is
        // its other role: the fill of the one affirmative button, with
        // accent-contrast as the label on top of it.
        var palette = DesignTokenPalettes.Resolve(paletteName);
        var label = DesignTokens.Colour(palette, paletteName, "--lx-color-accent-contrast");

        Assert.Multiple(() =>
        {
            foreach (var fill in new[] { "--lx-color-accent", "--lx-color-accent-hover" })
            {
                var ratio = DesignTokens.ContrastRatio(
                    label, DesignTokens.Colour(palette, paletteName, fill));

                Assert.That(ratio, Is.GreaterThanOrEqualTo(DesignTokens.TextMinimum),
                    $"{paletteName}: --lx-color-accent-contrast on {fill} is "
                    + $"{DesignTokens.Format(ratio)}, below the text minimum. A filled button whose own "
                    + "label is illegible is worse than an outlined one.");
            }
        });
    }

    [Test]
    [TestCase(DesignTokenPalettes.Dark)]
    [TestCase(DesignTokenPalettes.Light)]
    [TestCase(DesignTokenPalettes.DarkHighContrast)]
    [TestCase(DesignTokenPalettes.LightHighContrast)]
    public void No_measured_token_is_translucent(string paletteName)
    {
        // The focus ring and the danger callout's fill and border were rgba()
        // before issue #1846. A translucent value composites differently over
        // every surface, so the stylesheet cannot promise a ratio for it and the
        // guard cannot check one. The scrim stays translucent and stays out of
        // the measured set, because dimming what is behind it is its whole job
        // and nothing is ever drawn on top of it.
        var palette = DesignTokenPalettes.Resolve(paletteName);

        Assert.Multiple(() =>
        {
            foreach (var token in NonTextTokens
                .Concat(DesignTokens.BackgroundTokens)
                .Concat(["--lx-color-focus-ring-contrast", "--lx-color-accent-contrast"]))
            {
                // Colour asserts the literal-hex form; calling it is the check.
                DesignTokens.Colour(palette, paletteName, token);
            }

            Assert.That(palette["--lx-color-scrim"], Does.StartWith("rgba("),
                "the scrim is the one colour that is deliberately translucent");
        });
    }

    [Test]
    public void The_prefers_contrast_media_block_matches_the_explicit_high_contrast_block()
    {
        // CSS gives no way to share one declaration list between a selector and
        // a media query, so the high-contrast overlay is stated twice: once for
        // the control issue #1852 will add, and once for a reader who set the
        // preference at the operating system. Two copies is a drift risk, and
        // this is what converts it into a guarded invariant.
        var region = DesignTokens.Region(DesignTokens.PrefersContrastQuery);

        Assert.Multiple(() =>
        {
            AssertBlocksAgree(
                DesignTokens.Block(DesignTokens.DarkHighContrastSelector),
                DesignTokens.Block(DesignTokens.DarkHighContrastMediaSelector, region),
                "dark");
            AssertBlocksAgree(
                DesignTokens.Block(DesignTokens.LightHighContrastSelector),
                DesignTokens.Block(DesignTokens.LightHighContrastMediaSelector, region),
                "light");
        });
    }

    [Test]
    public void The_operating_system_hint_yields_to_an_explicit_standard_choice()
    {
        // A reader who has explicitly chosen the standard palette in the
        // Explorer keeps it even when the platform hint is set. Without the
        // :not(), the media query would silently overrule the control that
        // issue #1852 is going to add.
        var region = DesignTokens.Region(DesignTokens.PrefersContrastQuery);

        Assert.That(region, Does.Contain("[data-contrast=\"standard\"]"),
            "the prefers-contrast block must exclude readers who chose the standard palette");
    }

    [Test]
    public void The_forced_colors_block_hands_every_colour_token_to_the_platform()
    {
        // Forced colours means the reader has told the platform to pick the
        // colours. The only correct response is to hand every token to a system
        // colour keyword: a hard-coded hex here would defeat the mode rather
        // than honour it, which is why this block is asserted structurally and
        // never measured.
        var region = DesignTokens.Region(DesignTokens.ForcedColorsQuery);
        var block = DesignTokens.Block(DesignTokens.RootSelector, region);

        var systemColours = new[]
        {
            "Canvas", "CanvasText", "LinkText", "Highlight", "HighlightText",
            "ButtonBorder", "ButtonFace", "ButtonText", "GrayText", "Mark", "MarkText",
        };

        var failures = new List<string>();
        foreach (var (token, value) in block)
        {
            if (!token.StartsWith("--lx-color-", StringComparison.Ordinal))
            {
                continue;
            }

            if (!systemColours.Contains(value, StringComparer.Ordinal))
            {
                failures.Add($"{token} is '{value}', which is not a CSS system colour keyword");
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                "every colour token in the forced-colors block must resolve to a system colour."
                + Environment.NewLine
                + string.Join(Environment.NewLine, failures));

            // Every colour token the standard palettes declare has to be
            // remapped, or one of them survives as a hard-coded hex and paints
            // over the platform's choice.
            var dark = DesignTokenPalettes.Resolve(DesignTokenPalettes.Dark);
            foreach (var token in dark.Keys.Where(k => k.StartsWith("--lx-color-", StringComparison.Ordinal)))
            {
                Assert.That(block.ContainsKey(token), Is.True,
                    $"the forced-colors block must remap {token}");
            }

            // box-shadow is dropped in forced colours, so leaving the elevation
            // tokens in place would promise a depth cue that is not drawn.
            foreach (var elevation in new[]
                     {
                         "--lx-elevation-0", "--lx-elevation-1",
                         "--lx-elevation-2", "--lx-elevation-3",
                     })
            {
                Assert.That(block.ContainsKey(elevation), Is.True,
                    $"the forced-colors block must neutralise {elevation}");
                Assert.That(block[elevation], Is.EqualTo("none"),
                    $"{elevation} must be none in forced colours, where box-shadow is not painted");
            }
        });
    }

    [Test]
    public void The_two_high_contrast_overlays_are_mutually_exclusive_and_complete()
    {
        // Both overlays sit at the same specificity, so if the dark one could
        // match a light root, any token it sets and the light one omits would
        // win on document order - a dark high-contrast value painted into a
        // light palette, in the one mode whose entire purpose is legibility.
        // The stylesheet makes them mutually exclusive rather than relying on
        // the completeness this test also checks, so the two guards are belt and
        // braces on a failure that would be silent and severe.
        Assert.Multiple(() =>
        {
            Assert.That(DesignTokens.DarkHighContrastSelector,
                Does.Contain(":not([data-theme=\"light\"])"),
                "the dark overlay must exclude the light theme rather than rely on being out-specified");
            Assert.That(DesignTokens.DarkHighContrastMediaSelector,
                Does.Contain(":not([data-theme=\"light\"])"),
                "and so must its copy inside the prefers-contrast query");

            var dark = DesignTokens.Block(DesignTokens.DarkHighContrastSelector);
            var light = DesignTokens.Block(DesignTokens.LightHighContrastSelector);

            Assert.That(light.Keys.OrderBy(k => k, StringComparer.Ordinal),
                Is.EqualTo(dark.Keys.OrderBy(k => k, StringComparer.Ordinal)),
                "the two high-contrast overlays must cover exactly the same tokens, so neither palette "
                + "inherits a value the other one meant to replace");
        });
    }

    [Test]
    public void The_guard_rejects_the_exact_non_text_defects_reported_in_1846()
    {
        // The retired values, kept as literals so they stay measurable after the
        // stylesheet stops declaring them. Each pair is asserted twice: once at
        // the ratio the audit recorded, so a change to the maths is caught, and
        // once as a failure, so a change to the bar is caught.
        var defects = new (string Description, string Foreground, string Background, double Recorded)[]
        {
            ("dark border on surface", "#1e2532", "#0f131c", 1.21),
            ("dark border-strong on surface", "#2b3545", "#0f131c", 1.50),
            ("light border on surface", "#dfe4ec", "#ffffff", 1.28),
            ("light border-strong on surface", "#c4ccd8", "#ffffff", 1.62),
            ("active against inactive tab", "#4cc2ff", "#8a93a3", 1.54),
        };

        Assert.Multiple(() =>
        {
            foreach (var (description, foreground, background, recorded) in defects)
            {
                var measured = DesignTokens.ContrastRatio(foreground, background);

                Assert.That(measured, Is.EqualTo(recorded).Within(0.2),
                    $"{description}, as measured on issue #1846");
                Assert.That(measured, Is.LessThan(DesignTokens.NonTextMinimum),
                    $"{description} must be judged a failure, or this fixture guards nothing");
            }

            // The light palette's elevation collapse: three levels, one colour.
            var collapsed = DesignTokens.ContrastRatio("#ffffff", "#ffffff");
            Assert.That(collapsed, Is.LessThan(MinimumElevationStep),
                "three identical surface levels must be judged a collapse");
        });
    }

    [Test]
    public void An_unknown_palette_name_is_rejected_rather_than_silently_resolved()
    {
        // A typo in a [TestCase] argument must fail loudly. Returning an empty
        // palette instead would make every gate above pass vacuously.
        Assert.That(
            () => DesignTokenPalettes.Resolve("solarized"),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void An_accent_can_never_be_the_thing_that_carries_state()
    {
        // This is the constraint the state tokens were designed around, and it
        // is worth pinning because it is counter-intuitive: the obvious fix for
        // a 1.54:1 active tab is "make the accent brighter", and that fix cannot
        // work at any brightness.
        //
        // An inactive tab is a legal foreground, so it sits at 4.5:1 or better
        // on its surface, which bounds how dark it can be. Against a foreground
        // that dark, even pure white - the lightest colour that exists - is only
        // just over 3:1, so any accent with actual colour in it falls short.
        // State therefore has to be carried by a dedicated luminance ladder plus
        // the weight and indicator cues, which is what the token layer does.
        var dark = DesignTokenPalettes.Resolve(DesignTokenPalettes.Dark);
        var muted = DesignTokens.Colour(dark, DesignTokenPalettes.Dark, "--lx-color-text-muted");

        Assert.Multiple(() =>
        {
            var ceiling = DesignTokens.ContrastRatio("#ffffff", muted);
            Assert.That(ceiling, Is.LessThan(3.2),
                "pure white is the ceiling any accent is competing against, and it barely clears 3:1");

            var accent = DesignTokens.Colour(dark, DesignTokenPalettes.Dark, "--lx-color-accent");
            Assert.That(DesignTokens.ContrastRatio(accent, muted), Is.LessThan(ceiling),
                "and a coloured accent is necessarily below that ceiling");

            // Which is precisely why the state pair does not use the accent.
            var selected = DesignTokens.Colour(dark, DesignTokenPalettes.Dark, "--lx-color-state-selected-fg");
            var rest = DesignTokens.Colour(dark, DesignTokenPalettes.Dark, "--lx-color-state-rest-fg");
            Assert.That(DesignTokens.ContrastRatio(selected, rest),
                Is.GreaterThanOrEqualTo(DesignTokens.NonTextMinimum),
                "the dedicated state pair clears the bar the accent cannot");
            Assert.That(selected, Is.Not.EqualTo(accent),
                "the selected foreground must not be the accent");
        });
    }

    private static void AssertEveryNonTextTokenClears(string paletteName, double minimum)
    {
        var palette = DesignTokenPalettes.Resolve(paletteName);

        var failures = new List<string>();
        var measured = 0;

        foreach (var token in NonTextTokens)
        {
            var colour = DesignTokens.Colour(palette, paletteName, token);

            foreach (var background in DesignTokens.BackgroundTokens)
            {
                var surface = DesignTokens.Colour(palette, paletteName, background);
                var ratio = DesignTokens.ContrastRatio(colour, surface);
                measured++;

                if (ratio < minimum)
                {
                    failures.Add(
                        $"{token} ({colour}) on {background} ({surface}) is "
                        + $"{DesignTokens.Format(ratio)}, below the {DesignTokens.Format(minimum)} minimum");
                }
            }
        }

        // Without this the gate would pass vacuously if the palette parser ever
        // stopped finding declarations.
        Assert.That(measured, Is.EqualTo(NonTextTokens.Length * DesignTokens.BackgroundTokens.Length),
            "every boundary/background pair in the palette must be measured");

        Assert.That(failures, Is.Empty,
            $"{paletteName} in {DesignTokens.Stylesheet} fails WCAG 2.1 SC 1.4.11 (non-text contrast). "
            + "A border a reader cannot see is a border that is not doing its job: on a dark palette "
            + "lighten it, on a light palette darken it, and keep border-strong ahead of border."
            + Environment.NewLine
            + string.Join(Environment.NewLine, failures));
    }

    private static void AssertBlocksAgree(
        IReadOnlyDictionary<string, string> explicitBlock,
        IReadOnlyDictionary<string, string> mediaBlock,
        string palette)
    {
        Assert.That(mediaBlock.Keys.OrderBy(k => k, StringComparer.Ordinal),
            Is.EqualTo(explicitBlock.Keys.OrderBy(k => k, StringComparer.Ordinal)),
            $"the {palette} high-contrast overlay must declare the same tokens in both copies");

        foreach (var (token, value) in explicitBlock)
        {
            Assert.That(mediaBlock[token], Is.EqualTo(value),
                $"the {palette} high-contrast overlay declares {token} as '{value}' for an explicit "
                + $"choice but '{mediaBlock[token]}' for the platform hint. The two copies must agree.");
        }
    }
}
