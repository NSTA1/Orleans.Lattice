namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// The regression gate for the overflow menus: at every viewport width the
/// Explorer supports, and for any content width, the menu's box lies wholly
/// inside the viewport.
/// </summary>
/// <remarks>
/// <para>
/// The fault this closes was deterministic, not marginal. Anchored to the
/// little overflow-toggle box rather than to the strip, and aligned to that
/// box's trailing edge, a 12rem menu began 25.2px outside the viewport at
/// <em>every</em> compact width - and <c>.lx-shell</c> and <c>.lx-tabstrip</c>
/// both clip their overflow, so there was no scroll to recover it. The
/// navigation bar's menu had the same shape and the same latent fault.
/// </para>
/// <para>
/// These tests read the real shipped stylesheets and compute the resulting
/// geometry, so they fail if the declarations regress rather than if some
/// paraphrase of them changes.
/// <see cref="The_model_reproduces_the_clipping_the_audit_measured"/> is the
/// battery test for this smoke detector: it runs the same model over the
/// stylesheet as it shipped <em>before</em> the fix and asserts it reports the
/// measured -25.2px overhang, so a model that had quietly stopped detecting
/// anything could not pass.
/// </para>
/// <para>
/// Deterministic and browserless: no layout engine, no clock, no timing, no
/// ordering dependence. Every input is a literal in the test.
/// </para>
/// </remarks>
[TestFixture]
public sealed class OverflowMenuGeometryTests
{
    /// <summary>
    /// Every viewport width the shell is expected to work at: the whole compact
    /// band the audit measured, the band boundaries, and representative medium
    /// and expanded widths.
    /// </summary>
    private static readonly double[] ViewportWidths =
    [
        320, 360, 380, 400, 420, 480, 560, 599,
        600, 640, 768, 900, 1000, 1023,
        1024, 1280, 1440, 1920,
    ];

    /// <summary>
    /// Content widths spanning "narrower than the minimum" to "far wider than
    /// the viewport", so the invariant does not depend on a guess about how
    /// wide a particular label set happens to render.
    /// </summary>
    private static readonly double[] IntrinsicWidths = [0, 80, 192, 240, 400, 900, 4000];

    /// <summary>
    /// The tab strip's rules exactly as they shipped before this fix. The menu
    /// anchored to the overflow-toggle box and aligned to its trailing edge,
    /// with an unclamped 12rem minimum.
    /// </summary>
    private const string PreFixTabStripCss = """
        .lx-tabstrip-host {
            display: flex;
            align-items: stretch;
        }

        .lx-tabstrip-overflow-host {
            position: relative;
            display: flex;
        }

        .lx-tabstrip-overflow {
            position: absolute;
            top: 100%;
            right: 0;
            display: flex;
            min-width: 12rem;
        }
        """;

    private static CssBox Viewport(double width) => new(0, width);

    private static CssBox CompactToggleBox() => new(
        OverflowMenuGeometry.CompactOverflowToggleRightPx - OverflowMenuGeometry.CompactOverflowToggleWidthPx,
        OverflowMenuGeometry.CompactOverflowToggleWidthPx);

    /// <summary>
    /// The declarations in effect for the overflow-toggle box <em>inside the
    /// primitive</em>: the bare class rule, overridden by the more specific
    /// child rule the primitive scopes to its own host.
    /// </summary>
    private static IReadOnlyDictionary<string, string> OverflowHostInsideTheStrip(
        CssStylesheet primitives) =>
        OverflowMenuGeometry.Effective(
            primitives.Rule(".lx-tabstrip-overflow-host"),
            primitives.Rule(".lx-tabstrip-host > .lx-tabstrip-overflow-host"));

    private static CssBox? ResolveTabStripMenu(
        CssStylesheet primitives,
        IReadOnlyDictionary<string, string> tokens,
        double viewportWidth,
        double intrinsicWidth)
    {
        var viewport = Viewport(viewportWidth);

        // Nearest positioned ancestor first, exactly as the browser resolves a
        // containing block. The strip host spans the row it is laid out in; the
        // overflow-toggle box is the small element beside the tabs.
        var containingBlock = OverflowMenuGeometry.ContainingBlock(
            [
                (OverflowHostInsideTheStrip(primitives), CompactToggleBox()),
                (primitives.Rule(".lx-tabstrip-host"), viewport),
            ],
            viewport);

        return OverflowMenuGeometry.Resolve(
            primitives.Rule(".lx-tabstrip-overflow"),
            containingBlock,
            intrinsicWidth,
            tokens);
    }

    private static CssBox? ResolveNavMenu(
        CssStylesheet primitives,
        IReadOnlyDictionary<string, string> tokens,
        double viewportWidth,
        double intrinsicWidth)
    {
        var viewport = Viewport(viewportWidth);

        var containingBlock = OverflowMenuGeometry.ContainingBlock(
            [
                (primitives.Rule(".lx-nav-bottom"), viewport),
                (primitives.Rule(".lx-nav"), viewport),
            ],
            viewport);

        return OverflowMenuGeometry.Resolve(
            primitives.Rule(".lx-nav-overflow"),
            containingBlock,
            intrinsicWidth,
            tokens);
    }

    // ------------------------------------------------------- the shipped CSS

    [Test]
    public void The_tab_strip_overflow_menu_lies_within_the_viewport_at_every_width()
    {
        var primitives = OverflowMenuGeometry.LoadPrimitives();
        var tokens = OverflowMenuGeometry.LoadTokens().RootCustomProperties();

        var violations = new List<string>();
        foreach (var viewportWidth in ViewportWidths)
        {
            foreach (var intrinsicWidth in IntrinsicWidths)
            {
                var menu = ResolveTabStripMenu(primitives, tokens, viewportWidth, intrinsicWidth);

                Assert.That(menu, Is.Not.Null,
                    "the menu rule must offset the menu from an edge this model can resolve");

                if (menu!.Value.Left < 0 || menu.Value.Right > viewportWidth)
                {
                    violations.Add(
                        $"viewport {viewportWidth}px, content {intrinsicWidth}px: "
                        + $"menu spans {menu.Value.Left}..{menu.Value.Right}");
                }
            }
        }

        Assert.That(violations, Is.Empty,
            "The overflow menu must be fully within the viewport at every width. "
            + "It is clipped dead by .lx-shell and .lx-tabstrip, both of which hide their "
            + "overflow, so a menu that reaches a negative x is simply cut off with no scroll "
            + "to recover it."
            + Environment.NewLine
            + string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void The_navigation_overflow_menu_lies_within_the_viewport_at_every_width()
    {
        var primitives = OverflowMenuGeometry.LoadPrimitives();
        var tokens = OverflowMenuGeometry.LoadTokens().RootCustomProperties();

        var violations = new List<string>();
        foreach (var viewportWidth in ViewportWidths)
        {
            foreach (var intrinsicWidth in IntrinsicWidths)
            {
                var menu = ResolveNavMenu(primitives, tokens, viewportWidth, intrinsicWidth);

                Assert.That(menu, Is.Not.Null,
                    "the menu rule must offset the menu from an edge this model can resolve");

                if (menu!.Value.Left < 0 || menu.Value.Right > viewportWidth)
                {
                    violations.Add(
                        $"viewport {viewportWidth}px, content {intrinsicWidth}px: "
                        + $"menu spans {menu.Value.Left}..{menu.Value.Right}");
                }
            }
        }

        Assert.That(violations, Is.Empty,
            "The navigation overflow menu carries the same guarantee as the tab strip's."
            + Environment.NewLine
            + string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void The_tab_strip_overflow_menu_is_anchored_to_the_strip_not_to_its_toggle()
    {
        var primitives = OverflowMenuGeometry.LoadPrimitives();

        Assert.Multiple(() =>
        {
            Assert.That(
                OverflowMenuGeometry.IsPositioned(primitives.Rule(".lx-tabstrip-host")),
                Is.True,
                "the strip host is the menu's containing block, so the menu is clamped "
                + "against a box wide enough to hold it");
            Assert.That(
                OverflowMenuGeometry.IsPositioned(OverflowHostInsideTheStrip(primitives)),
                Is.False,
                "anchoring to the toggle box is the fault: a 12rem menu aligned to a 50px "
                + "box's trailing edge starts outside the viewport");
            Assert.That(
                OverflowMenuGeometry.IsPositioned(primitives.Rule(".lx-tabstrip-overflow-host")),
                Is.True,
                "the bare class stays positioned so the shell's hand-rolled copy, which "
                + "sits in an unpositioned wrapper, keeps the anchor it has today");
        });
    }

    [Test]
    public void Effective_appliesTheMoreSpecificRuleLast()
    {
        var less = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["position"] = "relative",
            ["display"] = "flex",
        };
        var more = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["position"] = "static",
        };

        var effective = OverflowMenuGeometry.Effective(less, more);

        Assert.Multiple(() =>
        {
            Assert.That(effective["position"], Is.EqualTo("static"));
            Assert.That(effective["display"], Is.EqualTo("flex"));
        });
    }

    [Test]
    public void Both_overflow_menus_clamp_their_minimum_width_as_well_as_their_maximum()
    {
        var primitives = OverflowMenuGeometry.LoadPrimitives();
        var tokens = OverflowMenuGeometry.LoadTokens().RootCustomProperties();

        // A 200px containing block is narrower than the 12rem the menus prefer,
        // which is the case an unclamped min-width silently overhangs.
        var narrow = new CssBox(0, 200);

        foreach (var selector in new[] { ".lx-tabstrip-overflow", ".lx-nav-overflow" })
        {
            var menu = OverflowMenuGeometry.Resolve(
                primitives.Rule(selector), narrow, intrinsicWidthPx: 4000, tokens);

            Assert.That(menu, Is.Not.Null, selector);
            Assert.That(menu!.Value.Left, Is.GreaterThanOrEqualTo(0), selector);
            Assert.That(menu.Value.Right, Is.LessThanOrEqualTo(narrow.Width), selector);
        }
    }

    // ------------------------------------------- the battery for the detector

    [Test]
    public void The_model_reproduces_the_clipping_the_audit_measured()
    {
        var preFix = CssStylesheet.Parse(PreFixTabStripCss);
        var tokens = OverflowMenuGeometry.LoadTokens().RootCustomProperties();

        // Only the compact band: the audit measured the overhang there, where
        // the toggle sits behind a single tab.
        double[] compactWidths = [320, 360, 380, 420, 480, 560, 599];

        var leadingEdges = new List<double>();
        foreach (var viewportWidth in compactWidths)
        {
            var menu = ResolveTabStripMenu(preFix, tokens, viewportWidth, intrinsicWidth: 0);

            Assert.That(menu, Is.Not.Null);
            leadingEdges.Add(menu!.Value.Left);
        }

        Assert.Multiple(() =>
        {
            foreach (var edge in leadingEdges)
            {
                Assert.That(edge, Is.EqualTo(-25.2).Within(0.001),
                    "the model must reproduce the browser measurement, or it is not "
                    + "modelling the fault the gate above claims to detect");
            }
        });
    }

    [Test]
    public void The_model_reads_the_grammar_the_primitives_spend()
    {
        var tokens = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["--lx-space-4"] = "0.5rem",
            ["--lx-space-2"] = "0.25rem",
        };

        Assert.Multiple(() =>
        {
            Assert.That(CssStylesheet.ResolveLength("12rem", 1000, tokens), Is.EqualTo(192));
            Assert.That(CssStylesheet.ResolveLength("8px", 1000, tokens), Is.EqualTo(8));
            Assert.That(CssStylesheet.ResolveLength("0", 1000, tokens), Is.EqualTo(0));
            Assert.That(CssStylesheet.ResolveLength("100%", 640, tokens), Is.EqualTo(640));
            Assert.That(CssStylesheet.ResolveLength("var(--lx-space-4)", 1000, tokens), Is.EqualTo(8));
            Assert.That(
                CssStylesheet.ResolveLength("calc(100% - (2 * var(--lx-space-4)))", 640, tokens),
                Is.EqualTo(624));
            Assert.That(
                CssStylesheet.ResolveLength(
                    "min(12rem, calc(100% - (2 * var(--lx-space-4))))", 640, tokens),
                Is.EqualTo(192));
            Assert.That(
                CssStylesheet.ResolveLength(
                    "min(12rem, calc(100% - (2 * var(--lx-space-4))))", 100, tokens),
                Is.EqualTo(84));
            Assert.That(CssStylesheet.ResolveLength("max(1rem, 4px)", 1000, tokens), Is.EqualTo(16));

            // A keyword, an unknown property, and a shorthand are all "not a
            // length", so the model never silently reads them as zero.
            Assert.That(CssStylesheet.ResolveLength("auto", 1000, tokens), Is.Null);
            Assert.That(CssStylesheet.ResolveLength("var(--lx-missing)", 1000, tokens), Is.Null);
            Assert.That(CssStylesheet.ResolveLength("2px solid red", 1000, tokens), Is.Null);
            Assert.That(CssStylesheet.ResolveLength(null, 1000, tokens), Is.Null);
            Assert.That(CssStylesheet.ResolveLength("   ", 1000, tokens), Is.Null);
        });
    }

    [Test]
    public void The_model_reads_rules_declarations_and_custom_properties()
    {
        var sheet = CssStylesheet.Parse("""
            /* a comment naming .lx-not-a-rule { position: absolute; } */
            :root {
                --lx-space-4: 0.5rem;
            }

            .lx-a,
            .lx-b {
                position: relative;
                min-width: 12rem;
            }

            @media (forced-colors: active) {
                .lx-a {
                    position: static;
                }
            }
            """);

        Assert.Multiple(() =>
        {
            Assert.That(sheet.HasRule(".lx-a"), Is.True);
            Assert.That(sheet.HasRule(".lx-b"), Is.True, "a selector list declares every rule in it");
            Assert.That(sheet.HasRule(".lx-not-a-rule"), Is.False, "comments are not rules");
            Assert.That(sheet.Rule(".lx-a")["min-width"], Is.EqualTo("12rem"));
            Assert.That(OverflowMenuGeometry.IsPositioned(sheet.Rule(".lx-b")), Is.True);
            Assert.That(sheet.RootCustomProperties()["--lx-space-4"], Is.EqualTo("0.5rem"));
            Assert.That(sheet.Rule(".lx-missing"), Is.Empty);
        });
    }

    [Test]
    public void Resolve_withNoOffsetFromEitherEdge_reportsThatItCannotPlaceTheMenu()
    {
        var declarations = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["position"] = "absolute",
            ["left"] = "auto",
            ["right"] = "auto",
        };

        Assert.That(
            OverflowMenuGeometry.Resolve(
                declarations,
                new CssBox(0, 640),
                intrinsicWidthPx: 100,
                new Dictionary<string, string>(StringComparer.Ordinal)),
            Is.Null);
    }

    [Test]
    public void ContainingBlock_withNoPositionedAncestor_fallsBackToTheViewport()
    {
        var unpositioned = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["display"] = "flex",
        };
        var viewport = new CssBox(0, 640);

        var resolved = OverflowMenuGeometry.ContainingBlock(
            [(unpositioned, new CssBox(100, 50))],
            viewport);

        Assert.That(resolved, Is.EqualTo(viewport));
    }

    [Test]
    public void ContainingBlock_prefersTheNearestPositionedAncestor()
    {
        var positioned = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["position"] = "relative",
        };
        var nearest = new CssBox(100, 50);

        var resolved = OverflowMenuGeometry.ContainingBlock(
            [(positioned, nearest), (positioned, new CssBox(0, 640))],
            new CssBox(0, 640));

        Assert.That(resolved, Is.EqualTo(nearest));
    }

    [Test]
    public void A_box_reports_its_trailing_edge()
    {
        Assert.That(new CssBox(12, 30).Right, Is.EqualTo(42));
    }
}
