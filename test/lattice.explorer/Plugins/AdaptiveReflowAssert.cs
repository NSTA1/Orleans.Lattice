using System.Text.RegularExpressions;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// The four assertions every plugin surface that ships a
/// <c>LatticeAdaptiveTable</c> has to satisfy, written once so a surface cannot
/// quietly hold itself to a weaker version of them (issue #1782).
/// </summary>
/// <remarks>
/// <para>
/// The reflow is the epic's headline user requirement, and it is exactly the
/// kind of behaviour a state-level test cannot see: issue #1758 found the shell
/// never hosting <c>LatticeAdaptiveRoot</c> at all, so the cascaded breakpoint
/// was pinned to <c>Expanded</c> and nothing reflowed anywhere - while every
/// test stayed green, because the declarative half worked and the imperative
/// half did not. Only a rendered-shape assertion catches that.
/// </para>
/// <para>
/// <b>On <see cref="ReflowsToCards"/>, which is the one that matters.</b> It
/// asserts the fragment contains no <c>&lt;table&gt;</c> at all, not merely
/// that it stopped naming <c>lx-table</c>. If a surface ever renders a second,
/// genuinely non-adaptive table and this fails for a reason unrelated to the
/// reflow, the correct repair is to <em>scope</em> the assertion to the
/// adaptive table's own subtree. Relaxing it to "does not contain
/// <c>lx-table</c>" is the wrong repair and it looks reasonable in review: a
/// card list and a table that has silently stopped reflowing both satisfy it,
/// so the guard becomes decoration. As of this writing no Explorer plugin
/// renders a raw <c>&lt;table&gt;</c> - every one of them goes through the
/// design system's primitive - so the literal form holds everywhere.
/// </para>
/// </remarks>
internal static class AdaptiveReflowAssert
{
    /// <summary>
    /// Assertion 1: at a wide breakpoint the surface renders a real table and
    /// no card list.
    /// </summary>
    /// <param name="html">The rendered markup.</param>
    /// <param name="surface">The surface's name, for the failure message.</param>
    public static void RendersATable(string html, string surface)
    {
        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("<table"),
                $"{surface}: a wide viewport gets real table semantics, not a list of cards");
            Assert.That(html, Does.Contain("lx-table"),
                $"{surface}: the table is the design system's, not a hand-rolled one");
            Assert.That(html, Does.Not.Contain("lx-cardlist"),
                $"{surface}: the card presentation belongs to compact alone");
        });
    }

    /// <summary>
    /// Assertion 2: at compact the surface reflows to a card list and renders
    /// no table at all, so the reflow genuinely fires rather than merely being
    /// declared.
    /// </summary>
    /// <param name="html">The rendered markup.</param>
    /// <param name="surface">The surface's name, for the failure message.</param>
    public static void ReflowsToCards(string html, string surface)
    {
        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-cardlist"),
                $"{surface}: a narrow viewport must get cards rather than a sideways scroll");
            Assert.That(html, Does.Contain("lx-card-title"),
                $"{surface}: each card must lead with the column that identifies its row");

            // Deliberately the element, not the class. See the type's remarks:
            // asserting the absence of `lx-table` would pass just as happily
            // while the reflow stopped firing.
            Assert.That(html, Does.Not.Contain("<table"),
                $"{surface}: the reflow must actually replace the table, not sit beside one");
        });
    }

    /// <summary>
    /// Assertion 3, table side: the column is a real header cell in the table
    /// presentation.
    /// </summary>
    /// <param name="html">The rendered markup.</param>
    /// <param name="header">The column's header text.</param>
    /// <param name="surface">The surface's name, for the failure message.</param>
    public static void TableShowsColumn(string html, string header, string surface) =>
        Assert.That(html, Does.Contain($"<th scope=\"col\">{header}</th>"),
            $"{surface}: the '{header}' column must be a scoped header cell in the table");

    /// <summary>
    /// Assertion 3, card side: the column survives the reflow as a labelled
    /// card field still carrying its own value.
    /// </summary>
    /// <param name="html">The rendered markup.</param>
    /// <param name="header">The column's header text.</param>
    /// <param name="value">A literal the column's cell must render.</param>
    /// <param name="surface">The surface's name, for the failure message.</param>
    public static void CardShowsField(string html, string header, string value, string surface)
    {
        var field = FieldBody(html, header);

        Assert.That(field, Is.Not.Null,
            $"{surface}: the '{header}' column was dropped from the card - a card that loses a "
            + "column is data loss, not a layout nit");
        Assert.That(field, Does.Contain(value),
            $"{surface}: the '{header}' card field kept its label but lost its value");
    }

    /// <summary>
    /// Assertion 3, primary column: the column that identifies a row is
    /// promoted to the card's title rather than becoming an ordinary field.
    /// </summary>
    /// <param name="html">The rendered markup.</param>
    /// <param name="value">A literal the primary cell must render.</param>
    /// <param name="surface">The surface's name, for the failure message.</param>
    public static void CardShowsTitle(string html, string value, string surface)
    {
        var title = Body(html, "<div class=\"lx-card-title\">");

        Assert.That(title, Is.Not.Null, $"{surface}: the card has no title");
        Assert.That(title, Does.Contain(value),
            $"{surface}: the column identifying a row must lead its card");
    }

    /// <summary>
    /// The counterpart to <see cref="CardShowsField"/> for a column that opts
    /// out of the card with <c>ShowOnCompact = false</c>. Pinning the opt-out
    /// is what keeps a deliberate omission distinguishable from a column that
    /// started disappearing by accident.
    /// </summary>
    /// <param name="html">The rendered markup.</param>
    /// <param name="header">The column's header text.</param>
    /// <param name="surface">The surface's name, for the failure message.</param>
    public static void CardOmitsField(string html, string header, string surface) =>
        Assert.That(FieldBody(html, header), Is.Null,
            $"{surface}: the '{header}' column declares ShowOnCompact = false, so it must not "
            + "appear as a card field");

    /// <summary>
    /// Assertion 4: a control an operator needs is reachable in both
    /// presentations, so a narrow viewport does not strand them.
    /// </summary>
    /// <param name="expanded">The markup rendered at a wide breakpoint.</param>
    /// <param name="compact">The markup rendered at compact.</param>
    /// <param name="marker">A literal identifying the control.</param>
    /// <param name="surface">The surface's name, for the failure message.</param>
    public static void ControlSurvivesTheReflow(
        string expanded, string compact, string marker, string surface)
    {
        Assert.Multiple(() =>
        {
            Assert.That(expanded, Does.Contain(marker),
                $"{surface}: '{marker}' must be reachable in the table presentation");
            Assert.That(compact, Does.Contain(marker),
                $"{surface}: '{marker}' must survive the reflow, or a narrow-viewport operator "
                + "is stranded without it");
        });
    }

    /// <summary>
    /// The rendered content of the card field labelled <paramref name="header"/>,
    /// or <see langword="null"/> when no such field is rendered.
    /// </summary>
    /// <param name="html">The rendered markup.</param>
    /// <param name="header">The column's header text.</param>
    private static string? FieldBody(string html, string header)
    {
        var match = Regex.Match(
            html,
            "lx-card-field-label\">" + Regex.Escape(header) + "</span>(?<body>.*?)</div>",
            RegexOptions.Singleline);

        return match.Success ? match.Groups["body"].Value : null;
    }

    /// <summary>
    /// The rendered content of the element opened by <paramref name="opening"/>,
    /// up to its first closing tag.
    /// </summary>
    /// <param name="html">The rendered markup.</param>
    /// <param name="opening">The literal opening tag to match.</param>
    private static string? Body(string html, string opening)
    {
        var start = html.IndexOf(opening, StringComparison.Ordinal);
        if (start < 0)
        {
            return null;
        }

        start += opening.Length;
        var end = html.IndexOf("</div>", start, StringComparison.Ordinal);
        return end < 0 ? html[start..] : html[start..end];
    }
}
