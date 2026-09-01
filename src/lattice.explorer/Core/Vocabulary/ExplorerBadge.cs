namespace Orleans.Lattice.Explorer.Core.Vocabulary;

/// <summary>
/// A catalog badge together with everything needed to render it accessibly: the
/// readable text, the abbreviation it may collapse to when space is tight, and
/// the expansion that always says the whole thing.
/// </summary>
/// <remarks>
/// <para>
/// Badges were the sharpest edge of the Explorer's vocabulary problem: <c>64 sh</c>
/// and <c>agg</c> are not words, and a native <c>title</c> attribute is invisible
/// on touch and unreachable by keyboard. A badge therefore carries three texts:
/// </para>
/// <list type="bullet">
///   <item><description><see cref="Text"/> - what to render by default. Readable.</description></item>
///   <item><description><see cref="ShortText"/> - the abbreviation, for a layout where brevity genuinely matters.</description></item>
///   <item><description><see cref="Expansion"/> - the accessible name, always readable, whichever of the two is rendered.</description></item>
/// </list>
/// <para>
/// A caller that renders <see cref="ShortText"/> must attach
/// <see cref="Expansion"/> as the accessible name, and should offer
/// <see cref="Explanation"/> through the help disclosure:
/// </para>
/// <code>
/// // readable by default
/// // &lt;span class="lx-badge"&gt;@badge.Text&lt;/span&gt;
/// //
/// // abbreviated, with the expansion carried accessibly
/// // &lt;span class="lx-badge"&gt;
/// //     &lt;span aria-hidden="true"&gt;@badge.ShortText&lt;/span&gt;
/// //     &lt;span class="lx-visually-hidden"&gt;@badge.Expansion&lt;/span&gt;
/// // &lt;/span&gt;
/// // &lt;LatticeHelp Term="@badge.Label" Explanation="@badge.Explanation" /&gt;
/// </code>
/// <para>
/// All three texts are composed when the badge is built, never when it is read,
/// so rendering one allocates nothing. See <see cref="ExplorerBadges"/> for how
/// the built badges are cached.
/// </para>
/// </remarks>
public readonly record struct ExplorerBadge
{
    /// <summary>
    /// The glossary term this badge stands for, from
    /// <see cref="ExplorerTermIds"/>.
    /// </summary>
    public required string TermId { get; init; }

    /// <summary>
    /// The term's short label, from the glossary - what a help trigger or a
    /// column header calls the badge.
    /// </summary>
    public required string Label { get; init; }

    /// <summary>
    /// The readable text: <c>64 shards</c>, <c>Aggregation</c>. Render this
    /// unless the layout genuinely cannot hold it.
    /// </summary>
    public required string Text { get; init; }

    /// <summary>
    /// The abbreviated text: <c>64 sh</c>, <c>agg</c>. Equal to
    /// <see cref="Text"/> when the badge has no shorter form. Rendering this
    /// obliges the caller to carry <see cref="Expansion"/> as the accessible
    /// name.
    /// </summary>
    public required string ShortText { get; init; }

    /// <summary>
    /// The full expansion, always readable: <c>64 shards</c>,
    /// <c>Aggregation view</c>, <c>Source tree: orders</c>. Use it as the
    /// accessible name whenever the rendered text is abbreviated.
    /// </summary>
    public required string Expansion { get; init; }

    /// <summary>
    /// The count the badge reports, or <see langword="null"/> when it is not a
    /// counting badge. Exposed so a caller can render the number and the unit as
    /// separate nodes rather than re-composing a string.
    /// </summary>
    public int? Count { get; init; }

    /// <summary>
    /// The runtime value the badge reports (a source tree id, a provider key, a
    /// projection version), or <see langword="null"/> when it reports no value.
    /// </summary>
    public string? Value { get; init; }

    /// <summary>
    /// Whether this badge is secondary context rather than a status, so it is
    /// rendered in the muted style.
    /// </summary>
    public bool IsMuted { get; init; }

    /// <summary>
    /// Whether <see cref="ShortText"/> says less than <see cref="Expansion"/>,
    /// and therefore needs the expansion carried accessibly beside it.
    /// </summary>
    public bool IsAbbreviated => !string.Equals(ShortText, Expansion, StringComparison.Ordinal);

    /// <summary>
    /// Whether this is the uninitialised default - the state of a slot in a
    /// caller-owned buffer that <see cref="ExplorerBadges.ForCatalogItem"/> did
    /// not fill. Respect the count that method returns and this is never true.
    /// </summary>
    public bool IsEmpty => TermId is null;

    /// <summary>The glossary entry behind this badge, or <see langword="null"/> for the default.</summary>
    public ExplorerTerm? Term => ExplorerGlossary.Find(TermId);

    /// <summary>
    /// The one-line explanation of what the badge means, or
    /// <see langword="null"/> for the default. Bind it straight onto the help
    /// disclosure.
    /// </summary>
    public string? Explanation => ExplorerGlossary.ExplanationFor(TermId);

    /// <summary>Where to read more, or <see langword="null"/> when nothing covers it.</summary>
    public string? DocsLink => ExplorerGlossary.DocsLinkFor(TermId);
}
