namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Estimates what a packed <c>repocontext_context</c> bundle actually costs a caller
/// <b>on the wire</b>, so the tool's budget can bound the response the caller receives
/// rather than only the source text inside it (issue #1811).
/// <para>
/// Two effects make a response larger than the sum of its content:
/// </para>
/// <list type="number">
/// <item>
/// <b>Per-entry envelope.</b> Every entry ships identifying and reuse metadata - its
/// path, match reasons, content hash, and one receipt/kind/symbol triple per unit -
/// plus JSON punctuation and property names. On a bundle of many small entries this
/// is not a rounding error; it can rival the content itself.
/// </item>
/// <item>
/// <b>Dual emission.</b> The MCP SDK serializes every tool result <b>twice</b> - once
/// as a structured-content block and once as a text block - from the same options
/// instance (see <c>LatticeApiMcpToolSerialization</c>). The text block is the same
/// JSON escaped inside a JSON string, so it is slightly larger than the structured
/// block rather than merely equal to it.
/// </item>
/// </list>
/// <para>
/// The estimate is deliberately conservative (it may over-state, never under-state),
/// because its purpose is to keep a response inside a caller's context budget: a
/// bundle that comes in slightly under the ceiling is a small waste, whereas one that
/// overruns is the failure this whole model exists to prevent.
/// </para>
/// </summary>
internal static class RepoContextResponseCost
{
    /// <summary>
    /// The dual-emission multiplier numerator. The SDK emits the payload as both a
    /// structured block and a text block, and the text block is the same JSON escaped
    /// inside a JSON string (every <c>"</c> becomes <c>\"</c>, every <c>\n</c> becomes
    /// <c>\\n</c>), so the pair costs materially more than 2x a single copy - the escape
    /// sequences also tokenize worse than the characters they replace, so the token
    /// ratio runs well above the character ratio.
    /// <para>
    /// <b>Measured, not guessed - and the worst case is not the obvious one.</b> Against
    /// the real serializer options the ratio measures about <b>2.8</b> for a
    /// <c>slices</c> bundle of multi-line C# source, but about <b>3.4</b> for a small
    /// <c>paths</c> bundle. Counter-intuitively the ratio is <i>worse</i> for small
    /// payloads: JSON scaffolding is quote-dense (every property name is quoted), quotes
    /// are what escaping inflates, and on a small payload the scaffolding dominates,
    /// whereas a large source body dilutes it. 35/10 bounds every shape measured.
    /// </para>
    /// <para>
    /// This is deliberately conservative for content-heavy bundles: over-estimating packs
    /// a slightly smaller bundle (and sets <c>truncated</c>, which a caller can answer by
    /// raising the budget), whereas under-estimating overruns the caller's context, which
    /// is the failure this model exists to prevent. The response-cost tests re-measure the
    /// ratio across payload shapes and fail if the constant ever stops bounding it.
    /// </para>
    /// </summary>
    internal const int DualEmissionNumerator = 35;

    /// <summary>The dual-emission multiplier denominator. See <see cref="DualEmissionNumerator"/>.</summary>
    internal const int DualEmissionDenominator = 10;

    /// <summary>
    /// Approximate token cost of one entry's JSON scaffolding: the object braces, the
    /// property names (<c>path</c>, <c>score</c>, <c>reasons</c>, <c>tokenCount</c>,
    /// <c>fullReadTokenCount</c>, <c>content</c>, <c>contentHash</c>, <c>units</c>),
    /// the numeric values, and the separators.
    /// </summary>
    private const int EntryScaffoldTokens = 24;

    /// <summary>
    /// Approximate token cost of one unit's JSON scaffolding: the object braces, the
    /// <c>receipt</c>/<c>kind</c>/<c>symbol</c>/<c>tokenCount</c> property names, the
    /// numeric value, and the separators.
    /// </summary>
    private const int UnitScaffoldTokens = 10;

    /// <summary>
    /// Approximate token cost of the bundle's own top-level scaffolding: <c>repoId</c>,
    /// <c>task</c>, <c>mode</c>, <c>detail</c>, the numeric budget/total fields,
    /// <c>truncated</c>, <c>session</c>, and the <c>entries</c>/<c>reused</c> arrays -
    /// plus the enclosing <c>CallToolResult</c> wrapper (the <c>content</c> array, the
    /// block type discriminators, the <c>structuredContent</c> key, and <c>isError</c>),
    /// which ships on every response and is easy to forget.
    /// <para>
    /// It is a fixed worst-case reserve rather than a measurement of the actual field
    /// values, deliberately: a reserve that varied with the rendered budget or detail
    /// label would differ between the pack that computes
    /// <see cref="RepoContextContextResult.RetryBudgetTokens"/> and the retry that spends
    /// it, so a caller retrying at exactly that figure could still fail closed.
    /// </para>
    /// </summary>
    internal const int BundleScaffoldTokens = 80;

    /// <summary>
    /// Estimates the wire cost, in tokens, of one packed entry: its delivered content,
    /// its own metadata, and its units' reuse metadata, before the dual-emission factor.
    /// </summary>
    /// <param name="contentTokens">The exact BPE token count of the entry's delivered content.</param>
    /// <param name="path">The entry's repository-relative path.</param>
    /// <param name="reasons">The entry's match reasons.</param>
    /// <param name="contentHash">The entry's content hash, or <see langword="null"/>.</param>
    /// <param name="units">The entry's rendered units, whose receipts and symbols ship on the wire.</param>
    /// <param name="counter">The exact-BPE token counter.</param>
    /// <returns>The estimated single-copy wire cost of the entry, in tokens.</returns>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    internal static int EntryEnvelopeTokens(
        int contentTokens,
        string path,
        IReadOnlyList<string> reasons,
        string? contentHash,
        IReadOnlyList<RepoContextRenderedUnit> units,
        IRepoContextTokenCounter counter)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(reasons);
        ArgumentNullException.ThrowIfNull(units);
        ArgumentNullException.ThrowIfNull(counter);

        var total = contentTokens + EntryScaffoldTokens + counter.CountTokens(path);

        for (var i = 0; i < reasons.Count; i++)
        {
            total += counter.CountTokens(reasons[i]);
        }

        if (!string.IsNullOrEmpty(contentHash))
        {
            total += counter.CountTokens(contentHash);
        }

        for (var u = 0; u < units.Count; u++)
        {
            var unit = units[u];
            total += UnitScaffoldTokens + counter.CountTokens(unit.Receipt) + counter.CountTokens(unit.Kind);
            if (!string.IsNullOrEmpty(unit.Symbol))
            {
                total += counter.CountTokens(unit.Symbol);
            }
        }

        return total;
    }

    /// <summary>
    /// Applies the SDK dual-emission factor to a single-copy cost, rounding up so the
    /// estimate never under-states what the caller receives.
    /// </summary>
    /// <param name="singleCopyTokens">The single-copy cost in tokens.</param>
    /// <returns>The estimated cost of the emitted pair, in tokens.</returns>
    internal static int WithDualEmission(int singleCopyTokens)
    {
        if (singleCopyTokens <= 0)
        {
            return 0;
        }

        // Integer math with a ceiling, so the estimate rounds against the caller's
        // budget rather than in favour of it.
        return (int)(((long)singleCopyTokens * DualEmissionNumerator + DualEmissionDenominator - 1)
            / DualEmissionDenominator);
    }
}
