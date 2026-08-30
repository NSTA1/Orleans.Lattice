namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// Reads the <c>aria-selected</c> attributes out of a plugin surface's rendered
/// markup, so a test can assert the WAI-ARIA contract for a tab strip or a
/// listbox by counting rather than by spot-checking one element.
/// </summary>
/// <remarks>
/// Counting is the assertion that actually catches the defect this exists for.
/// <c>aria-selected</c> is an <em>enumerated</em> ARIA attribute taking the
/// literal string <c>"true"</c> or <c>"false"</c>, but Blazor renders a
/// <see langword="bool"/> value as an HTML <em>boolean</em> attribute: the
/// selected element emits the bare attribute name - which the DOM reads back as
/// <c>aria-selected=""</c> - and every unselected one omits it entirely. A test
/// that only asserts the selected element carries <c>aria-selected</c> passes on
/// that broken form, because the attribute is present. Asserting that selected
/// plus unselected equals the item count, and that nothing carries a value other
/// than the two literals, does not.
/// </remarks>
internal static class PluginAriaMarkup
{
    private const string Name = "aria-selected";

    /// <summary>
    /// The tally of <c>aria-selected</c> attribute values in
    /// <paramref name="html"/>.
    /// </summary>
    /// <param name="html">The rendered markup to read.</param>
    /// <returns>How many read <c>"true"</c>, how many <c>"false"</c>, and how many neither.</returns>
    public static AriaSelectedTally TallyAriaSelected(string html)
    {
        ArgumentNullException.ThrowIfNull(html);

        var total = Count(html, Name);
        var yes = Count(html, Name + "=\"true\"");
        var no = Count(html, Name + "=\"false\"");

        // Anything left over is the boolean-attribute rendering: the bare
        // attribute name, or an explicit empty value. Derived rather than
        // matched literally, so no broken form can slip past by being spelled
        // in a way this file did not anticipate.
        return new AriaSelectedTally(yes, no, total - yes - no, total);
    }

    /// <summary>Counts non-overlapping ordinal occurrences of <paramref name="needle"/>.</summary>
    /// <param name="haystack">The text to search.</param>
    /// <param name="needle">The literal to count.</param>
    public static int Count(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
    }

    /// <summary>
    /// One surface's <c>aria-selected</c> tally.
    /// </summary>
    /// <param name="True">Elements reading the literal <c>"true"</c>.</param>
    /// <param name="False">Elements reading the literal <c>"false"</c>.</param>
    /// <param name="Invalid">
    /// Elements carrying the attribute with neither literal - the bare name or an
    /// empty value - which is what Blazor emits when the attribute was handed a
    /// <see langword="bool"/>. Always expected to be zero.
    /// </param>
    /// <param name="Total">Every occurrence of the attribute, whatever its value.</param>
    internal readonly record struct AriaSelectedTally(int True, int False, int Invalid, int Total)
    {
        /// <summary>How many elements carry a valid enumerated value.</summary>
        public int Valid => True + False;
    }
}
