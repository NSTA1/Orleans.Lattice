namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// Renders a label <em>value</em> into the double-quoted form a PromQL label
/// matcher requires. It is the single place a value the facade did not author
/// itself - the caller's optional tree filter - is turned into query text, so the
/// escaping rule is written and tested once rather than repeated at each call
/// site.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists at all.</b> The facade never accepts a query expression, but
/// it does accept a bounded tree-filter <em>value</em> that it injects into a
/// server-authored template as a label matcher. A value carrying a quote or a
/// backslash would otherwise terminate the matcher early and let the remainder be
/// read as query syntax, which is precisely the injection this type forecloses.
/// </para>
/// <para>
/// PromQL string literals escape <c>\</c> as <c>\\</c> and <c>"</c> as <c>\"</c>.
/// A control character has no literal form inside a matcher and no legitimate tree
/// id carries one, so such a value is refused outright by
/// <see cref="IsRenderable(string)"/> rather than encoded.
/// </para>
/// <para>
/// The common case - a value needing no escape - returns the input reference
/// unchanged, so the ordinary path allocates nothing.
/// </para>
/// </remarks>
internal static class PromQlLabelValue
{
    /// <summary>
    /// Returns <paramref name="value"/> with the two characters a PromQL string
    /// literal must escape replaced by their escaped forms, or the same reference
    /// when no character needed escaping.
    /// </summary>
    /// <param name="value">The raw label value.</param>
    /// <returns>The escaped label value.</returns>
    public static string Escape(string value)
    {
        var span = value.AsSpan();
        if (span.IndexOfAny('\\', '"') < 0)
        {
            return value;
        }

        return string.Create(span.Length + CountEscapes(span), value, static (destination, source) =>
        {
            var written = 0;
            foreach (var ch in source)
            {
                if (ch is '\\' or '"')
                {
                    destination[written++] = '\\';
                }

                destination[written++] = ch;
            }
        });
    }

    /// <summary>
    /// <see langword="true"/> when <paramref name="value"/> can be rendered as a
    /// PromQL label value at all: a control character has no literal form inside a
    /// matcher, and no legitimate tree id carries one, so such a value is refused
    /// rather than silently mangled.
    /// </summary>
    /// <param name="value">The raw label value.</param>
    /// <returns><see langword="true"/> when the value is renderable.</returns>
    public static bool IsRenderable(string value)
    {
        foreach (var ch in value)
        {
            if (char.IsControl(ch))
            {
                return false;
            }
        }

        return true;
    }

    private static int CountEscapes(ReadOnlySpan<char> value)
    {
        var count = 0;
        foreach (var ch in value)
        {
            if (ch is '\\' or '"')
            {
                count++;
            }
        }

        return count;
    }
}
