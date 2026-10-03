namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// Cuts text for a clipped preview without splitting a character in two.
/// </summary>
/// <remarks>
/// A character outside the Basic Multilingual Plane, such as an emoji, is two UTF-16
/// code units, a high surrogate followed by a low one. A cut between them leaves a lone
/// surrogate, which is not text: the page is sent as UTF-8, which has no encoding for it,
/// so it is drawn as the replacement character. The cut therefore steps back over a
/// trailing high surrogate, keeping the whole character out rather than half of it.
/// </remarks>
internal static class LtTextCut
{
    /// <summary>
    /// The first <paramref name="length"/> code units of <paramref name="text"/>, or one
    /// fewer when the last of them would be the first half of a surrogate pair; the whole
    /// text when it is no longer than <paramref name="length"/>, and nothing when
    /// <paramref name="length"/> is zero or less.
    /// </summary>
    /// <param name="text">The text to cut.</param>
    /// <param name="length">The most code units to keep.</param>
    /// <returns>The kept start of the text.</returns>
    public static ReadOnlySpan<char> Prefix(string text, int length)
    {
        ArgumentNullException.ThrowIfNull(text);
        if (length >= text.Length)
        {
            return text;
        }

        if (length <= 0)
        {
            return [];
        }

        return text.AsSpan(0, char.IsHighSurrogate(text[length - 1]) ? length - 1 : length);
    }
}
