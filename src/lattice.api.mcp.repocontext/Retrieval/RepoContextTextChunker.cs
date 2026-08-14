namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Splits a file's text into overlapping fixed-width windows so each window fits
/// inside the embedding model's context budget and content deep in a large file
/// becomes searchable. Whole-file embedding sees only the leading window of a
/// file; chunking embeds every window as its own passage, so a match far from the
/// top of a file still surfaces the file.
/// <para>
/// The split is deterministic and character-based: a code file has no reliable
/// sentence boundary, so a fixed window with a small overlap is both simple and
/// robust. The overlap keeps a declaration that straddles a window boundary intact
/// in at least one window. A file is capped at a bounded number of windows so a
/// single very large file cannot dominate an embed pass; content beyond the cap is
/// not embedded (its structural record still covers it for keyword recall).
/// </para>
/// </summary>
internal static class RepoContextTextChunker
{
    /// <summary>
    /// The default window width in characters. Sized below the model's 512-token
    /// context (code averages well under four characters per token) so a window
    /// embeds without the server truncating it.
    /// </summary>
    internal const int DefaultWindowChars = 1600;

    /// <summary>
    /// The default overlap in characters between adjacent windows, so a symbol or
    /// statement that straddles a boundary stays whole in at least one window.
    /// </summary>
    internal const int DefaultOverlapChars = 200;

    /// <summary>
    /// The default maximum number of windows emitted for a single file. Bounds the
    /// per-file embed cost so one very large file cannot dominate a run; content
    /// beyond the cap is left to keyword recall over the structural record.
    /// </summary>
    internal const int DefaultMaxChunks = 32;

    /// <summary>
    /// Splits <paramref name="text"/> into overlapping windows using the default
    /// window, overlap, and cap.
    /// </summary>
    /// <param name="text">The file text to split. Must not be <see langword="null"/>.</param>
    /// <returns>The ordered windows; a single window when the text fits, and an
    /// empty list when the text is null-empty or whitespace-only.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="text"/> is null.</exception>
    internal static IReadOnlyList<string> Chunk(string text) =>
        Chunk(text, DefaultWindowChars, DefaultOverlapChars, DefaultMaxChunks);

    /// <summary>
    /// Splits <paramref name="text"/> into overlapping windows of
    /// <paramref name="windowChars"/> characters that advance by
    /// <c>windowChars - overlapChars</c> each step, emitting at most
    /// <paramref name="maxChunks"/> windows. A window that is whitespace-only is
    /// skipped, since the embedding server rejects an empty passage.
    /// </summary>
    /// <param name="text">The file text to split. Must not be <see langword="null"/>.</param>
    /// <param name="windowChars">The window width in characters. Must be positive.</param>
    /// <param name="overlapChars">The overlap between adjacent windows in characters. Must be non-negative and less than <paramref name="windowChars"/>.</param>
    /// <param name="maxChunks">The maximum number of windows to emit. Must be positive.</param>
    /// <returns>The ordered windows; a single window when the text fits in one, and
    /// an empty list when the text is whitespace-only.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="text"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="windowChars"/>
    /// or <paramref name="maxChunks"/> is not positive, or
    /// <paramref name="overlapChars"/> is negative or not less than
    /// <paramref name="windowChars"/>.</exception>
    internal static IReadOnlyList<string> Chunk(
        string text, int windowChars, int overlapChars, int maxChunks)
    {
        ArgumentNullException.ThrowIfNull(text);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(windowChars);
        ArgumentOutOfRangeException.ThrowIfNegative(overlapChars);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(overlapChars, windowChars);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxChunks);

        if (string.IsNullOrWhiteSpace(text))
        {
            return Array.Empty<string>();
        }

        if (text.Length <= windowChars)
        {
            return new[] { text };
        }

        var step = windowChars - overlapChars;
        var chunks = new List<string>();
        for (var start = 0; start < text.Length && chunks.Count < maxChunks; start += step)
        {
            var length = Math.Min(windowChars, text.Length - start);
            var window = text.Substring(start, length);
            if (!string.IsNullOrWhiteSpace(window))
            {
                chunks.Add(window);
            }

            if (start + length >= text.Length)
            {
                break;
            }
        }

        return chunks;
    }
}
