namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Counts BPE tokens for a piece of text under a fixed tokenizer profile. It is the
/// shared seam the retrieval surface uses to measure and budget token economics, so
/// per-file token counts and per-answer ceilings are computed with the same encoding
/// an agent's model actually sees.
/// <para>
/// An implementation must construct its tokenizer once and reuse it for every call -
/// the counting path runs per file during reconcile - so the counting methods stay
/// allocation-frugal.
/// </para>
/// </summary>
internal interface IRepoContextTokenCounter
{
    /// <summary>
    /// Counts the BPE tokens in <paramref name="text"/>. An empty string counts as
    /// zero tokens.
    /// </summary>
    /// <param name="text">The text to count. Must not be <see langword="null"/>.</param>
    /// <returns>The number of tokens the configured encoding produces.</returns>
    int CountTokens(string text);

    /// <summary>
    /// Counts the BPE tokens in <paramref name="text"/>. An empty span counts as
    /// zero tokens. This overload avoids materialising a <see cref="string"/> when the
    /// caller already holds the characters in a span.
    /// </summary>
    /// <param name="text">The text to count.</param>
    /// <returns>The number of tokens the configured encoding produces.</returns>
    int CountTokens(ReadOnlySpan<char> text);
}
