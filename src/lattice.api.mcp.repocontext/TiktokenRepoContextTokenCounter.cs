using Microsoft.ML.Tokenizers;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The default <see cref="IRepoContextTokenCounter"/>, backed by
/// <see cref="TiktokenTokenizer"/> from <c>Microsoft.ML.Tokenizers</c>. The tokenizer
/// is constructed once from the configured
/// <see cref="RepoContextIndexingOptions.TokenizerProfile"/> and reused for every
/// count, so the per-file counting path allocates nothing beyond what the tokenizer
/// itself needs and never rebuilds the vocabulary. Registered as a singleton.
/// </summary>
internal sealed class TiktokenRepoContextTokenCounter : IRepoContextTokenCounter
{
    private readonly Tokenizer _tokenizer;

    /// <summary>
    /// Creates the counter, constructing the tiktoken tokenizer once for the profile
    /// selected by <paramref name="options"/>. The vocabulary is loaded from the
    /// embedded data package for the resolved encoding.
    /// </summary>
    /// <param name="options">The indexing options carrying the tokenizer profile.
    /// Must not be <see langword="null"/>.</param>
    public TiktokenRepoContextTokenCounter(RepoContextIndexingOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _tokenizer = TiktokenTokenizer.CreateForEncoding(ResolveEncoding(options.TokenizerProfile));
    }

    /// <inheritdoc />
    public int CountTokens(string text)
    {
        ArgumentNullException.ThrowIfNull(text);
        return text.Length == 0 ? 0 : _tokenizer.CountTokens(text);
    }

    /// <inheritdoc />
    public int CountTokens(ReadOnlySpan<char> text) =>
        text.IsEmpty ? 0 : _tokenizer.CountTokens(text);

    /// <summary>
    /// Maps a resolved (already-validated) tokenizer profile to the tiktoken encoding
    /// name whose vocabulary data package is referenced. Any value other than the
    /// cl100k profile resolves to o200k_base, matching the options' fail-closed
    /// default.
    /// </summary>
    /// <param name="profile">The resolved tokenizer profile.</param>
    /// <returns>The tiktoken encoding name.</returns>
    private static string ResolveEncoding(string profile) => profile switch
    {
        RepoContextIndexingOptions.TokenizerProfileCl100k => "cl100k_base",
        _ => "o200k_base",
    };
}
