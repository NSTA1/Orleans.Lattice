using FastBertTokenizer;

namespace Orleans.Lattice.Embedding.Onnx;

/// <summary>
/// The WordPiece tokenizer for <c>nomic-embed-text-v1</c>, wrapping
/// <c>FastBertTokenizer</c> behind a small seam so token output can be asserted
/// against a golden HuggingFace fixture without loading the ONNX model.
/// </summary>
/// <remarks>
/// <para>
/// The choice of tokenizer is load-bearing, not incidental. An implementation
/// that merely looks reasonable can silently produce a different token stream
/// for source code - dropping newlines, tabs, backticks, <c>=</c>, <c>&lt;</c>
/// and <c>&gt;</c> - which yields vectors that are still 768-dimensional, still
/// normalized, and still pass every structural check, while sitting measurably
/// off the reference embedding space. That failure is invisible without a
/// token-level comparison, which is why
/// <c>WordPieceTokenizerGoldenTests</c> pins this against HuggingFace output.
/// </para>
/// <para>
/// The vocabulary is lower-cased on input, matching the model's
/// <c>tokenizer_config.json</c> (<c>do_lower_case: true</c>).
/// </para>
/// </remarks>
internal sealed class WordPieceTokenizer
{
    private readonly BertTokenizer _tokenizer;

    private WordPieceTokenizer(BertTokenizer tokenizer) => _tokenizer = tokenizer;

    /// <summary>
    /// Loads the tokenizer from a WordPiece vocabulary file.
    /// </summary>
    /// <param name="vocabPath">Path to <c>vocab.txt</c>.</param>
    /// <returns>The loaded tokenizer.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="vocabPath"/> is null.</exception>
    public static WordPieceTokenizer FromVocabularyFile(string vocabPath)
    {
        ArgumentNullException.ThrowIfNull(vocabPath);

        var tokenizer = new BertTokenizer();
        using var reader = File.OpenText(vocabPath);
        tokenizer.LoadVocabulary(reader, convertInputToLowercase: true);
        return new WordPieceTokenizer(tokenizer);
    }

    /// <summary>
    /// Encodes one text to its input-id sequence, including the leading
    /// <c>[CLS]</c> and trailing <c>[SEP]</c>, truncated to
    /// <paramref name="maxTokens"/>.
    /// </summary>
    /// <param name="text">The text to encode. Null is treated as empty.</param>
    /// <param name="maxTokens">The inclusive token ceiling.</param>
    /// <returns>The input ids.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxTokens"/>
    /// is not positive.</exception>
    /// <remarks>
    /// This deliberately uses the caller-allocated span overload rather than the
    /// convenience overload that returns its own buffer. The convenience overload
    /// in FastBertTokenizer 1.0.28 reuses an internal buffer across calls and
    /// ignores a <em>smaller</em> ceiling once a larger one has been used, so a
    /// 512-token request followed by a 128-token request silently yields 512
    /// tokens. Writing into a span this method owns makes the ceiling bind by
    /// construction and keeps the tokenizer safe to share across requests.
    /// </remarks>
    public long[] Encode(string? text, int maxTokens)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxTokens);

        var inputIds = new long[maxTokens];
        var attentionMask = new long[maxTokens];
        var written = _tokenizer.Encode(text ?? string.Empty, inputIds, attentionMask, padTo: null);

        return written == maxTokens ? inputIds : inputIds[..written];
    }
}
