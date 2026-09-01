using Microsoft.ML.OnnxRuntime;
using Microsoft.ML.OnnxRuntime.Tensors;

// Both ASP.NET Core and ONNX Runtime define a SessionOptions; this file means
// the ONNX Runtime one everywhere.
using SessionOptions = Microsoft.ML.OnnxRuntime.SessionOptions;

namespace Orleans.Lattice.Embedding.Onnx;

/// <summary>
/// The embedding engine: an ONNX Runtime session over
/// <c>nomic-embed-text-v1</c>, plus the tokenization and pooling that reproduce
/// the sentence-transformers pipeline the Onyx model server runs.
/// </summary>
/// <remarks>
/// <para>
/// The pipeline is, in order: WordPiece tokenize (lower-cased, truncated),
/// transformer forward pass, mean-pool over the attention mask, L2-normalize.
/// Run in fp32 this is numerically equivalent to the Onyx container - measured
/// at cosine 1.000000 across a corpus of real repository chunks - which is what
/// makes this image a drop-in that leaves already-stored vectors valid.
/// </para>
/// <para>
/// No asymmetric task prefix is applied. The Onyx server takes its prefixes from
/// the request's <c>manual_query_prefix</c> / <c>manual_passage_prefix</c>
/// fields, which the repository-context client never sends, so the reference
/// behaviour for this caller is "no prefix". Adding one here would silently
/// define a different embedding space.
/// </para>
/// <para>
/// The session is created once and reused. ONNX Runtime sessions are
/// thread-safe for concurrent <c>Run</c> calls, so the single instance is
/// registered as a singleton and needs no external locking.
/// </para>
/// </remarks>
internal sealed class OnnxEmbedder : IDisposable
{
    private readonly InferenceSession _session;
    private readonly WordPieceTokenizer _tokenizer;
    private readonly string[] _inputNames;
    private readonly int _maxContextLength;

    /// <summary>
    /// Loads the model and tokenizer and binds the configured execution
    /// provider.
    /// </summary>
    /// <param name="options">The resolved server options.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    public OnnxEmbedder(EmbedServerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _maxContextLength = options.MaxContextLength;

        var sessionOptions = CreateSessionOptions(options);
        try
        {
            _session = new InferenceSession(options.ModelPath, sessionOptions);
        }
        catch
        {
            sessionOptions.Dispose();
            throw;
        }

        _inputNames = [.. _session.InputMetadata.Keys];
        _tokenizer = WordPieceTokenizer.FromVocabularyFile(options.VocabPath);
        ActiveProvider = options.Provider.ToString();
        ModelName = Path.GetFileName(options.ModelPath);

        // The output is [batch, sequence, hidden]; the hidden size is the vector
        // dimension the mean-pool produces and is what the client's fail-closed
        // dimension check compares against.
        var outputShape = _session.OutputMetadata.Values.First().Dimensions;
        Dimension = outputShape.Length >= 3 && outputShape[^1] > 0 ? outputShape[^1] : 0;
    }

    /// <summary>The execution provider the session bound.</summary>
    public string ActiveProvider { get; }

    /// <summary>The file name of the loaded model.</summary>
    public string ModelName { get; }

    /// <summary>The vector dimension the model produces, or zero if not declared.</summary>
    public int Dimension { get; }

    /// <summary>
    /// Embeds a batch of texts, returning one vector per input in input order.
    /// </summary>
    /// <param name="texts">The texts to embed.</param>
    /// <param name="maxContextLength">The caller's requested token ceiling,
    /// clamped to the server's configured maximum.</param>
    /// <param name="normalize">Whether to L2-normalize the vectors.</param>
    /// <returns>The embedding vectors.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="texts"/> is null.</exception>
    public IReadOnlyList<float[]> Embed(
        IReadOnlyList<string> texts,
        int maxContextLength,
        bool normalize)
    {
        ArgumentNullException.ThrowIfNull(texts);

        if (texts.Count == 0)
        {
            return [];
        }

        // Clamp rather than honour the request, so a caller cannot drive
        // unbounded per-token work by asking for a huge context.
        var tokenCeiling = maxContextLength > 0
            ? Math.Min(maxContextLength, _maxContextLength)
            : _maxContextLength;

        var batch = texts.Count;
        var encoded = new long[batch][];
        var sequenceLength = 1;
        for (var i = 0; i < batch; i++)
        {
            encoded[i] = _tokenizer.Encode(texts[i], tokenCeiling);
            sequenceLength = Math.Max(sequenceLength, encoded[i].Length);
        }

        // Exact-size buffers rather than pooled ones: a rented array is not
        // zeroed, and stale ids left in the padding region would be silently
        // embedded as real tokens. Inference dominates this method by orders of
        // magnitude, so the allocation is not the cost that matters here.
        var total = batch * sequenceLength;
        var inputIds = new long[total];
        var attentionMask = new long[total];
        var tokenTypeIds = new long[total];

        for (var i = 0; i < batch; i++)
        {
            var row = encoded[i];
            var offset = i * sequenceLength;
            for (var j = 0; j < row.Length; j++)
            {
                inputIds[offset + j] = row[j];
                attentionMask[offset + j] = 1;
            }
        }

        var shape = new[] { batch, sequenceLength };
        var inputs = new List<NamedOnnxValue>(_inputNames.Length);
        foreach (var name in _inputNames)
        {
            var data = SelectInput(name, inputIds, attentionMask, tokenTypeIds);
            inputs.Add(NamedOnnxValue.CreateFromTensor(name, new DenseTensor<long>(data, shape)));
        }

        using var results = _session.Run(inputs);
        var hidden = results[0].AsTensor<float>();
        var hiddenSize = hidden.Dimensions[^1];

        var vectors = new float[batch][];

        // The dense fast path exposes the whole [batch, sequence, hidden] block
        // as one contiguous span, so each row pools without copying. The
        // fallback covers any non-dense tensor an EP might return.
        if (hidden is DenseTensor<float> dense)
        {
            var buffer = dense.Buffer.Span;
            for (var i = 0; i < batch; i++)
            {
                var vector = new float[hiddenSize];
                var row = buffer.Slice(i * sequenceLength * hiddenSize, sequenceLength * hiddenSize);
                EmbeddingPooling.MeanPool(
                    row, encoded[i].Length, hiddenSize, normalize, vector);
                vectors[i] = vector;
            }

            return vectors;
        }

        var scratch = new float[sequenceLength * hiddenSize];
        for (var i = 0; i < batch; i++)
        {
            for (var j = 0; j < sequenceLength; j++)
            {
                for (var d = 0; d < hiddenSize; d++)
                {
                    scratch[(j * hiddenSize) + d] = hidden[i, j, d];
                }
            }

            var vector = new float[hiddenSize];
            EmbeddingPooling.MeanPool(scratch, encoded[i].Length, hiddenSize, normalize, vector);
            vectors[i] = vector;
        }

        return vectors;
    }

    /// <inheritdoc />
    public void Dispose() => _session.Dispose();

    private static long[] SelectInput(
        string name, long[] inputIds, long[] attentionMask, long[] tokenTypeIds) =>
        name.Contains("attention", StringComparison.OrdinalIgnoreCase) ? attentionMask
        : name.Contains("token_type", StringComparison.OrdinalIgnoreCase) ? tokenTypeIds
        : inputIds;

    private static SessionOptions CreateSessionOptions(EmbedServerOptions options)
    {
        var sessionOptions = new SessionOptions();
        try
        {
            switch (options.Provider)
            {
                case EmbedExecutionProvider.Cuda:
                    sessionOptions.AppendExecutionProvider_CUDA(options.DeviceId);
                    break;

                case EmbedExecutionProvider.DirectML:
                    // DirectML requires sequential execution with memory-pattern
                    // reuse disabled; without both, session creation fails.
                    sessionOptions.ExecutionMode = ExecutionMode.ORT_SEQUENTIAL;
                    sessionOptions.EnableMemoryPattern = false;
                    sessionOptions.AppendExecutionProvider_DML(options.DeviceId);
                    break;

                default:
                    if (options.IntraOpThreads > 0)
                    {
                        sessionOptions.IntraOpNumThreads = options.IntraOpThreads;
                    }

                    break;
            }

            return sessionOptions;
        }
        catch
        {
            sessionOptions.Dispose();
            throw;
        }
    }
}
