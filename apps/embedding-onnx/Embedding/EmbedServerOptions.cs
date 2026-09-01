namespace Orleans.Lattice.Embedding.Onnx;

/// <summary>
/// The server's startup configuration, read once from environment variables so
/// the container is configured the same way the rest of the repository-context
/// stack is (no config file, no command line).
/// </summary>
/// <remarks>
/// Every knob has a working default except the two asset paths, which are baked
/// into the image by the Dockerfile. Parsing is deliberately lenient: an
/// unparseable numeric or an unknown provider name falls back to the default
/// rather than aborting startup, because a model server that refuses to boot is
/// strictly worse for the caller than one that boots on the CPU. The one
/// exception is a missing model or vocabulary file, which is fatal - serving
/// wrong vectors is worse than serving none.
/// </remarks>
internal sealed record EmbedServerOptions
{
    /// <summary>
    /// The default listen port. Deliberately identical to the Onyx companion
    /// image's port so this server is a drop-in for it: the repository-context
    /// client's <c>LATTICE_EMBEDDING_ENDPOINT</c> (for example
    /// <c>http://embedder:9000</c>) needs no change when the image is swapped.
    /// </summary>
    public const int DefaultPort = 9000;

    /// <summary>The default maximum context length, matching the model card and
    /// the <c>OnyxEmbeddingOptions</c> default on the client.</summary>
    public const int DefaultMaxContextLength = 512;

    /// <summary>Absolute path to the ONNX model file.</summary>
    public required string ModelPath { get; init; }

    /// <summary>Absolute path to the WordPiece vocabulary file.</summary>
    public required string VocabPath { get; init; }

    /// <summary>The execution provider to run the session on.</summary>
    public EmbedExecutionProvider Provider { get; init; } = EmbedExecutionProvider.Cpu;

    /// <summary>The TCP port the HTTP listener binds.</summary>
    public int Port { get; init; } = DefaultPort;

    /// <summary>
    /// Intra-op thread count for the CPU provider. Zero lets ONNX Runtime pick,
    /// which is the right default under a container CPU quota.
    /// </summary>
    public int IntraOpThreads { get; init; }

    /// <summary>
    /// The device ordinal for an accelerated provider. Ignored by the CPU
    /// provider.
    /// </summary>
    public int DeviceId { get; init; }

    /// <summary>
    /// The hard ceiling on tokens per text. A request asking for more is clamped
    /// to this, so a caller cannot drive unbounded work by asking for a huge
    /// context.
    /// </summary>
    public int MaxContextLength { get; init; } = DefaultMaxContextLength;

    /// <summary>
    /// Reads the options from the supplied environment accessor.
    /// </summary>
    /// <param name="read">Reads a named environment variable, returning
    /// <see langword="null"/> when unset.</param>
    /// <returns>The resolved options.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="read"/> is null.</exception>
    /// <exception cref="InvalidOperationException">A required asset path is
    /// unset, or points at a file that does not exist.</exception>
    public static EmbedServerOptions FromEnvironment(Func<string, string?> read)
    {
        ArgumentNullException.ThrowIfNull(read);

        var modelPath = Require(read, "EMBED_MODEL_PATH");
        var vocabPath = Require(read, "EMBED_VOCAB_PATH");

        return new EmbedServerOptions
        {
            ModelPath = modelPath,
            VocabPath = vocabPath,
            Provider = ParseProvider(read("EMBED_PROVIDER")),
            Port = ParsePositiveInt(read("EMBED_PORT"), DefaultPort),
            IntraOpThreads = ParseNonNegativeInt(read("EMBED_INTRA_THREADS"), 0),
            DeviceId = ParseNonNegativeInt(read("EMBED_DEVICE_ID"), 0),
            MaxContextLength = ParsePositiveInt(
                read("EMBED_MAX_CONTEXT_LENGTH"), DefaultMaxContextLength),
        };
    }

    /// <summary>
    /// Maps an <c>EMBED_PROVIDER</c> value to a provider, falling back to
    /// <see cref="EmbedExecutionProvider.Cpu"/> for null, empty, or unknown
    /// input.
    /// </summary>
    /// <param name="value">The raw environment value.</param>
    /// <returns>The resolved provider.</returns>
    public static EmbedExecutionProvider ParseProvider(string? value) =>
        (value ?? string.Empty).Trim().ToLowerInvariant() switch
        {
            "cuda" or "gpu" or "nvidia" => EmbedExecutionProvider.Cuda,
            "dml" or "directml" => EmbedExecutionProvider.DirectML,
            _ => EmbedExecutionProvider.Cpu,
        };

    /// <summary>
    /// Resolves the listen port from a raw environment value, falling back to
    /// <see cref="DefaultPort"/>. Shared with the health probe so the probe
    /// always targets the port the server actually bound.
    /// </summary>
    /// <param name="value">The raw <c>EMBED_PORT</c> value.</param>
    /// <returns>The resolved port.</returns>
    public static int ParsePositivePort(string? value) => ParsePositiveInt(value, DefaultPort);

    private static string Require(Func<string, string?> read, string name)
    {
        var value = read(name);
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new InvalidOperationException(
                $"The environment variable '{name}' is required but was not set.");
        }

        if (!File.Exists(value))
        {
            throw new InvalidOperationException(
                $"The environment variable '{name}' points at '{value}', which does not exist.");
        }

        return value;
    }

    private static int ParsePositiveInt(string? value, int fallback) =>
        int.TryParse(value, out var parsed) && parsed > 0 ? parsed : fallback;

    private static int ParseNonNegativeInt(string? value, int fallback) =>
        int.TryParse(value, out var parsed) && parsed >= 0 ? parsed : fallback;
}
