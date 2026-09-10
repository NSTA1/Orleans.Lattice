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

    /// <summary>
    /// The environment variable that declares the intra-op thread count
    /// explicitly, overriding the derivation from the container CPU grant.
    /// </summary>
    public const string IntraOpThreadsKey = "EMBED_INTRA_THREADS";

    /// <summary>
    /// The value of <see cref="IntraOpThreadsKey"/> that hands the decision back
    /// to ONNX Runtime. Retained as an explicit escape hatch, but note that it
    /// is the setting this server stopped defaulting to: ONNX Runtime sizes its
    /// pool from the host core count and ignores the container CPU quota.
    /// </summary>
    public const int LetRuntimeChoose = 0;

    /// <summary>Absolute path to the ONNX model file.</summary>
    public required string ModelPath { get; init; }

    /// <summary>Absolute path to the WordPiece vocabulary file.</summary>
    public required string VocabPath { get; init; }

    /// <summary>The execution provider to run the session on.</summary>
    public EmbedExecutionProvider Provider { get; init; } = EmbedExecutionProvider.Cpu;

    /// <summary>The TCP port the HTTP listener binds.</summary>
    public int Port { get; init; } = DefaultPort;

    /// <summary>
    /// The resolved intra-op thread count for the CPU provider, with its
    /// provenance.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This previously defaulted to zero, documented as "zero lets ONNX Runtime
    /// pick, which is the right default under a container CPU quota". That claim
    /// was false and was measured to be false (issue #2606). ONNX Runtime sizes
    /// its intra-op pool from the host core count and does not consult the
    /// cgroup quota, so under a CPU limit it oversubscribes by the ratio between
    /// the two.
    /// </para>
    /// <para>
    /// Measured on the gate deployment: a 4.0-CPU grant
    /// (<c>cpu.max = "400000 100000"</c>) on a 16-core host produced an intra-op
    /// pool of 16, a 4x oversubscription. The kernel throttled the cgroup in
    /// <b>296 of 298</b> consecutive scheduling periods, and the pool spent
    /// 346.3 CPU-seconds stalled against 118.8 CPU-seconds running, a ratio of
    /// 2.91. That figure is not incidental: 16 threads exhaust a 400ms quota in
    /// 25ms of wall time and are then frozen for the remaining 75ms, predicting
    /// 75:25 = 3.0, which the measurement matched within 3%.
    /// </para>
    /// <para>
    /// The cost is worse than proportional because ONNX Runtime synchronises its
    /// intra-op threads at every operator boundary. A barrier requires every
    /// thread to be scheduled, so a freeze landing mid-barrier stalls the whole
    /// operator, and a transformer inference crosses hundreds of barriers.
    /// </para>
    /// </remarks>
    public IntraOpThreadCount IntraOpThreadCount { get; init; }

    /// <summary>
    /// The intra-op thread count handed to ONNX Runtime. Zero means the runtime
    /// chooses for itself, which is only safe when no CPU quota is enforced.
    /// </summary>
    public int IntraOpThreads => IntraOpThreadCount.Threads;

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
            IntraOpThreadCount = ResolveIntraOpThreads(
                read(IntraOpThreadsKey), ContainerCpuGrant.Read(), Environment.ProcessorCount),
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
    /// Resolves the intra-op thread count, preferring an explicit declaration,
    /// then the enforced container CPU grant, and only then the process's
    /// reported processor count.
    /// </summary>
    /// <remarks>
    /// The grant is preferred over <paramref name="processorCount"/> because the
    /// two can disagree and the grant is the one the kernel enforces.
    /// <c>DOTNET_PROCESSOR_COUNT</c> overrides
    /// <see cref="Environment.ProcessorCount"/> and wins over the quota, and the
    /// sample compose project sets that variable on the sibling
    /// repository-context service, so a deployment can arrive at a processor
    /// count that has nothing to do with what this container may actually use.
    /// Deriving from the quota is immune to that.
    /// </remarks>
    /// <param name="declared">The raw <see cref="IntraOpThreadsKey"/> value.</param>
    /// <param name="containerCpuGrant">The enforced CPU grant, or
    /// <see langword="null"/> when unlimited or unreadable.</param>
    /// <param name="processorCount">The process's reported processor count.</param>
    /// <returns>The resolved count and its provenance.</returns>
    public static IntraOpThreadCount ResolveIntraOpThreads(
        string? declared, int? containerCpuGrant, int processorCount)
    {
        if (int.TryParse((declared ?? string.Empty).Trim(), out var parsed) && parsed >= 0)
        {
            return new IntraOpThreadCount(parsed, IntraOpThreadSource.Declared);
        }

        return containerCpuGrant is int grant
            ? new IntraOpThreadCount(
                Math.Max(1, grant), IntraOpThreadSource.ContainerCpuGrant)
            : new IntraOpThreadCount(
                Math.Max(1, processorCount), IntraOpThreadSource.ProcessorCount);
    }

    /// <summary>
    /// Describes a disagreement between the enforced CPU grant and the process's
    /// reported processor count, which means something has overridden the
    /// latter.
    /// </summary>
    /// <remarks>
    /// This is reported rather than silently resolved because the disagreement
    /// is itself the interesting fact. A process that believes it has sixteen
    /// processors while the kernel grants it four will oversubscribe every pool
    /// sized from the former, not only this one.
    /// </remarks>
    /// <param name="containerCpuGrant">The enforced CPU grant, or
    /// <see langword="null"/> when unlimited or unreadable.</param>
    /// <param name="processorCount">The process's reported processor count.</param>
    /// <returns>A warning to log, or <see langword="null"/> when the two agree
    /// or no grant is enforced.</returns>
    public static string? DescribeProcessorCountDisagreement(
        int? containerCpuGrant, int processorCount)
    {
        if (containerCpuGrant is not int grant || grant == processorCount)
        {
            return null;
        }

        return $"CPU GRANT MISMATCH: the enforced container CPU grant is {grant} " +
            $"but Environment.ProcessorCount reports {processorCount}. Something is " +
            "overriding the processor count (DOTNET_PROCESSOR_COUNT is the usual " +
            $"cause). {IntraOpThreadsKey} has been derived from the enforced grant, " +
            "but any other pool sized from the processor count is oversubscribed by " +
            $"a factor of {(double)processorCount / grant:0.##}.";
    }

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
