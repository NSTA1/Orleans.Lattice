namespace Orleans.Lattice.Embedding.Onnx;

/// <summary>
/// The pooling half of the sentence-transformers pipeline for
/// <c>nomic-embed-text-v1</c>: mean-pool the transformer's last hidden state
/// over the attention mask, then optionally L2-normalize.
/// </summary>
/// <remarks>
/// <para>
/// This is kept as a pure function over spans, separate from the ONNX session,
/// so the numerics are unit-testable without loading a half-gigabyte model.
/// The order (mean, then normalize) and the mask-aware divisor are what make the
/// output byte-comparable with the Onyx model server, whose
/// <c>modules.json</c> declares <c>Transformer -&gt; Pooling(mean) -&gt;
/// Normalize</c> with <c>pooling_mode_mean_tokens</c>.
/// </para>
/// <para>
/// The accumulator is <see cref="double"/> rather than <see cref="float"/>: the
/// sum runs over up to 512 tokens, and accumulating in single precision drifts
/// far enough from the reference implementation to move the cosine similarity
/// off 1.0. It writes into a caller-supplied destination span so the hot path
/// allocates exactly one vector per text, in the caller's own batch buffer.
/// </para>
/// </remarks>
internal static class EmbeddingPooling
{
    /// <summary>
    /// Mean-pools <paramref name="hiddenStates"/> over its first
    /// <paramref name="tokenCount"/> token rows and writes the result to
    /// <paramref name="destination"/>, optionally L2-normalizing it.
    /// </summary>
    /// <param name="hiddenStates">The row-major last hidden state for one text,
    /// laid out as <c>[sequenceLength, hiddenSize]</c>. Only the first
    /// <paramref name="tokenCount"/> rows are read, which is what applies the
    /// attention mask for right-padded input.</param>
    /// <param name="tokenCount">The number of real (unpadded) tokens.</param>
    /// <param name="hiddenSize">The model's hidden dimension.</param>
    /// <param name="normalize">Whether to L2-normalize the pooled vector.</param>
    /// <param name="destination">Receives the pooled vector. Must be at least
    /// <paramref name="hiddenSize"/> long.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="hiddenSize"/>
    /// or <paramref name="tokenCount"/> is not positive.</exception>
    /// <exception cref="ArgumentException">A span is too short for the declared
    /// shape.</exception>
    public static void MeanPool(
        ReadOnlySpan<float> hiddenStates,
        int tokenCount,
        int hiddenSize,
        bool normalize,
        Span<float> destination)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(hiddenSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(tokenCount);

        if (destination.Length < hiddenSize)
        {
            throw new ArgumentException(
                $"The destination span is {destination.Length} long but the hidden size is "
                + $"{hiddenSize}.",
                nameof(destination));
        }

        if (hiddenStates.Length < tokenCount * hiddenSize)
        {
            throw new ArgumentException(
                $"The hidden-state span is {hiddenStates.Length} long but {tokenCount} tokens of "
                + $"{hiddenSize} dimensions need {tokenCount * hiddenSize}.",
                nameof(hiddenStates));
        }

        Span<double> accumulator = hiddenSize <= 1024
            ? stackalloc double[hiddenSize]
            : new double[hiddenSize];
        accumulator.Clear();

        for (var token = 0; token < tokenCount; token++)
        {
            var row = hiddenStates.Slice(token * hiddenSize, hiddenSize);
            for (var dimension = 0; dimension < hiddenSize; dimension++)
            {
                accumulator[dimension] += row[dimension];
            }
        }

        double squaredSum = 0;
        for (var dimension = 0; dimension < hiddenSize; dimension++)
        {
            var mean = accumulator[dimension] / tokenCount;
            accumulator[dimension] = mean;
            squaredSum += mean * mean;
        }

        // A zero vector cannot be normalized; emit it unchanged rather than
        // dividing by zero and producing NaN, which would poison the index.
        var scale = 1.0;
        if (normalize)
        {
            var norm = Math.Sqrt(squaredSum);
            if (norm > 0)
            {
                scale = 1.0 / norm;
            }
        }

        for (var dimension = 0; dimension < hiddenSize; dimension++)
        {
            destination[dimension] = (float)(accumulator[dimension] * scale);
        }
    }
}
