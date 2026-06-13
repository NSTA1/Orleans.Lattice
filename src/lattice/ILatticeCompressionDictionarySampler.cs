namespace Orleans.Lattice;

/// <summary>
/// Optional companion to <see cref="ILatticeCompressionDictionaryProvider"/>
/// that accepts representative payload bytes for possible inclusion in a
/// training corpus. A provider implements this when it trains a shared
/// compression dictionary at runtime - the auto-training provider samples a
/// bounded reservoir of the bytes fed here and periodically trains a dictionary
/// off the hot path. The opted-in replication capture path feeds every
/// locally-originating value into <see cref="Observe"/> so training needs no
/// host code. Providers that do not train (the operator-supplied provider) do
/// not implement this interface; the capture path then samples nothing.
/// </summary>
public interface ILatticeCompressionDictionarySampler
{
    /// <summary>
    /// Observes a payload for possible inclusion in the training corpus. The
    /// implementation copies any bytes it retains, so the caller may reuse
    /// <paramref name="payload"/> after the call returns. Implementations must
    /// be safe to call concurrently and must never throw for an ordinary
    /// payload; a disabled or saturated sampler simply drops the sample.
    /// </summary>
    /// <param name="payload">The payload bytes to sample.</param>
    void Observe(ReadOnlySpan<byte> payload);
}
