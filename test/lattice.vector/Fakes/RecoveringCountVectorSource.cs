using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Fakes;

/// <summary>
/// A source whose count fails a fixed number of times and then succeeds, so a
/// fixture can prove the build RETRIES a count it failed to obtain rather than
/// giving up on it for the life of the index.
/// <para>
/// <see cref="CountFailingVectorSource"/> fails forever, which pins the
/// degradation but cannot distinguish "never retried" from "retried and still
/// failing" - the two readings a recovery fix has to tell apart.
/// </para>
/// </summary>
internal sealed class RecoveringCountVectorSource(ListVectorSource inner, int failures) : IVectorSource
{
    /// <summary>How many times the build asked for a count.</summary>
    internal int CountAttempts { get; private set; }

    public int Dimensions => inner.Dimensions;

    public IAsyncEnumerable<VectorSourceEntry> EnumerateAsync(
        string? afterIdExclusive, CancellationToken cancellationToken = default)
        => inner.EnumerateAsync(afterIdExclusive, cancellationToken);

    public Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        CountAttempts++;
        return CountAttempts <= failures
            ? Task.FromException<int>(new InvalidOperationException("no count yet"))
            : inner.CountAsync(cancellationToken);
    }

    public Task<bool> ContainsAsync(string id, CancellationToken cancellationToken = default)
        => inner.ContainsAsync(id, cancellationToken);
}
