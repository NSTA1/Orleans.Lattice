using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Fakes;

/// <summary>
/// A source that streams normally but fails when asked how many vectors it holds,
/// so a build can be observed against the one call whose own contract says it may
/// be wrong.
/// <para>
/// The two halves are deliberately independent. A source that failed to enumerate
/// would test nothing interesting - a build that cannot read its corpus has
/// genuinely failed - whereas a source that enumerates perfectly and only refuses
/// to count isolates the question that matters: does an unusable HINT stop a build
/// that has everything it actually needs?
/// </para>
/// </summary>
internal sealed class CountFailingVectorSource(ListVectorSource inner, Func<Exception> fault) : IVectorSource
{
    /// <summary>How many times the build asked for a count, so a fixture can prove it asked at all.</summary>
    internal int CountAttempts { get; private set; }

    public int Dimensions => inner.Dimensions;

    public IAsyncEnumerable<VectorSourceEntry> EnumerateAsync(
        string? afterIdExclusive, CancellationToken cancellationToken = default)
        => inner.EnumerateAsync(afterIdExclusive, cancellationToken);

    public Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        CountAttempts++;
        return Task.FromException<int>(fault());
    }

    public Task<bool> ContainsAsync(string id, CancellationToken cancellationToken = default)
        => inner.ContainsAsync(id, cancellationToken);
}
