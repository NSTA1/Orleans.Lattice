using System.Runtime.CompilerServices;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Fakes;

/// <summary>
/// Wraps a source and records, per enumeration, whether the consumer read it all
/// the way to the end. That distinction is the whole subject of the completeness
/// fixtures: a build may only report a finished corpus on the strength of the
/// source signalling exhaustion, and the only way to observe that signal from
/// outside is to watch whether the enumerator was actually drained.
/// </summary>
internal sealed class ObservingVectorSource(IVectorSource inner) : IVectorSource
{
    private readonly IVectorSource _inner = inner;

    /// <summary>Number of enumerations the consumer started.</summary>
    internal int Enumerations { get; private set; }

    /// <summary>Number of enumerations the consumer drained to the end.</summary>
    internal int Drained { get; private set; }

    /// <summary>Entries yielded across every enumeration.</summary>
    internal int Yielded { get; private set; }

    /// <summary>Number of times the corpus size was requested.</summary>
    internal int Counts { get; private set; }

    public int Dimensions => _inner.Dimensions;

    public Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        Counts++;
        return _inner.CountAsync(cancellationToken);
    }

    public Task<bool> ContainsAsync(string id, CancellationToken cancellationToken = default)
        => _inner.ContainsAsync(id, cancellationToken);

    public async IAsyncEnumerable<VectorSourceEntry> EnumerateAsync(
        string? afterIdExclusive,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Enumerations++;
        await foreach (var entry in _inner
            .EnumerateAsync(afterIdExclusive, cancellationToken)
            .WithCancellation(cancellationToken)
            .ConfigureAwait(false))
        {
            Yielded++;
            yield return entry;
        }

        // Only reached when the consumer did not break out early, which is
        // precisely the signal a build is entitled to treat as exhaustion.
        Drained++;
    }
}
