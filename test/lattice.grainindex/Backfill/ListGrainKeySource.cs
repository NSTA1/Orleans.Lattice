using System.Runtime.CompilerServices;

namespace Orleans.Lattice.GrainIndex.Tests.Backfill;

/// <summary>
/// An <see cref="IGrainKeySource"/> over a fixed, ordered list of keys, which is
/// the deterministic stand-in for whatever an application would enumerate.
/// </summary>
/// <remarks>
/// It records the resume key of every enumeration and how many keys each one
/// yielded, so a test can prove a pass took exactly one batch and that the next
/// pass asked for the keys after it - without measuring time or racing a
/// scheduler.
/// </remarks>
internal sealed class ListGrainKeySource : IGrainKeySource
{
    private readonly List<string> _keys;

    /// <summary>Initialises a source over <paramref name="keys"/>, which it sorts ordinally.</summary>
    /// <param name="keys">The population to crawl. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="keys"/> is <c>null</c>.</exception>
    internal ListGrainKeySource(IEnumerable<string> keys)
    {
        ArgumentNullException.ThrowIfNull(keys);
        _keys = [.. keys.OrderBy(static k => k, StringComparer.Ordinal)];
    }

    /// <summary>The resume key each enumeration was asked to start after, in order.</summary>
    internal List<string?> ResumeKeys { get; } = [];

    /// <summary>The number of keys each enumeration actually yielded, in order.</summary>
    internal List<int> Yielded { get; } = [];

    /// <summary>An exception the enumeration throws on its first move, or <c>null</c>.</summary>
    internal Exception? Fault { get; set; }

    /// <summary>
    /// The population size this source reports, or <c>null</c> for a source that
    /// cannot bound itself - which is the default, and the shape every source
    /// written before the bound existed has.
    /// </summary>
    internal long? ApproximateCount { get; set; }

    /// <inheritdoc />
    public ValueTask<long?> TryGetApproximateCountAsync(CancellationToken cancellationToken) =>
        ValueTask.FromResult(ApproximateCount);

    /// <inheritdoc />
    public async IAsyncEnumerable<string> EnumerateKeysAsync(
        string? resumeAfterExclusive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ResumeKeys.Add(resumeAfterExclusive);
        var index = Yielded.Count;
        Yielded.Add(0);

        if (Fault is { } fault)
            throw fault;

        for (var i = 0; i < _keys.Count; i++)
        {
            var key = _keys[i];
            if (resumeAfterExclusive is not null && string.CompareOrdinal(key, resumeAfterExclusive) <= 0)
                continue;

            cancellationToken.ThrowIfCancellationRequested();
            Yielded[index]++;
            yield return key;
            await Task.Yield();
        }
    }
}
