using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Tests.Fakes;

/// <summary>
/// Builds an NSubstitute-backed <see cref="ISystemLattice"/> substitute
/// wired to an in-memory <see cref="SortedDictionary{TKey, TValue}"/>.
/// Lives as a builder rather than a concrete <c>ISystemLattice</c>
/// implementation so Orleans's TestCluster grain-type discovery does
/// not pick up the test type as a competing implementation of the
/// system-lattice grain interface.
/// </summary>
internal static class FakeSystemLattice
{
    /// <summary>
    /// Creates a fresh in-memory store and the
    /// <see cref="ISystemLattice"/> substitute that operates on it.
    /// Returns both so tests can inspect the underlying data.
    /// </summary>
    public static (ISystemLattice store, SortedDictionary<string, byte[]> data) Create()
    {
        var data = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        var store = Substitute.For<ISystemLattice>();

        store.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(data.TryGetValue(ci.Arg<string>(), out var v) ? v : null));

        store.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                data[ci.Arg<string>()] = ci.Arg<byte[]>();
                return Task.CompletedTask;
            });

        store.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(data.Remove(ci.Arg<string>())));

        store.ExistsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(data.ContainsKey(ci.Arg<string>())));

        store.EntriesAsync(
                Arg.Any<string?>(),
                Arg.Any<string?>(),
                Arg.Any<bool>(),
                Arg.Any<bool?>(),
                Arg.Any<CancellationToken>())
            .Returns(ci => RangeEntries(data, ci.ArgAt<string?>(0), ci.ArgAt<string?>(1), ci.ArgAt<bool>(2), ci.ArgAt<CancellationToken>(4)));

        store.KeysAsync(
                Arg.Any<string?>(),
                Arg.Any<string?>(),
                Arg.Any<bool>(),
                Arg.Any<bool?>(),
                Arg.Any<CancellationToken>())
            .Returns(ci => RangeKeys(data, ci.ArgAt<string?>(0), ci.ArgAt<string?>(1), ci.ArgAt<bool>(2), ci.ArgAt<CancellationToken>(4)));

        return (store, data);
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> RangeEntries(
        SortedDictionary<string, byte[]> data,
        string? start,
        string? end,
        bool reverse,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        var pairs = data.AsEnumerable();
        if (reverse) pairs = pairs.Reverse();
        foreach (var kvp in pairs)
        {
            ct.ThrowIfCancellationRequested();
            if (start is not null && string.CompareOrdinal(kvp.Key, start) < 0) continue;
            if (end is not null && string.CompareOrdinal(kvp.Key, end) >= 0) continue;
            yield return kvp;
            await Task.Yield();
        }
    }

    private static async IAsyncEnumerable<string> RangeKeys(
        SortedDictionary<string, byte[]> data,
        string? start,
        string? end,
        bool reverse,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var kvp in RangeEntries(data, start, end, reverse, ct).ConfigureAwait(false))
        {
            yield return kvp.Key;
        }
    }
}
