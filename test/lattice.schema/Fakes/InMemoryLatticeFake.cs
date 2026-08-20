using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Builds an <see cref="ILattice"/> substitute backed by an in-memory
/// ordinal-sorted byte store. It implements exactly the byte-level surface the
/// reserved-tree schema stores dogfood - point get / set / delete, ranged count,
/// and forward range enumeration - so the typed <c>ILattice</c> extension methods
/// (JSON serialize / deserialize, resilient scan) run against it end-to-end with
/// no cluster. Deterministic and race-free.
/// </summary>
internal static class InMemoryLatticeFake
{
    public static ILattice Create(SortedDictionary<string, byte[]> store)
    {
        ArgumentNullException.ThrowIfNull(store);
        var lattice = Substitute.For<ILattice>();

        lattice.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult<byte[]?>(
                store.TryGetValue(ci.ArgAt<string>(0), out var value) ? value : null));

        lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                store[ci.ArgAt<string>(0)] = ci.ArgAt<byte[]>(1);
                return Task.CompletedTask;
            });

        lattice.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(store.Remove(ci.ArgAt<string>(0))));

        lattice.CountAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(
                Range(store, ci.ArgAt<string?>(0), ci.ArgAt<string?>(1)).Count()));

        lattice.EntriesAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci => ToAsync(Range(store, ci.ArgAt<string?>(0), ci.ArgAt<string?>(1))));

        return lattice;
    }

    private static IEnumerable<KeyValuePair<string, byte[]>> Range(
        SortedDictionary<string, byte[]> store, string? startInclusive, string? endExclusive)
    {
        foreach (var pair in store)
        {
            if (startInclusive is not null &&
                string.CompareOrdinal(pair.Key, startInclusive) < 0)
            {
                continue;
            }

            if (endExclusive is not null &&
                string.CompareOrdinal(pair.Key, endExclusive) >= 0)
            {
                continue;
            }

            yield return pair;
        }
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> ToAsync(
        IEnumerable<KeyValuePair<string, byte[]>> source)
    {
        foreach (var pair in source)
        {
            yield return pair;
        }

        await Task.CompletedTask;
    }
}
