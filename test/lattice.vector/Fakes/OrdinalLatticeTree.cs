using NSubstitute;

namespace Orleans.Lattice.Vector.Tests.Fakes;

/// <summary>
/// An <see cref="ILattice"/> that behaves the way a real tree does for the small
/// set of operations the durable index uses: ordinal key order, half-open ranges,
/// and per-key atomic writes.
/// <para>
/// It exists so the adapter and the engine can be exercised together without a
/// silo. Everything the index does not use is left unconfigured, so a future call
/// to an unmodelled operation surfaces as an obviously wrong default rather than
/// as a silently plausible one.
/// </para>
/// </summary>
internal static class OrdinalLatticeTree
{
    internal static ILattice Create()
    {
        var records = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        var tree = Substitute.For<ILattice>();

        tree.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(
                records.TryGetValue(call.ArgAt<string>(0), out var value) ? value : null));

        tree.GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var found = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                foreach (var key in call.ArgAt<List<string>>(0))
                {
                    if (records.TryGetValue(key, out var value))
                    {
                        found[key] = value;
                    }
                }

                return Task.FromResult(found);
            });

        tree.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                foreach (var entry in call.ArgAt<List<KeyValuePair<string, byte[]>>>(0))
                {
                    records[entry.Key] = entry.Value;
                }

                return Task.CompletedTask;
            });

        tree.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(records.Remove(call.ArgAt<string>(0))));

        tree.DeleteRangeAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var doomed = Range(records, call.ArgAt<string>(0), call.ArgAt<string>(1))
                    .Select(entry => entry.Key)
                    .ToArray();

                foreach (var key in doomed)
                {
                    records.Remove(key);
                }

                return Task.FromResult(doomed.Length);
            });

        tree.EntriesAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(),
                Arg.Any<CancellationToken>())
            .Returns(call => Enumerate(
                Range(records, call.ArgAt<string?>(0), call.ArgAt<string?>(1)).ToArray()));

        tree.KeysAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(),
                Arg.Any<CancellationToken>())
            .Returns(call => EnumerateKeys(
                Range(records, call.ArgAt<string?>(0), call.ArgAt<string?>(1))
                    .Select(entry => entry.Key)
                    .ToArray()));

        return tree;
    }

    private static IEnumerable<KeyValuePair<string, byte[]>> Range(
        SortedDictionary<string, byte[]> records, string? startInclusive, string? endExclusive) =>
        records.Where(entry =>
            (startInclusive is null || string.CompareOrdinal(entry.Key, startInclusive) >= 0)
            && (endExclusive is null || string.CompareOrdinal(entry.Key, endExclusive) < 0));

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Enumerate(
        KeyValuePair<string, byte[]>[] page)
    {
        foreach (var entry in page)
        {
            yield return entry;
            await Task.CompletedTask.ConfigureAwait(false);
        }
    }

    private static async IAsyncEnumerable<string> EnumerateKeys(string[] page)
    {
        foreach (var key in page)
        {
            yield return key;
            await Task.CompletedTask.ConfigureAwait(false);
        }
    }
}
