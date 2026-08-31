using NSubstitute;

namespace Orleans.Lattice.GrainIndex.Tests.Query;

/// <summary>
/// An in-memory stand-in for an index's backing tree, exposed as a substituted
/// <see cref="ILattice"/> so the query executor can be unit tested against the
/// real scan, cursor, and predicate-push-down surface with no cluster, no
/// TestingHost, and no timing.
/// <para>
/// Ordering, range bounds, and predicate evaluation are the production ones: the
/// store is ordinal-sorted exactly as a lattice tree is, and push-down is
/// answered by <see cref="LatticePredicateEvaluation.Matches"/>, which is the
/// same call the real leaf scan makes.
/// </para>
/// </summary>
internal sealed class FakeIndexTree
{
    private readonly SortedDictionary<string, byte[]> _entries = new(StringComparer.Ordinal);
    private readonly Dictionary<string, FakeCursor> _cursors = new(StringComparer.Ordinal);
    private int _nextCursor;

    internal FakeIndexTree()
    {
        Lattice = Substitute.For<ILattice>();
        Configure();
    }

    /// <summary>The substituted tree the executor talks to.</summary>
    internal ILattice Lattice { get; }

    /// <summary>Cursor ids opened but not yet closed.</summary>
    internal IReadOnlyCollection<string> OpenCursors => _cursors.Keys;

    /// <summary>How many cursors have been opened over this tree's lifetime.</summary>
    internal int CursorsOpened => _nextCursor;

    /// <summary>Writes one entry.</summary>
    internal void Put(string key, byte[] value) => _entries[key] = value;

    private void Configure()
    {
        Lattice.KeysAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(call => ToAsync(Select(call.ArgAt<string?>(0), call.ArgAt<string?>(1), null), static pair => pair.Key));

        Lattice.KeysWherePredicateAsync(
                Arg.Any<LatticePredicateNode>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(call => ToAsync(
                Select(call.ArgAt<string?>(1), call.ArgAt<string?>(2), call.ArgAt<LatticePredicateNode>(0)),
                static pair => pair.Key));

        Lattice.EntriesAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(call => ToAsync(Select(call.ArgAt<string?>(0), call.ArgAt<string?>(1), null), static pair => pair));

        Lattice.EntriesWherePredicateAsync(
                Arg.Any<LatticePredicateNode>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(call => ToAsync(
                Select(call.ArgAt<string?>(1), call.ArgAt<string?>(2), call.ArgAt<LatticePredicateNode>(0)),
                static pair => pair));

        Lattice.OpenKeyCursorAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(Open(call.ArgAt<string?>(0), call.ArgAt<string?>(1), null)));

        Lattice.OpenEntryCursorAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(Open(call.ArgAt<string?>(0), call.ArgAt<string?>(1), null)));

        Lattice.OpenSnapshotKeyCursorAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(Open(call.ArgAt<string?>(0), call.ArgAt<string?>(1), null)));

        Lattice.OpenSnapshotEntryCursorAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(Open(call.ArgAt<string?>(0), call.ArgAt<string?>(1), null)));

        Lattice.OpenKeyCursorWherePredicateAsync(
                Arg.Any<LatticePredicateNode>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(Open(call.ArgAt<string?>(1), call.ArgAt<string?>(2), call.ArgAt<LatticePredicateNode>(0))));

        Lattice.OpenEntryCursorWherePredicateAsync(
                Arg.Any<LatticePredicateNode>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(Open(call.ArgAt<string?>(1), call.ArgAt<string?>(2), call.ArgAt<LatticePredicateNode>(0))));

        Lattice.OpenSnapshotKeyCursorWherePredicateAsync(
                Arg.Any<LatticePredicateNode>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(Open(call.ArgAt<string?>(1), call.ArgAt<string?>(2), call.ArgAt<LatticePredicateNode>(0))));

        Lattice.OpenSnapshotEntryCursorWherePredicateAsync(
                Arg.Any<LatticePredicateNode>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(Open(call.ArgAt<string?>(1), call.ArgAt<string?>(2), call.ArgAt<LatticePredicateNode>(0))));

        Lattice.NextKeysAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var page = Page(call.ArgAt<string>(0), call.ArgAt<int>(1));
                var keys = new string[page.Rows.Count];
                for (var i = 0; i < page.Rows.Count; i++)
                {
                    keys[i] = page.Rows[i].Key;
                }

                return Task.FromResult(new LatticeCursorKeysPage { Keys = keys, HasMore = page.HasMore });
            });

        Lattice.NextEntriesAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var page = Page(call.ArgAt<string>(0), call.ArgAt<int>(1));
                return Task.FromResult(new LatticeCursorEntriesPage { Entries = page.Rows, HasMore = page.HasMore });
            });

        Lattice.CloseCursorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                _cursors.Remove(call.ArgAt<string>(0));
                return Task.CompletedTask;
            });
    }

    private string Open(string? start, string? end, LatticePredicateNode? predicate)
    {
        string id = "cursor-" + (++_nextCursor).ToString(System.Globalization.CultureInfo.InvariantCulture);
        _cursors[id] = new FakeCursor(Select(start, end, predicate));
        return id;
    }

    private (IReadOnlyList<KeyValuePair<string, byte[]>> Rows, bool HasMore) Page(string cursorId, int pageSize)
    {
        if (!_cursors.TryGetValue(cursorId, out var cursor))
            throw new InvalidOperationException($"Cursor '{cursorId}' is not open.");

        return cursor.Next(pageSize);
    }

    private List<KeyValuePair<string, byte[]>> Select(string? start, string? end, LatticePredicateNode? predicate)
    {
        var rows = new List<KeyValuePair<string, byte[]>>();
        foreach (var pair in _entries)
        {
            if (start is not null && string.CompareOrdinal(pair.Key, start) < 0)
                continue;
            if (end is not null && string.CompareOrdinal(pair.Key, end) >= 0)
                continue;
            if (predicate is { } node && !LatticePredicateEvaluation.Matches(pair.Value, in node))
                continue;

            rows.Add(pair);
        }

        return rows;
    }

    private static async IAsyncEnumerable<TResult> ToAsync<TResult>(
        List<KeyValuePair<string, byte[]>> rows,
        Func<KeyValuePair<string, byte[]>, TResult> projection)
    {
        foreach (var row in rows)
        {
            await Task.Yield();
            yield return projection(row);
        }
    }

    private sealed class FakeCursor(List<KeyValuePair<string, byte[]>> rows)
    {
        private int _position;

        internal (IReadOnlyList<KeyValuePair<string, byte[]>> Rows, bool HasMore) Next(int pageSize)
        {
            int take = Math.Min(pageSize, rows.Count - _position);
            var page = rows.GetRange(_position, take);
            _position += take;
            return (page, _position < rows.Count);
        }
    }
}
