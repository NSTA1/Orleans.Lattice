using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Explorer.UI.Areas.Data;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// An in-memory Data API range read for export tests: it pages a sorted key set the way
/// the facade does, reports a chosen gate coverage on a fresh scan that asks for it, and
/// can expire a continuation on demand. Every request it serves is recorded.
/// </summary>
internal sealed class FakeDataExportSource : IDataExportSource
{
    private readonly SortedDictionary<string, byte[]> _entries = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _cursors = new(StringComparer.Ordinal);
    private int _nextCursor;

    /// <summary>What the gate reports for a fresh scan that asks; <see langword="null"/> models a tree the caller cannot observe.</summary>
    public LatticeRangeReadGateCoverage? Coverage { get; set; } = LatticeRangeReadGateCoverage.Unrestricted;

    /// <summary>How many of the next continuation reads to fail as expired.</summary>
    public int ExpireNextContinuations { get; set; }

    /// <summary>Fails every read with this exception, when set.</summary>
    public Exception? Fault { get; set; }

    /// <summary>The requests served, in order.</summary>
    public List<DataRangeRequest> Requests { get; } = [];

    /// <summary>Whether the source was disposed.</summary>
    public bool Disposed { get; private set; }

    /// <summary>Adds an entry.</summary>
    public FakeDataExportSource With(string key, byte[] value)
    {
        _entries[key] = value;
        return this;
    }

    /// <inheritdoc />
    public Task<DataRangePage> ReadAsync(DataRangeRequest request, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Requests.Add(request);
        if (Fault is { } fault)
        {
            throw fault;
        }

        string? after = null;
        string? end;
        LatticeRangeReadGateCoverage? coverage = null;
        if (!string.IsNullOrEmpty(request.ContinuationToken))
        {
            if (ExpireNextContinuations > 0)
            {
                ExpireNextContinuations--;
                throw new DataExportCursorExpiredException();
            }

            var cursor = _cursors[request.ContinuationToken];
            var split = cursor.IndexOf('\u0001', StringComparison.Ordinal);
            after = cursor[..split];
            end = cursor[(split + 1)..];
            end = end.Length == 0 ? null : end;
        }
        else
        {
            end = request.EndExclusive;
            if (request.IncludeGateCoverage)
            {
                coverage = Coverage;
                if (coverage is null or LatticeRangeReadGateCoverage.Denied)
                {
                    return Task.FromResult(new DataRangePage { TreeId = request.TreeId, GateCoverage = coverage });
                }
            }
        }

        var page = new List<DataEntry>();
        string? last = null;
        var more = false;
        foreach (var (key, value) in _entries)
        {
            if (after is not null ? string.CompareOrdinal(key, after) <= 0 : request.StartInclusive is { } start && string.CompareOrdinal(key, start) < 0)
            {
                continue;
            }

            if (end is not null && string.CompareOrdinal(key, end) >= 0)
            {
                break;
            }

            if (page.Count == request.PageSize)
            {
                more = true;
                break;
            }

            page.Add(new DataEntry { Key = key, Value = value });
            last = key;
        }

        string? token = null;
        if (more)
        {
            token = "cursor-" + _nextCursor++;
            _cursors[token] = last + '\u0001' + (end ?? string.Empty);
        }

        return Task.FromResult(new DataRangePage { TreeId = request.TreeId, Entries = page, ContinuationToken = token, GateCoverage = coverage });
    }

    /// <inheritdoc />
    public void Dispose() => Disposed = true;
}
