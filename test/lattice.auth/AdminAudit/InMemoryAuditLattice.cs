using System.Runtime.CompilerServices;
using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Auth.Tests.AdminAudit;

/// <summary>
/// A dictionary-backed <see cref="ILattice"/> substitute for the admin audit tests:
/// point reads and writes with a strictly increasing version per write, create-only
/// <see cref="ILattice.SetIfVersionAsync"/> against <see cref="HybridLogicalClock.Zero"/>,
/// ordered range scans, and a scripted history page.
/// </summary>
internal sealed class InMemoryAuditLattice
{
    private readonly SortedDictionary<string, (byte[] Value, HybridLogicalClock Version)> _rows = new(StringComparer.Ordinal);
    private long _clock = 1_000;

    public InMemoryAuditLattice()
    {
        Lattice = Substitute.For<ILattice>();
        Lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                if (FailWrites)
                {
                    return Task.FromException(new TimeoutException("store unavailable"));
                }

                Put(call.ArgAt<string>(0), call.ArgAt<byte[]>(1));
                return Task.CompletedTask;
            });
        Lattice.SetIfVersionAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var key = call.ArgAt<string>(0);
                var expected = call.ArgAt<HybridLogicalClock>(2);
                var current = _rows.TryGetValue(key, out var row) ? row.Version : HybridLogicalClock.Zero;
                if (!current.Equals(expected))
                {
                    return Task.FromResult(false);
                }

                Put(key, call.ArgAt<byte[]>(1));
                return Task.FromResult(true);
            });
        Lattice.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(_rows.Remove(call.ArgAt<string>(0))));
        Lattice.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(_rows.TryGetValue(call.ArgAt<string>(0), out var row) ? row.Value : null));
        Lattice.GetWithVersionAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(_rows.TryGetValue(call.ArgAt<string>(0), out var row)
                ? new VersionedValue { Value = row.Value, Version = row.Version }
                : new VersionedValue { Value = null, Version = HybridLogicalClock.Zero }));
        Lattice.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(call => Range(call.ArgAt<string?>(0), call.ArgAt<string?>(1)));
        Lattice.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(call => Keys(call.ArgAt<string?>(0), call.ArgAt<string?>(1)));
        Lattice.ScanEntryHistoryAsync(
                Arg.Any<string>(), Arg.Any<HybridLogicalClock?>(), Arg.Any<HybridLogicalClock?>(), Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(History));
    }

    /// <summary>The substitute.</summary>
    public ILattice Lattice { get; }

    /// <summary>When set, <see cref="ILattice.SetAsync(string, byte[], CancellationToken)"/> fails.</summary>
    public bool FailWrites { get; set; }

    /// <summary>The page every history scan returns.</summary>
    public EntryHistoryPage History { get; set; } = new() { Source = EntryHistorySource.View };

    /// <summary>The stored rows.</summary>
    public IReadOnlyDictionary<string, (byte[] Value, HybridLogicalClock Version)> Rows => _rows;

    /// <summary>Writes a row with a fresh version and returns that version.</summary>
    public HybridLogicalClock Put(string key, byte[] value)
    {
        var version = new HybridLogicalClock { WallClockTicks = ++_clock };
        _rows[key] = (value, version);
        return version;
    }

    /// <summary>The decoded admin audit records, keyed as stored.</summary>
    public Dictionary<string, LatticeAdminAuditRecord> Records() =>
        _rows.ToDictionary(r => r.Key, r => AdminAuditStore.Deserialize(r.Value.Value)!, StringComparer.Ordinal);

    private async IAsyncEnumerable<KeyValuePair<string, byte[]>> Range(
        string? start, string? end, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        foreach (var row in _rows.ToList())
        {
            if ((start is null || string.CompareOrdinal(row.Key, start) >= 0)
                && (end is null || string.CompareOrdinal(row.Key, end) < 0))
            {
                yield return new(row.Key, row.Value.Value);
            }
        }
    }

    private async IAsyncEnumerable<string> Keys(
        string? start, string? end, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var row in Range(start, end, cancellationToken))
        {
            yield return row.Key;
        }
    }
}
