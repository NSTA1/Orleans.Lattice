using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Api.State;

/// <summary>
/// Default <see cref="ILatticeStateObserver"/> implementation. Tails the
/// requested tree's durable per-partition write-ahead log by sequence cursor,
/// maps each observable <see cref="WalRecord"/> onto a
/// <see cref="StateChangeNotification"/>, and yields the merged stream live
/// until the caller cancels. Registered as a silo singleton by
/// <c>AddLatticeStateApi</c>.
/// </summary>
/// <remarks>
/// Tailing the durable WAL by cursor (rather than buffering in memory) is what
/// gives the binding its back-pressure guarantee: there is no per-subscription
/// buffer to overflow and a foreground writer is never blocked by an observer.
/// A consumer that falls behind the WAL retention window observes an explicit
/// <see cref="LatticeStateCursorExpiredException"/> on resume rather than a
/// silent gap.
/// </remarks>
internal sealed class LatticeStateObserver(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> options,
    IOptions<LatticeApiStateOptions> apiOptions) : ILatticeStateObserver
{
    private const string TokenVersion = "1";

    private readonly IGrainFactory _grainFactory = grainFactory
        ?? throw new ArgumentNullException(nameof(grainFactory));

    private readonly IOptionsMonitor<LatticeOptions> _options = options
        ?? throw new ArgumentNullException(nameof(options));

    private readonly LatticeApiStateOptions _apiOptions = (apiOptions
        ?? throw new ArgumentNullException(nameof(apiOptions))).Value;

    /// <inheritdoc />
    public async IAsyncEnumerable<StateChangeNotification> ObserveAsync(
        StateObserveRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrEmpty(request.TreeId);

        var tree = _grainFactory.GetGrain<ILattice>(request.TreeId);
        if (!await tree.TreeExistsAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new KeyNotFoundException($"Tree '{request.TreeId}' was not found.");
        }

        var registry = _grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var physicalTreeId = await registry.ResolveAsync(request.TreeId).ConfigureAwait(false)
            ?? request.TreeId;
        var entry = await registry.GetEntryAsync(request.TreeId).ConfigureAwait(false);
        var partitions = Math.Max(1, entry?.WalPartitions ?? _options.Get(request.TreeId).WalPartitions);

        var cursor = await SeedCursorAsync(physicalTreeId, partitions, request.ContinuationToken, cancellationToken)
            .ConfigureAwait(false);

        var pageSize = Math.Max(1, _apiOptions.ChangeObservationPageSize);
        var pollInterval = _apiOptions.ChangeObservationPollInterval;
        if (pollInterval <= TimeSpan.Zero)
        {
            pollInterval = TimeSpan.FromMilliseconds(250);
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            var anyFullPage = false;

            for (var partition = 0; partition < partitions; partition++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var grain = _grainFactory.GetGrain<IWalShardGrain>($"{physicalTreeId}/{partition}");
                var page = await grain.ReadAsync(cursor[partition], pageSize, cancellationToken).ConfigureAwait(false);
                var pageEntries = page.Entries;
                if (pageEntries.Count == 0)
                {
                    continue;
                }

                for (var i = 0; i < pageEntries.Count; i++)
                {
                    var sequenced = pageEntries[i];
                    // Advance the per-partition cursor past every entry read
                    // (emitted or filtered) so filtered entries are never
                    // re-read; the cursor map is the resume position.
                    cursor[partition] = sequenced.Sequence + 1;

                    if (!TryProject(request, sequenced.Entry, out var kind))
                    {
                        continue;
                    }

                    yield return new StateChangeNotification
                    {
                        TreeId = request.TreeId,
                        Key = sequenced.Entry.Key,
                        EndExclusiveKey = kind == StateChangeKind.DeleteRange ? sequenced.Entry.EndExclusiveKey : null,
                        Kind = kind,
                        Hlc = sequenced.Entry.Timestamp,
                        Category = sequenced.Entry.Category,
                        Position = EncodeToken(cursor),
                    };
                }

                if (pageEntries.Count >= pageSize)
                {
                    anyFullPage = true;
                }
            }

            if (!anyFullPage)
            {
                await DelayAsync(pollInterval, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task<long[]> SeedCursorAsync(
        string physicalTreeId,
        int partitions,
        string? continuationToken,
        CancellationToken cancellationToken)
    {
        var cursor = new long[partitions];

        if (string.IsNullOrEmpty(continuationToken))
        {
            // Fresh subscription: start from the live tail so only changes
            // committed after the subscription opens are delivered.
            for (var p = 0; p < partitions; p++)
            {
                var grain = _grainFactory.GetGrain<IWalShardGrain>($"{physicalTreeId}/{p}");
                cursor[p] = await grain.GetNextSequenceAsync(cancellationToken).ConfigureAwait(false);
            }

            return cursor;
        }

        var decoded = DecodeToken(continuationToken, partitions);
        for (var p = 0; p < partitions; p++)
        {
            var grain = _grainFactory.GetGrain<IWalShardGrain>($"{physicalTreeId}/{p}");
            var nextSequence = await grain.GetNextSequenceAsync(cancellationToken).ConfigureAwait(false);
            var liveCount = await grain.GetLiveEntryCountAsync(cancellationToken).ConfigureAwait(false);
            var earliestRetained = nextSequence - liveCount;

            // A resume offset below the earliest still-retained sequence means
            // the missed changes have been trimmed: surface an explicit expiry
            // rather than a silent gap.
            if (decoded[p] < earliestRetained)
            {
                throw new LatticeStateCursorExpiredException();
            }

            cursor[p] = decoded[p];
        }

        return cursor;
    }

    private bool TryProject(StateObserveRequest request, WalRecord entry, out StateChangeKind kind)
    {
        kind = default;

        switch (entry.Op)
        {
            case MutationKind.Set:
                kind = StateChangeKind.Set;
                break;
            case MutationKind.Delete:
                kind = StateChangeKind.Delete;
                break;
            case MutationKind.DeleteRange:
                kind = StateChangeKind.DeleteRange;
                break;
            default:
                // Saga-terminal (TxCommit/TxAbort) and tombstone-reap records
                // are not observable state changes.
                return false;
        }

        if (entry.Category == MutationCategory.Maintenance && !request.IncludeMaintenance)
        {
            return false;
        }

        return kind == StateChangeKind.DeleteRange
            ? RangeOverlaps(entry.Key, entry.EndExclusiveKey, request.StartInclusive, request.EndExclusive)
            : KeyInRange(entry.Key, request.StartInclusive, request.EndExclusive);
    }

    private static bool KeyInRange(string key, string? startInclusive, string? endExclusive)
    {
        if (startInclusive is not null && string.CompareOrdinal(key, startInclusive) < 0)
        {
            return false;
        }

        if (endExclusive is not null && string.CompareOrdinal(key, endExclusive) >= 0)
        {
            return false;
        }

        return true;
    }

    private static bool RangeOverlaps(string rangeStart, string? rangeEnd, string? filterStart, string? filterEnd)
    {
        // Two half-open ranges [rangeStart, rangeEnd) and [filterStart, filterEnd)
        // overlap iff rangeStart < filterEnd && filterStart < rangeEnd, with
        // null bounds treated as -inf / +inf.
        if (filterEnd is not null && string.CompareOrdinal(rangeStart, filterEnd) >= 0)
        {
            return false;
        }

        if (filterStart is not null && rangeEnd is not null && string.CompareOrdinal(filterStart, rangeEnd) >= 0)
        {
            return false;
        }

        return true;
    }

    private static string EncodeToken(long[] cursor)
    {
        var builder = new StringBuilder(TokenVersion);
        for (var p = 0; p < cursor.Length; p++)
        {
            builder.Append('|').Append(cursor[p]);
        }

        return Convert.ToBase64String(Encoding.ASCII.GetBytes(builder.ToString()));
    }

    private static long[] DecodeToken(string token, int partitions)
    {
        string decoded;
        try
        {
            decoded = Encoding.ASCII.GetString(Convert.FromBase64String(token));
        }
        catch (FormatException ex)
        {
            throw new ArgumentException("The change-observation continuation token is malformed.", nameof(token), ex);
        }

        var parts = decoded.Split('|');
        if (parts.Length != partitions + 1 || parts[0] != TokenVersion)
        {
            // A token whose partition count no longer matches the tree's WAL
            // topology cannot be resumed gap-free.
            throw new LatticeStateCursorExpiredException(
                "The change-observation continuation token does not match the tree's current WAL topology. "
                + "Restart the subscription from the live tail.");
        }

        var cursor = new long[partitions];
        for (var p = 0; p < partitions; p++)
        {
            if (!long.TryParse(parts[p + 1], out var value) || value < 0)
            {
                throw new ArgumentException("The change-observation continuation token is malformed.", nameof(token));
            }

            cursor[p] = value;
        }

        return cursor;
    }

    private static async Task DelayAsync(TimeSpan interval, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Cancellation tears the subscription down cleanly.
        }
    }
}
