using System.Diagnostics;
using Orleans.Lattice;

namespace VehicleFleetSimulator.AzureThroughput.Engine;

/// <summary>
/// Read-mode keyspace pre-seed. The read modes (get-point, get-many) only
/// measure a read path when their keys exist; against an empty tree every
/// call is a miss, which exercises a different (serial fallback) path and
/// reports a number that is not the read ceiling (#3474). The key and payload
/// derivation is shared with the single-VM silo host; the chunked bulk-load
/// seed (<see cref="SeedAsync"/>) is the Layer 3 cluster-mode producer's.
/// </summary>
internal static class BenchPreseed
{
    /// <summary>
    /// Payload size per seeded key: the producer's measured JSON payload p50,
    /// so read latency compares like-for-like with write latency.
    /// </summary>
    public const int PayloadBytes = 245;

    /// <summary>
    /// Whether a cohort in <paramref name="mode"/> with
    /// <paramref name="keyCount"/> vehicles must be pre-seeded. Write modes
    /// never seed: seeding the keys they target would turn "write keys" into
    /// "update existing keys", a different latency profile.
    /// </summary>
    public static bool IsRequired(BenchWorkloadMode mode, int keyCount) =>
        keyCount > 0 && mode is BenchWorkloadMode.GetPoint or BenchWorkloadMode.GetMany;

    /// <summary>Smallest preseed budget, whatever the key count.</summary>
    public const int MinBudgetSec = 300;

    /// <summary>
    /// The seed rate the budget assumes, in keys/s. The keyspace grows with
    /// vehicles x silos while one storage account absorbs the seed, so a flat
    /// budget fails the larger cells: 512,000 keys reached 401,408 in 300 s.
    /// </summary>
    public const int BudgetFloorKeysPerSec = 1000;

    /// <summary>
    /// The wall-clock allowed to seed <paramref name="keyCount"/> keys:
    /// <see cref="MinBudgetSec"/>, or one second per
    /// <see cref="BudgetFloorKeysPerSec"/> keys when that is longer.
    /// <c>run-cohort-aca.ps1</c> derives the same figure for its execution
    /// ceiling, so the two must change together.
    /// </summary>
    public static TimeSpan BudgetFor(int keyCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(keyCount);
        var scaled = ((long)keyCount + BudgetFloorKeysPerSec - 1) / BudgetFloorKeysPerSec;
        return TimeSpan.FromSeconds(Math.Max(MinBudgetSec, scaled));
    }

    /// <summary>
    /// The key for vehicle <paramref name="index"/>. Mirrors the producer's
    /// vehicle-id derivation exactly, so the read modes look up the keys the
    /// seed wrote.
    /// </summary>
    public static string KeyFor(int index)
    {
        Span<byte> idBytes = stackalloc byte[16];
        BitConverter.TryWriteBytes(idBytes[..4], index);
        BitConverter.TryWriteBytes(idBytes.Slice(4, 4), 0xC0FFEE);
        BitConverter.TryWriteBytes(idBytes.Slice(8, 4), 0xDEADBEEF);
        BitConverter.TryWriteBytes(idBytes.Slice(12, 4), 0xCAFEBABE);
        return new Guid(idBytes).ToString("N");
    }

    /// <summary>
    /// Deterministic <see cref="PayloadBytes"/>-byte payload for vehicle
    /// <paramref name="index"/>, so re-runs write bit-identical rows.
    /// </summary>
    public static byte[] PayloadFor(int index)
    {
        var payload = new byte[PayloadBytes];
        for (var b = 0; b < PayloadBytes; b++) payload[b] = (byte)((index + b) & 0xFF);
        return payload;
    }

    /// <summary>
    /// Entries per <see cref="ILattice.BulkAppendChunkAsync"/> call in the
    /// Layer 3 seed: about 4.7 MB per call, and about 256 keys per shard of a
    /// 64-shard tree, so each chunk adds roughly two packed leaves per shard.
    /// </summary>
    public const int ChunkSize = 16_384;

    /// <summary>
    /// The bulk-load operation id for the chunk starting at
    /// <paramref name="offset"/>. Stable for a given chunk, so a re-driven
    /// chunk is an idempotent no-op on every shard that already applied it.
    /// </summary>
    public static string ChunkOperationId(string treeId, int offset) => $"preseed-{treeId}-{offset}";

    /// <summary>Builds the <paramref name="count"/> seed entries.</summary>
    public static List<KeyValuePair<string, byte[]>> BuildEntries(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        var entries = new List<KeyValuePair<string, byte[]>>(count);
        for (var i = 0; i < count; i++)
        {
            entries.Add(new KeyValuePair<string, byte[]>(KeyFor(i), PayloadFor(i)));
        }
        return entries;
    }

    /// <summary>
    /// Bulk-loads the <paramref name="count"/> seed entries into the cohort's
    /// fresh tree through <see cref="ILattice.BulkAppendChunkAsync"/>, in
    /// ascending-key chunks of at most <paramref name="chunkSize"/> entries.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A bulk load packs each leaf to capacity and appends at each shard's
    /// right edge with no splits: about one batched WAL append per leaf rather
    /// than per-entry routing and splits through SetManyAsync, whose per-shard
    /// fan-out stopped settling part-way through the 768,000-key six-silo seed.
    /// A read cohort's tree is fresh (a new tree id and freshly named tables),
    /// so its shards start empty. The tree-size ladder instead grows one tree
    /// segment by segment: each segment's keys sort above every key already
    /// present, so they still append at each shard's right edge.
    /// </para>
    /// <para>
    /// Entries are generated chunk by chunk in index order, which is
    /// ascending ordinal key order (see <see cref="KeyFor"/>), so the seed's
    /// memory is one chunk however large <paramref name="count"/> is.
    /// </para>
    /// <para>
    /// A retry resumes from <paramref name="startOffset"/>, the offset
    /// <paramref name="onSliceWritten"/> last reported (#3588), and re-drives
    /// the failed chunk under the same operation id
    /// (<see cref="ChunkOperationId"/>), which is the protocol's idempotence
    /// contract: only the last un-acknowledged chunk is ever re-driven.
    /// </para>
    /// </remarks>
    /// <param name="lattice">The tree to seed.</param>
    /// <param name="count">The number of entries in the whole seed.</param>
    /// <param name="chunkSize">The maximum entries per BulkAppendChunkAsync call.</param>
    /// <param name="treeId">The tree id, which scopes the chunk operation ids.</param>
    /// <param name="ct">Cancels the seed between chunks.</param>
    /// <param name="startOffset">The first entry to write; entries before it have already landed.</param>
    /// <param name="onSliceWritten">Called with the offset of the next unwritten entry after each chunk lands.</param>
    /// <returns>The number of entries in the whole seed, <paramref name="count"/>.</returns>
    public static async Task<int> SeedAsync(
        ILattice lattice,
        int count,
        int chunkSize,
        string treeId,
        CancellationToken ct,
        int startOffset = 0,
        Action<int>? onSliceWritten = null)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(chunkSize);
        ArgumentOutOfRangeException.ThrowIfNegative(startOffset);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(startOffset, count);
        var payloads = new byte[256][];
        for (var p = 0; p < payloads.Length; p++) payloads[p] = PayloadFor(p);
        for (var offset = startOffset; offset < count; offset += chunkSize)
        {
            ct.ThrowIfCancellationRequested();
            var chunk = BuildChunk(offset, Math.Min(chunkSize, count - offset), payloads);
            await lattice.BulkAppendChunkAsync(ChunkOperationId(treeId, offset), chunk, ct).ConfigureAwait(false);
            onSliceWritten?.Invoke(offset + chunk.Length);
        }
        return count;
    }

    /// <summary>
    /// The seed entries <c>[offset, offset + length)</c>, generated on demand
    /// so a multi-million-key seed never holds the whole keyspace in memory.
    /// Index order is ascending ordinal key order: <see cref="KeyFor"/> puts
    /// the index in the Guid's first field, which formats as eight big-endian
    /// lower-case hex digits ahead of a constant suffix.
    /// </summary>
    private static KeyValuePair<string, byte[]>[] BuildChunk(int offset, int length, byte[][] payloads)
    {
        var chunk = new KeyValuePair<string, byte[]>[length];
        for (var j = 0; j < length; j++)
        {
            var i = offset + j;
            chunk[j] = new KeyValuePair<string, byte[]>(KeyFor(i), payloads[i & 0xFF]);
        }
        return chunk;
    }

    /// <summary>
    /// Seeds entries <c>[startOffset, count)</c> with retries, under a
    /// wall-clock <paramref name="budget"/>. A non-zero
    /// <paramref name="startOffset"/> grows a tree that already holds the
    /// first <paramref name="startOffset"/> keys (the tree-size ladder):
    /// chunk operation ids use absolute offsets, so a segment never collides
    /// with an earlier one.
    /// </summary>
    internal static async Task SeedWithRetryAsync(
        ILattice lattice, int count, int chunkSize, string treeId, TimeSpan budget, CancellationToken ct, int startOffset = 0)
    {
        const int MaxAttemptsPerSlice = 8;
        const int ProgressEveryKeys = 1_048_576;
        ArgumentOutOfRangeException.ThrowIfNegative(startOffset);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(startOffset, count);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
        deadline.CancelAfter(budget);
        var token = deadline.Token;
        var landed = startOffset;
        var attempts = 0;
        var sliceAttempt = 1;
        var sw = Stopwatch.StartNew();
        try
        {
            while (true)
            {
                token.ThrowIfCancellationRequested();
                attempts++;
                try
                {
                    await SeedAsync(lattice, count, chunkSize, treeId, token, landed, offset =>
                    {
                        if (offset / ProgressEveryKeys != landed / ProgressEveryKeys)
                        {
                            Console.WriteLine($"[producer] preseed-progress treeId={treeId} landed={offset}/{count} elapsedMs={sw.Elapsed.TotalMilliseconds:F0}");
                        }
                        landed = offset;
                        sliceAttempt = 1;
                    }).WaitAsync(token).ConfigureAwait(false);
                    Console.WriteLine($"[producer] preseed treeId={treeId} entries={count} payloadBytes={PayloadBytes} method=bulk-append chunkSize={chunkSize} attempts={attempts} elapsedMs={sw.Elapsed.TotalMilliseconds:F0} segmentStart={startOffset} segmentEntries={count - startOffset}");
                    return;
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested) { throw; }
                catch (Exception ex) when (sliceAttempt < MaxAttemptsPerSlice
                    && (BenchExceptionHelpers.IsOrleansMessageRejection(ex)
                        || WarmUpRetryClassifier.IsTransientActivationCancellation(ex)
                        || WarmUpRetryClassifier.IsTransientPlacementConvergence(ex)
                        || WarmUpRetryClassifier.IsTransientSaturation(ex)
                        || WarmUpRetryClassifier.IsTransientRequestTimeout(ex)
                        || WarmUpRetryClassifier.IsTransientStorageTimeout(ex)
                        || WarmUpRetryClassifier.IsTransientEtagConflict(ex)))
                {
                    var backoffMs = WarmUpRetryClassifier.IsTransientSaturation(ex)
                        ? Math.Min(2500 * sliceAttempt, 20000)
                        : Math.Min(1000 * sliceAttempt, 5000);
                    Console.WriteLine($"[producer] preseed treeId={treeId} transient ({ex.GetType().Name}) at offset={landed}/{count}; retrying in {backoffMs}ms (sliceAttempt={sliceAttempt}/{MaxAttemptsPerSlice} attempts={attempts}): {ex.Message}");
                    sliceAttempt++;
                    await Task.Delay(backoffMs, token).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[producer] ERROR preseed treeId={treeId} entries={count} offset={landed}/{count} attempts={attempts} elapsedMs={sw.Elapsed.TotalMilliseconds:F0} budgetSec={budget.TotalSeconds:F0} FAILED: {ex.GetType().Name}: {ex.Message}");
            throw;
        }
    }
}
