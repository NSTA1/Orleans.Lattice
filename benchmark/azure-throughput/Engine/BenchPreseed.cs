using System.Diagnostics;
using Orleans.Lattice;

namespace VehicleFleetSimulator.AzureThroughput.Engine;

/// <summary>
/// Read-mode keyspace pre-seed shared by the single-VM silo host and the
/// Layer 3 cluster-mode producer. The read modes (get-point, get-many) only
/// measure a read path when their keys exist; against an empty tree every
/// call is a miss, which exercises a different (serial fallback) path and
/// reports a number that is not the read ceiling (#3474).
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
    /// Writes the <paramref name="count"/> seed entries through
    /// <see cref="ILattice.SetManyAsync"/> in slices of at most
    /// <paramref name="sliceSize"/> keys. SetManyAsync rather than
    /// BulkLoadAsync because the warm-up has already materialised the root
    /// leaves, and a re-run against retained storage is a populated tree.
    /// Every entry is deterministic, so a retried slice is idempotent.
    /// </summary>
    /// <remarks>
    /// A retry resumes from <paramref name="startOffset"/>, the offset
    /// <paramref name="onSliceWritten"/> last reported, rather than from zero
    /// (#3588): restarting re-wrote every slice that had already landed, so a
    /// large seed that failed late could never finish within its attempts.
    /// </remarks>
    /// <param name="lattice">The tree to seed.</param>
    /// <param name="count">The number of entries in the whole seed.</param>
    /// <param name="sliceSize">The maximum entries per SetManyAsync call.</param>
    /// <param name="ct">Cancels the seed between slices.</param>
    /// <param name="startOffset">The first entry to write; entries before it have already landed.</param>
    /// <param name="onSliceWritten">Called with the offset of the next unwritten entry after each slice lands.</param>
    /// <returns>The number of entries in the whole seed, <paramref name="count"/>.</returns>
    public static async Task<int> SeedAsync(
        ILattice lattice,
        int count,
        int sliceSize,
        CancellationToken ct,
        int startOffset = 0,
        Action<int>? onSliceWritten = null)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(sliceSize);
        ArgumentOutOfRangeException.ThrowIfNegative(startOffset);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(startOffset, count);
        var entries = BuildEntries(count);
        for (var offset = startOffset; offset < entries.Count; offset += sliceSize)
        {
            ct.ThrowIfCancellationRequested();
            var slice = entries.GetRange(offset, Math.Min(sliceSize, entries.Count - offset));
            await lattice.SetManyAsync(slice, ct).ConfigureAwait(false);
            onSliceWritten?.Invoke(offset + slice.Count);
        }
        return entries.Count;
    }

    internal static async Task SeedWithRetryAsync(
        ILattice lattice, int count, int sliceSize, string treeId, TimeSpan budget, CancellationToken ct)
    {
        const int MaxAttemptsPerSlice = 8;
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
        deadline.CancelAfter(budget);
        var token = deadline.Token;
        var landed = 0;
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
                    await SeedAsync(lattice, count, sliceSize, token, landed, offset =>
                    {
                        landed = offset;
                        sliceAttempt = 1;
                    }).WaitAsync(token).ConfigureAwait(false);
                    Console.WriteLine($"[producer] preseed treeId={treeId} entries={count} payloadBytes={PayloadBytes} attempts={attempts} elapsedMs={sw.Elapsed.TotalMilliseconds:F0}");
                    return;
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested) { throw; }
                catch (Exception ex) when (sliceAttempt < MaxAttemptsPerSlice
                    && (BenchExceptionHelpers.IsOrleansMessageRejection(ex)
                        || WarmUpRetryClassifier.IsTransientActivationCancellation(ex)
                        || WarmUpRetryClassifier.IsTransientPlacementConvergence(ex)
                        || WarmUpRetryClassifier.IsTransientSaturation(ex)
                        || WarmUpRetryClassifier.IsTransientRequestTimeout(ex)))
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
