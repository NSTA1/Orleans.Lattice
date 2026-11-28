using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The benchmark census of what the replay permit holders are doing (#6166). Each period
/// the diagnostic watchdog writes one <c>[await-diag] state=holder</c> line per permit held
/// past the threshold, naming the replay it serves - an activation's, cold or warm, or a
/// starvation drive's, by origin - the partition it is reading, the partition's persisted
/// checkpoint, where the read started, how far it has scanned and the head it reads to;
/// and one <c>state=holders</c> line counting every holder by kind. Permit holds and the
/// permits served are counted by the same kind.
/// <para>
/// Everything here is gated on <see cref="AwaitDiagnostics.Enabled"/>: disabled, no progress
/// record is allocated, nothing is registered and the per-leaf cost is nothing.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>The replay each leaf is running, while the diagnostic is enabled.</summary>
    private static readonly ConcurrentDictionary<BPlusLeafGrain, ReplayProgress> ReplayProgressByLeaf =
        new(ReferenceEqualityComparer.Instance);

    private static int _holderSnapshotRegistered;

    /// <summary>One replay's position, written by the replay and read by the watchdog.</summary>
    private sealed class ReplayProgress(string kind)
    {
        internal readonly string Kind = kind;
        internal int Partition = -1;
        internal long Persisted = -1;
        internal long Start = -1;
        internal long Scanned = -1;
        internal long Head = -1;
        internal int Partitions;
    }

    /// <summary>Records the start of a replay of <paramref name="kind"/>; <see langword="null"/> when the census is off.</summary>
    private ReplayProgress? BeginReplayProgress(string kind)
    {
        if (!AwaitDiagnostics.Enabled)
        {
            return null;
        }

        if (Interlocked.Exchange(ref _holderSnapshotRegistered, 1) == 0)
        {
            AwaitDiagnostics.RegisterSnapshot(WriteReplayHolderSnapshot);
        }

        var progress = new ReplayProgress(kind);
        ReplayProgressByLeaf[this] = progress;
        return progress;
    }

    /// <summary>Ends the replay <see cref="BeginReplayProgress"/> recorded.</summary>
    private void EndReplayProgress(ReplayProgress? progress)
    {
        if (progress is not null)
        {
            ReplayProgressByLeaf.TryRemove(new KeyValuePair<BPlusLeafGrain, ReplayProgress>(this, progress));
        }
    }

    /// <summary>Records that the running replay starts reading <paramref name="partition"/>.</summary>
    private ReplayProgress? NoteReplayPartitionStart(int partition, long start, long head)
    {
        if (!AwaitDiagnostics.Enabled || !ReplayProgressByLeaf.TryGetValue(this, out var progress))
        {
            return null;
        }

        progress.Persisted = GetPersistedCheckpointForPartition(partition);
        progress.Start = start;
        progress.Scanned = start;
        progress.Head = head;
        progress.Partitions++;
        Volatile.Write(ref progress.Partition, partition);
        return progress;
    }

    /// <summary>Whether any of the leaf's <paramref name="partitions"/> has a persisted checkpoint.</summary>
    private bool AnyPersistedCheckpoint(int partitions)
    {
        for (var p = 0; p < partitions; p++)
        {
            if (GetPersistedCheckpointForPartition(p) >= 0)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>The census kind of a starvation drive's replay and permit, by origin; a constant.</summary>
    private static string DriveCensusKind(StarvationDriveOrigin origin) => origin switch
    {
        StarvationDriveOrigin.WalGcSweep => "drive.wal-gc-sweep",
        StarvationDriveOrigin.CoverageLagTimer => "drive.coverage-lag-timer",
        _ => "drive.other",
    };

    /// <summary>Counts one permit hold of <paramref name="kind"/> and its length on the census.</summary>
    private static void CensusPermitHold(string kind, long acquiredAt)
    {
        if (AwaitDiagnostics.Enabled)
        {
            AwaitDiagnostics.Count(string.Concat("wal.replay.permit.served.", kind));
            AwaitDiagnostics.Add(
                string.Concat("wal.replay.permit.hold.ms.", kind),
                (long)Stopwatch.GetElapsedTime(acquiredAt).TotalMilliseconds);
        }
    }

    /// <summary>Writes the holder lines; runs on the watchdog's timer thread.</summary>
    private static void WriteReplayHolderSnapshot(long now, Action<string> write)
    {
        Dictionary<string, int>? byKind = null;
        for (var i = 0; i < ReplayPermitCensus.Length; i++)
        {
            var holder = Volatile.Read(ref ReplayPermitCensus[i]);
            if (holder is null)
            {
                continue;
            }

            var heldMs = Stopwatch.GetElapsedTime(Volatile.Read(ref ReplayPermitCensusSince[i]), now).TotalMilliseconds;
            ReplayProgressByLeaf.TryGetValue(holder, out var progress);
            var kind = progress?.Kind ?? "no-replay";
            byKind ??= new Dictionary<string, int>(StringComparer.Ordinal);
            byKind[kind] = byKind.GetValueOrDefault(kind) + 1;
            if (heldMs < AwaitDiagnostics.ThresholdMs)
            {
                continue;
            }

            var detail = progress is null
                ? string.Create(CultureInfo.InvariantCulture, $"kind={kind} out={Volatile.Read(ref holder._replayPermitsSuspended) > 0}")
                : string.Create(
                    CultureInfo.InvariantCulture,
                    $"kind={kind} partition={Volatile.Read(ref progress.Partition)} partitionsRead={progress.Partitions} persisted={progress.Persisted} start={progress.Start} scanned={progress.Scanned} head={progress.Head} owed={Math.Max(0, progress.Head - 1 - progress.Scanned)} out={Volatile.Read(ref holder._replayPermitsSuspended) > 0}");
            write(AwaitDiagnostics.FormatLine("holder", "wal.replay.permit", heldMs, holder.CensusGrainId.ToString(), detail));
        }

        if (byKind is not null)
        {
            write(AwaitDiagnostics.FormatLine(
                "holders",
                "wal.replay.permit",
                0,
                null,
                string.Join(' ', byKind.Select(static k => string.Concat(k.Key, "=", k.Value.ToString(CultureInfo.InvariantCulture))))
                    + string.Create(CultureInfo.InvariantCulture, $" free={Volatile.Read(ref _replayConcurrencyGate)?.CurrentCount ?? 0} queued={QueuedReplayPermitWaiters}")));
        }
    }
}
