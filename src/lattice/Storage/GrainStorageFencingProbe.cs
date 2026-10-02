using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice;

/// <summary>
/// Checks whether a grain storage provider enforces ETags on write: it writes a
/// reserved probe row twice, then writes it again presenting the ETag the first
/// write returned. A provider that enforces ETags must reject that last write
/// with <see cref="InconsistentStateException"/>.
/// </summary>
/// <remarks>
/// The probe row lives under a reserved grain type and state name in the
/// <c>_lattice_</c> namespace, at one fixed key shared by every silo, so the
/// probe leaves exactly one small row behind however often silos restart.
/// Because the key is shared, silos starting together can race: a write that
/// presents the ETag just read and is rejected lost to another probe, so the
/// sequence is retried from a fresh read. The stale write itself is race-free:
/// a provider that enforces ETags rejects it whoever wrote last, and one that
/// does not accepts it regardless.
/// </remarks>
internal static class GrainStorageFencingProbe
{
    /// <summary>The reserved grain type the probe row is written under.</summary>
    internal const string GrainTypeName = "_lattice_grain-storage-fencing-probe";

    /// <summary>The reserved state name the probe row is written under.</summary>
    internal const string StateName = "_lattice_grain-storage-fencing-probe";

    /// <summary>The fixed key of the probe row.</summary>
    internal const string GrainKey = "probe";

    /// <summary>How many times the sequence is retried after losing a race.</summary>
    internal const int MaxAttempts = 3;

    /// <summary>The grain id the probe row is written under.</summary>
    internal static readonly GrainId ProbeGrainId =
        GrainId.Create(GrainType.Create(GrainTypeName), IdSpan.Create(GrainKey));

    /// <summary>
    /// Runs the probe against <paramref name="storage"/>.
    /// </summary>
    internal static async Task<GrainStorageFencingProbeResult> RunAsync(IGrainStorage storage)
    {
        ArgumentNullException.ThrowIfNull(storage);

        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            string? staleETag;
            try
            {
                var current = new GrainState<GrainStorageFencingProbeState>(new GrainStorageFencingProbeState());
                await storage.ReadStateAsync(StateName, ProbeGrainId, current).ConfigureAwait(false);
                current.State ??= new GrainStorageFencingProbeState();

                current.State.Sequence++;
                await storage.WriteStateAsync(StateName, ProbeGrainId, current).ConfigureAwait(false);
                staleETag = current.ETag;

                current.State.Sequence++;
                await storage.WriteStateAsync(StateName, ProbeGrainId, current).ConfigureAwait(false);
            }
            catch (InconsistentStateException)
            {
                // Another silo's probe wrote between this read and write.
                continue;
            }
            catch (Exception ex)
            {
                return new GrainStorageFencingProbeResult(
                    GrainStorageFencingVerdict.Inconclusive,
                    "the provider faulted while writing the probe row",
                    ex);
            }

            var stale = new GrainState<GrainStorageFencingProbeState>(new GrainStorageFencingProbeState { Sequence = -1 })
            {
                ETag = staleETag!,
                RecordExists = true,
            };

            try
            {
                await storage.WriteStateAsync(StateName, ProbeGrainId, stale).ConfigureAwait(false);
            }
            catch (InconsistentStateException)
            {
                return new GrainStorageFencingProbeResult(
                    GrainStorageFencingVerdict.Fenced,
                    "the provider rejected a write carrying a stale ETag");
            }
            catch (Exception ex)
            {
                return new GrainStorageFencingProbeResult(
                    GrainStorageFencingVerdict.Inconclusive,
                    "the provider faulted on the stale-ETag write with an exception other than InconsistentStateException",
                    ex);
            }

            return new GrainStorageFencingProbeResult(
                GrainStorageFencingVerdict.Unfenced,
                "the provider accepted a write carrying a stale ETag");
        }

        return new GrainStorageFencingProbeResult(
            GrainStorageFencingVerdict.Inconclusive,
            $"every one of {MaxAttempts} attempts lost a race to a concurrent probe write");
    }
}
