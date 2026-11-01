namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Import;

/// <summary>
/// The verdict of the bounded-memory proof for the streaming import (#5365, #5475, #5558):
/// what a process's live heap may do while a file of a given size is imported. It holds two
/// bounds, because a process's live heap rises for two different reasons during an import.
/// </summary>
/// <remarks>
/// <para>
/// <b>In flight.</b> While the file streams, a process holds the batches on their way through
/// it. That is bounded by the import's batching, not by the file, so the live heap's peak may
/// not rise more than <see cref="InFlightCeilingBytes"/> - a sixth of a 200 MB file - above
/// both its settled baseline and the heap it settles to once the import finishes. A process
/// holding a sixth of the file at any instant fails.
/// </para>
/// <para>
/// <b>Kept.</b> The first time the upsert path runs through a silo, the silo keeps state the
/// path builds (#5558). Over 74 silo readings of the 200 MB upsert recorded by CI before this
/// verdict, that state came to at most 53 MB across the quarter-size warm-up and the import
/// together, and the 50 MB warm-up alone reached that much: four times the bytes added none.
/// Which of the two imports pays for it varies by run and by silo, and when the import paid
/// for most of it, a single bound on the import's growth above its baseline read it as the
/// file (35.3 MB on run 41265312788, 38.6 MB on run 41254018379, against 32 MiB). So what the
/// two imports keep is bounded together, from the settled heap before the warm-up to the
/// settled heap after the import: at most <see cref="KeptCeilingBytes"/>. A process keeping
/// about a quarter of every byte streamed through it fails even with no one-time state, and
/// every recorded silo carried 40 MB or more of that state, so in practice far less fails.
/// </para>
/// </remarks>
internal static class ImportMemoryBudget
{
    /// <summary>How far a process's live heap may rise above both its settled readings while an import streams.</summary>
    public const long InFlightCeilingBytes = 32L * 1024 * 1024;

    /// <summary>
    /// How much a process may keep across the warm-up and the measured import together: the
    /// 53 MB of one-time state recorded on CI, with room for run-to-run variance.
    /// </summary>
    public const long KeptCeilingBytes = 64L * 1024 * 1024;

    /// <summary>Every way <paramref name="reading"/> breaks the budget, as assertion messages; empty when it holds.</summary>
    /// <param name="reading">One process's readings across the import.</param>
    /// <param name="mode">The import's mode, for the messages.</param>
    /// <param name="fileBytes">The size of the imported file, in bytes.</param>
    /// <returns>The violations.</returns>
    public static IReadOnlyList<string> Violations(WorkingSetGrowth reading, string mode, long fileBytes)
    {
        ArgumentNullException.ThrowIfNull(reading);
        var violations = new List<string>();
        if (reading.LiveInFlight is not { } inFlight || reading.LiveKept is not { } kept)
        {
            violations.Add($"{reading.Process}'s live heap was not read before, during and after the {mode}");
            return violations;
        }

        if (inFlight >= InFlightCeilingBytes)
        {
            violations.Add($"{reading.Process}'s live heap peaked {WorkingSetGrowth.Mb(inFlight)} above both its {WorkingSetGrowth.Mb(reading.LiveBaseline ?? 0)} settled baseline and its {WorkingSetGrowth.Mb(reading.LiveAfter ?? 0)} settled heap afterwards while a {mode} of a {WorkingSetGrowth.Mb(fileBytes)} file streamed (ceiling {WorkingSetGrowth.Mb(InFlightCeilingBytes)})");
        }

        if (kept >= KeptCeilingBytes)
        {
            violations.Add($"{reading.Process} kept {WorkingSetGrowth.Mb(kept)} of live heap across the warm-up and a {mode} of a {WorkingSetGrowth.Mb(fileBytes)} file, from {WorkingSetGrowth.Mb(reading.WarmUpLiveBaseline ?? reading.LiveBaseline ?? 0)} to {WorkingSetGrowth.Mb(reading.LiveAfter ?? 0)} (ceiling {WorkingSetGrowth.Mb(KeptCeilingBytes)})");
        }

        return violations;
    }
}
