namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Import;

/// <summary>
/// The verdict of the bounded-memory proof for the streaming import (#5365, #5475): what a
/// process's live heap may do while a file of a given size is imported.
/// </summary>
internal static class ImportMemoryBudget
{
    /// <summary>How far a process's live heap may rise above its settled baseline during an import.</summary>
    public const long LiveHeapCeilingBytes = 32L * 1024 * 1024;

    /// <summary>Every way <paramref name="reading"/> breaks the budget, as assertion messages; empty when it holds.</summary>
    /// <param name="reading">One process's readings across the import.</param>
    /// <param name="mode">The import's mode, for the messages.</param>
    /// <param name="fileBytes">The size of the imported file, in bytes.</param>
    /// <returns>The violations.</returns>
    public static IReadOnlyList<string> Violations(WorkingSetGrowth reading, string mode, long fileBytes)
    {
        ArgumentNullException.ThrowIfNull(reading);
        var violations = new List<string>();
        if (reading.LiveGrowth is not { } growth || growth >= LiveHeapCeilingBytes)
        {
            violations.Add($"{reading.Process}'s live heap rose {WorkingSetGrowth.Mb(reading.LiveGrowth ?? 0)} above its {WorkingSetGrowth.Mb(reading.LiveBaseline ?? 0)} settled baseline during a {mode} of a {WorkingSetGrowth.Mb(fileBytes)} file");
        }

        return violations;
    }
}
