using System.Globalization;
using System.Text;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Import;

/// <summary>One process's working set across an import.</summary>
/// <param name="Process">The process's name in the estate.</param>
/// <param name="Baseline">Its working set before the import, in bytes.</param>
/// <param name="Peak">The largest working set sampled while the import ran, in bytes.</param>
/// <param name="Samples">How many samples were taken.</param>
internal sealed record WorkingSetGrowth(string Process, long Baseline, long Peak, int Samples)
{
    /// <summary>How far the peak rose above the baseline, in bytes.</summary>
    public long Growth => Math.Max(0, Peak - Baseline);

    /// <summary>A markdown table of <paramref name="readings"/> for a job summary or a PR body.</summary>
    /// <param name="title">The table's heading.</param>
    /// <param name="fileBytes">The size of the imported file, in bytes.</param>
    /// <param name="readings">The readings.</param>
    /// <returns>The table.</returns>
    public static string ToMarkdown(string title, long fileBytes, IReadOnlyList<WorkingSetGrowth> readings)
    {
        var text = new StringBuilder();
        text.AppendLine(CultureInfo.InvariantCulture, $"### {title} ({Mb(fileBytes)} file)");
        text.AppendLine();
        text.AppendLine("| Process | Baseline | Peak | Growth | Samples |");
        text.AppendLine("|---|---:|---:|---:|---:|");
        foreach (var reading in readings)
        {
            text.AppendLine(CultureInfo.InvariantCulture,
                $"| {reading.Process} | {Mb(reading.Baseline)} | {Mb(reading.Peak)} | {Mb(reading.Growth)} | {reading.Samples} |");
        }

        return text.ToString();
    }

    /// <summary>Formats <paramref name="bytes"/> in mebibytes with one decimal.</summary>
    /// <param name="bytes">The byte count.</param>
    /// <returns>The text, such as <c>12.3 MB</c>.</returns>
    public static string Mb(long bytes) => string.Create(CultureInfo.InvariantCulture, $"{bytes / (1024.0 * 1024.0):0.0} MB");
}
