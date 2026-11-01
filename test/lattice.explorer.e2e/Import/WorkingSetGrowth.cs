using System.Globalization;
using System.Text;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Import;

/// <summary>
/// One process's memory across an import, read two ways (#5475). The live heap is what the
/// process references, read straight after a forced full compacting collection, so it does
/// not depend on when the garbage collector last ran; it is what the proof gates on. The
/// working set is what the operating system sees, and also carries whatever the server
/// collector keeps committed for its allocation budgets; it is reported, and bounded only
/// against the working set the same path already reached with the warm-up's smaller file.
/// </summary>
/// <param name="Process">The process's name in the estate.</param>
/// <param name="Baseline">Its working set before the import, once its memory was settled, in bytes.</param>
/// <param name="Peak">The largest working set sampled while the import ran, in bytes.</param>
/// <param name="Samples">How many working-set samples were taken.</param>
internal sealed record WorkingSetGrowth(string Process, long Baseline, long Peak, int Samples)
{
    /// <summary>
    /// How far the working set rose above the larger of <see cref="Baseline"/> and
    /// <see cref="WarmUpPeak"/>, in bytes. The warm-up took the same path with a quarter of the
    /// file, so a working set the collector returned between the warm-up and the baseline is
    /// not growth the larger file caused.
    /// </summary>
    public long Growth => Math.Max(0, Peak - Math.Max(Baseline, WarmUpPeak ?? Baseline));

    /// <summary>The working-set peak the same process reached during the warm-up import, or <see langword="null"/>.</summary>
    public long? WarmUpPeak { get; init; }

    /// <summary>The live managed heap before the import, after its memory was settled, in bytes.</summary>
    public long? LiveBaseline { get; init; }

    /// <summary>The largest live managed heap a forced collection found while the import ran, in bytes.</summary>
    public long? LivePeak { get; init; }

    /// <summary>How many forced-collection readings of the live heap were taken while the import ran.</summary>
    public int LiveSamples { get; init; }

    /// <summary>The live managed heap once the import had finished and its memory was settled, in bytes.</summary>
    public long? LiveAfter { get; init; }

    /// <summary>The working set once the import had finished and its memory was settled, in bytes.</summary>
    public long? SettledAfter { get; init; }

    /// <summary>The live-heap growth the same process showed during the warm-up import, in bytes.</summary>
    public long? WarmUpLiveGrowth { get; init; }

    /// <summary>The settled live heap the same process had before the warm-up import, in bytes.</summary>
    public long? WarmUpLiveBaseline { get; init; }

    /// <summary>The process's garbage-collector configuration, as its probe reported it.</summary>
    public string? Collector { get; init; }

    /// <summary>
    /// How far the live heap rose above <see cref="LiveBaseline"/> at any forced collection
    /// during the import or once it settled, in bytes; <see langword="null"/> without a live reading.
    /// </summary>
    public long? LiveGrowth => LiveBaseline is { } baseline && (LivePeak ?? LiveAfter) is not null
        ? Math.Max(0, Math.Max(LivePeak ?? 0, LiveAfter ?? 0) - baseline)
        : null;

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
        text.AppendLine("| Process | Live heap baseline | Live heap peak | Live heap settled | Live growth | Warm-up live growth | Working set baseline | Warm-up working set peak | Working set peak | Working set growth | Working set settled | Samples (live / working set) | Collector |");
        text.AppendLine("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|");
        foreach (var reading in readings)
        {
            text.AppendLine(CultureInfo.InvariantCulture,
                $"| {reading.Process} | {Mb(reading.LiveBaseline)} | {Mb(reading.LivePeak)} | {Mb(reading.LiveAfter)} | {Mb(reading.LiveGrowth)} | {Mb(reading.WarmUpLiveGrowth)} | {Mb(reading.Baseline)} | {Mb(reading.WarmUpPeak)} | {Mb(reading.Peak)} | {Mb(reading.Growth)} | {Mb(reading.SettledAfter)} | {reading.LiveSamples} / {reading.Samples} | {reading.Collector ?? "-"} |");
        }

        return text.ToString();
    }

    /// <summary>Formats <paramref name="bytes"/> in mebibytes with one decimal.</summary>
    /// <param name="bytes">The byte count.</param>
    /// <returns>The text, such as <c>12.3 MB</c>.</returns>
    public static string Mb(long bytes) => string.Create(CultureInfo.InvariantCulture, $"{bytes / (1024.0 * 1024.0):0.0} MB");

    private static string Mb(long? bytes) => bytes is { } value ? Mb(value) : "-";
}
