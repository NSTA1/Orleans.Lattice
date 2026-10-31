using System.Globalization;
using System.Text;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Reports;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>Renders and publishes a story run's outcome: one row per chapter, blocked chapters named with what blocked them.</summary>
public static class StoryReport
{
    /// <summary>Renders <paramref name="runner"/>'s outcome as markdown.</summary>
    /// <param name="runner">The run.</param>
    /// <param name="title">The report's title, such as <c>Story run: contoso</c>.</param>
    public static string ToMarkdown(StoryRunner runner, string title)
    {
        ArgumentNullException.ThrowIfNull(runner);
        var results = runner.Results.ToDictionary(result => result.Id, StringComparer.Ordinal);
        var markdown = new StringBuilder();
        markdown.AppendLine("## " + title);
        markdown.AppendLine();
        var counts = Enum.GetValues<ChapterStatus>()
            .Select(status => $"{results.Values.Count(result => result.Status == status)} {Label(status)}");
        markdown.AppendLine($"{runner.Order.Count} chapters in dependency order: {string.Join(", ", counts)}, {runner.Order.Count - results.Count} not run.");
        markdown.AppendLine();
        markdown.AppendLine("| # | Chapter | Consumes | Outcome | Detail | Time |");
        markdown.AppendLine("|---:|---|---|---|---|---:|");
        for (var index = 0; index < runner.Order.Count; index++)
        {
            var entry = runner.Order[index];
            var outline = StoryOutline.Find(entry.Id);
            var name = outline is null ? entry.Id : $"{entry.Id} {outline.Title} (#{outline.Issue})";
            var consumes = entry.After.Count == 0 ? "-" : string.Join(", ", entry.After);
            var row = results.TryGetValue(entry.Id, out var result)
                ? $"{Label(result.Status)} | {Escape(result.Detail)} | {result.Elapsed.TotalSeconds.ToString("0.0", CultureInfo.InvariantCulture)} s"
                : "not run | | ";
            markdown.AppendLine($"| {index + 1} | {name} | {consumes} | {row} |");
        }

        if (runner.Session.Ledger.Keys.Count > 0)
        {
            markdown.AppendLine();
            markdown.AppendLine("Ledger: " + string.Join(", ", runner.Session.Ledger.Keys.Select(key => $"`{key}` (by {runner.Session.Ledger.WriterOf(key) ?? "?"})")) + ".");
        }

        return markdown.ToString();
    }

    /// <summary>
    /// Writes the report and the ledger beside the run's artifacts (<c>story-{name}.md</c>,
    /// <c>story-{name}-ledger.json</c>) and appends the report to the job summary.
    /// </summary>
    /// <param name="runner">The run.</param>
    /// <param name="name">The file-name stem, such as <c>contoso</c>.</param>
    /// <param name="title">The report's title.</param>
    public static string Publish(StoryRunner runner, string name, string title)
    {
        ArgumentNullException.ThrowIfNull(runner);
        var markdown = ToMarkdown(runner, title);
        Directory.CreateDirectory(runner.Session.ArtifactsDirectory);
        File.WriteAllText(Path.Combine(runner.Session.ArtifactsDirectory, $"story-{name}.md"), markdown);
        File.WriteAllText(Path.Combine(runner.Session.ArtifactsDirectory, $"story-{name}-ledger.json"), runner.Session.Ledger.ToJson());
        JobSummary.Append(markdown);
        return markdown;
    }

    private static string Label(ChapterStatus status) => status switch
    {
        ChapterStatus.Passed => "passed",
        ChapterStatus.StoodIn => "stood in",
        ChapterStatus.Failed => "FAILED",
        _ => "BLOCKED",
    };

    private static string Escape(string text) => text.Replace("|", "\\|", StringComparison.Ordinal);
}
