using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Reports;

/// <summary>
/// Writes markdown to the CI job summary (<c>GITHUB_STEP_SUMMARY</c>) when the run has one, and
/// always to <c>artifacts/job-summary.md</c>, which the lane uploads: the coverage report, the
/// pending-wrapper list and the story reports land there.
/// </summary>
public static class JobSummary
{
    private static readonly Lock Gate = new();

    /// <summary>The artifact copy of the summary.</summary>
    public static string ArtifactPath => Path.Combine(JourneyPaths.ProjectDirectory, "artifacts", "job-summary.md");

    /// <summary>Appends <paramref name="markdown"/> as one section.</summary>
    /// <param name="markdown">The section.</param>
    public static void Append(string markdown)
    {
        ArgumentNullException.ThrowIfNull(markdown);
        var section = markdown.TrimEnd() + Environment.NewLine + Environment.NewLine;
        lock (Gate)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(ArtifactPath)!);
            File.AppendAllText(ArtifactPath, section);
            var step = Environment.GetEnvironmentVariable("GITHUB_STEP_SUMMARY");
            if (!string.IsNullOrEmpty(step))
            {
                File.AppendAllText(step, section);
            }
        }
    }
}
