using System.Collections.Concurrent;
using System.Text;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;

/// <summary>
/// The pending wrappers: every <see cref="KnownDefect"/> and <see cref="KnownGap"/> written in the
/// journeys (found by scanning their source, so a wrapper is listed whether or not its step ran)
/// and every one that ran in this run. I1 (#5400) requires the written list to be empty.
/// </summary>
public static partial class PendingWrappers
{
    private static readonly ConcurrentQueue<PendingWrapper> Ran = new();

    /// <summary>
    /// The directories of this project the scan skips: build output and artifacts, the journey
    /// hosts, the wrappers' own definitions, and the self-tests, which construct wrappers to test
    /// them rather than to wrap a step.
    /// </summary>
    public static IReadOnlyList<string> SkippedDirectories { get; } = ["bin", "obj", "artifacts", "Hosts", "Pending", "SelfTests"];

    /// <summary>The wrappers that ran in this run, in order.</summary>
    public static IReadOnlyList<PendingWrapper> RanThisRun => [.. Ran];

    /// <summary>Records a wrapper that ran and still holds.</summary>
    /// <param name="wrapper">The wrapper.</param>
    public static void Record(PendingWrapper wrapper)
    {
        ArgumentNullException.ThrowIfNull(wrapper);
        Ran.Enqueue(wrapper);
    }

    /// <summary>
    /// Scans every C# file under <paramref name="projectDirectory"/> (except
    /// <see cref="SkippedDirectories"/>) for wrappers. A wrapper whose issue or summary is not a
    /// string literal cannot be listed, so it is returned in <paramref name="unreadable"/>.
    /// </summary>
    /// <param name="projectDirectory">This project's directory.</param>
    /// <param name="repositoryRoot">The repository root, for relative paths.</param>
    /// <param name="unreadable">Each <c>file:line</c> holding a wrapper the scan cannot read.</param>
    public static IReadOnlyList<PendingWrapper> Scan(string projectDirectory, string repositoryRoot, out IReadOnlyList<string> unreadable)
    {
        var found = new List<PendingWrapper>();
        var unparsed = new List<string>();
        foreach (var file in Directory.EnumerateFiles(projectDirectory, "*.cs", SearchOption.AllDirectories).Order(StringComparer.Ordinal))
        {
            var relativeToProject = Path.GetRelativePath(projectDirectory, file);
            var top = relativeToProject.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)[0];
            if (SkippedDirectories.Contains(top, StringComparer.Ordinal))
            {
                continue;
            }

            var text = File.ReadAllText(file);
            var relative = Path.GetRelativePath(repositoryRoot, file).Replace('\\', '/');
            var readable = new HashSet<int>();
            foreach (Match match in Literal().Matches(text))
            {
                readable.Add(match.Index);
                found.Add(new PendingWrapper(match.Groups[1].Value, match.Groups[2].Value, Unescape(match.Groups[3].Value), relative, LineOf(text, match.Index)));
            }

            foreach (Match match in Any().Matches(text))
            {
                if (!readable.Contains(match.Index))
                {
                    unparsed.Add($"{relative}:{LineOf(text, match.Index)}");
                }
            }
        }

        unreadable = unparsed;
        return found;
    }

    /// <summary>Renders <paramref name="written"/> and <paramref name="ran"/> for a CI job summary.</summary>
    /// <param name="written">The wrappers in the source.</param>
    /// <param name="ran">The wrappers that ran.</param>
    /// <param name="hard">Whether a wrapper fails the gate.</param>
    public static string ToMarkdown(IReadOnlyList<PendingWrapper> written, IReadOnlyList<PendingWrapper> ran, bool hard)
    {
        ArgumentNullException.ThrowIfNull(written);
        ArgumentNullException.ThrowIfNull(ran);
        var markdown = new StringBuilder();
        markdown.AppendLine("## Pending wrappers");
        markdown.AppendLine();
        markdown.AppendLine(hard
            ? "The list must be empty: a wrapper fails the run."
            : "I1 (#5400) requires this list to be empty; until then it is reported.");
        markdown.AppendLine();
        if (written.Count == 0)
        {
            markdown.AppendLine("No `KnownDefect` or `KnownGap` wrapper is written in the journeys.");
            return markdown.ToString();
        }

        markdown.AppendLine("| Wrapper | Issue | What | Where | Ran this run |");
        markdown.AppendLine("|---|---|---|---|---|");
        foreach (var wrapper in written)
        {
            var didRun = ran.Any(candidate => candidate.Kind == wrapper.Kind && candidate.Issue == wrapper.Issue && candidate.Summary == wrapper.Summary);
            markdown.AppendLine($"| {wrapper.Kind} | {wrapper.Issue} | {wrapper.Summary.Replace("|", "\\|", StringComparison.Ordinal)} | `{wrapper.File}:{wrapper.Line}` | {(didRun ? "yes" : "no")} |");
        }

        return markdown.ToString();
    }

    private static int LineOf(string text, int index) => text.AsSpan(0, index).Count('\n') + 1;

    private static string Unescape(string literal) => Regex.Unescape(literal);

    [GeneratedRegex("new\\s+(KnownDefect|KnownGap)\\s*\\(\\s*\"(#[0-9]+)\"\\s*,\\s*\"((?:[^\"\\\\]|\\\\.)*)\"")]
    private static partial Regex Literal();

    [GeneratedRegex("new\\s+(KnownDefect|KnownGap)\\s*\\(")]
    private static partial Regex Any();
}
