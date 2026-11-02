using System.Text.RegularExpressions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;

/// <summary>Reads the headings of a markdown document as the anchors GitHub and the docs site link to.</summary>
public static partial class DocAnchors
{
    /// <summary>Every heading of <paramref name="markdown"/> outside fenced code, with duplicate anchors suffixed <c>-1</c>, <c>-2</c>.</summary>
    /// <param name="markdown">The document's text.</param>
    public static IReadOnlyList<DocHeading> Headings(string markdown)
    {
        ArgumentNullException.ThrowIfNull(markdown);
        var headings = new List<DocHeading>();
        var seen = new Dictionary<string, int>(StringComparer.Ordinal);
        var fenced = false;
        var lines = markdown.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        for (var index = 0; index < lines.Length; index++)
        {
            var line = lines[index];
            if (line.TrimStart().StartsWith("```", StringComparison.Ordinal))
            {
                fenced = !fenced;
                continue;
            }

            var match = fenced ? Match.Empty : Heading().Match(line);
            if (!match.Success)
            {
                continue;
            }

            var text = match.Groups[2].Value.Trim();
            var anchor = Slug(text);
            if (seen.TryGetValue(anchor, out var count))
            {
                seen[anchor] = count + 1;
                anchor = $"{anchor}-{count}";
            }
            else
            {
                seen[anchor] = 1;
            }

            headings.Add(new DocHeading(match.Groups[1].Value.Length, text, anchor, index + 1));
        }

        return headings;
    }

    /// <summary>The anchor GitHub gives a heading: lower case, punctuation dropped, spaces as hyphens.</summary>
    /// <param name="heading">The heading's text.</param>
    public static string Slug(string heading)
    {
        ArgumentNullException.ThrowIfNull(heading);
        var text = Link().Replace(heading, "$1").ToLowerInvariant();
        return NotSlug().Replace(text, string.Empty).Replace(' ', '-');
    }

    [GeneratedRegex("^(#{1,6})\\s+(.+?)\\s*#*\\s*$")]
    private static partial Regex Heading();

    [GeneratedRegex("\\[([^\\]]*)\\]\\([^)]*\\)")]
    private static partial Regex Link();

    [GeneratedRegex("[^a-z0-9 _-]")]
    private static partial Regex NotSlug();
}
