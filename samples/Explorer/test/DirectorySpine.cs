using System.Text.RegularExpressions;

namespace Orleans.Lattice.Samples.Explorer.Tests;

/// <summary>
/// Reads the Explorer's directory spine out of a server-rendered page. The
/// spine renders one link per shown area, carrying
/// <c>data-lt-command="go.{area}"</c>; an area that is shown but unavailable
/// carries the <c>--unavailable</c> modifier, and a hidden area is not rendered.
/// </summary>
internal static partial class DirectorySpine
{
    /// <summary>Every area on the spine, and whether it is visible (shown and available).</summary>
    /// <param name="html">The page.</param>
    public static IReadOnlyDictionary<string, bool> ReadAreas(string html)
    {
        ArgumentNullException.ThrowIfNull(html);

        var areas = new SortedDictionary<string, bool>(StringComparer.Ordinal);
        foreach (Match link in SpineLink().Matches(html))
        {
            var area = link.Groups["area"].Value;
            if (area != "home")
            {
                areas[area] = !link.Groups["class"].Value.Contains("--unavailable", StringComparison.Ordinal);
            }
        }

        return areas;
    }

    [GeneratedRegex("""<a class="(?<class>[^"]*lt-shell-directory__link[^"]*)"[^>]*data-lt-command="go\.(?<area>[a-z-]+)""")]
    private static partial Regex SpineLink();
}
