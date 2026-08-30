using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Scans every C# source file under <c>src/</c> for metric emission sites - a
/// <c>Counter</c> / <c>Histogram</c> / <c>UpDownCounter</c> <c>Add</c> or
/// <c>Record</c> call, or a <see cref="System.Diagnostics.Metrics.Measurement{T}"/>
/// construction - and reports the tag expression each one passes.
/// </summary>
/// <remarks>
/// Deliberately a source scan rather than a reflection walk: an instrument's tag
/// set is chosen per call site, so only the source says whether every emission of
/// an instrument carries the derived <c>tenant</c> dimension. The scan is
/// deterministic and order-independent - it never runs the product, starts a
/// cluster, or depends on timing.
/// </remarks>
internal static class MetricEmissionScanner
{
    /// <summary>One metric emission site and the tag expression it passes.</summary>
    /// <param name="RelativePath">The repo-relative source path.</param>
    /// <param name="Line">The 1-based line the call opens on.</param>
    /// <param name="Instrument">The instrument identifier the call targets.</param>
    /// <param name="Tags">The call's tag arguments, whitespace-collapsed.</param>
    internal sealed record EmissionSite(string RelativePath, int Line, string Instrument, string Tags);

    private static readonly Regex InstrumentDeclaration = new(
        @"(?:Counter|Histogram|UpDownCounter)<[^>]+>\s+([A-Za-z0-9_]+)\s*=",
        RegexOptions.Compiled);

    private static readonly Regex InstrumentAssignment = new(
        @"\b([_A-Za-z][A-Za-z0-9_]*)\s*=\s*[A-Za-z0-9_.]*\.?Create(?:Counter|Histogram|UpDownCounter)<",
        RegexOptions.Compiled);

    private static readonly Regex MeasurementConstruction = new(
        @"\bnew Measurement<[^>]+>\(",
        RegexOptions.Compiled);

    /// <summary>
    /// Matches any reference to the tenant dimension: the derived classifier, one
    /// of the cached sibling tag identifiers the hot paths hold, or the tenancy
    /// meter's own per-tenant tag key.
    /// </summary>
    internal static readonly Regex TenantDimension = new(
        @"LatticeTenantLabel\.|[Tt]enantTag|TagTenant|StageTagTenant|stageTagTenant|ViewTenantTag",
        RegexOptions.Compiled);

    /// <summary>Matches the constant platform sentinel (never a derived tenant).</summary>
    internal static readonly Regex PlatformSentinel = new(
        @"LatticeTenantLabel\.(Platform\b|PlatformMeasurement)",
        RegexOptions.Compiled);

    /// <summary>Matches a genuinely derived (tree-classified) tenant tag.</summary>
    internal static readonly Regex DerivedTenant = new(
        @"LatticeTenantLabel\.ForTree|LatticeTenantLabel\.ForTenant|[Tt]enantTag|StageTagTenant|stageTagTenant|ViewTenantTag",
        RegexOptions.Compiled);

    /// <summary>Enumerates every metric emission site under <c>src/</c>.</summary>
    /// <param name="repoRoot">The repository root.</param>
    /// <returns>Every emission site found, in file then position order.</returns>
    internal static IReadOnlyList<EmissionSite> Scan(string repoRoot)
    {
        var files = new List<(string Relative, string Text)>();
        var names = new HashSet<string>(StringComparer.Ordinal);

        foreach (var path in Directory.EnumerateFiles(Path.Combine(repoRoot, "src"), "*.cs", SearchOption.AllDirectories))
        {
            if (HygieneRepository.HasExcludedSegment(path))
            {
                continue;
            }

            var text = File.ReadAllText(path);
            // Normalize to '/' so a repo-relative path is the same string on
            // every platform. The registries that name a site (for example
            // TenantMetricDimensionHygieneTests.PreBuiltTagCollections) compare
            // against this ordinally, and Path.GetRelativePath yields '\' on
            // Windows and '/' elsewhere - so an un-normalized path matches the
            // registry on a developer machine and matches nothing in Linux CI,
            // silently turning every exemption lookup into a miss.
            files.Add((Path.GetRelativePath(repoRoot, path).Replace('\\', '/'), text));
            foreach (Match m in InstrumentDeclaration.Matches(text))
            {
                names.Add(m.Groups[1].Value);
            }
            foreach (Match m in InstrumentAssignment.Matches(text))
            {
                names.Add(m.Groups[1].Value);
            }
        }

        if (names.Count == 0)
        {
            throw new InvalidOperationException("No metric instruments were discovered under src/; the scanner is broken.");
        }

        var identifiers = string.Join('|', names.OrderByDescending(static n => n.Length).Select(Regex.Escape));
        var call = new Regex(
            @"\b(?:(?:[A-Za-z0-9_]*Metrics)\.([A-Za-z0-9_]+)|(" + identifiers + @"))\.(?:Add|Record)\(",
            RegexOptions.Compiled);

        var sites = new List<EmissionSite>();
        foreach (var (relative, text) in files)
        {
            // An identifier-keyed match is only trusted in a file that actually
            // talks to the metrics surface, so an unrelated collection that
            // happens to share a name is never mistaken for an instrument.
            var metricFile = text.Contains("Metrics", StringComparison.Ordinal)
                || text.Contains("Meter", StringComparison.Ordinal);

            foreach (Match m in call.Matches(text))
            {
                if (m.Groups[2].Success && !metricFile)
                {
                    continue;
                }

                Add(sites, relative, text, m.Index, m.Index + m.Length - 1,
                    m.Groups[1].Success ? m.Groups[1].Value : m.Groups[2].Value);
            }

            foreach (Match m in MeasurementConstruction.Matches(text))
            {
                Add(sites, relative, text, m.Index, m.Index + m.Length - 1, "Measurement");
            }
        }

        return sites;
    }

    private static void Add(List<EmissionSite> sites, string relative, string text, int start, int open, string instrument)
    {
        var close = MatchParenthesis(text, open);
        if (close < 0)
        {
            return;
        }

        var inner = text[(open + 1)..close];
        var comma = SplitFirstArgument(inner);
        var tags = comma < 0 ? string.Empty : inner[(comma + 1)..];
        sites.Add(new EmissionSite(
            relative,
            text.Take(start).Count(static c => c == '\n') + 1,
            instrument,
            Regex.Replace(tags, @"\s+", " ").Trim()));
    }

    private static int MatchParenthesis(string text, int open)
    {
        var depth = 0;
        for (var i = open; i < text.Length; i++)
        {
            var c = text[i];
            if (c == '"')
            {
                i++;
                while (i < text.Length)
                {
                    if (text[i] == '\\') { i += 2; continue; }
                    if (text[i] == '"') break;
                    i++;
                }
            }
            else if (c == '(')
            {
                depth++;
            }
            else if (c == ')')
            {
                depth--;
                if (depth == 0) return i;
            }
        }

        return -1;
    }

    private static int SplitFirstArgument(string inner)
    {
        var depth = 0;
        var angle = 0;
        for (var i = 0; i < inner.Length; i++)
        {
            var c = inner[i];
            if (c == '"')
            {
                i++;
                while (i < inner.Length)
                {
                    if (inner[i] == '\\') { i++; continue; }
                    if (inner[i] == '"') break;
                    i++;
                }
            }
            else if (c is '(' or '[' or '{') depth++;
            else if (c is ')' or ']' or '}') depth--;
            else if (c == '<') angle++;
            else if (c == '>' && angle > 0) angle--;
            else if (c == ',' && depth == 0 && angle == 0) return i;
        }

        return -1;
    }
}
