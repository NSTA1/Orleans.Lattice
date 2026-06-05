using System.Globalization;
using System.IO;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Regression: <c>docs/lattice/performance-single-silo.md</c> contains
/// mechanically-managed marker blocks (<c>perf-table:layer1</c>,
/// <c>perf-table:layer2</c>) that <c>benchmark/performance-report.ps1</c>
/// rewrites on every invocation. The marker contract is enforced here so
/// that a hand-edit between the markers, or a stale schema bump, or a
/// missing required key, fails at PR time rather than at script-run time
/// against a freshly-provisioned VM (where the failure costs ~80 minutes
/// of wall-clock and ~$0.50 of Azure spend). See
/// <see href="https://github.com/NSTA1/Orleans.Lattice/issues/598"/>
/// for the full contract.
/// <para>
/// The rules:
/// <list type="number">
///   <item>Every <c>:start</c> marker has a matching <c>:end</c>; markers
///         are non-overlapping; the file's marker count is even.</item>
///   <item>Every <c>:start</c> block contains a <c>schema=</c> key and the
///         <c>DO-NOT-HAND-EDIT-BETWEEN-MARKERS</c> notice.</item>
///   <item>The required per-layer keys are present:
///         <list type="bullet">
///           <item>layer1: <c>host</c>, <c>dotnet</c>, <c>cohortN</c>,
///                 <c>bdnFidelity</c>, <c>bdnToolchain</c>,
///                 <c>rowsMeasured</c>, <c>methodology</c></item>
///           <item>layer2: <c>host</c>, <c>region</c>, <c>dotnet</c>,
///                 <c>walPartitions</c>, <c>walMaxPendingBatches</c>,
///                 <c>rung</c>, <c>responseTimeoutSec</c>, <c>cohortN</c>,
///                 <c>rowsMeasured</c>, <c>methodology</c></item>
///         </list></item>
///   <item><c>rowsMeasured</c> parses as ISO-8601 and is not in the future.</item>
///   <item>The first non-blank line between the closing <c>--&gt;</c> and the
///         <c>:end</c> marker is a markdown table header (starts with
///         <c>|</c>), and the layer's expected column count matches.</item>
/// </list>
/// </para>
/// </summary>
[TestFixture]
public class PerformanceReportMarkerHygieneTests
{
    private const string DocRelativePath = "docs/lattice/performance-single-silo.md";

    private static readonly HashSet<string> Layer1RequiredKeys = new(StringComparer.Ordinal)
    {
        "schema", "host", "dotnet", "cohortN",
        "bdnFidelity", "bdnToolchain",
        "rowsMeasured", "methodology",
    };

    private static readonly HashSet<string> Layer2RequiredKeys = new(StringComparer.Ordinal)
    {
        "schema", "host", "region", "dotnet",
        "walPartitions", "walMaxPendingBatches",
        "rung", "responseTimeoutSec", "cohortN",
        "rowsMeasured", "methodology",
    };

    // The expected layer-keyed table-header column count (one |-separated
    // cell per logical column, ignoring the empty leading / trailing pipes).
    // Layer 1: Operation | Per-call p50 | Allocations | Single-thread ceiling.
    // Layer 2: Operation | Sustained throughput | Per-call p50 | Per-call p99.
    private const int Layer1ExpectedColumns = 4;
    private const int Layer2ExpectedColumns = 4;

    private static readonly Regex StartMarkerRegex = new(
        @"<!--\s*perf-table:(?<layer>[a-z0-9_-]+):start\s*\r?\n(?<body>.*?)\r?\n-->\r?\n",
        RegexOptions.Compiled | RegexOptions.Singleline);

    private static readonly Regex EndMarkerRegex = new(
        @"<!--\s*perf-table:(?<layer>[a-z0-9_-]+):end\s*-->",
        RegexOptions.Compiled);

    /// <summary>
    /// Validates that every required marker contract holds on the
    /// performance-single-silo.md doc. A failure of any sub-rule is reported
    /// with the line of the offending marker so the fix is mechanical.
    /// </summary>
    [Test]
    public void Performance_single_silo_markers_are_well_formed()
    {
        var docPath = ResolveDocPath();
        var content = File.ReadAllText(docPath);

        var starts = StartMarkerRegex.Matches(content);
        var ends = EndMarkerRegex.Matches(content);

        var violations = new List<string>();

        // Rule 1: balanced markers.
        var startLayers = starts.Select(m => m.Groups["layer"].Value).ToList();
        var endLayers = ends.Select(m => m.Groups["layer"].Value).ToList();
        if (startLayers.Count != endLayers.Count)
        {
            violations.Add(
                $"start-marker count ({startLayers.Count}) != end-marker count ({endLayers.Count}); "
                + $"starts=[{string.Join(", ", startLayers)}] ends=[{string.Join(", ", endLayers)}]");
        }

        var startLayerSet = startLayers.OrderBy(s => s, StringComparer.Ordinal).ToList();
        var endLayerSet = endLayers.OrderBy(s => s, StringComparer.Ordinal).ToList();
        if (!startLayerSet.SequenceEqual(endLayerSet, StringComparer.Ordinal))
        {
            violations.Add(
                $"start/end marker layer sets disagree: starts=[{string.Join(", ", startLayerSet)}] "
                + $"ends=[{string.Join(", ", endLayerSet)}]");
        }

        // Rule 1b: non-overlapping (each :start is followed by its matching
        // :end before any other :start of the same layer appears).
        for (var i = 0; i < starts.Count; i++)
        {
            var start = starts[i];
            var layer = start.Groups["layer"].Value;
            var matchingEnd = ends.OfType<Match>()
                .FirstOrDefault(e => e.Index > start.Index
                    && string.Equals(e.Groups["layer"].Value, layer, StringComparison.Ordinal));
            if (matchingEnd is null)
            {
                violations.Add($"perf-table:{layer}:start at offset {start.Index} has no matching :end after it");
                continue;
            }
            // Any other :start of the same layer between start and matchingEnd
            // is an overlap.
            for (var j = 0; j < starts.Count; j++)
            {
                if (j == i) continue;
                var other = starts[j];
                if (string.Equals(other.Groups["layer"].Value, layer, StringComparison.Ordinal)
                    && other.Index > start.Index
                    && other.Index < matchingEnd.Index)
                {
                    violations.Add(
                        $"perf-table:{layer}:start at offset {start.Index} overlaps another :start "
                        + $"at offset {other.Index} before its :end at offset {matchingEnd.Index}");
                }
            }
        }

        // Per-block checks (Rules 2-5).
        for (var i = 0; i < starts.Count; i++)
        {
            var start = starts[i];
            var layer = start.Groups["layer"].Value;
            var body = start.Groups["body"].Value;
            var startLine = LineNumberOf(content, start.Index);

            var matchingEnd = ends.OfType<Match>()
                .FirstOrDefault(e => e.Index > start.Index
                    && string.Equals(e.Groups["layer"].Value, layer, StringComparison.Ordinal));
            if (matchingEnd is null) continue; // already reported

            var meta = ParseMetaHeader(body);

            // Rule 2: schema= present + DO-NOT-HAND-EDIT-BETWEEN-MARKERS present.
            if (!meta.ContainsKey("schema"))
            {
                violations.Add($"perf-table:{layer}:start (line {startLine}) is missing 'schema='");
            }
            if (!body.Contains("DO-NOT-HAND-EDIT-BETWEEN-MARKERS", StringComparison.Ordinal))
            {
                violations.Add(
                    $"perf-table:{layer}:start (line {startLine}) is missing the "
                    + "DO-NOT-HAND-EDIT-BETWEEN-MARKERS notice in the meta header");
            }

            // Rule 3: per-layer required keys present.
            var requiredKeys = layer switch
            {
                "layer1" => Layer1RequiredKeys,
                "layer2" => Layer2RequiredKeys,
                _ => null,
            };
            if (requiredKeys is null)
            {
                violations.Add(
                    $"perf-table:{layer}:start (line {startLine}) uses an unknown layer id; "
                    + "expected 'layer1' or 'layer2'");
            }
            else
            {
                foreach (var key in requiredKeys)
                {
                    if (!meta.ContainsKey(key))
                    {
                        violations.Add(
                            $"perf-table:{layer}:start (line {startLine}) is missing required key '{key}'");
                    }
                }
            }

            // Rule 4: rowsMeasured parses + not in the future.
            if (meta.TryGetValue("rowsMeasured", out var rowsMeasured))
            {
                if (!DateTime.TryParse(
                        rowsMeasured,
                        CultureInfo.InvariantCulture,
                        DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                        out var parsed))
                {
                    violations.Add(
                        $"perf-table:{layer}:start (line {startLine}) has unparseable "
                        + $"rowsMeasured='{rowsMeasured}' (expected ISO-8601 date or date-range start)");
                }
                else if (parsed > DateTime.UtcNow.AddDays(1))
                {
                    // +1 day grace for tz-skew between operator clock and CI.
                    violations.Add(
                        $"perf-table:{layer}:start (line {startLine}) has rowsMeasured='{rowsMeasured}' "
                        + $"which is in the future (parsed={parsed:o}, now={DateTime.UtcNow:o})");
                }
            }

            // Rule 5: first non-blank line between '-->' and :end is a markdown
            // table header line with the expected column count.
            var bodyStart = start.Index + start.Length; // first char after the start marker's trailing newline
            var bodyEnd = matchingEnd.Index;
            if (bodyEnd > bodyStart)
            {
                var between = content[bodyStart..bodyEnd];
                var firstNonBlank = between
                    .Split('\n')
                    .Select(s => s.TrimEnd('\r'))
                    .FirstOrDefault(s => !string.IsNullOrWhiteSpace(s));
                if (firstNonBlank is null || !firstNonBlank.TrimStart().StartsWith('|'))
                {
                    violations.Add(
                        $"perf-table:{layer}:start (line {startLine}) is not followed by a markdown table "
                        + $"header; first non-blank line was: '{firstNonBlank?.Trim() ?? "<none>"}'");
                }
                else
                {
                    var expectedCols = layer switch
                    {
                        "layer1" => Layer1ExpectedColumns,
                        "layer2" => Layer2ExpectedColumns,
                        _ => -1,
                    };
                    if (expectedCols > 0)
                    {
                        var cols = CountTableColumns(firstNonBlank);
                        if (cols != expectedCols)
                        {
                            violations.Add(
                                $"perf-table:{layer}:start (line {startLine}) header has {cols} columns; "
                                + $"expected {expectedCols} for layer '{layer}'");
                        }
                    }
                }
            }
        }

        Assert.That(violations, Is.Empty,
            "performance-single-silo.md marker hygiene violations found. "
            + "These markers are mechanically managed by benchmark/performance-report.ps1 "
            + "(see https://github.com/NSTA1/Orleans.Lattice/issues/598) "
            + "and must satisfy the schema. Fix each line listed and re-run."
            + Environment.NewLine
            + string.Join(Environment.NewLine, violations));
    }

    /// <summary>
    /// Validates the script-managed provenance note ("&gt; Measured ...") that
    /// appears immediately after every perf-table :end marker. The note is
    /// rendered by <c>benchmark/performance-report.ps1</c> and carries the
    /// host SKU, .NET version, git sha, cohort N, and (per layer) BDN
    /// fidelity or VM region + rung. The contract:
    /// <list type="number">
    ///   <item>Every <c>:end</c> marker is followed (modulo a single blank
    ///         line) by a <c>&gt; Measured </c> line.</item>
    ///   <item>The note line carries the full prefix (date, host, git sha,
    ///         cohort N).</item>
    ///   <item>No <c>&gt; Measured </c> orphans exist elsewhere in the file
    ///         (i.e. every note is anchored to a :end marker).</item>
    /// </list>
    /// </summary>
    [Test]
    public void Performance_single_silo_provenance_notes_are_well_formed()
    {
        var docPath = ResolveDocPath();
        var content = File.ReadAllText(docPath);
        var lines = content.Split('\n').Select(s => s.TrimEnd('\r')).ToArray();

        var violations = new List<string>();

        // Walk every line; for each :end marker, the next non-blank line must
        // be a "> Measured " line. Track which :end markers we saw, and which
        // "> Measured" lines we accounted for, so orphans on either side are
        // surfaced.
        var seenEnds = new HashSet<int>();
        var consumedNotes = new HashSet<int>();
        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i];
            if (!line.Contains("<!-- perf-table:") || !line.Contains(":end -->")) continue;
            seenEnds.Add(i);
            // Find the next non-blank line.
            var j = i + 1;
            while (j < lines.Length && string.IsNullOrWhiteSpace(lines[j])) { j++; }
            if (j >= lines.Length)
            {
                violations.Add($"line {i + 1}: :end marker '{line.Trim()}' has no following non-blank line; expected '> Measured ...'");
                continue;
            }
            if (!lines[j].StartsWith("> Measured ", StringComparison.Ordinal))
            {
                violations.Add(
                    $"line {i + 1}: :end marker '{line.Trim()}' not followed by '> Measured ...'; "
                    + $"line {j + 1} is '{lines[j].Trim()}'");
                continue;
            }
            consumedNotes.Add(j);

            // Sanity-check the note's content: contains 'on ', '.NET ', 'git sha ', 'cohorts'.
            // Each is a strong signal that the renderer filled in the slot rather than emitting an 'unknown'.
            var note = lines[j];
            foreach (var required in new[] { "on ", ".NET ", "git sha ", "cohorts" })
            {
                if (!note.Contains(required, StringComparison.Ordinal))
                {
                    violations.Add(
                        $"line {j + 1}: '> Measured ' note is missing the required substring '{required.Trim()}'; "
                        + $"full line: '{note.Trim()}'");
                }
            }
        }

        // Orphan notes: any '> Measured ' line that wasn't claimed by a
        // preceding :end marker.
        for (var i = 0; i < lines.Length; i++)
        {
            if (!lines[i].StartsWith("> Measured ", StringComparison.Ordinal)) continue;
            if (consumedNotes.Contains(i)) continue;
            violations.Add(
                $"line {i + 1}: orphan '> Measured ' note (not preceded by a :end marker after at most one blank line); "
                + $"full line: '{lines[i].Trim()}'");
        }

        Assert.That(violations, Is.Empty,
            "performance-single-silo.md provenance-note hygiene violations found. "
            + "Each perf-table :end marker must be followed by a '> Measured ...' line generated by "
            + "benchmark/performance-report.ps1 (see https://github.com/NSTA1/Orleans.Lattice/issues/598). "
            + "Fix each line listed and re-run."
            + Environment.NewLine
            + string.Join(Environment.NewLine, violations));
    }

    private static Dictionary<string, string> ParseMetaHeader(string body)
    {
        // The meta header is a list of "  key=value" lines (two-space indent
        // for readability inside the comment). Values can contain '=' (e.g.
        // rung=4000vehicles/5Hz/45s); split on the first '=' only.
        var dict = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var raw in body.Split('\n'))
        {
            var line = raw.Trim().TrimEnd('\r');
            if (string.IsNullOrEmpty(line) || line.StartsWith("DO-NOT-", StringComparison.Ordinal))
            {
                continue;
            }
            var idx = line.IndexOf('=');
            if (idx < 0) continue;
            var key = line[..idx].Trim();
            var value = line[(idx + 1)..].Trim();
            if (key.Length > 0)
            {
                dict[key] = value;
            }
        }
        return dict;
    }

    private static int CountTableColumns(string headerLine)
    {
        // Markdown table headers are pipe-delimited with optional leading
        // and trailing pipes. Counting cells = pipes - 1 when both leading
        // and trailing pipes are present; we just split, trim, and drop
        // empty leading/trailing entries to be robust against either form.
        var trimmed = headerLine.Trim();
        var parts = trimmed.Split('|').Select(p => p.Trim()).ToList();
        // Drop leading empty (when line starts with |) and trailing empty (ends with |).
        if (parts.Count > 0 && parts[0].Length == 0) parts.RemoveAt(0);
        if (parts.Count > 0 && parts[^1].Length == 0) parts.RemoveAt(parts.Count - 1);
        return parts.Count;
    }

    private static int LineNumberOf(string text, int index)
    {
        var line = 1;
        for (var i = 0; i < index && i < text.Length; i++)
        {
            if (text[i] == '\n') line++;
        }
        return line;
    }

    private static string ResolveDocPath()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            var candidate = Path.Combine(dir.FullName, DocRelativePath.Replace('/', Path.DirectorySeparatorChar));
            if (File.Exists(candidate))
            {
                return candidate;
            }
            dir = dir.Parent;
        }
        throw new InvalidOperationException(
            $"Could not find {DocRelativePath} walking up from {AppContext.BaseDirectory}");
    }
}
