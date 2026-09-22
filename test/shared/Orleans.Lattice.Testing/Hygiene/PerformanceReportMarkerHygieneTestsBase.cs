using System.Globalization;
using System.IO;
using System.Text.RegularExpressions;
using NUnit.Framework;

namespace Orleans.Lattice.Testing.Hygiene;

/// <summary>
/// Regression: <c>docs/lattice/performance-single-silo.md</c> and
/// <c>docs/lattice/performance-multi-silo.md</c> contain
/// mechanically-managed marker blocks (<c>perf-table:layer1</c>,
/// <c>perf-table:layer2</c>, <c>perf-table:layer3</c>,
/// <c>perf-chart:layer3</c>) that <c>benchmark/performance-report.ps1</c>
/// rewrites on every invocation. The marker contract is enforced here so
/// that a hand-edit between the markers, or a stale schema bump, or a
/// missing required key, fails at PR time rather than at script-run time
/// against a freshly-provisioned VM or ACA environment (where the failure
/// costs ~80 minutes of wall-clock and real Azure spend). See
/// <see href="https://github.com/NSTA1/Orleans.Lattice/issues/598"/>
/// for the full contract.
/// <para>
/// A marker block carries a <em>kind</em> as well as a layer id. A
/// <c>perf-table</c> block must be followed by a markdown table header with
/// the layer's expected column count; a <c>perf-chart</c> block must be
/// followed by a mermaid fence. Both kinds share the structural rules
/// (balance, non-overlap, a <c>schema=</c> key and the
/// DO-NOT-HAND-EDIT-BETWEEN-MARKERS notice, a recognised layer id), which is
/// why the kind is a captured group on one regex rather than a second pair
/// of regexes: a new kind then inherits the shared rules automatically and
/// only has to declare its own body check.
/// </para>
/// <para>
/// Two rules are <em>table-only</em>: the full per-layer required-key set,
/// and the trailing <c>&gt; Measured ...</c> provenance note. Both describe
/// the provenance of a set of figures, and a layer's chart and table are
/// rendered from one state object in a single atomic doc update - so the
/// chart can never disagree with the table beside it, and a second copy
/// would pin nothing new while duplicating every key (including the
/// multi-paragraph <c>methodology</c> value) and repeating the same
/// sentence on screen for one set of numbers.
/// </para>
/// <para>
/// The docs are repo-level files owned by exactly one fixture (the core test
/// project), so this base is subclassed only there.
/// </para>
/// </summary>
public abstract class PerformanceReportMarkerHygieneTestsBase
{
    // Both mechanically-managed performance docs. Every rule runs against
    // every doc: a gate that covered only the first would pass a
    // multi-silo doc whose markers had rotted, which is exactly the failure
    // mode adding a second doc introduces.
    private static readonly string[] DocRelativePaths =
    [
        "docs/lattice/performance-single-silo.md",
        "docs/lattice/performance-multi-silo.md",
    ];

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

    // Layer 3 is a multi-silo ACA topology, so its provenance has to pin the
    // things that make a scaling curve interpretable and that Layer 2 has no
    // concept of: how many silos each cell used, how each silo was sized, the
    // per-silo rung the offered load was derived from, and the fixed shard
    // count (without which a reader cannot tell a compute knee from a
    // shard-count knee).
    private static readonly HashSet<string> Layer3RequiredKeys = new(StringComparer.Ordinal)
    {
        "schema", "host", "region", "dotnet",
        "siloCounts", "siloSize", "shardCount",
        "walPartitions", "walMaxPendingBatches",
        "rungPerSilo", "responseTimeoutSec", "cohortN",
        "rowsMeasured", "methodology",
    };

    // The expected layer-keyed table-header column count (one |-separated
    // cell per logical column, ignoring the empty leading / trailing pipes).
    // Layer 1: Operation | Per-call p50 | Per-call p75 | Per-call p90 | Per-call p99 | Allocations | Per-thread call rate.
    // Layer 2: Operation | Sustained throughput | Per-call p50 | Per-call p75 | Per-call p90 | Per-call p99.
    // Layer 3: Operation | Silos | Offered | Sustained throughput | Speedup vs 1 silo | Per-silo efficiency | Per-call p50 | Per-call p99.
    private const int Layer1ExpectedColumns = 7;
    private const int Layer2ExpectedColumns = 6;
    // Mirrors Render-Layer3Table in benchmark/performance-report.ps1; change
    // both together or the gate fails on the next regeneration.
    private const int Layer3ExpectedColumns = 8;

    private static readonly Regex StartMarkerRegex = new(
        @"<!--\s*perf-(?<kind>table|chart):(?<layer>[a-z0-9_-]+):start\s*\r?\n(?<body>.*?)\r?\n-->\r?\n",
        RegexOptions.Compiled | RegexOptions.Singleline);

    private static readonly Regex EndMarkerRegex = new(
        @"<!--\s*perf-(?<kind>table|chart):(?<layer>[a-z0-9_-]+):end\s*-->",
        RegexOptions.Compiled);

    /// <summary>
    /// Validates that every required marker contract holds on both
    /// mechanically-managed performance docs. A failure of any sub-rule is
    /// reported with the doc and line of the offending marker so the fix is
    /// mechanical.
    /// </summary>
    [Test]
    public void Performance_doc_markers_are_well_formed()
    {
        var violations = new List<string>();
        var examined = 0;

        foreach (var docPath in ResolveDocPaths())
        {
            var content = File.ReadAllText(docPath);
            var docName = Path.GetFileName(docPath);

            var starts = StartMarkerRegex.Matches(content);
            var ends = EndMarkerRegex.Matches(content);
            examined += starts.Count;

            ValidateDoc(docName, content, starts, ends, violations);
        }

        // Anti-vacuity control (issue #2275). Every rule is driven off
        // `starts`, so with no marker blocks found the balance check compares
        // 0 to 0, the layer-set comparison compares two empty lists, every
        // per-block loop body is skipped, and the gate reports a pass. Deleting
        // the markers from a doc - or a regex that stopped matching them -
        // therefore disables this gate silently, which is why the denominator
        // is asserted and not the violation list. The denominator is summed
        // across docs so that a doc losing all its markers is still caught by
        // the per-doc marker checks rather than masked by the other doc's
        // count.
        HygieneDenominator.RequireExamined(
            examined, nameof(PerformanceReportMarkerHygieneTestsBase), "perf marker blocks",
            string.Join(", ", DocRelativePaths));

        Assert.That(violations, Is.Empty,
            "performance doc marker hygiene violations found. "
            + "These markers are mechanically managed by benchmark/performance-report.ps1 "
            + "(see https://github.com/NSTA1/Orleans.Lattice/issues/598) "
            + "and must satisfy the schema. Fix each line listed and re-run."
            + Environment.NewLine
            + string.Join(Environment.NewLine, violations));
    }

    private static void ValidateDoc(
        string docName,
        string content,
        MatchCollection starts,
        MatchCollection ends,
        List<string> violations)
    {
        // A marker's identity is (kind, layer), not layer alone: a doc can
        // legitimately carry both perf-chart:layer3 and perf-table:layer3,
        // and matching on layer alone would pair a chart :start with a table
        // :end and report a spurious overlap.
        static string Id(Match m) => $"{m.Groups["kind"].Value}:{m.Groups["layer"].Value}";

        // Rule 1: balanced markers.
        var startIds = starts.Select(Id).ToList();
        var endIds = ends.Select(Id).ToList();
        if (startIds.Count != endIds.Count)
        {
            violations.Add(
                $"{docName}: start-marker count ({startIds.Count}) != end-marker count ({endIds.Count}); "
                + $"starts=[{string.Join(", ", startIds)}] ends=[{string.Join(", ", endIds)}]");
        }

        var startIdSet = startIds.OrderBy(s => s, StringComparer.Ordinal).ToList();
        var endIdSet = endIds.OrderBy(s => s, StringComparer.Ordinal).ToList();
        if (!startIdSet.SequenceEqual(endIdSet, StringComparer.Ordinal))
        {
            violations.Add(
                $"{docName}: start/end marker sets disagree: starts=[{string.Join(", ", startIdSet)}] "
                + $"ends=[{string.Join(", ", endIdSet)}]");
        }

        // Rule 1b: non-overlapping (each :start is followed by its matching
        // :end before any other :start of the same (kind, layer) appears).
        for (var i = 0; i < starts.Count; i++)
        {
            var start = starts[i];
            var id = Id(start);
            var matchingEnd = ends.OfType<Match>()
                .FirstOrDefault(e => e.Index > start.Index && string.Equals(Id(e), id, StringComparison.Ordinal));
            if (matchingEnd is null)
            {
                violations.Add($"{docName}: perf-{id}:start at offset {start.Index} has no matching :end after it");
                continue;
            }
            for (var j = 0; j < starts.Count; j++)
            {
                if (j == i) continue;
                var other = starts[j];
                if (string.Equals(Id(other), id, StringComparison.Ordinal)
                    && other.Index > start.Index
                    && other.Index < matchingEnd.Index)
                {
                    violations.Add(
                        $"{docName}: perf-{id}:start at offset {start.Index} overlaps another :start "
                        + $"at offset {other.Index} before its :end at offset {matchingEnd.Index}");
                }
            }
        }

        // Per-block checks (Rules 2-5).
        for (var i = 0; i < starts.Count; i++)
        {
            var start = starts[i];
            var kind = start.Groups["kind"].Value;
            var layer = start.Groups["layer"].Value;
            var id = Id(start);
            var body = start.Groups["body"].Value;
            var startLine = LineNumberOf(content, start.Index);

            var matchingEnd = ends.OfType<Match>()
                .FirstOrDefault(e => e.Index > start.Index && string.Equals(Id(e), id, StringComparison.Ordinal));
            if (matchingEnd is null) continue; // already reported

            var meta = ParseMetaHeader(body);

            // Rule 2: schema= present + DO-NOT-HAND-EDIT-BETWEEN-MARKERS present.
            if (!meta.ContainsKey("schema"))
            {
                violations.Add($"{docName}: perf-{id}:start (line {startLine}) is missing 'schema='");
            }
            if (!body.Contains("DO-NOT-HAND-EDIT-BETWEEN-MARKERS", StringComparison.Ordinal))
            {
                violations.Add(
                    $"{docName}: perf-{id}:start (line {startLine}) is missing the "
                    + "DO-NOT-HAND-EDIT-BETWEEN-MARKERS notice in the meta header");
            }

            // Rule 3: per-layer required keys present, on TABLE blocks only.
            // The layer id must still be recognised for either kind, so a typo
            // in a chart's layer name is caught rather than silently skipped.
            //
            // The full provenance set is deliberately required on the table and
            // not on the chart. Both blocks for a layer are rendered from one
            // state object in a single atomic doc update, so a chart can never
            // disagree with the table beside it and a second copy of the keys
            // would pin nothing the table does not already pin. It would,
            // however, duplicate every key - including the multi-paragraph
            // 'methodology' value - directly above the figure, roughly doubling
            // the doc's marker bulk for no reader or tooling benefit. What both
            // kinds do carry is 'schema' and the DO-NOT-HAND-EDIT notice
            // (checked above), because those are what identify a block as
            // mechanically managed at all.
            var requiredKeys = layer switch
            {
                "layer1" => Layer1RequiredKeys,
                "layer2" => Layer2RequiredKeys,
                "layer3" => Layer3RequiredKeys,
                _ => null,
            };
            if (requiredKeys is null)
            {
                violations.Add(
                    $"{docName}: perf-{id}:start (line {startLine}) uses an unknown layer id; "
                    + "expected 'layer1', 'layer2' or 'layer3'");
            }
            else if (string.Equals(kind, "table", StringComparison.Ordinal))
            {
                foreach (var key in requiredKeys)
                {
                    if (!meta.ContainsKey(key))
                    {
                        violations.Add(
                            $"{docName}: perf-{id}:start (line {startLine}) is missing required key '{key}'");
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
                        $"{docName}: perf-{id}:start (line {startLine}) has unparseable "
                        + $"rowsMeasured='{rowsMeasured}' (expected ISO-8601 date or date-range start)");
                }
                else if (parsed > DateTime.UtcNow.AddDays(1))
                {
                    // +1 day grace for tz-skew between operator clock and CI.
                    violations.Add(
                        $"{docName}: perf-{id}:start (line {startLine}) has rowsMeasured='{rowsMeasured}' "
                        + $"which is in the future (parsed={parsed:o}, now={DateTime.UtcNow:o})");
                }
            }

            // Rule 5: the block body matches its kind. A table must open with
            // a markdown table header of the layer's column count; a chart
            // must open with a mermaid fence. Checking the wrong one would
            // either reject every chart or accept a chart region that the
            // renderer had silently emptied.
            var bodyStart = start.Index + start.Length; // first char after the start marker's trailing newline
            var bodyEnd = matchingEnd.Index;
            if (bodyEnd <= bodyStart) continue;

            var between = content[bodyStart..bodyEnd];
            var betweenLines = between
                .Split('\n')
                .Select(s => s.TrimEnd('\r'))
                .ToArray();
            var firstNonBlank = betweenLines.FirstOrDefault(s => !string.IsNullOrWhiteSpace(s));

            if (string.Equals(kind, "chart", StringComparison.Ordinal))
            {
                if (firstNonBlank is null || !firstNonBlank.TrimStart().StartsWith("```mermaid", StringComparison.Ordinal))
                {
                    violations.Add(
                        $"{docName}: perf-{id}:start (line {startLine}) is not followed by a mermaid fence; "
                        + $"first non-blank line was: '{firstNonBlank?.Trim() ?? "<none>"}'");
                }
                else
                {
                    // Look for a bare closing fence on its own line, after the
                    // opening one. This is deliberately line-based: the docs are
                    // written with CRLF, so a substring probe for "```\n" never
                    // matches (the newline is preceded by \r) and every chart
                    // block would be reported as unterminated.
                    var openIndex = Array.FindIndex(betweenLines, s => s.TrimStart().StartsWith("```mermaid", StringComparison.Ordinal));
                    var closed = false;
                    for (var k = openIndex + 1; k < betweenLines.Length; k++)
                    {
                        if (string.Equals(betweenLines[k].Trim(), "```", StringComparison.Ordinal))
                        {
                            closed = true;
                            break;
                        }
                    }
                    if (!closed)
                    {
                        violations.Add(
                            $"{docName}: perf-{id}:start (line {startLine}) opens a mermaid fence that is never closed");
                    }
                }
                continue;
            }

            if (firstNonBlank is null || !firstNonBlank.TrimStart().StartsWith('|'))
            {
                violations.Add(
                    $"{docName}: perf-{id}:start (line {startLine}) is not followed by a markdown table "
                    + $"header; first non-blank line was: '{firstNonBlank?.Trim() ?? "<none>"}'");
                continue;
            }

            var expectedCols = layer switch
            {
                "layer1" => Layer1ExpectedColumns,
                "layer2" => Layer2ExpectedColumns,
                "layer3" => Layer3ExpectedColumns,
                _ => -1,
            };
            if (expectedCols > 0)
            {
                var cols = CountTableColumns(firstNonBlank);
                if (cols != expectedCols)
                {
                    violations.Add(
                        $"{docName}: perf-{id}:start (line {startLine}) header has {cols} columns; "
                        + $"expected {expectedCols} for layer '{layer}'");
                }
            }
        }
    }

    /// <summary>
    /// Validates the script-managed provenance note ("&gt; Measured ...") that
    /// appears immediately after every perf-table :end marker, across both
    /// mechanically-managed performance docs. Chart blocks are excluded: the
    /// note states the provenance of the numbers, and a layer's chart is
    /// rendered from the same state as its table in one atomic update, so the
    /// single note under the table already covers both. Requiring it twice
    /// would put the same sentence on screen twice for one set of figures.
    /// </summary>
    [Test]
    public void Performance_doc_provenance_notes_are_well_formed()
    {
        var violations = new List<string>();
        var examined = 0;

        foreach (var docPath in ResolveDocPaths())
        {
            var content = File.ReadAllText(docPath);
            var docName = Path.GetFileName(docPath);
            var lines = content.Split('\n').Select(s => s.TrimEnd('\r')).ToArray();

            // Walk every line; for each :end marker, the next non-blank line must
            // be a "> Measured " line. Track which :end markers we saw, and which
            // "> Measured" lines we accounted for, so orphans on either side are
            // surfaced.
            var seenEnds = new HashSet<int>();
            var consumedNotes = new HashSet<int>();
            for (var i = 0; i < lines.Length; i++)
            {
                var line = lines[i];
                var isEnd = line.Contains("<!-- perf-table:") && line.Contains(":end -->");
                if (!isEnd) continue;
                seenEnds.Add(i);
                // Find the next non-blank line.
                var j = i + 1;
                while (j < lines.Length && string.IsNullOrWhiteSpace(lines[j])) { j++; }
                if (j >= lines.Length)
                {
                    violations.Add($"{docName} line {i + 1}: :end marker '{line.Trim()}' has no following non-blank line; expected '> Measured ...'");
                    continue;
                }
                if (!lines[j].StartsWith("> Measured ", StringComparison.Ordinal))
                {
                    violations.Add(
                        $"{docName} line {i + 1}: :end marker '{line.Trim()}' not followed by '> Measured ...'; "
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
                            $"{docName} line {j + 1}: '> Measured ' note is missing the required substring '{required.Trim()}'; "
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
                    $"{docName} line {i + 1}: orphan '> Measured ' note (not preceded by a :end marker after at most one blank line); "
                    + $"full line: '{lines[i].Trim()}'");
            }

            examined += seenEnds.Count;
        }

        // Anti-vacuity control (issue #2275). With no :end markers in the files
        // the walk above never enters its body, no note is ever required, and
        // the gate passes. The denominator is the number of :end markers the
        // walk actually reached, summed across docs.
        HygieneDenominator.RequireExamined(
            examined, nameof(PerformanceReportMarkerHygieneTestsBase), "perf :end markers",
            string.Join(", ", DocRelativePaths));

        Assert.That(violations, Is.Empty,
            "performance doc provenance-note hygiene violations found. "
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

    private static IEnumerable<string> ResolveDocPaths()
        => DocRelativePaths.Select(ResolveDocPath);

    private static string ResolveDocPath(string relativePath)
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            var candidate = Path.Combine(dir.FullName, relativePath.Replace('/', Path.DirectorySeparatorChar));
            if (File.Exists(candidate))
            {
                return candidate;
            }
            dir = dir.Parent;
        }
        // Deliberately a hard failure, not a skip. A doc that has been renamed
        // or deleted must break this gate loudly: silently dropping it from the
        // sweep is exactly how a mechanically-managed doc stops being checked.
        throw new InvalidOperationException(
            $"Could not find {relativePath} walking up from {AppContext.BaseDirectory}");
    }
}
