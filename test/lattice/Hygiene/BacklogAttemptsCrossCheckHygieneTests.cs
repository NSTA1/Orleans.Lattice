using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Pins the agent-operated backlog's cross-check of the derived <c>attempts</c>
/// count against the claim's fencing token (issue #2466).
/// <para>
/// <c>attempts</c> is derived by counting claim markers on the mirrored issue,
/// and a marker is an artefact a worker can omit. An audit found markers on 6
/// of 32 items, so the poison-item threshold read zero for the rest however
/// often they had been drawn - and zero is also what a never-drawn item reads,
/// so total instrumentation failure presented as health. The remedy reads the
/// fencing token, which the claim grant itself advances, beside the marker
/// count and separates "never claimed" from "claimed but unmarked".
/// </para>
/// <para>
/// The rule is prose in <c>samples/AgentBacklog/template/</c>, consumed by the
/// backlog agents under <c>.github/agents/</c>, so no product code can witness
/// it. Rather than grep for a token (which the opposite claim would also
/// satisfy), this fixture parses the protocol's decision table and evaluates
/// it: the table must be a complete, disjoint partition of every
/// (token, marker) pair, and it must classify the measured failure - six
/// grants, zero markers - as a defect rather than as a fresh item. Both agent
/// bases must link that one definition at the point where they count
/// attempts, following the one-definition-both-prompts-link rule settled for
/// the <c>state:</c> vocabulary.
/// </para>
/// </summary>
[TestFixture]
public sealed class BacklogAttemptsCrossCheckHygieneTests
{
    private const string TemplateDirectory = "samples/AgentBacklog/template";
    private const string ProtocolFile = "backlog-protocol.md";
    private const string WorkerBaseFile = "backlog-worker.base.md";
    private const string PmBaseFile = "backlog-pm.base.md";

    private const string CrossCheckHeading = "### Cross-checking `attempts` against the fencing token";
    private const string DefectConditionsHeading = "### Defect conditions the ready-set computation must surface";

    private const string NeverClaimed = "never claimed";
    private const string ClaimedButUnmarked = "claimed but unmarked";

    private static readonly Regex TableRow = new(@"^\|(?<cells>.+)\|\s*$", RegexOptions.Compiled);

    [Test]
    public void Protocol_cross_check_table_partitions_every_token_and_marker_pair_exactly_once()
    {
        var rows = ParseCrossCheckTable();

        var examined = 0;
        foreach (long? token in new long?[] { null, 0, 1, 2, 3, 6, 9 })
        {
            for (var markers = 0; markers <= 9; markers++)
            {
                var matches = rows.Where(r => r.Matches(token, markers)).ToList();
                Assert.That(
                    matches,
                    Has.Count.EqualTo(1),
                    $"fencingToken={token?.ToString() ?? "absent"}, markers={markers} must match exactly one row "
                    + $"of the cross-check table in {ProtocolFile}; matched: "
                    + string.Join(", ", matches.Select(m => $"'{m.Reading}'")));
                examined++;
            }
        }

        Assert.That(examined, Is.EqualTo(70), "the partition grid must actually be walked");
    }

    [Test]
    public void Protocol_cross_check_table_classifies_granted_but_unmarked_as_a_defect_not_fresh()
    {
        var rows = ParseCrossCheckTable();

        var neverDrawn = Classify(rows, token: null, markers: 0);
        Assert.That(neverDrawn.Reading, Is.EqualTo(NeverClaimed));
        Assert.That(neverDrawn.Action, Does.StartWith("none"));

        // The measured case from the issue: six grants, zero markers on the issue.
        var unmarked = Classify(rows, token: 6, markers: 0);
        Assert.That(unmarked.Reading, Is.EqualTo(ClaimedButUnmarked));
        Assert.That(unmarked.Action, Does.StartWith("defect"));
        Assert.That(unmarked.Action, Does.Contain("not zero"));

        // The token over-counts (a same-owner re-claim advances it without a
        // marker), so where markers exist the marker count stays authoritative.
        var consistent = Classify(rows, token: 2, markers: 2);
        Assert.That(consistent.Action, Does.Contain("marker count"));
        Assert.That(consistent.Action, Does.Not.StartWith("defect"));

        var ahead = Classify(rows, token: 6, markers: 2);
        Assert.That(ahead.Action, Does.Contain("marker count"));
        Assert.That(ahead.Action, Does.Contain("report the gap"));
        Assert.That(ahead.Action, Does.Not.StartWith("defect"));

        var behind = Classify(rows, token: null, markers: 2);
        Assert.That(behind.Action, Does.StartWith("defect"));
    }

    [Test]
    public void Protocol_ready_set_defect_conditions_surface_claimed_but_unmarked()
    {
        var protocol = ReadTemplate(ProtocolFile);
        var section = Section(protocol, DefectConditionsHeading, nextHeadingPrefix: "### ");

        var entry = section
            .Split('\n')
            .SkipWhile(line => !line.StartsWith("- **Claimed but unmarked.**", StringComparison.Ordinal))
            .TakeWhile((line, index) => index == 0 || !line.StartsWith("- ", StringComparison.Ordinal))
            .ToList();

        Assert.That(entry, Is.Not.Empty, $"'{DefectConditionsHeading}' must list the claimed-but-unmarked defect");
        Assert.That(string.Join(" ", entry), Does.Contain($"(#{CrossCheckAnchor(protocol)})"));
    }

    [Test]
    public void Worker_base_links_the_cross_check_where_it_counts_attempts()
    {
        var anchor = CrossCheckAnchor(ReadTemplate(ProtocolFile));
        var section = Section(
            ReadTemplate(WorkerBaseFile),
            "### Attempts, and the poison threshold",
            nextHeadingPrefix: "### ");

        Assert.That(section, Does.Contain("startswith(\"<!-- backlog-worker: claim \")"), "the section that counts attempts");
        Assert.That(section, Does.Contain($"{ProtocolFile}#{anchor}"));
    }

    [Test]
    public void Pm_base_links_the_cross_check_where_its_parking_sweep_counts_attempts()
    {
        var anchor = CrossCheckAnchor(ReadTemplate(ProtocolFile));
        var pm = ReadTemplate(PmBaseFile);
        var start = pm.IndexOf("2. **Park and unpark poison items.**", StringComparison.Ordinal);
        var end = pm.IndexOf("3. **Promote durable findings", start + 1, StringComparison.Ordinal);
        Assert.That(start, Is.GreaterThanOrEqualTo(0), "the parking sweep step must exist");
        Assert.That(end, Is.GreaterThan(start), "the parking sweep step must be bounded by the next step");

        var step = pm[start..end];
        Assert.That(step, Does.Contain("startswith(\"<!-- backlog-worker: claim \")"), "the step that counts attempts");
        Assert.That(step, Does.Contain($"{ProtocolFile}#{anchor}"));
    }

    private static CrossCheckRow Classify(IReadOnlyList<CrossCheckRow> rows, long? token, int markers) =>
        rows.Single(r => r.Matches(token, markers));

    private static List<CrossCheckRow> ParseCrossCheckTable()
    {
        var section = Section(ReadTemplate(ProtocolFile), CrossCheckHeading, nextHeadingPrefix: "#");

        var rows = new List<CrossCheckRow>();
        foreach (var line in section.Split('\n'))
        {
            var match = TableRow.Match(line.TrimEnd('\r'));
            if (!match.Success)
            {
                continue;
            }

            var cells = match.Groups["cells"].Value.Split('|').Select(Normalise).ToArray();
            if (cells.Length != 4 || cells[0] == "fencingtoken" || cells.All(c => c.Trim('-').Length == 0))
            {
                continue;
            }

            rows.Add(new CrossCheckRow(ParseToken(cells[0]), ParseMarkers(cells[1]), cells[2], cells[3]));
        }

        Assert.That(rows, Has.Count.EqualTo(5), $"the cross-check table under '{CrossCheckHeading}' must have five rows");
        return rows;
    }

    private static Func<long, int, bool> ParseToken(string cell) => cell switch
    {
        "absent or 0" => (token, _) => token == 0,
        "> 0" => (token, _) => token > 0,
        "= markers" => (token, markers) => token == markers,
        "> markers" => (token, markers) => token > markers,
        "< markers" => (token, markers) => token < markers,
        _ => throw new AssertionException($"unrecognised fencingToken condition '{cell}' in {ProtocolFile}"),
    };

    private static Func<int, bool> ParseMarkers(string cell) => cell switch
    {
        "0" => markers => markers == 0,
        "> 0" => markers => markers > 0,
        _ => throw new AssertionException($"unrecognised claim-marker condition '{cell}' in {ProtocolFile}"),
    };

    private static string Normalise(string cell) =>
        Regex.Replace(cell.Replace("`", string.Empty), @"\s+", " ").Trim().ToLowerInvariant();

    private static string CrossCheckAnchor(string protocol)
    {
        Assert.That(
            protocol.Replace("\r\n", "\n").Contains(CrossCheckHeading + "\n", StringComparison.Ordinal),
            Is.True,
            $"{ProtocolFile} must define the heading '{CrossCheckHeading}'");
        var text = CrossCheckHeading.TrimStart('#').Trim().ToLowerInvariant();
        text = Regex.Replace(text, @"[^a-z0-9 \-]", string.Empty);
        return text.Replace(' ', '-');
    }

    private static string Section(string document, string heading, string nextHeadingPrefix)
    {
        var start = document.IndexOf(heading + "\n", StringComparison.Ordinal);
        if (start < 0)
        {
            start = document.IndexOf(heading + "\r\n", StringComparison.Ordinal);
        }

        Assert.That(start, Is.GreaterThanOrEqualTo(0), $"heading '{heading}' not found");

        var body = document[(start + heading.Length)..].Replace("\r\n", "\n");
        var next = Regex.Match(body, "\n" + Regex.Escape(nextHeadingPrefix) + "[^\n]*");
        while (next.Success && IsInsideFence(body, next.Index))
        {
            next = next.NextMatch();
        }

        return next.Success ? body[..next.Index] : body;
    }

    private static bool IsInsideFence(string body, int index) =>
        Regex.Matches(body[..index], @"^```", RegexOptions.Multiline).Count % 2 == 1;

    private static string ReadTemplate(string file) =>
        File.ReadAllText(Path.Combine(HygieneRepository.FindRepoRoot(), TemplateDirectory, file));

    private sealed record CrossCheckRow(Func<long, int, bool> Token, Func<int, bool> Markers, string Reading, string Action)
    {
        public bool Matches(long? token, int markers) => Token(token ?? 0, markers) && Markers(markers);
    }
}
