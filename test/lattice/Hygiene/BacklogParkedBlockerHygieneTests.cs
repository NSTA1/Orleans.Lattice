using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Evaluates the backlog protocol's authoring and readiness decisions for parked
/// blockers and its mandatory ruling route. The protocol is the agent runtime's
/// input; these tests do not claim the generic memory API enforces backlog rules.
/// </summary>
[TestFixture]
public sealed class BacklogParkedBlockerHygieneTests
{
    private const string Heading = "### Parked blockers and the ruling route";
    private const string Link = "backlog-protocol.md#parked-blockers-and-the-ruling-route";

    [Test]
    public void Blocker_table_partitions_states_and_distinguishes_stalled_from_waiting()
    {
        AssertBlockerDecisions(ReadTemplate("backlog-protocol.md"));
    }

    [Test]
    public void Parking_table_requires_both_owner_and_question()
    {
        AssertParkingDecisions(ReadTemplate("backlog-protocol.md"));
    }

    [TestCase("backlog-worker.base.md", "## Phase 1 - Compute the ready set")]
    [TestCase("backlog-worker.base.md", "## Phase 7 - Complete or release")]
    [TestCase("backlog-pm.base.md", "## Phase 0 - Ground yourself, unprompted, on every session start")]
    [TestCase("backlog-pm.base.md", "## Phase 5 - Author the grouping, mirror it, and gate it")]
    [TestCase("backlog-pm.base.md", "## Phase 7 - Maintain the backlog")]
    public void Agent_links_single_definition_at_each_decision_point(string file, string heading)
    {
        AssertLink(ReadTemplate(file), heading);
    }

    [Test]
    public void Protocol_scan_and_defect_report_link_rule_before_candidate_narrowing()
    {
        var protocol = ReadTemplate("backlog-protocol.md");
        var computation = Section(protocol, "## Computing the ready set");
        var narrowing = computation.IndexOf("\n2. ", StringComparison.Ordinal);
        Assert.That(narrowing, Is.GreaterThan(0));
        Assert.That(computation[..narrowing], Does.Contain("(#parked-blockers-and-the-ruling-route)"));
        Assert.That(
            Section(protocol, "### Defect conditions the ready-set computation must surface"),
            Does.Contain("(#parked-blockers-and-the-ruling-route)"));
        Assert.That(
            Section(protocol, "### Parking an item - both sides, and not only on exhaustion"),
            Does.Contain("(#parked-blockers-and-the-ruling-route)"));
    }

    [TestCase("| parked | reject | stalled | dependent, blocker, ruling route |",
        "| parked | allow | stalled | dependent, blocker, ruling route |")]
    [TestCase("| parked | reject | stalled | dependent, blocker, ruling route |",
        "| parked | reject | waiting | dependent, blocker, ruling route |")]
    [TestCase("| parked | reject | stalled | dependent, blocker, ruling route |",
        "| parked | reject | stalled | none |")]
    [TestCase("| live | allow | waiting | dependent, blocker |",
        "| live | reject | stalled | dependent, blocker |")]
    public void Mutating_blocker_verdicts_or_reporting_is_detected(string before, string after)
    {
        var protocol = ReadTemplate("backlog-protocol.md");
        AssertBlockerDecisions(protocol);
        var mutant = ReplaceOnce(protocol, before, after);
        Assert.Throws<AssertionException>(() => AssertBlockerDecisions(mutant));
    }

    [TestCase("| no | yes | reject |", "| no | yes | allow |")]
    [TestCase("| yes | no | reject |", "| yes | no | allow |")]
    public void Mutating_either_ruling_requirement_is_detected(string before, string after)
    {
        var protocol = ReadTemplate("backlog-protocol.md");
        AssertParkingDecisions(protocol);
        var mutant = ReplaceOnce(protocol, before, after);
        Assert.Throws<AssertionException>(() => AssertParkingDecisions(mutant));
    }

    [Test]
    public void Removing_agent_rule_link_is_detected()
    {
        const string heading = "## Phase 1 - Compute the ready set";
        var worker = ReadTemplate("backlog-worker.base.md");
        AssertLink(worker, heading);
        var section = Section(worker, heading);
        var mutantSection = ReplaceOnce(section, Link, "backlog-protocol.md");
        var mutant = ReplaceOnce(worker, section, mutantSection);
        Assert.Throws<AssertionException>(() => AssertLink(mutant, heading));
    }

    private static void AssertBlockerDecisions(string protocol)
    {
        var rows = Table(Section(protocol, Heading), "Target observation", 4);
        Assert.That(rows, Has.Count.EqualTo(5));
        var observations = new[]
        {
            (Exists: false, States: Array.Empty<string>(), Observation: "missing", Authoring: "reject",
                Readiness: "invalid", Report: "dependent, blocker"),
            (Exists: true, States: Array.Empty<string>(), Observation: "live", Authoring: "allow",
                Readiness: "waiting", Report: "dependent, blocker"),
            (Exists: true, States: new[] { "complete" }, Observation: "complete", Authoring: "allow",
                Readiness: "satisfied", Report: "none"),
            (Exists: true, States: new[] { "parked" }, Observation: "parked", Authoring: "reject",
                Readiness: "stalled", Report: "dependent, blocker, ruling route"),
            (Exists: true, States: new[] { "ready" }, Observation: "invalid", Authoring: "reject",
                Readiness: "invalid", Report: "dependent, blocker"),
            (Exists: true, States: new[] { "complete", "parked" }, Observation: "invalid", Authoring: "reject",
                Readiness: "invalid", Report: "dependent, blocker"),
        };

        foreach (var input in observations)
        {
            var matches = rows.Where(row => Matches(row[0], input.Exists, input.States)).ToArray();
            Assert.That(matches, Has.Length.EqualTo(1), $"Partition must cover {input.Observation} exactly once");
            Assert.That(matches[0][1], Is.EqualTo(input.Authoring), $"{input.Observation}: authoring");
            Assert.That(matches[0][2], Is.EqualTo(input.Readiness), $"{input.Observation}: readiness");
            Assert.That(matches[0][3], Is.EqualTo(input.Report), $"{input.Observation}: report");
        }
    }

    private static bool Matches(string condition, bool exists, string[] states) => condition switch
    {
        "missing" => !exists,
        "invalid" => exists && (states.Length > 1 || states.Any(s => s is not ("parked" or "complete"))),
        "parked" => exists && states.SequenceEqual(new[] { "parked" }),
        "live" => exists && states.Length == 0,
        "complete" => exists && states.SequenceEqual(new[] { "complete" }),
        _ => throw new AssertionException($"Unknown blocker predicate '{condition}'"),
    };

    private static void AssertParkingDecisions(string protocol)
    {
        var rows = Table(Section(protocol, Heading), "Ruling owner named", 3);
        Assert.That(rows, Has.Count.EqualTo(4));
        foreach (var owner in new[] { false, true })
        {
            foreach (var question in new[] { false, true })
            {
                var matches = rows.Where(row => Boolean(row[0]) == owner && Boolean(row[1]) == question).ToArray();
                Assert.That(matches, Has.Length.EqualTo(1), $"owner={owner}, question={question}");
                Assert.That(matches[0][2], Is.EqualTo(owner && question ? "allow" : "reject"),
                    $"owner={owner}, question={question}");
            }
        }
    }

    private static bool Boolean(string cell) => cell switch
    {
        "yes" => true,
        "no" => false,
        _ => throw new AssertionException($"Unknown ruling predicate '{cell}'"),
    };

    private static List<string[]> Table(string section, string firstHeader, int width)
    {
        var lines = section.Split('\n');
        var headers = lines.Select((line, index) => (line, index))
            .Where(p => p.line.StartsWith($"| {firstHeader} |", StringComparison.Ordinal)).ToArray();
        Assert.That(headers, Has.Length.EqualTo(1), $"Exactly one '{firstHeader}' table is required");
        var table = lines.Skip(headers[0].index + 1)
            .TakeWhile(line => line.StartsWith('|'))
            .Select(line => line.Trim().Trim('|').Split('|').Select(c => c.Trim()).ToArray())
            .ToArray();
        Assert.That(table, Is.Not.Empty);
        Assert.That(table[0].All(c => c.Length > 0 && c.All(ch => ch == '-')), Is.True, "table separator");
        var rows = table.Skip(1).ToList();
        Assert.That(rows.All(row => row.Length == width), Is.True, "every row must have the expected cells");
        return rows;
    }

    private static void AssertLink(string document, string heading) =>
        Assert.That(Section(document, heading), Does.Contain($"({Link})"), heading);

    private static string ReplaceOnce(string text, string before, string after)
    {
        Assert.That(Regex.Matches(text, Regex.Escape(before)), Has.Count.EqualTo(1), "mutation must hit exactly once");
        return text.Replace(before, after, StringComparison.Ordinal);
    }

    private static string Section(string document, string heading)
    {
        document = document.Replace("\r\n", "\n", StringComparison.Ordinal);
        var matches = Regex.Matches(document, "^" + Regex.Escape(heading) + "$", RegexOptions.Multiline);
        Assert.That(matches, Has.Count.EqualTo(1), $"Exactly one '{heading}' definition is required");
        var start = matches[0].Index + heading.Length;
        var level = heading.TakeWhile(c => c == '#').Count();
        var next = Regex.Match(document[start..], @"^#{1," + level + @"} ", RegexOptions.Multiline);
        return next.Success ? document.Substring(start, next.Index) : document[start..];
    }

    private static string ReadTemplate(string file) =>
        File.ReadAllText(Path.Combine(HygieneRepository.FindRepoRoot(), "samples", "AgentBacklog", "template", file))
            .Replace("\r\n", "\n", StringComparison.Ordinal);
}
