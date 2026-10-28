using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Docs;

/// <summary>Guards the subsystem augmentation required by scheduling assurance.</summary>
[TestFixture]
[Category("Docs")]
public sealed class SchedulingDocumentationTests
{
    private static readonly Regex MarkdownLink = new(
        @"\[[^\]\r\n]+\]\((?<target>[^)\s]+)\)");

    private static readonly Regex NonProse = new(
        @"(?ms)^```.*?^```[^\r\n]*|<!--.*?-->");

    private static IEnumerable<TestCaseData> RequiredLinks()
    {
        yield return Link("lattice/wal-saturation-signal.md", "## Formal model", "WalAdmission");
        yield return Link("lattice/wal-saturation-signal.md", "## Formal model", "WalBackPressure");
        yield return Link("lattice/wal.md", "### Scheduling", "WalGcStarvationDrive");
        yield return Link("lattice/wal.md", "### Activation-time replay under `WalPartitions > 1`", "ReplayPermits");
        yield return Link("lattice/wal.md", "## Recovery and rebuild", "ReplayPermits");
        yield return Link("lattice/wal-tuning.md", "## Formal model and tuning limits", "WalAdmission");
        yield return Link("lattice/wal-tuning.md", "## Formal model and tuning limits", "WalBackPressure");
        yield return Link("lattice/wal-tuning.md", "## Formal model and tuning limits", "ReplayPermits");
        yield return Link("lattice/wal-tuning.md", "## Formal model and tuning limits", "WalGcStarvationDrive");
        yield return Link("lattice/wal-tuning.md", "## Formal model and tuning limits", "MaterialiserDrain");
        yield return Link("lattice/projection-rebuild.md", "### Starvation-drive admission", "WalGcStarvationDrive");
        yield return Link("lattice/projection-rebuild.md", "### Resumable cold replay", "ReplayPermits");
        yield return Link("lattice/materialised-views.md", "## Observing lag and forcing a rebuild", "MaterialiserDrain");
        yield return Link("lattice/architecture.md", "### Tag-index reconciliation", "TagIndexReconcile");
        yield return Link("lattice.replication/architecture.md", "## Formal verification", "ReplicationShipping");
        yield return Link("lattice/verified-atomic-commit.md", "## The TLA+ specification", "SagaReadProgress");
    }

    [TestCaseSource(nameof(RequiredLinks))]
    public void Subsystem_section_links_its_scheduling_refinement(
        string page, string heading, string module)
    {
        var section = Section(Read(page), heading);
        Assert.That(HasLink(section, module), Is.True,
            $"{page} {heading} must link {module}'s refinement and explain its conditional guarantee.");
    }

    [TestCaseSource(nameof(RequiredLinks))]
    public void Removing_required_link_from_corrected_section_is_detected(
        string page, string heading, string module)
    {
        var section = Section(Read(page), heading);
        Assert.That(HasLink(section, module), Is.True, "The control must contain the required link.");

        var perturbed = MarkdownLink.Replace(section, match =>
            IsTarget(match, module) ? match.Value.Replace("](", "] (", StringComparison.Ordinal) : match.Value);

        Assert.That(perturbed, Is.Not.EqualTo(section), "The mutation must remove a real link.");
        Assert.That(HasLink(perturbed, module), Is.False,
            "Removing only the required link must fail even when its name and guarantee remain.");
    }

    [TestCaseSource(nameof(RequiredLinks))]
    public void Required_refinement_target_exists(string page, string heading, string module)
    {
        Assert.That(File.Exists(Path.Combine(
            HygieneRepository.FindRepoRoot(), "spec", "scheduling", $"{module}Refinement.md")), Is.True,
            $"{page} {heading} must not publish a link before the module refinement exists.");
    }

    [Test]
    public void Scheduling_overview_keeps_existing_root_route_and_manifest_front_matter()
    {
        var root = HygieneRepository.FindRepoRoot();
        Assert.That(File.ReadAllText(Path.Combine(root, "README.md")),
            Does.Match(@"\[[^\]]+\]\(docs/lattice/verified-scheduling\.md\)"));
        var overview = Read("lattice/verified-scheduling.md");
        var frontMatter = Regex.Match(overview, @"\A---\r?\n(?<body>.*?)\r?\n---", RegexOptions.Singleline);
        Assert.That(frontMatter.Success, Is.True);
        var pointer = Regex.Match(frontMatter.Groups["body"].Value,
            @"(?m)^agent_spec: ""docs/agents/(?<path>[^""]+)""\r?$");
        Assert.That(pointer.Success, Is.True, "The overview must name a manifest-listed agent specification.");
        var manifest = File.ReadAllText(Path.Combine(root, "docs", "agents", "index.json"));
        Assert.That(manifest, Does.Contain($"\"path\": \"{pointer.Groups["path"].Value}\""));
        Assert.That(File.Exists(Path.Combine(root, "docs", "agents", pointer.Groups["path"].Value)), Is.True);
    }

    [Test]
    public void Replay_subsystem_summary_distinguishes_observed_admission_from_a_hard_queue_cap()
    {
        var recovery = Section(Read("lattice/wal.md"), "## Recovery and rebuild");
        Assert.Multiple(() =>
        {
            Assert.That(recovery, Does.Contain("not a hard queue-cardinality cap"),
                "Replay QueueBounded checks an observed admission transition, not queue length after concurrent arrivals.");
            Assert.That(recovery, Does.Contain("runtime opportunities"),
                "Drive residency counts budget continuations, not elapsed time supplied by fairness.");
        });
    }

    [Test]
    public void Shipping_subsystem_summary_retains_elapsed_time_reseed_and_history_premises()
    {
        var verification = Section(Read("lattice.replication/architecture.md"), "## Formal verification");
        Assert.Multiple(() =>
        {
            Assert.That(verification, Does.Contain("elapsed-time opportunities"),
                "Repeated retries alone do not advance the receiver deferral clock.");
            Assert.That(verification, Does.Contain("completed reseeding"),
                "Conditional recovery cannot assume a poison disposition has already completed re-seeding.");
            Assert.That(verification, Does.Contain("retained retirement history"),
                "The finite no-reseed-cycle check does not certify arbitrary histories that evict the bounded marker.");
        });
    }

    [TestCase("lattice/wal.md", "### Scheduling")]
    [TestCase("lattice/wal-tuning.md", "## Formal model and tuning limits")]
    [TestCase("lattice/projection-rebuild.md", "### Starvation-drive admission")]
    public void Gc_subsystem_summary_distinguishes_admission_from_timely_floor_progress(
        string page, string heading)
    {
        var section = Regex.Replace(Section(Read(page), heading), @"\s+", " ");
        Assert.Multiple(() =>
        {
            Assert.That(section, Does.Contain("returned physical capacity"),
                "A starvation reservation is not physical replay capacity.");
            Assert.That(section, Does.Contain("timely durable progress within the drive budget"),
                "Eventual storage completion alone does not establish FloorAdvances.");
            Assert.That(section, Does.Contain("finite timer competition and finite WAL append"),
                "The finite cohort does not certify arbitrary competing traffic or an unbounded WAL head.");
            Assert.That(section, Does.Contain("runtime opportunities"),
                "Fair continuation opportunities are not a wall-clock latency bound.");
        });
    }

    private static TestCaseData Link(string page, string heading, string module) =>
        new(page, heading, module);

    private static string Read(string page) =>
        File.ReadAllText(Path.Combine(HygieneRepository.FindRepoRoot(), "docs", page));

    private static bool HasLink(string section, string module) =>
        MarkdownLink.Matches(NonProse.Replace(section, string.Empty)).Any(match => IsTarget(match, module));

    private static bool IsTarget(Match match, string module) =>
        match.Groups["target"].Value == $"../../spec/scheduling/{module}Refinement.md";

    private static string Section(string markdown, string heading)
    {
        var lines = markdown.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        var start = Array.IndexOf(lines, heading);
        Assert.That(start, Is.GreaterThanOrEqualTo(0), $"Missing subsystem section: {heading}");
        var level = heading.TakeWhile(c => c == '#').Count();
        var end = start + 1;
        while (end < lines.Length)
        {
            var next = Regex.Match(lines[end], @"^(#{1,6}) ");
            if (next.Success && next.Groups[1].Length <= level)
                break;
            end++;
        }
        return string.Join('\n', lines[(start + 1)..end]);
    }
}
