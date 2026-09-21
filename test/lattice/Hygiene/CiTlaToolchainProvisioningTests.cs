using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The TLA+ toolchain provisioning gate: every workflow lane that runs .NET
/// tests must either provision the toolchain <c>TlcModelCheckTests</c> needs,
/// or say in the file itself why it does not.
/// <para>
/// <b>What went wrong.</b> <c>TlcModelCheckTests</c> is tagged
/// <c>[Category("Tlc")]</c>, and the deterministic tier is the complement of
/// <c>Chaos</c> and <c>Coyote</c> - so every ordinary test filter in this
/// repository SELECTS it without naming it. The fixture then fails closed
/// under <c>GITHUB_ACTIONS</c> when the toolchain is missing, deliberately, so
/// that an absent tool cannot report a green model-checking gate that checked
/// no model. Only <c>ci.yml</c> ever provisioned it. The coverage lane runs the
/// same suites with the same filter, so every run of it failed
/// <c>Orleans.Lattice.Tests</c> at <c>OneTimeSetUp</c> and dropped that
/// project's coverage, and the release lane carried the same latent break for
/// a core-package release.
/// </para>
/// <para>
/// <b>Why this is a guard and not just a fix.</b> The provisioning lived in one
/// workflow and nothing tied it to the others, so the obligation was invisible
/// to the author of every later lane - the defect was a missing relationship,
/// not a typo, and re-adding two steps in three files does not stop the fourth
/// lane from repeating it. This fixture makes the obligation explicit and
/// forces a decision: a lane that runs tests provisions the toolchain, or
/// carries an opt-out marker stating why it need not. Silence is no longer a
/// possible state.
/// </para>
/// <para>
/// <b>Why the classification is conservative.</b> The population is derived
/// from the workflow files, not listed here, so a new lane joins it the moment
/// it exists. A lane counts as running tests when it invokes <c>dotnet test</c>
/// or the leg runner, and the default obligation is to provision - because the
/// project set and the filter are usually shell variables resolved at run time,
/// so the scan cannot prove that a given run will not select
/// <c>test/lattice</c>. The escape hatch is a human statement, reviewed like
/// any other line of the workflow, rather than a heuristic that has to be right
/// about a value it cannot see.
/// </para>
/// <para>
/// The pinned release and digest are read out of the canonical lane rather than
/// duplicated here, so this gate cannot become a third copy that drifts. What
/// it enforces is that every provisioning lane agrees with that one, and that
/// each verifies the download - a jar on the classpath of a verification gate
/// is the one dependency where a silent substitution would be least visible.
/// </para>
/// </summary>
[TestFixture]
public sealed class CiTlaToolchainProvisioningTests
{
    /// <summary>The lane whose pinned release and digest every other lane must match.</summary>
    private const string CanonicalWorkflow = "ci.yml";

    /// <summary>
    /// Lanes that must be in the derived population. Anti-vacuity: a scan that
    /// stopped recognising test steps would report zero violations, which is
    /// indistinguishable from a clean result.
    /// </summary>
    private static readonly string[] ExpectedTestRunningWorkflows =
    [
        "ci.yml",
        "coverage.yml",
        "publish.yml",
    ];

    /// <summary>
    /// The marker a lane carries to declare that it needs no TLA+ toolchain.
    /// A reason must follow it, because the whole point is to record the
    /// judgement rather than to silence the gate.
    /// </summary>
    private static readonly Regex OptOutMarker = new(
        @"#\s*tla-toolchain:\s*not-required\s*-\s*(?<reason>\S.*)$",
        RegexOptions.Compiled | RegexOptions.Multiline);

    /// <summary>Invokes the .NET test runner, directly or through the leg runner.</summary>
    private static readonly Regex TestRunner = new(
        @"dotnet\s+test\b|run-test-leg\.py",
        RegexOptions.Compiled);

    private static readonly Regex JarFetch = new(
        @"tlaplus/releases/download/(?<version>v[0-9][0-9A-Za-z.\-]*)/tla2tools\.jar",
        RegexOptions.Compiled);

    private static readonly Regex JarDigest = new(
        @"(?<digest>\b[0-9a-f]{64}\b)\s+tools/tla2tools\.jar",
        RegexOptions.Compiled);

    [Test]
    public void Every_lane_that_runs_tests_provisions_the_tla_toolchain_or_records_why_it_need_not()
    {
        var running = new List<string>();
        var provisioning = new List<string>();
        var optedOut = new List<string>();
        var silent = new List<string>();

        foreach (var workflow in Workflows())
        {
            var text = File.ReadAllText(workflow.Path);
            if (!TestRunner.IsMatch(text))
            {
                continue;
            }

            running.Add(workflow.Name);

            if (JarFetch.IsMatch(text))
            {
                provisioning.Add(workflow.Name);
            }
            else if (OptOutMarker.IsMatch(text))
            {
                optedOut.Add(workflow.Name);
            }
            else
            {
                silent.Add(workflow.Name);
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                running,
                Is.SupersetOf(ExpectedTestRunningWorkflows),
                "the scan did not recognise test steps in " + string.Join(", ", ExpectedTestRunningWorkflows)
                    + ", which all run .NET tests today. It found: "
                    + (running.Count == 0 ? "<nothing>" : string.Join(", ", running))
                    + ". A clean result from a scan that matches nothing means nothing, so fix the scan "
                    + "rather than the workflows.");

            // Both arms have to be demonstrable, or the classifier has never
            // shown it can tell a provisioned lane from an exempt one.
            Assert.That(
                provisioning,
                Is.Not.Empty,
                "no workflow provisions the TLA+ toolchain at all; the fetch scan is broken");

            Assert.That(
                optedOut,
                Is.Not.Empty,
                "no workflow carries the opt-out marker, so this gate has never demonstrated that it can "
                    + "recognise one - and the next lane that legitimately needs no toolchain would be "
                    + "reported as a violation");

            Assert.That(
                silent,
                Is.Empty,
                "these workflows run .NET tests but neither provision the TLA+ toolchain nor say why they "
                    + "need not: " + string.Join(", ", silent) + ". The deterministic tier is the complement "
                    + "of Chaos and Coyote, so an ordinary filter SELECTS the Tlc category without naming it, "
                    + "and TlcModelCheckTests fails closed under GITHUB_ACTIONS when the toolchain is absent. "
                    + "Either copy the 'Setup Java' and 'Fetch TLA+ tools' steps from " + CanonicalWorkflow
                    + ", or add a '# tla-toolchain: not-required - <reason>' comment stating why this lane "
                    + "cannot select that category. See spec/README.md.");
        });
    }

    [Test]
    public void Every_provisioning_lane_pins_the_same_release_and_verifies_its_digest()
    {
        var versions = new Dictionary<string, string>(StringComparer.Ordinal);
        var digests = new Dictionary<string, string>(StringComparer.Ordinal);
        var unverified = new List<string>();

        foreach (var workflow in Workflows())
        {
            var text = File.ReadAllText(workflow.Path);
            var fetch = JarFetch.Match(text);
            if (!fetch.Success)
            {
                continue;
            }

            versions[workflow.Name] = fetch.Groups["version"].Value;

            var digest = JarDigest.Match(text);
            if (!digest.Success || !text.Contains("sha256sum --check", StringComparison.Ordinal))
            {
                unverified.Add(workflow.Name);
                continue;
            }

            digests[workflow.Name] = digest.Groups["digest"].Value;
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                versions.Keys,
                Does.Contain(CanonicalWorkflow),
                CanonicalWorkflow + " is the lane this gate reads the pinned release from and it no longer "
                    + "fetches the jar; the scan or the workflow is broken");

            Assert.That(
                unverified,
                Is.Empty,
                "these workflows download tla2tools.jar without checking its sha256: "
                    + string.Join(", ", unverified)
                    + ". A release asset can be replaced in place, and this jar is the classpath of a "
                    + "verification gate. Pipe the pinned digest through 'sha256sum --check --strict'.");

            Assert.That(
                versions.Values.Distinct(StringComparer.Ordinal).Count(),
                Is.EqualTo(1),
                "the lanes pin different TLA+ releases, so they are no longer checking the same model with "
                    + "the same tool: " + Describe(versions)
                    + ". A half-landed bump is exactly what this assertion exists to catch.");

            Assert.That(
                digests.Values.Distinct(StringComparer.Ordinal).Count(),
                Is.EqualTo(1),
                "the lanes pin different sha256 digests for tla2tools.jar: " + Describe(digests)
                    + ". One of them is verifying an artefact the others would reject.");
        });
    }

    private static string Describe(Dictionary<string, string> byWorkflow) =>
        string.Join(", ", byWorkflow.OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .Select(pair => pair.Key + " = " + pair.Value));

    private sealed record Workflow(string Name, string Path);

    private static IEnumerable<Workflow> Workflows()
    {
        var directory = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            ".github",
            "workflows");

        return Directory.EnumerateFiles(directory, "*.yml", SearchOption.TopDirectoryOnly)
            .OrderBy(path => path, StringComparer.Ordinal)
            .Select(path => new Workflow(Path.GetFileName(path), path));
    }
}
