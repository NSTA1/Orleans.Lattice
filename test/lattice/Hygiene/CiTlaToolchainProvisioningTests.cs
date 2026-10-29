using System.Text.RegularExpressions;
using System.Text.Json;
using System.Security.Cryptography;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The TLA+ toolchain provisioning gate: every workflow lane that runs .NET
/// tests must either provision the toolchain <c>TlcModelCheckTests</c> needs,
/// or say in the file itself why it does not; and every lane that provisions
/// it must do so through the one composite action, reading every pin from
/// <c>tools/tlc/toolchain.json</c>.
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
/// <b>Why one action and one pin file (#5151).</b> The provisioning used to be
/// three copied step sequences, each inlining the jar URL, its digest and the
/// JDK, held together only by this gate asserting that the copies agreed. A
/// toolchain change was therefore a workflow-only change, which can take the
/// lightweight lane and run no TLC at all. The pins now live in
/// <c>tools/tlc/toolchain.json</c> (contract C3 of #5148), the steps live in
/// <c>.github/actions/tla-toolchain</c>, and this gate refuses any workflow
/// that inlines a pin or provisions some other way, so there is no second copy
/// left to drift. A jar on the classpath of a verification gate is the one
/// dependency where a silent substitution would be least visible.
/// </para>
/// </summary>
[TestFixture]
public sealed partial class CiTlaToolchainProvisioningTests
{
    /// <summary>The workflow the negative controls inject inlined pins into.</summary>
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

    /// <summary>Lanes that provision the toolchain today; each must use the action.</summary>
    private static readonly string[] ExpectedProvisioningWorkflows =
    [
        "ci-serial-old.yml",
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

    /// <summary>A step that provisions the toolchain through the composite action.</summary>
    private static readonly Regex ActionUse = new(
        @"^\s*(?:-\s+)?uses:\s*\./\.github/actions/tla-toolchain\s*$",
        RegexOptions.Compiled | RegexOptions.Multiline);

    private static readonly Regex JarReleaseUrl = new(
        @"tlaplus/tlaplus/releases/download/\S*tla2tools\.jar",
        RegexOptions.Compiled);

    private static readonly Regex Digest = new(@"\b[0-9A-Fa-f]{64}\b", RegexOptions.Compiled);

    private static readonly Regex SetupJava = new(@"uses:\s*actions/setup-java@", RegexOptions.Compiled);

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
            if (ActionUse.IsMatch(text))
            {
                provisioning.Add(workflow.Name);
            }

            if (!TestRunner.IsMatch(text))
            {
                continue;
            }

            running.Add(workflow.Name);

            if (ActionUse.IsMatch(text))
            {
                continue;
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
                Is.SupersetOf(ExpectedProvisioningWorkflows),
                "these lanes provisioned the TLA+ toolchain and no longer reference "
                    + "./.github/actions/tla-toolchain: the action scan or the workflows are broken");

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
                    + "Either add a 'uses: ./.github/actions/tla-toolchain' step after checkout, or add a "
                    + "'# tla-toolchain: not-required - <reason>' comment stating why this lane cannot select "
                    + "that category. See spec/README.md.");
        });
    }

    [Test]
    public void No_workflow_inlines_a_toolchain_pin_or_provisions_outside_the_action()
    {
        var pins = LoadToolchainPins();
        var violations = Workflows()
            .SelectMany(workflow => InlinedToolchain(File.ReadAllText(workflow.Path), pins)
                .Select(violation => workflow.Name + ": " + violation))
            .ToArray();

        Assert.That(
            violations,
            Is.Empty,
            "every TLA+ toolchain pin comes from " + ToolchainFile + " and every lane provisions through "
                + "./.github/actions/tla-toolchain. A pin copied into a workflow is a second source that can "
                + "drift from the reviewed one, and a workflow-only change can take the lightweight lane and "
                + "run no TLC. Violations: " + string.Join("; ", violations));
    }

    [TestCase("a different jar digest", "echo \"{0}  tools/tla2tools.jar\" | sha256sum --check --strict")]
    [TestCase("the pinned jar digest", "echo \"{1}  tools/tla2tools.jar\" | sha256sum --check --strict")]
    [TestCase("a jar release URL", "curl -o tools/tla2tools.jar https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar")]
    [TestCase("a Java setup", "uses: actions/setup-java@v5")]
    [TestCase("an overlay build", "run: python3 tools/tlc/build.py --java-home \"$JAVA_HOME\" --jar tools/tla2tools.jar --output tools/tlc/build")]
    public void A_workflow_that_inlines_a_toolchain_pin_is_refused(string kind, string step)
    {
        var pins = LoadToolchainPins();
        var workflow = File.ReadAllText(Workflows().Single(workflow => workflow.Name == CanonicalWorkflow).Path);
        var different = new string(pins.JarSha256.Select(c => c == '0' ? '1' : '0').ToArray());
        var injected = workflow + "\n      - name: Inlined " + kind + "\n        "
            + string.Format(System.Globalization.CultureInfo.InvariantCulture, step, different, pins.JarSha256) + "\n";

        Assert.Multiple(() =>
        {
            Assert.That(InlinedToolchain(workflow, pins), Is.Empty, "the positive control must be clean");
            Assert.That(InlinedToolchain(injected, pins), Is.Not.Empty, kind + " must be reported");
        });
    }

    /// <summary>
    /// Everything a workflow must not do itself: name a jar release, carry a jar
    /// digest, set up Java, or build the checkpoint overlay. Each belongs to the
    /// action, which reads the pins from <c>toolchain.json</c>.
    /// </summary>
    private static List<string> InlinedToolchain(string text, ToolchainPins pins)
    {
        var violations = new List<string>();
        if (JarReleaseUrl.IsMatch(text))
        {
            violations.Add("inlines a tla2tools.jar release URL");
        }

        if (text.Contains(pins.JarSha256, StringComparison.OrdinalIgnoreCase))
        {
            violations.Add("inlines the pinned tla2tools.jar digest");
        }

        foreach (var line in text.Split('\n'))
        {
            if (line.Contains("tla2tools", StringComparison.Ordinal) && Digest.IsMatch(line))
            {
                violations.Add("inlines a tla2tools.jar digest: " + line.Trim());
            }
        }

        if (SetupJava.IsMatch(text))
        {
            violations.Add("sets up Java itself instead of through the action");
        }

        if (text.Contains("tools/tlc/build.py", StringComparison.Ordinal))
        {
            violations.Add("builds the checkpoint overlay itself instead of through the action");
        }

        return violations;
    }

    [Test]
    public void Checkpoint_inputs_preserve_reviewed_byte_hashes_and_the_toolchain_jar_pin()
    {
        var directory = Path.Combine(HygieneRepository.FindRepoRoot(), "tools", "tlc");
        using var metadata = JsonDocument.Parse(File.ReadAllBytes(Path.Combine(directory, "patch.json")));
        var pin = metadata.RootElement;
        Assert.That(pin.GetProperty("originalJarSha256").GetString(),
            Is.EqualTo(LoadToolchainPins().JarSha256.ToUpperInvariant()),
            "the overlay was reviewed against the jar " + ToolchainFile + " pins");
        var fixtures = pin.GetProperty("fixtureSha256").EnumerateObject().ToArray();
        Assert.That(fixtures.Length, Is.EqualTo(4), "probe and frozen tooling-only model/configurations");
        foreach (var fixture in fixtures)
        {
            var bytes = File.ReadAllBytes(Path.Combine(directory, fixture.Name));
            Assert.That(Convert.ToHexString(SHA256.HashData(bytes)), Is.EqualTo(fixture.Value.GetString()),
                fixture.Name + " changed or checkout line endings broke its exact-byte contract");
        }
        Assert.That(File.ReadAllText(Path.Combine(HygieneRepository.FindRepoRoot(), ".gitattributes")),
            Does.Contain("tools/tlc/** text eol=lf"));
    }

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
