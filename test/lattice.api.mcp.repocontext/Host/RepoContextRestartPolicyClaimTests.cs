using System.Text.RegularExpressions;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Pins the fact that issue #2906 found asserted backwards in eight places: a Docker
/// restart policy acts on process <b>exit</b> and never reads health, so
/// <c>restart: unless-stopped</c> does not restart an unhealthy-but-running container
/// and no health verdict can crash-loop one.
/// <para>
/// <b>Why a guard and not just a corrected comment.</b> This is the third time on epic
/// #2368 that the defect has been prose asserting a mechanism the code does not
/// implement, where the prose is plausible, specific, self-consistent, and nothing
/// throws (#2902, #2906). Nothing in a build reads a comment, so a corrected comment
/// decays back the moment someone reasons from first principles and re-derives the
/// intuitive-but-wrong model. The remedy has to be enforced at build time, which is
/// the same argument that makes a tally assertion beat a caveat in a pull request
/// body.
/// </para>
/// <para>
/// <b>Why the scan is by directory and not by file list.</b> Issue #2906 named one
/// site; there were eight, two of them in production source and three inside a single
/// test fixture (two of those being assertion messages, which teach the false model to
/// whoever makes the test fail). A file list would have gone stale the same way. A
/// directory walk covers a file added tomorrow.
/// </para>
/// <para>
/// <b>What this does not claim.</b> It reads tracked text and says the repository is
/// self-consistent about a documented Docker behaviour. It exercises no container. The
/// behavioural evidence is the incident in issue #2868: the container reported
/// <c>Health=unhealthy</c> continuously for 43 minutes, and because a restart resets
/// the health log and re-enters <c>starting</c>, an unbroken 43-minute unhealthy run is
/// only possible if no restart ever occurred.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextRestartPolicyClaimTests
{
    private static string RepoRoot => Path.GetFullPath(
        Path.Combine(TestContext.CurrentContext.TestDirectory, "..", "..", "..", "..", ".."));

    /// <summary>
    /// Text that mentions a crash loop within this many characters of a restart-policy
    /// token is treated as commenting on the two together. Wide enough to span the
    /// wrapped multi-line comments these claims live in.
    /// </summary>
    private const int WindowChars = 600;

    private static readonly Regex CrashLoop =
        new(@"crash[- ]?loop", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex RestartPolicy =
        new(@"unless-stopped", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    /// <summary>
    /// The closed set of mechanism statements. The requirement is not that these two
    /// subjects never appear together - the corrected comments must discuss them together,
    /// that being the entire remedy - but that wherever they do, the text states how a
    /// restart policy actually behaves.
    /// <para>
    /// An earlier draft accepted any denial ("does not", "never", "cannot"). Building the
    /// perturbation harness disproved that: the Dockerfile's original false comment read
    /// "a silo still joining ... does not crash-loop the container under compose's
    /// restart: unless-stopped", which contains a denial and would have passed, while
    /// still asserting the false model by implication - that without the start period it
    /// WOULD crash-loop. Requiring the mechanism rather than a denial closes that, because
    /// the false model cannot be stated in these terms at all.
    /// </para>
    /// </summary>
    private static readonly Regex MechanismStatement = new(
        @"never reads health|never read health|does not read health|acts on process exit"
        + @"|acts on container exit|regardless of exit code|not restart an unhealthy",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    /// <summary>The canonical denial every file carrying the restart-policy rationale must state.</summary>
    private static readonly Regex CanonicalDenial =
        new(@"never reads health", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    /// <summary>
    /// Directories whose tracked text must not couple a health verdict to a restart.
    /// Scanned wholesale so a file added later is covered without editing this fixture.
    /// </summary>
    private static readonly (string RelativePath, string[] Patterns)[] ScannedRoots =
    [
        (Path.Combine("apps", "repocontext"), ["*.cs", "Dockerfile"]),
        (Path.Combine("samples", "RepoContextContainer"), ["*.yml", "*.md", "*.ps1"]),
        (Path.Combine("docs", "lattice.api.mcp.repocontext"), ["*.md"]),
        (Path.Combine("test", "lattice.api.mcp.repocontext"), ["*.cs"]),
    ];

    /// <summary>
    /// The files that carry the restart-policy rationale and must therefore state the
    /// denial outright. This is the known-positive control for the absence arm below: a
    /// scan pointed at the wrong root, or one silently reading nothing, reddens here
    /// instead of reporting a clean and entirely meaningless zero.
    /// </summary>
    private static readonly string[] RationaleSites =
    [
        Path.Combine("samples", "RepoContextContainer", "docker-compose.yml"),
        Path.Combine("apps", "repocontext", "Dockerfile"),
        Path.Combine("apps", "repocontext", "Hosting", "RepoContextSiloHealthCheck.cs"),
        Path.Combine("docs", "lattice.api.mcp.repocontext", "container.md"),
        Path.Combine("test", "lattice.api.mcp.repocontext", "Host", "RepoContextComposeHealthcheckTests.cs"),
        Path.Combine("test", "lattice.api.mcp.repocontext", "Host", "RepoContextSiloHealthCheckTests.cs"),
    ];

    /// <summary>
    /// Returns the offending excerpts: every place a crash-loop mention sits within
    /// <see cref="WindowChars"/> of a restart-policy token without the mechanism being
    /// stated between them.
    /// </summary>
    private static List<string> FindUnexplainedCouplings(string text)
    {
        var offences = new List<string>();

        foreach (Match crash in CrashLoop.Matches(text))
        {
            var start = Math.Max(0, crash.Index - WindowChars);
            var end = Math.Min(text.Length, crash.Index + crash.Length + WindowChars);
            var window = text[start..end];

            if (RestartPolicy.IsMatch(window) && !MechanismStatement.IsMatch(window))
            {
                offences.Add(window.ReplaceLineEndings(" ").Trim());
            }
        }

        return offences;
    }

    /// <summary>
    /// This fixture's own source, which necessarily contains an undenied specimen of the
    /// false claim and would otherwise flag itself. Excluded by name, and the exclusion is
    /// not taken on trust: the absence arm asserts this file IS flagged when read from
    /// disk, which proves the whole pipeline (enumerate, read, match) fires on real file
    /// content rather than only on an inline constant.
    /// </summary>
    private const string SpecimenFileName = "RepoContextRestartPolicyClaimTests.cs";

    private static IEnumerable<string> ScannedFiles()
    {
        foreach (var (relativePath, patterns) in ScannedRoots)
        {
            var root = Path.Combine(RepoRoot, relativePath);
            if (!Directory.Exists(root))
            {
                continue;
            }

            foreach (var pattern in patterns)
            {
                foreach (var file in Directory.EnumerateFiles(root, pattern, SearchOption.AllDirectories))
                {
                    if (Path.GetFileName(file) == SpecimenFileName)
                    {
                        continue;
                    }

                    yield return file;
                }
            }
        }
    }

    // --- Controls on the predicate itself -----------------------------------------
    //
    // The two arms below interrogate the detector rather than the repository. Without
    // them a green absence arm is uninterpretable: a predicate that never fires and a
    // repository that is clean produce identical output. They are deliberately paired
    // over the SAME subject matter with opposite expected verdicts, so a predicate that
    // always answers the same way fails one of them whichever way it is stuck.

    [Test]
    public void The_detector_fires_on_the_false_claim_as_it_was_actually_written()
    {
        // The real text from docker-compose.yml before issue #2906 corrected it.
        const string FalseClaim =
            "Getting it too short matters here because restart: unless-stopped is set below, "
            + "so a silo still joining that were reported unhealthy would crash-loop during normal boot.";

        Assert.That(
            FindUnexplainedCouplings(FalseClaim),
            Is.Not.Empty,
            "the detector must fire on the exact wording issue #2906 was filed against, or a clean "
            + "scan of the repository proves nothing whatsoever");
    }

    [Test]
    public void The_detector_fires_on_a_false_claim_that_is_phrased_as_a_denial()
    {
        // The real text from apps/repocontext/Dockerfile before issue #2906 corrected it.
        // It contains "does not", so a predicate that accepted any denial would have
        // passed it - while it still asserts the false model by implication, namely that
        // without the start period the container WOULD crash-loop on its health verdict.
        // This arm exists because an earlier draft of this fixture had exactly that hole,
        // and it was found by perturbation rather than by reading the predicate.
        const string FalseClaimPhrasedAsDenial =
            "so a silo still joining reads as \"starting\" rather than a failure and does "
            + "not crash-loop the container under compose's restart: unless-stopped.";

        Assert.That(
            FindUnexplainedCouplings(FalseClaimPhrasedAsDenial),
            Is.Not.Empty,
            "a denial is not a mechanism: the detector must still fire on text that denies a "
            + "crash loop while implying the health verdict could cause one");
    }

    [Test]
    public void The_detector_stays_silent_on_the_corrected_claim()
    {
        // Same subject matter, same two tokens, opposite meaning. If the detector simply
        // flagged any co-occurrence it would fire here too, and the guard would forbid
        // the repository from documenting the correction at all.
        const string CorrectedClaim =
            "A Docker restart policy acts on process exit and never reads health, so "
            + "restart: unless-stopped does not restart an unhealthy-but-running container "
            + "and a silo still joining cannot crash-loop because of its health verdict.";

        Assert.That(
            FindUnexplainedCouplings(CorrectedClaim),
            Is.Empty,
            "the detector must accept a stated mechanism, or the only way to pass it would be to "
            + "delete the explanation rather than correct it");
    }

    // --- The repository arms -------------------------------------------------------

    [Test]
    public void Every_file_carrying_the_restart_policy_rationale_denies_the_health_coupling()
    {
        Assert.Multiple(() =>
        {
            foreach (var relativePath in RationaleSites)
            {
                var absolute = Path.Combine(RepoRoot, relativePath);

                Assert.That(
                    File.Exists(absolute),
                    Is.True,
                    $"{relativePath} is missing; this fixture's paths have drifted and its absence arm "
                    + "is reading nothing");

                Assert.That(
                    CanonicalDenial.IsMatch(File.ReadAllText(absolute)),
                    Is.True,
                    $"{relativePath} explains the healthcheck's start_period but never states that a "
                    + "restart policy 'never reads health'. Issue #2906: the intuitive model is wrong "
                    + "and self-consistent, so the next reader re-derives it unless the denial is written "
                    + "down at the site");
            }
        });
    }

    [Test]
    public void No_repocontext_container_text_asserts_that_a_health_verdict_restarts_a_container()
    {
        var scanned = 0;
        var offences = new List<string>();

        foreach (var file in ScannedFiles())
        {
            scanned++;
            foreach (var offence in FindUnexplainedCouplings(File.ReadAllText(file)))
            {
                offences.Add($"{Path.GetRelativePath(RepoRoot, file)}: ...{offence}...");
            }
        }

        Assert.Multiple(() =>
        {
            // Known-positive control on the walk, not on the predicate: an empty verdict
            // from a scan that opened no files is not evidence of anything.
            Assert.That(
                scanned,
                Is.GreaterThan(RationaleSites.Length),
                "the scan read too few files to have covered the rationale sites; its clean verdict "
                + "would be an artefact of reading nothing");

            // Known-positive control on the FULL pipeline, read from disk. The one file
            // excluded above holds an undenied specimen of the false claim, so it must
            // still be flagged when read the same way every scanned file is read. If this
            // reddens, the exclusion is hiding a detector that no longer fires.
            var specimen = Path.Combine(
                RepoRoot, "test", "lattice.api.mcp.repocontext", "Host", SpecimenFileName);

            Assert.That(File.Exists(specimen), Is.True, $"{SpecimenFileName} has moved; the exclusion no longer names a real file");
            Assert.That(
                FindUnexplainedCouplings(File.ReadAllText(specimen)),
                Is.Not.Empty,
                "the excluded specimen file must still contain an undenied specimen read from disk, or "
                + "the clean scan below is a detector that has never been shown to fire on file content");

            Assert.That(
                offences,
                Is.Empty,
                "a Docker restart policy acts on process exit and never reads health, so no health "
                + "verdict restarts or crash-loops a container (issue #2906). Health-triggered restart "
                + "is a Swarm and Kubernetes feature, not a restart: policy. Offending text:"
                + Environment.NewLine
                + string.Join(Environment.NewLine, offences));
        });
    }
}
