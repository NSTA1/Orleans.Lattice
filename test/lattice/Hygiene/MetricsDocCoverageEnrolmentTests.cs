using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Repository-wide gate on the <b>enrolment</b> of packages into the
/// instrument-documentation guard family. Asserts that every package under
/// <c>src/</c> which publishes at least one metric instrument is covered by some
/// <c>MetricsDocCoverageTestsBase</c> subclass, so a package cannot quietly sit
/// outside the guard the way seven of them did.
/// </summary>
/// <remarks>
/// <para>
/// This exists because the per-package doc-coverage fixtures fail in the one mode
/// they cannot report on: <b>absence</b>. A package that never subclasses the base
/// has no fixture, so it emits no assertion, no warning, and no output at all - the
/// suite is green and the coverage is zero. Every individual fixture was working
/// correctly while most of the instrument surface went unguarded, because "is this
/// package enrolled?" is a question no per-package fixture is in a position to ask.
/// Only a gate that enumerates <i>both</i> sides can ask it, which is what this one
/// does.
/// </para>
/// <para>
/// The failure it prevents is therefore not a red test that someone ignored. It is
/// a green suite that means less than it appears to, which is strictly worse: a
/// coverage guard enumerating the wrong set reports success and carries no
/// information. That makes the anti-vacuity floor below load-bearing rather than
/// ceremonial - a meta-guard that silently discovers nothing would reproduce the
/// exact defect it was written to close.
/// </para>
/// <para>
/// Every assertion is a deterministic file scan. Nothing depends on a
/// <c>MeterListener</c>, on timing, or on test ordering, so this cannot flake and
/// cannot be perturbed by another fixture running first.
/// </para>
/// </remarks>
[TestFixture]
public sealed class MetricsDocCoverageEnrolmentTests
{
    /// <summary>
    /// Calls that construct a metric instrument. A package containing any of these
    /// publishes instruments and therefore owes a documentation fixture.
    /// </summary>
    private static readonly string[] InstrumentFactoryCalls =
    [
        "CreateCounter<",
        "CreateUpDownCounter<",
        "CreateHistogram<",
        "CreateObservableCounter",
        "CreateObservableUpDownCounter",
        "CreateObservableGauge",
    ];

    /// <summary>
    /// Instrument-publishing packages deliberately exempt from enrolment, each with
    /// the reason it is exempt. An entry here is a standing claim that wants
    /// revisiting, not a permanent excuse, so the set is asserted to be exactly
    /// this small.
    /// </summary>
    /// <remarks>
    /// Deliberately empty. Every instrument-publishing package is enrolled, so the
    /// exemption test below iterates nothing and passes vacuously - which is the
    /// honest reading of "every exemption is still justified" when there are none.
    /// No anti-vacuity floor is asserted here on purpose: a floor requiring at
    /// least one exemption would make the fully-enrolled state unreachable, which
    /// is the state this gate exists to drive the repository towards.
    /// </remarks>
    private static readonly (string Package, string Reason)[] ExemptPackages = [];

    /// <summary>
    /// A package known to publish instruments, asserted to be discovered by the
    /// scan below. This is the positive control: it proves the scan can find
    /// anything at all, so a zero result is evidence of absence rather than
    /// evidence of a broken scan.
    /// </summary>
    private const string PositiveControlPackage = "lattice";

    /// <summary>
    /// A package known <b>not</b> to publish instruments, asserted to be absent from
    /// the scan. This is the negative control: it proves the scan discriminates,
    /// so a package appearing in the publishing set means something.
    /// </summary>
    private const string NegativeControlPackage = "lattice.schema";

    [Test]
    public void Every_instrument_publishing_package_is_enrolled_in_the_documentation_guard()
    {
        var publishing = DiscoverInstrumentPublishingPackages();
        var enrolled = DiscoverEnrolledPackages();

        // Anti-vacuity. Both scans must find something, and the controls must
        // behave, before any conclusion drawn from them means anything.
        Assert.Multiple(() =>
        {
            Assert.That(
                publishing,
                Does.Contain(PositiveControlPackage),
                $"Positive control failed: the instrument scan did not find '{PositiveControlPackage}', "
                    + "which is known to publish instruments. The scan is broken, so its result carries "
                    + "no information and a green run here would be meaningless.");

            Assert.That(
                publishing,
                Does.Not.Contain(NegativeControlPackage),
                $"Negative control failed: the instrument scan found '{NegativeControlPackage}', "
                    + "which publishes no instruments. The scan is over-matching, so the publishing set "
                    + "cannot be trusted.");

            Assert.That(
                enrolled,
                Is.Not.Empty,
                "The enrolment scan found no MetricsDocCoverageTestsBase subclass anywhere under test/. "
                    + "Either every fixture was deleted or the scan is broken; both make this gate vacuous.");

            // Self-validation of the parser. Every name it lifts out of a
            // SourceRoots literal must be a real package directory; anything else
            // means the scan matched something that is not a source root, and a
            // set containing junk cannot be trusted to prove enrolment.
            var notRealPackages = enrolled
                .Where(p => !Directory.Exists(Path.Combine(HygieneRepository.FindRepoRoot(), "src", p)))
                .OrderBy(p => p, StringComparer.Ordinal)
                .ToArray();

            Assert.That(
                notRealPackages,
                Is.Empty,
                "The enrolment scan produced entries that are not directories under src/: "
                    + $"{string.Join(", ", notRealPackages)}. The SourceRoots literal parser is matching "
                    + "text that is not a source root, so the enrolled set is polluted and any package it "
                    + "appears to cover may not actually be covered.");
        });

        var exempt = ExemptPackages.Select(e => e.Package).ToHashSet(StringComparer.Ordinal);
        var unenrolled = publishing
            .Where(p => !enrolled.Contains(p) && !exempt.Contains(p))
            .OrderBy(p => p, StringComparer.Ordinal)
            .ToArray();

        Assert.That(
            unenrolled,
            Is.Empty,
            "These packages publish metric instruments but no MetricsDocCoverageTestsBase subclass covers "
                + $"them, so their instruments are documented by nobody's assertion: {string.Join(", ", unenrolled)}. "
                + "Add a fixture whose SourceRoots names the package (see BackupMetricsDocCoverageTests for "
                + "the minimal shape), or add the package to ExemptPackages with a reason.");
    }

    [Test]
    public void Every_exempt_package_publishes_instruments_and_is_not_already_enrolled()
    {
        var publishing = DiscoverInstrumentPublishingPackages();
        var enrolled = DiscoverEnrolledPackages();

        Assert.Multiple(() =>
        {
            foreach (var (package, reason) in ExemptPackages)
            {
                Assert.That(
                    publishing,
                    Does.Contain(package),
                    $"'{package}' is listed exempt (reason: {reason}) but publishes no instruments, so the "
                        + "exemption is dead configuration. Remove it.");

                Assert.That(
                    enrolled,
                    Does.Not.Contain(package),
                    $"'{package}' is listed exempt (reason: {reason}) but a fixture now covers it. The "
                        + "exemption is stale and is masking real coverage. Remove it.");
            }
        });
    }

    /// <summary>
    /// Packages under <c>src/</c> whose sources construct at least one instrument.
    /// Build output is excluded so generated code cannot inflate the set.
    /// </summary>
    private static SortedSet<string> DiscoverInstrumentPublishingPackages()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var srcRoot = Path.Combine(repoRoot, "src");
        var publishing = new SortedSet<string>(StringComparer.Ordinal);

        foreach (var packageDir in Directory.EnumerateDirectories(srcRoot))
        {
            var package = Path.GetFileName(packageDir);

            foreach (var file in Directory.EnumerateFiles(packageDir, "*.cs", SearchOption.AllDirectories))
            {
                if (IsBuildOutput(file))
                {
                    continue;
                }

                var text = File.ReadAllText(file);
                if (InstrumentFactoryCalls.Any(call => text.Contains(call, StringComparison.Ordinal)))
                {
                    publishing.Add(package);
                    break;
                }
            }
        }

        return publishing;
    }

    /// <summary>
    /// Packages named by the <c>SourceRoots</c> of some
    /// <c>MetricsDocCoverageTestsBase</c> subclass. Reading the declared source
    /// roots, rather than inferring coverage from the fixture's own directory, is
    /// what makes a fixture that points at the wrong package visible as a gap.
    /// </summary>
    private static SortedSet<string> DiscoverEnrolledPackages()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var testRoot = Path.Combine(repoRoot, "test");
        var enrolled = new SortedSet<string>(StringComparer.Ordinal);
        var sourceRootLiteral = new Regex(@"""src/([^""]+)""", RegexOptions.CultureInvariant);

        // Match a genuine subclass DECLARATION, not merely the text of the base
        // class name. This file names that text itself (in the scan below), so a
        // substring test would make the scanner match itself and lift its own
        // regex source into the enrolled set as a bogus package. Worse, any
        // "src/<pkg>" string appearing anywhere in this file would then read as a
        // real enrolment and could mask a package whose fixture had been deleted.
        var subclassDeclaration = new Regex(
            @"class\s+\w+\s*:\s*MetricsDocCoverageTestsBase\b",
            RegexOptions.CultureInvariant);

        foreach (var file in Directory.EnumerateFiles(testRoot, "*.cs", SearchOption.AllDirectories))
        {
            if (IsBuildOutput(file))
            {
                continue;
            }

            var text = File.ReadAllText(file);
            if (!subclassDeclaration.IsMatch(text))
            {
                continue;
            }

            foreach (Match match in sourceRootLiteral.Matches(text))
            {
                enrolled.Add(match.Groups[1].Value);
            }
        }

        return enrolled;
    }

    private static bool IsBuildOutput(string path) =>
        path.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
        || path.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal);
}
