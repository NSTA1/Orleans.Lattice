using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Repository-wide gate on the <b>enrolment</b> of meters into the dashboard-coverage
/// guard family. Asserts that every meter some package under <c>src/</c> publishes
/// instruments onto is covered by some charting guard, so a meter cannot quietly go
/// unpaneled and unguarded the way the repository-context meter did.
/// </summary>
/// <remarks>
/// <para>
/// The sibling <see cref="MetricsDocCoverageEnrolmentTests"/> closes this hole for
/// documentation. This closes it for charting, and the two are not interchangeable:
/// a documented instrument nobody charts is invisible on the dashboards that get
/// looked at during an incident, which is when the absence costs the most.
/// </para>
/// <para>
/// <b>Keyed on meters, not on fixtures.</b> The obvious implementation - enumerate
/// <c>MeterDashboardCoverageTestsBase</c> subclasses and check every package has one -
/// is wrong here, and wrong in the direction that produces a confident false pass.
/// <c>DashboardJsonTests</c> in <c>test/lattice.dashboards</c> is a legitimate and
/// thorough charting guard that does <b>not</b> subclass the base, and it is the only
/// guard for <c>orleans.lattice</c> and <c>orleans.lattice.replication</c> - the two
/// largest meters in the repository. A subclass-keyed gate would report both as
/// unguarded, and the natural way to make such a gate green is to add exemptions for
/// them, at which point the gate is asserting the opposite of the truth about the
/// surface that matters most.
/// </para>
/// <para>
/// So coverage is expressed in the unit the question is actually about - the meter -
/// and guards that do not subclass the base are declared in
/// <see cref="NonSubclassChartingGuards"/>. That registry is the one hand-maintained
/// input here, so it is <b>validated rather than trusted</b>: each entry must name a
/// file that exists and that still references the meter constants it claims to cover.
/// Deleting or renaming that guard breaks this gate, rather than silently shrinking
/// coverage while the gate goes on reporting success.
/// </para>
/// <para>
/// Every assertion is a deterministic file scan. Nothing depends on a
/// <c>MeterListener</c>, on timing, or on test ordering, so this cannot flake and
/// cannot be perturbed by another fixture running first. In particular it does not
/// depend on which instruments happen to have been constructed by the time it runs,
/// which is the failure mode that makes a live snapshot unusable for a coverage
/// question.
/// </para>
/// </remarks>
[TestFixture]
public sealed class MeterDashboardCoverageEnrolmentTests
{
    /// <summary>
    /// Calls that construct a metric instrument. A package containing any of these
    /// publishes instruments and therefore owes charting coverage for the meter it
    /// publishes them onto.
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
    /// Charting guards that cover one or more meters without subclassing
    /// <c>MeterDashboardCoverageTestsBase</c>, with the meter constants each claims.
    /// </summary>
    /// <remarks>
    /// Hand-maintained, and therefore the weakest input to this gate, which is why
    /// <see cref="Every_registered_non_subclass_charting_guard_still_exists_and_covers_what_it_claims"/>
    /// checks it against the file system rather than believing it. An entry that has
    /// gone stale would silently credit coverage to a guard that no longer exists,
    /// which is the precise shape of failure this whole fixture is written to prevent.
    /// </remarks>
    private static readonly (string FixturePath, string[] MeterConstantTokens)[] NonSubclassChartingGuards =
    [
        (
            "test/lattice.dashboards/DashboardJsonTests.cs",
            ["LatticeMetrics.MeterName", "LatticeReplicationMetrics.MeterName"]
        ),
    ];

    /// <summary>
    /// Meters deliberately charted by no dashboard, each with the reason. An entry is
    /// a recorded decision that a reader can find and challenge, not a silence.
    /// </summary>
    /// <remarks>
    /// The distinction this fixture turns on is between a meter nobody charted and a
    /// meter somebody decided not to chart. Both look identical from the dashboards;
    /// only one of them is a decision. An entry here converts the first into the
    /// second, and the companion staleness test stops it decaying back.
    /// </remarks>
    private static readonly (string Meter, string Reason)[] IntentionallyUnpaneledMeters =
    [
        (
            "Orleans.Lattice.Api.Mcp.RepoContext",
            "No bundled dashboard charts the repository-context surface, and the gap is recorded row by row "
                + "in docs/lattice.dashboards/metrics-to-panel-map.md rather than left to be inferred. "
                + "Charting it is blocked on a structural property of the container's exposition rather than "
                + "on effort: a Histogram<T> renders there as a Prometheus summary carrying _sum and _count "
                + "and no _bucket, so a histogram_quantile panel over repocontext.retrieval.ready_seconds "
                + "returns nothing, and the usual 'or vector(0)' idiom would substitute a literal zero "
                + "indistinguishable from a genuine sustained-zero fault. Shipping that panel would "
                + "manufacture the exact reading this guard family exists to make impossible. "
                + "RepoContextMetricsToPanelMapTests holds the recorded state in place."
        ),
    ];

    /// <summary>
    /// A meter known to be published and charted, asserted to be discovered on both
    /// sides. The positive control: it proves each scan can find anything at all, so
    /// an empty result is evidence of absence rather than of a broken scan.
    /// </summary>
    private const string PositiveControlMeter = "orleans.lattice";

    /// <summary>
    /// A package known <b>not</b> to publish instruments, asserted to be absent from
    /// the publishing scan. The negative control: it proves the scan discriminates.
    /// </summary>
    private const string NegativeControlPackage = "lattice.schema";

    /// <summary>
    /// The number of meter-name constants below which the resolution map is assumed
    /// broken rather than merely small.
    /// </summary>
    /// <remarks>
    /// A floor, not a count. It is deliberately far below the ten constants that
    /// exist today, so adding or removing a package does not touch it, while a regex
    /// that stops matching - which would resolve every meter to nothing and report
    /// perfect coverage - still fails here.
    /// </remarks>
    private const int MinimumMeterConstants = 5;

    [Test]
    public void Every_published_meter_is_covered_by_some_charting_guard()
    {
        var meterConstants = ResolveMeterNameConstants();
        var publishing = DiscoverPublishedMeters(meterConstants);
        var covered = DiscoverChartingGuardMeters(meterConstants);

        var publishedMeters = publishing.Values
            .SelectMany(v => v)
            .ToHashSet(StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(
                meterConstants,
                Has.Count.GreaterThanOrEqualTo(MinimumMeterConstants),
                $"Only {meterConstants.Count} 'const string MeterName' declaration(s) resolved under src/, "
                    + $"below the floor of {MinimumMeterConstants}. The parser has most likely stopped "
                    + "matching, in which case every meter resolves to nothing and this gate would report "
                    + "total coverage of an empty set.");

            Assert.That(
                publishedMeters,
                Does.Contain(PositiveControlMeter),
                $"Positive control failed: the publishing scan did not find '{PositiveControlMeter}', which "
                    + "is published by the core package. The scan is broken, so a green run here would carry "
                    + "no information.");

            Assert.That(
                covered,
                Does.Contain(PositiveControlMeter),
                $"Positive control failed: no charting guard was found covering '{PositiveControlMeter}', "
                    + "which DashboardJsonTests covers. The guard-side scan is broken, so every meter would "
                    + "read as uncovered and the failure list below would be noise.");

            Assert.That(
                publishing.Keys,
                Does.Not.Contain(NegativeControlPackage),
                $"Negative control failed: the publishing scan found '{NegativeControlPackage}', which "
                    + "publishes no instruments. The scan is over-matching, so the published set cannot be "
                    + "trusted.");
        });

        var exempt = IntentionallyUnpaneledMeters.Select(e => e.Meter).ToHashSet(StringComparer.Ordinal);
        var uncovered = publishing
            .SelectMany(p => p.Value.Select(meter => (Package: p.Key, Meter: meter)))
            .Where(x => !covered.Contains(x.Meter) && !exempt.Contains(x.Meter))
            .Select(x => $"{x.Meter}  (published by src/{x.Package})")
            .Distinct(StringComparer.Ordinal)
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToArray();

        TestContext.Out.WriteLine(
            $"Examined {publishedMeters.Count} published meter(s) from {publishing.Count} package(s) against "
                + $"{covered.Count} meter(s) covered by charting guards, resolved from {meterConstants.Count} "
                + "meter-name constant(s).");

        Assert.That(
            uncovered,
            Is.Empty,
            "These meters carry published instruments that no charting guard covers, so nothing fails if they "
                + $"are charted nowhere:{Environment.NewLine}  - "
                + string.Join(Environment.NewLine + "  - ", uncovered)
                + $"{Environment.NewLine}Add a MeterDashboardCoverageTestsBase subclass whose MeterName names "
                + "the meter (see BackupMeterDashboardCoverageTests for the minimal shape); or, if the guard "
                + "does not subclass the base, register it in NonSubclassChartingGuards; or, if the meter is "
                + "deliberately charted nowhere, add it to IntentionallyUnpaneledMeters with the reason.");
    }

    [Test]
    public void Every_publishing_package_resolves_to_at_least_one_meter()
    {
        var meterConstants = ResolveMeterNameConstants();
        var packages = DiscoverInstrumentPublishingPackages();
        var publishing = DiscoverPublishedMeters(meterConstants);

        Assert.That(
            packages,
            Is.Not.Empty,
            "No package under src/ was found to publish instruments at all. The factory-call scan is broken.");

        var unresolved = packages
            .Where(p => !publishing.ContainsKey(p))
            .OrderBy(p => p, StringComparer.Ordinal)
            .ToArray();

        Assert.That(
            unresolved,
            Is.Empty,
            "These packages construct metric instruments but no meter name could be resolved for them: "
                + $"{string.Join(", ", unresolved)}. This gate must fail rather than skip them. A package "
                + "whose meter cannot be resolved contributes nothing to the published set, so it would be "
                + "reported as fully covered while being charted by nobody - a package publishing onto a "
                + "meter this fixture cannot name is exactly the blind spot it exists to remove. Declare a "
                + "'const string MeterName' on the package's metrics class, or extend "
                + "ResolveMeterNameConstants to understand the form used.");
    }

    [Test]
    public void Every_charting_guard_meter_expression_resolves_to_a_known_meter()
    {
        var meterConstants = ResolveMeterNameConstants();
        var unresolved = DiscoverSubclassMeterExpressions()
            .Where(e => !meterConstants.ContainsKey(e.TypeName))
            .Select(e => $"{e.TypeName}.MeterName  (in {e.Fixture})")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToArray();

        Assert.That(
            unresolved,
            Is.Empty,
            "These charting fixtures name a meter constant this gate cannot resolve, so the meters they "
                + $"cover are not counted as covered:{Environment.NewLine}  - "
                + string.Join(Environment.NewLine + "  - ", unresolved)
                + $"{Environment.NewLine}An unresolved expression silently shrinks the covered set, which "
                + "makes real coverage read as a gap and invites an exemption that asserts the opposite of "
                + "the truth. Extend ResolveMeterNameConstants rather than working around it.");
    }

    [Test]
    public void Every_registered_non_subclass_charting_guard_still_exists_and_covers_what_it_claims()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();

        Assert.That(
            NonSubclassChartingGuards,
            Is.Not.Empty,
            "The non-subclass charting-guard registry is empty. DashboardJsonTests covers the two largest "
                + "meters without subclassing the base, so an empty registry means those meters now read as "
                + "uncovered and somebody is about to exempt them.");

        Assert.Multiple(() =>
        {
            foreach (var (fixturePath, tokens) in NonSubclassChartingGuards)
            {
                var path = Path.Combine(repoRoot, fixturePath.Replace('/', Path.DirectorySeparatorChar));

                Assert.That(
                    File.Exists(path),
                    Is.True,
                    $"Registered charting guard '{fixturePath}' does not exist. The registry is crediting "
                        + "coverage to a fixture that has been deleted or moved, so the meters it claims are "
                        + "in fact guarded by nothing.");

                if (!File.Exists(path))
                {
                    continue;
                }

                var text = File.ReadAllText(path);
                foreach (var token in tokens)
                {
                    Assert.That(
                        text,
                        Does.Contain(token),
                        $"Registered charting guard '{fixturePath}' no longer references '{token}', so it "
                            + "has probably stopped covering that meter while this registry goes on claiming "
                            + "it does. Update the fixture or the registry entry together.");
                }
            }
        });
    }

    [Test]
    public void Every_unpaneled_meter_exemption_is_published_uncovered_and_reasoned()
    {
        var meterConstants = ResolveMeterNameConstants();
        var published = DiscoverPublishedMeters(meterConstants)
            .Values
            .SelectMany(v => v)
            .ToHashSet(StringComparer.Ordinal);
        var covered = DiscoverChartingGuardMeters(meterConstants);

        Assert.Multiple(() =>
        {
            foreach (var (meter, reason) in IntentionallyUnpaneledMeters)
            {
                Assert.That(
                    published,
                    Does.Contain(meter),
                    $"'{meter}' is recorded as intentionally unpaneled but no package publishes onto it, so "
                        + "the entry is dead configuration describing a meter that no longer exists.");

                Assert.That(
                    covered,
                    Does.Not.Contain(meter),
                    $"'{meter}' is recorded as intentionally unpaneled but a charting guard now covers it. "
                        + "The entry is stale and is suppressing a check that would otherwise pass, so remove "
                        + "it and let the coverage be asserted.");

                Assert.That(
                    reason,
                    Is.Not.Null.And.Not.Empty,
                    $"'{meter}' is recorded as intentionally unpaneled with no reason. An exemption that does "
                        + "not say why cannot be reviewed and will never be revisited.");
            }
        });
    }

    /// <summary>
    /// Maps the declaring type name of every <c>const string MeterName</c> under
    /// <c>src/</c> to the literal meter name it resolves to, following a single alias
    /// hop such as <c>GrainIndexMetrics.MeterName = LatticeMetrics.MeterName</c>.
    /// </summary>
    /// <returns>Type name to meter name.</returns>
    /// <remarks>
    /// One hop, not arbitrary depth, because one hop is what the tree contains and a
    /// general resolver would be speculative. Depth is not a silent limitation: an
    /// alias this cannot follow leaves its type out of the map, and both
    /// <see cref="Every_publishing_package_resolves_to_at_least_one_meter"/> and
    /// <see cref="Every_charting_guard_meter_expression_resolves_to_a_known_meter"/>
    /// fail loudly on the omission rather than treating it as coverage.
    /// </remarks>
    private static Dictionary<string, string> ResolveMeterNameConstants()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var srcRoot = Path.Combine(repoRoot, "src");

        var typeDeclaration = new Regex(
            @"\b(?:class|struct|interface|record(?:\s+(?:class|struct))?)\s+(\w+)",
            RegexOptions.CultureInvariant);
        var meterNameConstant = new Regex(
            @"const\s+string\s+MeterName\s*=\s*(?:""(?<literal>[^""]+)""|(?<alias>[\w.]+))\s*;",
            RegexOptions.CultureInvariant);

        var literals = new Dictionary<string, string>(StringComparer.Ordinal);
        var aliases = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var file in Directory.EnumerateFiles(srcRoot, "*.cs", SearchOption.AllDirectories))
        {
            if (IsBuildOutput(file))
            {
                continue;
            }

            var text = File.ReadAllText(file);
            foreach (Match constant in meterNameConstant.Matches(text))
            {
                var owner = typeDeclaration.Matches(text)
                    .Where(t => t.Index < constant.Index)
                    .Select(t => t.Groups[1].Value)
                    .LastOrDefault();

                if (owner is null)
                {
                    continue;
                }

                if (constant.Groups["literal"].Success)
                {
                    literals[owner] = constant.Groups["literal"].Value;
                }
                else
                {
                    // "SomeType.MeterName" - keep the declaring type only.
                    var alias = constant.Groups["alias"].Value;
                    var dot = alias.IndexOf('.', StringComparison.Ordinal);
                    aliases[owner] = dot < 0 ? alias : alias[..dot];
                }
            }
        }

        foreach (var (owner, aliasedType) in aliases)
        {
            if (literals.TryGetValue(aliasedType, out var resolved))
            {
                literals[owner] = resolved;
            }
        }

        return literals;
    }

    /// <summary>
    /// Packages under <c>src/</c> whose sources construct at least one instrument.
    /// </summary>
    /// <returns>The package directory names.</returns>
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
    /// Maps each instrument-publishing package to the meter names it publishes onto.
    /// </summary>
    /// <param name="meterConstants">The resolved meter-name constants.</param>
    /// <returns>Package name to the meter names it publishes onto.</returns>
    /// <remarks>
    /// <para>
    /// Attribution is to the <b>meter</b>, not to the package, and the two come
    /// apart. A package's own <c>MeterName</c> constant is the obvious source and is
    /// read first, but instruments here are idiomatically constructed from another
    /// type's meter: <c>TagIndexReconcileGrain</c> and <c>WalSaturationSignal</c>
    /// both create theirs through <c>LatticeMetrics.Meter</c> while declaring no
    /// meter of their own, which the repository conventions call out as the safe
    /// form precisely because those classes have no <c>Meter</c> field that could be
    /// read null during re-entrant instrument publication.
    /// </para>
    /// <para>
    /// Reading only a package's own constant would get those two right by accident,
    /// since they live in the package that declares the meter they use. It would be
    /// wrong in the case that matters: a package declaring a charted meter while
    /// constructing its instruments on an uncharted meter belonging to a package
    /// that publishes nothing itself. That meter would then appear in no package's
    /// set at all, and this gate would report full coverage of a surface it never
    /// saw - the same false pass, one level up, that the gate exists to prevent. So
    /// cross-type references are read as well and the union taken, which can only
    /// widen the published set. The failure mode of this resolution is therefore a
    /// spurious demand for coverage, which is visible and arguable, rather than a
    /// silent absence of it.
    /// </para>
    /// </remarks>
    private static Dictionary<string, SortedSet<string>> DiscoverPublishedMeters(
        IReadOnlyDictionary<string, string> meterConstants)
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var srcRoot = Path.Combine(repoRoot, "src");
        var typeDeclaration = new Regex(
            @"\b(?:class|struct|interface|record(?:\s+(?:class|struct))?)\s+(\w+)",
            RegexOptions.CultureInvariant);

        // "SomeMetrics.Meter" - an instrument constructed from another type's meter.
        var crossTypeMeterUse = new Regex(@"\b(\w+)\s*\.\s*Meter\b", RegexOptions.CultureInvariant);

        // "new Meter(SomeType.MeterName)" - a meter constructed from another type's
        // name constant, which is how the repository-context package does it.
        var crossTypeMeterConstruction = new Regex(
            @"new\s+Meter\s*\(\s*(\w+)\s*\.\s*MeterName", RegexOptions.CultureInvariant);

        var result = new Dictionary<string, SortedSet<string>>(StringComparer.Ordinal);

        foreach (var package in DiscoverInstrumentPublishingPackages())
        {
            var packageDir = Path.Combine(srcRoot, package);
            var meters = new SortedSet<string>(StringComparer.Ordinal);

            foreach (var file in Directory.EnumerateFiles(packageDir, "*.cs", SearchOption.AllDirectories))
            {
                if (IsBuildOutput(file))
                {
                    continue;
                }

                var text = File.ReadAllText(file);

                if (text.Contains("const string MeterName", StringComparison.Ordinal))
                {
                    foreach (Match declaration in typeDeclaration.Matches(text))
                    {
                        if (meterConstants.TryGetValue(declaration.Groups[1].Value, out var declared))
                        {
                            meters.Add(declared);
                        }
                    }
                }

                foreach (Match use in crossTypeMeterUse.Matches(text))
                {
                    if (meterConstants.TryGetValue(use.Groups[1].Value, out var referenced))
                    {
                        meters.Add(referenced);
                    }
                }

                foreach (Match construction in crossTypeMeterConstruction.Matches(text))
                {
                    if (meterConstants.TryGetValue(construction.Groups[1].Value, out var constructed))
                    {
                        meters.Add(constructed);
                    }
                }
            }

            if (meters.Count > 0)
            {
                result[package] = meters;
            }
        }

        return result;
    }

    /// <summary>
    /// The <c>MeterName</c> expressions declared by
    /// <c>MeterDashboardCoverageTestsBase</c> subclasses.
    /// </summary>
    /// <returns>The fixture path and the type whose constant it names.</returns>
    private static List<(string Fixture, string TypeName)> DiscoverSubclassMeterExpressions()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var testRoot = Path.Combine(repoRoot, "test");

        // Match a genuine subclass DECLARATION rather than the base class name as
        // text. This file names that text itself, so a substring test would make the
        // scanner match itself and lift its own regex source into the covered set.
        var subclassDeclaration = new Regex(
            @"class\s+\w+\s*:\s*MeterDashboardCoverageTestsBase\b",
            RegexOptions.CultureInvariant);
        var meterOverride = new Regex(
            @"override\s+string\s+MeterName\s*=>\s*(\w+)\s*\.\s*MeterName\s*;",
            RegexOptions.CultureInvariant);

        var found = new List<(string, string)>();
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

            var relative = Path.GetRelativePath(repoRoot, file).Replace(Path.DirectorySeparatorChar, '/');
            foreach (Match match in meterOverride.Matches(text))
            {
                found.Add((relative, match.Groups[1].Value));
            }
        }

        return found;
    }

    /// <summary>
    /// Every meter name some charting guard covers, from both subclasses of the
    /// dashboard-coverage base and the registered non-subclass guards.
    /// </summary>
    /// <param name="meterConstants">The resolved meter-name constants.</param>
    /// <returns>The covered meter names.</returns>
    private static SortedSet<string> DiscoverChartingGuardMeters(
        IReadOnlyDictionary<string, string> meterConstants)
    {
        var covered = new SortedSet<string>(StringComparer.Ordinal);

        foreach (var (_, typeName) in DiscoverSubclassMeterExpressions())
        {
            if (meterConstants.TryGetValue(typeName, out var meter))
            {
                covered.Add(meter);
            }
        }

        foreach (var (_, tokens) in NonSubclassChartingGuards)
        {
            foreach (var token in tokens)
            {
                var typeName = token.Split('.')[0];
                if (meterConstants.TryGetValue(typeName, out var meter))
                {
                    covered.Add(meter);
                }
            }
        }

        return covered;
    }

    private static bool IsBuildOutput(string path) =>
        path.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
        || path.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal);
}
