using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Resolves the metric names the shipped query catalogue carries across an artefact
/// boundary, where nothing else re-resolves them.
/// </summary>
/// <remarks>
/// <para>
/// <b>The crossing.</b> An instrument is declared once, in source, by its dotted
/// OpenTelemetry name. It then crosses into
/// <see cref="LatticeTelemetryQueries"/> twice over, in two independent and
/// unrelated spellings: a <c>Descriptor.Instruments</c> entry repeats the dotted
/// name, and <c>QueryTemplate</c> repeats it again in the underscored Prometheus
/// exporter form inside a PromQL string. Neither copy is resolved by any compiler.
/// A C# string literal naming an instrument that does not exist compiles exactly as
/// cleanly as one that does, so both copies are inert text on the far side of the
/// boundary that validated them.
/// </para>
/// <para>
/// <b>Why the existing audits do not cover this.</b>
/// <see cref="LatticeTelemetryQueriesTests.Every_rendered_template_scans_to_a_resolvable_metric_name_set"/>
/// is the closest neighbour and is a purely syntactic check: it asserts that a
/// concrete name can be <em>extracted</em> from the PromQL - not named by pattern,
/// anchored to a selector, non-empty - and never that the extracted name
/// corresponds to a real instrument. A template naming
/// <c>orleans_lattice_completely_made_up_total</c> satisfies every one of its
/// assertions. Separately,
/// <see cref="LatticeTelemetryQueriesTests.The_catalogue_covers_both_the_core_and_the_tenancy_meters"/>
/// reads the descriptor's <c>Meter</c> field rather than the template, so the two
/// spellings are never compared with each other either.
/// </para>
/// <para>
/// <b>Why it matters more here than in a document.</b> A dead name in this
/// catalogue does not fail. It returns zero series forever, which renders
/// identically to a correctly-instrumented but idle deployment, and the catalogue is
/// a shipped public surface whose entries are also matched by name against the
/// telemetry allow list. A wrong name is therefore a permanently empty query that
/// looks like a working one.
/// </para>
/// <para>
/// <b>What this fixture does NOT prove, stated so a reader cannot infer coverage
/// that is not here.</b>
/// </para>
/// <list type="number">
/// <item>
/// <description>
/// <b>It is additive-blind.</b> It proves that every name the catalogue
/// <em>does</em> carry resolves. It cannot prove that every name which
/// <em>should</em> be carried is present: an entry silently deleted from the
/// catalogue, or an instrument dropped from a descriptor's list, passes cleanly.
/// The asymmetry is inherent to the direction of the check - it walks the names
/// that exist and has no way to enumerate the names that ought to. Do not read a
/// green run as "the catalogue is complete".
/// </description>
/// </item>
/// <item>
/// <description>
/// <b>It resolves names against source, not against a live deployment.</b> It
/// therefore catches a name that resolves to nothing, and cannot catch a name that
/// is spelled correctly but which no running process ever emits - a declared
/// instrument that is never recorded, or is compiled out. That is a genuinely
/// different failure and needs a scrape to detect.
/// </description>
/// </item>
/// <item>
/// <description>
/// <b>A golden-scrape arm is deliberately absent rather than stubbed.</b> The
/// obvious complement to this fixture asserts each name against a captured
/// production scrape, which would close the gap in (2). No scrape artefact exists
/// in this repository, so that arm is not built here. It was not stubbed, skipped,
/// or made conditional on a live endpoint on purpose: a verification arm that
/// quietly evaporates still reads as coverage, and an NUnit inconclusive is
/// invisible in a run that prints "Passed!" with "Skipped: 0". An absent arm that
/// is documented as absent is strictly safer than a present one that never
/// executes. Adding it requires committing a scrape together with an assertion on
/// its vintage that fails the build when it goes stale, because a golden file
/// rots in precisely the way a dead name does.
/// </description>
/// </item>
/// </list>
/// <para>
/// <b>The guard's own two crossings.</b> This fixture has exactly the defect it
/// checks for, twice: it names a source tree to harvest declared instrument names
/// from, and it names a catalogue to harvest references from. A wrong source root
/// harvests zero declared names and every reference then fails loudly, which is
/// safe. A wrong or emptied catalogue harvests zero references and every
/// comparison is <em>vacuously satisfied</em>, which is not. Both populations are
/// therefore asserted against a floor rather than merely against non-empty: a
/// harvest that collapses to a handful is as blind as one that collapses to
/// nothing, and is far easier to introduce by refactor. A guard that resolves zero
/// names is byte-identical to a clean repository.
/// </para>
/// <para>
/// <b>Why the catalogue is excluded from the declared-name scan, which is
/// load-bearing and not an optimisation.</b> The catalogue is itself a file under
/// <c>src</c>. Scanning all of <c>src</c> for declaration literals therefore
/// harvests the catalogue's own references and enters them into the set the
/// catalogue is being resolved against, so every name resolves to itself and the
/// primary arm can never fail. That is not a hypothetical: it was the fixture's
/// first behaviour, and a perturbation injecting a plainly non-existent instrument
/// name into a descriptor passed cleanly. A reference cannot be resolved against
/// itself, so the artefact under audit is removed from the corpus that defines
/// validity. The exclusion is asserted to have matched exactly one file, because an
/// exclusion that silently stops matching - a moved or renamed catalogue - restores
/// the vacuous behaviour without any other symptom.
/// </para>
/// </remarks>
[TestFixture]
public sealed class TelemetryQueryMetricNameResolutionTests
{
    /// <summary>
    /// Minimum number of source files the declared-name scan must read. The real
    /// figure is in the thousands; this floor only has to be large enough that a
    /// scan pointed at the wrong directory, or at a single project, cannot satisfy
    /// it.
    /// </summary>
    private const int SourceFileFloor = 500;

    /// <summary>
    /// Minimum number of distinct dotted instrument names the source scan must
    /// find. The repository declares several hundred; a harvest below this means
    /// the regex or the root stopped matching what it used to and every resolution
    /// below is being decided against a set that is no longer representative.
    /// </summary>
    private const int DeclaredNameFloor = 200;

    /// <summary>
    /// Minimum number of instrument references the catalogue must contribute. The
    /// catalogue declares one or more per entry; this floor is what stops an
    /// emptied or unparsed catalogue reporting a clean run.
    /// </summary>
    private const int CatalogueReferenceFloor = 10;

    /// <summary>
    /// The artefact under audit, relative to <c>src</c>. It is excluded from the
    /// declared-name scan because a reference cannot be resolved against itself:
    /// this file is under <c>src</c>, so scanning it would enter the catalogue's own
    /// references into the set of valid names and make the primary arm vacuous.
    /// </summary>
    private static readonly string CatalogueRelativePath = Path.Combine(
        "lattice.api.telemetry", "Catalog", "LatticeTelemetryQueries.cs");

    /// <summary>
    /// Dotted instrument-name literals as they are declared in source. Anchored to
    /// the <c>orleans.lattice</c> prefix and allowing underscores inside a segment,
    /// because several instruments carry one (for example
    /// <c>orleans.lattice.storage.total_bytes</c>).
    /// </summary>
    private static readonly Regex DeclaredNameRegex = new(
        "\"(orleans\\.lattice(?:\\.[a-z0-9_]+)+)\"",
        RegexOptions.Compiled);

    /// <summary>
    /// The only tokens a Prometheus exporter may append to a dotted instrument name
    /// when it renders the underscored series name: the unit it appends for a
    /// dimensioned instrument, and the suffix it appends for a counter or a
    /// histogram family.
    /// </summary>
    /// <remarks>
    /// This set is deliberately small, and every addition to it weakens the guard.
    /// The resolution rule below accepts a reference only when the entire remainder
    /// after a declared base decomposes into these tokens, which is what prevents a
    /// genuine name segment being mistaken for a suffix. Were "reads" admitted
    /// here, <c>orleans_lattice_shard_reads_total</c> would resolve against a bare
    /// <c>orleans.lattice.shard</c> and the check would stop discriminating.
    /// </remarks>
    private static readonly HashSet<string> ExporterSuffixTokens =
        new(StringComparer.Ordinal)
        {
            "total",
            "bucket",
            "sum",
            "count",
            "bytes",
            "seconds",
            "milliseconds",
            "ratio",
        };

    private static IReadOnlyList<TelemetryQueryDefinition> Definitions
        => LatticeTelemetryQueries.Definitions;

    /// <summary>
    /// ARM 3 (primary). Every dotted instrument name the catalogue declares in a
    /// descriptor resolves to an instrument name declared in repository source.
    /// </summary>
    /// <remarks>
    /// This is the arm that would have caught both of the dead references this
    /// guard was written for, because both were names spelled wrongly against a
    /// declaration that existed the whole time. It compares against a set
    /// discovered by scanning source rather than against a list written here: a
    /// guard that hard-codes the names it checks has the identical defect it is
    /// checking for.
    /// </remarks>
    [Test]
    public void Every_descriptor_instrument_name_resolves_to_a_name_declared_in_source()
    {
        var declared = ScanDeclaredInstrumentNames();
        var referenced = CatalogueDescriptorNames();

        Assert.That(referenced, Has.Count.GreaterThanOrEqualTo(CatalogueReferenceFloor),
            $"The catalogue yielded only {referenced.Count} descriptor instrument name(s), below the floor of "
            + $"{CatalogueReferenceFloor}. Every resolution below would be vacuously satisfied, so this fails "
            + "rather than reporting a clean run over a population that was never read.");

        var dead = referenced
            .Where(name => !declared.Contains(name))
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.That(dead, Is.Empty,
            "The following instrument names are declared by a catalogue descriptor but are declared nowhere in "
            + "source. A name that resolves to nothing is indistinguishable from a correct one at this boundary, "
            + "and the query built on it returns zero series forever while presenting as a measurement:"
            + Environment.NewLine + "  - " + string.Join(Environment.NewLine + "  - ", dead));
    }

    /// <summary>
    /// Every metric name inside a rendered query template agrees, under the
    /// Prometheus exporter transformation, with an instrument that same entry
    /// declares.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the second copy of the name, and the two copies are authored
    /// independently: the descriptor carries the dotted form and the template
    /// carries the underscored one. Nothing else in the suite compares them, so a
    /// template may name an instrument its own descriptor never claims to read.
    /// </para>
    /// <para>
    /// It is scoped per entry rather than against the whole declared corpus on
    /// purpose. Resolving a template name against every instrument in the
    /// repository would accept a real name belonging to an unrelated instrument,
    /// which is the dropped-infix failure exactly: a wrong name that is
    /// nevertheless somebody's right name. Requiring agreement with the entry's own
    /// declared instruments is what makes the check discriminating.
    /// </para>
    /// <para>
    /// The template is read through the product's own renderer and extractor rather
    /// than a regex local to this fixture, so the names checked here are the names
    /// the running facade actually resolves.
    /// </para>
    /// </remarks>
    [Test]
    public void Every_template_metric_name_agrees_with_its_entrys_declared_instruments()
    {
        var examined = 0;
        var mismatches = new List<string>();

        foreach (var definition in Definitions)
        {
            var rendered = TelemetryQueryTemplate
                .Parse(definition.QueryTemplate)
                .Render(TelemetryScopeSelector.Unscoped, TelemetryRateWindow.Default);

            var bases = definition.Descriptor.Instruments
                .Select(instrument => instrument.Name.Replace('.', '_'))
                .OrderByDescending(name => name.Length)
                .ToArray();

            foreach (var referenced in PromQlMetricExtractor.ExtractReferences(rendered).Names)
            {
                examined++;
                if (!bases.Any(root => AgreesWithExporterForm(referenced, root)))
                {
                    mismatches.Add(
                        $"{definition.Descriptor.QueryId}: template names '{referenced}', which agrees with none "
                        + "of the instruments that entry declares ["
                        + string.Join(", ", definition.Descriptor.Instruments.Select(i => i.Name)) + "]");
                }
            }
        }

        Assert.That(examined, Is.GreaterThanOrEqualTo(CatalogueReferenceFloor),
            $"Only {examined} template metric name(s) were examined, below the floor of {CatalogueReferenceFloor}. "
            + "The templates were not parsed or the catalogue was not read, so every comparison was vacuous.");

        Assert.That(mismatches, Is.Empty,
            "The following query templates name a metric that does not agree with the instruments their own "
            + "descriptor declares. The two spellings are authored independently and nothing else compares them:"
            + Environment.NewLine + "  - " + string.Join(Environment.NewLine + "  - ", mismatches));
    }

    /// <summary>
    /// The source scan this fixture resolves against actually read a repository.
    /// </summary>
    /// <remarks>
    /// This is not hygiene around the guard; it is the guard's coverage of its own
    /// first crossing. The source root named here is a reference that crosses out of
    /// this file, and nothing resolves it either. It is asserted separately from the
    /// arms above so that "the scan broke" and "a name is dead" are never reported
    /// by the same failure, which would reproduce the original ambiguity inside the
    /// guard.
    /// </remarks>
    [Test]
    public void The_declared_name_scan_reads_a_populated_source_tree()
    {
        var scan = ScanSource();

        Assert.That(Directory.Exists(scan.Root), Is.True,
            $"The source root '{scan.Root}' does not exist, so the declared-name scan would harvest nothing and "
            + "every resolution in this fixture would fail for the wrong reason.");

        Assert.Multiple(() =>
        {
            Assert.That(scan.Files, Is.GreaterThanOrEqualTo(SourceFileFloor),
                $"The scan read {scan.Files} source file(s), below the floor of {SourceFileFloor}.");
            Assert.That(scan.Names, Has.Count.GreaterThanOrEqualTo(DeclaredNameFloor),
                $"The scan harvested {scan.Names.Count} declared instrument name(s), below the floor of "
                + $"{DeclaredNameFloor}. A near-empty harvest is as blind as an empty one and is much easier to "
                + "introduce, so this asserts a floor rather than merely non-empty.");
            Assert.That(scan.Excluded, Is.EqualTo(1),
                $"The scan excluded {scan.Excluded} file(s) as the artefact under audit, expected exactly 1 "
                + $"('{CatalogueRelativePath}'). If the catalogue moved, the exclusion stops matching and its own "
                + "references re-enter the set of valid names, at which point every name resolves to itself and "
                + "the primary arm passes unconditionally. That failure has no other symptom, so it is asserted "
                + "here rather than inferred.");
        });
    }

    /// <summary>
    /// Whether an underscored Prometheus series name is the exporter rendering of a
    /// dotted instrument name.
    /// </summary>
    /// <param name="referenced">The underscored name read out of a query template.</param>
    /// <param name="root">The dotted instrument name, with dots already replaced by underscores.</param>
    /// <returns><see langword="true"/> when the reference is that instrument's exporter form.</returns>
    /// <remarks>
    /// The exporter renders a dotted name by replacing dots with underscores, then
    /// optionally appending a unit and a family suffix. The reference therefore
    /// agrees when it equals the base outright, or extends it by a remainder that
    /// decomposes entirely into <see cref="ExporterSuffixTokens"/>. Requiring the
    /// whole remainder to decompose is what stops a real name segment being read as
    /// a suffix, so a longer declared base is always preferred over a shorter one
    /// that happens to be a prefix of it.
    /// </remarks>
    private static bool AgreesWithExporterForm(string referenced, string root)
    {
        if (string.Equals(referenced, root, StringComparison.Ordinal))
        {
            return true;
        }

        if (!referenced.StartsWith(root + "_", StringComparison.Ordinal))
        {
            return false;
        }

        var remainder = referenced[(root.Length + 1)..].Split('_');
        return remainder.Length > 0 && remainder.All(ExporterSuffixTokens.Contains);
    }

    private static IReadOnlyCollection<string> CatalogueDescriptorNames()
        => Definitions
            .SelectMany(definition => definition.Descriptor.Instruments)
            .Select(instrument => instrument.Name)
            .Distinct(StringComparer.Ordinal)
            .ToArray();

    private static HashSet<string> ScanDeclaredInstrumentNames()
    {
        var scan = ScanSource();
        return scan.Names;
    }

    private static SourceScan ScanSource()
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        var files = 0;
        var excluded = 0;
        var root = Path.Combine(HygieneRepository.FindRepoRoot(), "src");

        if (Directory.Exists(root))
        {
            foreach (var file in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
            {
                if (file.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                    || file.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                {
                    continue;
                }

                if (file.EndsWith(CatalogueRelativePath, StringComparison.Ordinal))
                {
                    excluded++;
                    continue;
                }

                files++;
                foreach (Match match in DeclaredNameRegex.Matches(File.ReadAllText(file)))
                {
                    names.Add(match.Groups[1].Value);
                }
            }
        }

        return new SourceScan(root, files, excluded, names);
    }

    private sealed record SourceScan(string Root, int Files, int Excluded, HashSet<string> Names);
}
