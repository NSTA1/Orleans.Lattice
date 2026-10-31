using System.Text.RegularExpressions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;

/// <summary>
/// Keeps <c>explorer-features.tsv</c> the definition of "every feature" (#5361). Its reflected
/// half fails when the Explorer assemblies declare a route, subroute, tab, command, query key or
/// hosting option the catalogue lacks (and prints the rows to add); its curated half fails when
/// a cited doc anchor disappears, or a use-case section of the Explorer docs is cited by no row.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed partial class FeatureCatalogueTests
{
    /// <summary>The Explorer guides whose every use-case section the curated half must cite.</summary>
    public static IReadOnlyList<string> UseCaseDocuments { get; } =
    [
        "areas.md",
        "managing-access.md",
        "managing-backups.md",
        "managing-schema.md",
        "lattice-apps.md",
        "tenant-scope.md",
        "running-the-explorer.md",
    ];

    /// <summary>
    /// Sections of <see cref="UseCaseDocuments"/> that describe no use case a person performs in the
    /// UI, each with the reason. Every other section below the title (and above See also) is cited.
    /// </summary>
    public static IReadOnlyDictionary<string, string> NotUseCases { get; } = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["running-the-explorer.md#package-shape"] = "Which packages a host references: a build concern, not a UI path.",
        ["running-the-explorer.md#static-web-assets-in-a-thin-host"] = "How a thin host serves the static assets: a build concern the base-path use case exercises.",
        ["running-the-explorer.md#signalr-receive-size"] = "A transport limit for hosts, with no UI of its own.",
        ["lattice-apps.md#writing-an-app-ui"] = "A guide for app authors; the frame it describes is cited through the-app-frame.",
    };

    private static readonly FeatureCatalogue Catalogue = FeatureCatalogue.Load();

    /// <summary>Every row is well formed: unique id prefixed by its known kind, an outline chapter as owner, and a source of its half's shape.</summary>
    [Test]
    public void Every_row_is_well_formed()
    {
        var problems = new List<string>();
        foreach (var duplicate in Catalogue.Rows.GroupBy(row => row.Id, StringComparer.Ordinal).Where(group => group.Count() > 1))
        {
            problems.Add($"'{duplicate.Key}' appears on lines {string.Join(", ", duplicate.Select(row => row.Line))}");
        }

        foreach (var row in Catalogue.Rows)
        {
            var where = $"line {row.Line} ({row.Id})";
            if (!FeatureKinds.IsKnown(row.Kind))
            {
                problems.Add($"{where}: unknown kind '{row.Kind}'");
            }

            if (!row.Id.StartsWith(row.Kind + ":", StringComparison.Ordinal))
            {
                problems.Add($"{where}: the id must start with '{row.Kind}:'");
            }

            if (StoryOutline.Find(row.Journey) is null)
            {
                problems.Add($"{where}: owner '{row.Journey}' is not a chapter of the story outline");
            }

            if (FeatureKinds.Curated.Contains(row.Kind) && !DocAnchorSource().IsMatch(row.Source))
            {
                problems.Add($"{where}: a curated row cites docs/lattice.explorer/<file>.md#<anchor>, not '{row.Source}'");
            }

            if (row.IsPending && (!FeatureKinds.Reflected.Contains(row.Kind) || !PendingSource().IsMatch(row.Source)))
            {
                problems.Add($"{where}: only a reflected row may be pending, as 'pending:#N'");
            }
        }

        Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// Every route, subroute, tab, command, query key and hosting option the Explorer assemblies
    /// declare has a row. A failure prints the rows to add (owner <c>?</c>: choose the chapter that
    /// proves it) and writes them to <c>artifacts/explorer-features.missing.tsv</c>.
    /// </summary>
    [Test]
    public void Every_reflected_explorer_feature_is_catalogued()
    {
        var missing = ExplorerSurface.Reflect(JourneyPaths.RepositoryRoot).Where(feature => !Catalogue.Contains(feature.Id)).ToList();
        if (missing.Count > 0)
        {
            var rows = missing.Select(feature => $"{feature.Id}\t{feature.Kind}\t{feature.Source}\t<describe it>\t?").ToList();
            var file = Path.Combine(JourneyPaths.ProjectDirectory, "artifacts", "explorer-features.missing.tsv");
            Directory.CreateDirectory(Path.GetDirectoryName(file)!);
            File.WriteAllLines(file, rows);
            Assert.Fail($"{missing.Count} Explorer feature(s) are not in {FeatureCatalogue.FileName}; add these rows and name the chapter that proves each:{Environment.NewLine}{string.Join(Environment.NewLine, rows)}");
        }
    }

    /// <summary>
    /// Every reflected row names a feature the assemblies still declare, as the same kind; a
    /// pending row's feature has not merged yet (once it has, the row cites its declaring member).
    /// </summary>
    [Test]
    public void Every_reflected_row_matches_the_explorer()
    {
        var surface = ExplorerSurface.Reflect(JourneyPaths.RepositoryRoot).ToDictionary(feature => feature.Id, StringComparer.Ordinal);
        var problems = new List<string>();
        foreach (var row in Catalogue.Rows.Where(row => FeatureKinds.Reflected.Contains(row.Kind)))
        {
            var declared = surface.GetValueOrDefault(row.Id);
            if (row.IsPending)
            {
                if (declared is not null)
                {
                    problems.Add($"line {row.Line}: {row.Id} has landed ({declared.Source}); replace '{row.Source}' with that source");
                }
            }
            else if (declared is null)
            {
                problems.Add($"line {row.Line}: {row.Id} is no longer declared by the Explorer; remove the row or correct it");
            }
            else if (declared.Kind != row.Kind)
            {
                problems.Add($"line {row.Line}: {row.Id} is a {declared.Kind}, not a {row.Kind}");
            }
        }

        Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
    }

    /// <summary>Every curated row cites a heading that exists in its Explorer doc.</summary>
    [Test]
    public void Every_curated_row_cites_a_live_doc_anchor()
    {
        var anchors = new Dictionary<string, IReadOnlySet<string>>(StringComparer.Ordinal);
        var problems = new List<string>();
        foreach (var row in Catalogue.Rows.Where(row => FeatureKinds.Curated.Contains(row.Kind)))
        {
            var parts = row.Source.Split('#', 2);
            var path = Path.Combine(JourneyPaths.RepositoryRoot, parts[0]);
            if (!File.Exists(path))
            {
                problems.Add($"line {row.Line}: {parts[0]} does not exist");
                continue;
            }

            if (!anchors.TryGetValue(path, out var live))
            {
                live = DocAnchors.Headings(File.ReadAllText(path)).Select(heading => heading.Anchor).ToHashSet(StringComparer.Ordinal);
                anchors[path] = live;
            }

            if (parts.Length < 2 || !live.Contains(parts[1]))
            {
                problems.Add($"line {row.Line}: {row.Source} names no heading of {parts[0]}");
            }
        }

        Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// Every use-case section of the Explorer guides - each heading below the title, other than
    /// See also and the sections in <see cref="NotUseCases"/> - is cited by at least one curated row.
    /// </summary>
    [Test]
    public void Every_use_case_section_of_the_explorer_docs_is_cited()
    {
        var cited = Catalogue.Rows.Where(row => FeatureKinds.Curated.Contains(row.Kind)).Select(row => row.Source).ToHashSet(StringComparer.Ordinal);
        var uncited = new List<string>();
        foreach (var document in UseCaseDocuments)
        {
            var path = Path.Combine(JourneyPaths.RepositoryRoot, "docs", "lattice.explorer", document);
            foreach (var heading in DocAnchors.Headings(File.ReadAllText(path)).Where(heading => heading.Level > 1 && heading.Anchor != "see-also"))
            {
                var key = $"{document}#{heading.Anchor}";
                if (!NotUseCases.ContainsKey(key) && !cited.Contains("docs/lattice.explorer/" + key))
                {
                    uncited.Add($"docs/lattice.explorer/{key} (line {heading.Line}, \"{heading.Text}\")");
                }
            }
        }

        Assert.That(uncited, Is.Empty,
            "Explorer doc sections no catalogue row cites; add a use-case or dialog row for each (or, if a section describes no use case, list it in NotUseCases with the reason):" + Environment.NewLine + string.Join(Environment.NewLine, uncited));
    }

    /// <summary>Every <see cref="NotUseCases"/> entry names a section that still exists.</summary>
    [Test]
    public void Every_excluded_section_still_exists()
    {
        var stale = NotUseCases.Keys.Where(key =>
        {
            var parts = key.Split('#', 2);
            var path = Path.Combine(JourneyPaths.RepositoryRoot, "docs", "lattice.explorer", parts[0]);
            return !File.Exists(path) || DocAnchors.Headings(File.ReadAllText(path)).All(heading => heading.Anchor != parts[1]);
        }).ToList();

        Assert.That(stale, Is.Empty);
    }

    /// <summary>Every documented query value belongs to a catalogued query key: <c>query-value:data.tab=keys</c> needs <c>query:data.tab</c>.</summary>
    [Test]
    public void Every_query_value_names_a_catalogued_query_key()
    {
        var orphans = Catalogue.Rows
            .Where(row => row.Kind == FeatureKinds.QueryValue)
            .Where(row =>
            {
                var key = row.Id[(FeatureKinds.QueryValue.Length + 1)..].Split('=', 2)[0];
                return !Catalogue.Contains($"{FeatureKinds.Query}:{key}");
            })
            .Select(row => row.Id)
            .ToList();

        Assert.That(orphans, Is.Empty);
    }

    /// <summary>Every exemption names a catalogue row and gives its reason; none is duplicated.</summary>
    [Test]
    public void Every_exemption_names_a_catalogue_row()
    {
        var exemptions = FeatureCatalogue.LoadExemptions();
        Assert.Multiple(() =>
        {
            Assert.That(exemptions.Select(exemption => exemption.Id).Where(id => !Catalogue.Contains(id)), Is.Empty);
            Assert.That(exemptions.Select(exemption => exemption.Id), Is.Unique);
            Assert.That(exemptions.Where(exemption => exemption.Reason.Length < 20).Select(exemption => exemption.Id), Is.Empty, "an exemption explains itself");
        });
    }

    /// <summary>The reflection finds what it must, so the completeness test cannot pass vacuously.</summary>
    [Test]
    public void The_surface_reflection_is_not_vacuous()
    {
        var kinds = ExplorerSurface.Reflect(JourneyPaths.RepositoryRoot).GroupBy(feature => feature.Kind).ToDictionary(group => group.Key, group => group.Count());
        Assert.Multiple(() =>
        {
            foreach (var kind in FeatureKinds.Reflected)
            {
                Assert.That(kinds.GetValueOrDefault(kind), Is.GreaterThan(0), $"the reflection found no {kind}");
            }

            Assert.That(kinds.GetValueOrDefault(FeatureKinds.Route), Is.GreaterThanOrEqualTo(50), "the Explorer declares dozens of routes");
            Assert.That(Catalogue.Rows.Count(row => FeatureKinds.Curated.Contains(row.Kind)), Is.GreaterThanOrEqualTo(100), "the curated half is populated");
        });
    }

    [GeneratedRegex("^docs/lattice\\.explorer/[a-z0-9-]+\\.md#[a-z0-9_-]+$")]
    private static partial Regex DocAnchorSource();

    [GeneratedRegex("^pending:#[1-9][0-9]*$")]
    private static partial Regex PendingSource();
}
