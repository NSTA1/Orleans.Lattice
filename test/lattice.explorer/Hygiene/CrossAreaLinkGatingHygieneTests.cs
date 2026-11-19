using System.IO;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests;

/// <summary>
/// Every Explorer component that builds an address in another area checks that the
/// directory shows that area before it links there.
/// </summary>
/// <remarks>
/// An area is hidden, unavailable or not checked per host and per caller: Replication
/// without the replication facades, Apps without the catalogue, Tenancy with tenancy
/// off. A link into such an area lands the user on a page they cannot open, and the
/// author never sees it, because their own host serves every area. The cluster
/// overview's "Open Replication" link on a host with no replication (#6108) was this
/// bug; the gate below fails any component that reaches into another area's
/// addresses without consulting <c>Shows(areaKey)</c>.
/// </remarks>
[TestFixture]
public sealed class CrossAreaLinkGatingHygieneTests
{
    private const string AreasRoot = "src/lattice.explorer/UI/Areas";

    private static readonly IReadOnlyDictionary<string, string> AreaKeyByFolder = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["Access"] = "access",
        ["Apps"] = "apps",
        ["Backups"] = "backups",
        ["Cluster"] = "cluster",
        ["Data"] = "data",
        ["Replication"] = "replication",
        ["Schema"] = "schema",
        ["Telemetry"] = "telemetry",
        ["Tenancy"] = "tenancy",
    };

    // Each pattern names an address builder or area key, and the area it leads into.
    // A builder hosted in one area but leading into another (TenancyRoutes.Apps) is
    // listed with its real target, ahead of the hosting class's catch-all.
    private static readonly (Regex Pattern, string Target)[] Targets =
    [
        (Token(@"\bAccessRoutes\.AppRoles\b"), "apps"),
        (Token(@"\bAccessRoutes\."), "access"),
        (Token(@"\bAppBindingGroups\.JoinAddress\b"), "access"),
        (Token(@"\bTenancyRoutes\.Apps\("), "apps"),
        (Token(@"\bTenancyRoutes\."), "tenancy"),
        (Token(@"\bSchemaAddresses\.(App\(|AppsAreaKey\b)"), "apps"),
        (Token(@"\bSchemaAddresses\."), "schema"),
        (Token(@"\bAppPageAddresses\.DataArea\b"), "data"),
        (Token(@"\bAppPageAddresses\.ReplicationArea\b"), "replication"),
        (Token(@"\bAppPageAddresses\."), "apps"),
        (Token(@"\bAppsRoutes\."), "apps"),
        (Token(@"\bAppsAreaKey\b|ForArea\(\s*""apps"""), "apps"),
        (Token(@"\bReplicationTreeOwnership\.AppReplicationAddress\b"), "apps"),
        (Token(@"\bReplicationAddresses\.|\bReplicationAreaKey\b"), "replication"),
        (Token(@"\bDataArea\.AreaKey\b|\bDataAreaKey\b"), "data"),
        (Token(@"\bClusterAddresses\."), "cluster"),
        (Token(@"\bBackupsAddresses\."), "backups"),
    ];

    private static readonly Regex ShowsCall = new(@"\bShows\((?<arg>[^()]*(\([^()]*\))?[^()]*)\)", RegexOptions.Compiled);

    [Test]
    public void Every_component_linking_into_another_area_checks_the_directory_shows_it()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var root = Path.Combine(repoRoot, AreasRoot.Replace('/', Path.DirectorySeparatorChar));
        var violations = new List<string>();
        var components = 0;
        var crossing = 0;

        foreach (var razor in Directory.EnumerateFiles(root, "*.razor", SearchOption.AllDirectories))
        {
            var folder = Path.GetRelativePath(root, razor).Split(Path.DirectorySeparatorChar)[0];
            if (!AreaKeyByFolder.TryGetValue(folder, out var home))
            {
                continue;
            }

            components++;
            var foreign = ForeignTargets(home, ComponentText(razor), out var gated);
            if (foreign.Count == 0)
            {
                continue;
            }

            crossing++;
            if (!gated)
            {
                violations.Add($"{Path.GetRelativePath(repoRoot, razor).Replace('\\', '/')} links into {string.Join(", ", foreign)} without Shows(areaKey)");
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(components, Is.GreaterThan(0), "the scan found no area components, so it proved nothing");
            Assert.That(crossing, Is.GreaterThan(0), "the scan found no cross-area link, so its patterns have drifted from the address builders");
            Assert.That(
                violations,
                Is.Empty,
                "A link into another area must be rendered only when the directory shows that area "
                + "(ExplorerPage.Shows or ExplorerLocation.Shows), or it strands the user on an area this host or caller cannot open.\n"
                + string.Join('\n', violations));
        });
    }

    [Test]
    public void Scanner_detects_an_ungated_cross_area_link_it_is_shown()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ForeignTargets("cluster", "href=\"@Href(ReplicationAddresses.Estate)\"", out var gated), Is.EqualTo(new[] { "replication" }));
            Assert.That(gated, Is.False);

            Assert.That(ForeignTargets("tenancy", "<a href=\"@Href(TenancyRoutes.Apps(id))\">", out _), Is.EqualTo(new[] { "apps" }), "a builder hosted here may lead elsewhere");
            Assert.That(ForeignTargets("tenancy", "<a href=\"@Href(TenancyRoutes.Tenant(id))\">", out _), Is.Empty, "a link within the area needs no gate");
            Assert.That(ForeignTargets("telemetry", "ExplorerAddress.ForTree(DataAreaKey, tree)", out _), Is.EqualTo(new[] { "data" }));

            Assert.That(ForeignTargets("schema", "@if (Shows(SchemaAddresses.AppsAreaKey)) { <a href=\"@Href(SchemaAddresses.App(slug))\"> }", out gated), Is.EqualTo(new[] { "apps" }));
            Assert.That(gated, Is.True);

            Assert.That(ForeignTargets("access", "@if (Shows(Apps.Catalogue.AppsRoutes.AreaKey)) { }", out _), Is.Empty, "the key named in the gate is not itself a link");
        });
    }

    private static IReadOnlyList<string> ForeignTargets(string home, string text, out bool gated)
    {
        gated = ShowsCall.IsMatch(text);
        var scanned = ShowsCall.Replace(text, "Shows()");
        var found = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var (pattern, target) in Targets)
        {
            if (!string.Equals(target, home, StringComparison.Ordinal) && pattern.IsMatch(scanned))
            {
                found.Add(target);
            }

            scanned = pattern.Replace(scanned, string.Empty);
        }

        return [.. found];
    }

    private static string ComponentText(string razor)
    {
        var directory = Path.GetDirectoryName(razor)!;
        var name = Path.GetFileNameWithoutExtension(razor);
        // The markup plus its code-behind and any further partials (X.razor.cs, X.Lifecycle.cs).
        var parts = new List<string> { File.ReadAllText(razor) };
        foreach (var partial in Directory.EnumerateFiles(directory, name + ".*.cs"))
        {
            parts.Add(File.ReadAllText(partial));
        }

        return string.Join('\n', parts);
    }

    private static Regex Token(string pattern) => new(pattern, RegexOptions.Compiled);
}
