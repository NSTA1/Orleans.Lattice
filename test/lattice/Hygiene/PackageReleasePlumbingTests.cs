using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// The release-plumbing gate: every package this repository builds can actually
/// be published, and the two places that say so agree with the projects on disk.
/// <para>
/// A package ships only when three things line up: a project that packs, a tag
/// glob in <c>.github/workflows/publish.yml</c> that fires the publish workflow,
/// and a row in <c>docs/RELEASING.md</c> telling a releaser which tag to push.
/// Nothing in CI checks the alignment, because nothing in CI publishes - so a
/// new package can sit in the solution for months, building and testing green,
/// and simply never reach NuGet. Three packages were in exactly that state
/// before this gate existed.
/// </para>
/// <para>
/// The checks run in both directions on purpose. A missing glob means a package
/// that cannot ship; an orphan glob or table row means a name that no longer
/// exists, which is worse than useless because it reads as coverage. Everything
/// is discovered from the projects, the workflow and the document - there is no
/// hand-maintained list here, because a list is the same failure mode one level
/// up.
/// </para>
/// </summary>
[TestFixture]
public sealed class PackageReleasePlumbingTests
{
    private const string PublishWorkflow = ".github/workflows/publish.yml";
    private const string ReleasingDocument = "docs/RELEASING.md";

    /// <summary>
    /// A tag trigger in the publish workflow, e.g. <c>- 'lattice.api.state-v*'</c>.
    /// </summary>
    private static readonly Regex TagGlob = new(
        @"^\s*-\s*'(?<prefix>[A-Za-z0-9\.]+)-v\*'\s*$",
        RegexOptions.Compiled | RegexOptions.Multiline);

    /// <summary>
    /// A two-column table row whose cells are both code-fenced, e.g.
    /// <c>| `Orleans.Lattice` | `src/lattice/Orleans.Lattice.csproj` |</c>.
    /// </summary>
    private static readonly Regex DocumentRow = new(
        @"^\|\s*`(?<package>[^`]+)`\s*\|\s*`(?<value>[^`]+)`\s*\|",
        RegexOptions.Compiled | RegexOptions.Multiline);

    [Test]
    public void The_scan_finds_the_repositorys_packages()
    {
        // Without this every assertion below would pass vacuously if src/ moved.
        Assert.That(
            Packages(),
            Has.Count.GreaterThan(30),
            "the scan must reach the repository's packable projects");
    }

    [Test]
    public void Every_package_has_a_publish_tag_glob()
    {
        var globs = TagPrefixes();

        var offenders = Packages()
            .Where(package => !globs.Contains(package.TagPrefix))
            .Select(package => $"{package.PackageId}  (expected trigger: '{package.TagPrefix}-v*')")
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            $"these packages build but can never ship: {PublishWorkflow} has no tag trigger for them, so pushing "
            + "their release tag fires no workflow and produces no NuGet package and no GitHub release."
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders));
    }

    [Test]
    public void Every_publish_tag_glob_names_a_real_package()
    {
        var expected = Packages().Select(package => package.TagPrefix).ToHashSet(StringComparer.Ordinal);

        var offenders = TagPrefixes()
            .Where(prefix => !expected.Contains(prefix))
            .Select(prefix => $"'{prefix}-v*'")
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            $"these {PublishWorkflow} triggers name no packable project - a renamed or retired package leaves a "
            + "trigger that reads as coverage but fires on a tag nobody will ever push."
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders));
    }

    [Test]
    public void Every_package_is_listed_in_the_releasing_packages_table_at_its_real_path()
    {
        var rows = DocumentRows(value => value.EndsWith(".csproj", StringComparison.Ordinal));

        var missing = Packages()
            .Where(package => !rows.ContainsKey(package.PackageId))
            .Select(package => $"{package.PackageId} (missing; path is {package.RelativePath})")
            .ToArray();

        var wrongPath = Packages()
            .Where(package => rows.TryGetValue(package.PackageId, out var path)
                && !string.Equals(path, package.RelativePath, StringComparison.Ordinal))
            .Select(package => $"{package.PackageId}: document says '{rows[package.PackageId]}', actual '{package.RelativePath}'")
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(
                missing,
                Is.Empty,
                $"a releaser reads {ReleasingDocument} to find what to cut; a package absent from it is a package "
                + "nobody knows to release."
                + Environment.NewLine
                + string.Join(Environment.NewLine, missing));

            Assert.That(
                wrongPath,
                Is.Empty,
                $"{ReleasingDocument} points at a csproj path that has moved."
                + Environment.NewLine
                + string.Join(Environment.NewLine, wrongPath));
        });
    }

    [Test]
    public void Every_package_is_listed_in_the_releasing_tag_shape_table_with_its_real_tag()
    {
        var rows = DocumentRows(value => value.Contains("-v<", StringComparison.Ordinal));

        var offenders = Packages()
            .Select(package => new
            {
                package.PackageId,
                Expected = package.TagPrefix + "-v<X.Y.Z>",
                Actual = rows.GetValueOrDefault(package.PackageId),
            })
            .Where(row => row.Actual != row.Expected)
            .Select(row => $"{row.PackageId}: expected '{row.Expected}', document has '{row.Actual ?? "(no row)"}'")
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            $"the tag-shape table in {ReleasingDocument} is what a releaser copies the tag from; a wrong or "
            + "missing shape produces a tag that matches no publish trigger."
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders));
    }

    [Test]
    public void Neither_releasing_table_lists_a_package_that_does_not_exist()
    {
        var expected = Packages().Select(package => package.PackageId).ToHashSet(StringComparer.Ordinal);

        var offenders = DocumentRows(value => value.EndsWith(".csproj", StringComparison.Ordinal)).Keys
            .Concat(DocumentRows(value => value.Contains("-v<", StringComparison.Ordinal)).Keys)
            .Where(package => !expected.Contains(package))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(package => package, StringComparer.Ordinal)
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            $"{ReleasingDocument} documents packages that no project produces."
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders));
    }

    /// <summary>
    /// The tag prefix a package releases under: its id without the leading
    /// <c>Orleans.</c>, lowercased. <c>Orleans.Lattice.Api.State</c> ships as
    /// <c>lattice.api.state-v&lt;X.Y.Z&gt;</c>.
    /// </summary>
    private static string ToTagPrefix(string packageId)
    {
        const string vendorPrefix = "Orleans.";
        var trimmed = packageId.StartsWith(vendorPrefix, StringComparison.Ordinal)
            ? packageId[vendorPrefix.Length..]
            : packageId;

        return trimmed.ToLowerInvariant();
    }

    private static IReadOnlySet<string> TagPrefixes()
    {
        var text = ReadRepositoryFile(PublishWorkflow);
        var prefixes = TagGlob.Matches(text)
            .Select(match => match.Groups["prefix"].Value)
            .ToHashSet(StringComparer.Ordinal);

        Assert.That(prefixes, Is.Not.Empty, $"expected tag triggers in {PublishWorkflow}");
        return prefixes;
    }

    /// <summary>
    /// The rows of whichever <c>RELEASING.md</c> table has values matching
    /// <paramref name="valueFilter"/>, keyed by package id. The two tables are
    /// told apart by the shape of their second column rather than by position, so
    /// reordering the document cannot silently empty this.
    /// </summary>
    private static Dictionary<string, string> DocumentRows(Func<string, bool> valueFilter)
    {
        var rows = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (Match row in DocumentRow.Matches(ReadRepositoryFile(ReleasingDocument)))
        {
            var value = row.Groups["value"].Value;
            if (valueFilter(value))
            {
                rows[row.Groups["package"].Value] = value;
            }
        }

        Assert.That(rows, Is.Not.Empty, $"expected a populated table in {ReleasingDocument}");
        return rows;
    }

    private static string ReadRepositoryFile(string relativePath)
    {
        var path = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            relativePath.Replace('/', Path.DirectorySeparatorChar));

        Assert.That(File.Exists(path), Is.True, $"expected {relativePath}");
        return File.ReadAllText(path);
    }

    private static IReadOnlyList<PackageProject> Packages()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var sourceRoot = Path.Combine(repoRoot, "src");
        var packages = new List<PackageProject>();

        foreach (var path in HygieneRepository.EnumerateFiles(sourceRoot, "*.csproj"))
        {
            var text = File.ReadAllText(path);
            if (Regex.IsMatch(text, @"<IsPackable>\s*false\s*</IsPackable>", RegexOptions.IgnoreCase))
            {
                continue;
            }

            var match = Regex.Match(text, @"<PackageId>(?<id>[^<]+)</PackageId>");
            if (!match.Success)
            {
                continue;
            }

            var packageId = match.Groups["id"].Value.Trim();
            packages.Add(new PackageProject(
                PackageId: packageId,
                TagPrefix: ToTagPrefix(packageId),
                RelativePath: Path.GetRelativePath(repoRoot, path).Replace('\\', '/')));
        }

        return packages;
    }

    /// <summary>One packable project, flattened to what release plumbing needs.</summary>
    private sealed record PackageProject(string PackageId, string TagPrefix, string RelativePath);
}
