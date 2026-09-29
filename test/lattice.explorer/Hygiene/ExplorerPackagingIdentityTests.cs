using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.Hygiene;

/// <summary>
/// The Explorer's packaging-identity gate: what every project under
/// <c>src/lattice.explorer/</c> is allowed to call itself.
/// <para>
/// This exists because the defect it catches is <em>invisible in this
/// repository</em>. Everything here wires up through <c>ProjectReference</c>, so
/// a package with the wrong id, the wrong root namespace, or a stale
/// <c>_content/</c> link still compiles, still passes every other test, and
/// still runs. It breaks only in a consumer's <c>restore</c> after publish -
/// which is to say, after it is too late. Nothing in CI publishes, so nothing in
/// CI can notice.
/// </para>
/// </summary>
[TestFixture]
public sealed class ExplorerPackagingIdentityTests
{
    private const string ExplorerRoot = "src/lattice.explorer";

    private static readonly Regex ContentRoot = new(
        @"_content/(?<assembly>[A-Za-z0-9_.]+)/",
        RegexOptions.Compiled);

    [Test]
    public void The_scan_finds_the_explorer_packages()
    {
        // Without this the whole fixture would pass vacuously if the layout moved.
        Assert.That(
            Packages(),
            Has.Count.GreaterThanOrEqualTo(4),
            "the scan must reach the Explorer's packable projects");
    }

    [Test]
    public void Every_package_id_matches_its_project_file_name()
    {
        var offenders = Packages()
            .Where(package => !string.Equals(package.PackageId, package.FileName, StringComparison.Ordinal))
            .Select(package => $"{package.RelativePath}: <PackageId>{package.PackageId}</PackageId>")
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            "a package id that disagrees with its project file name is how a rename slips through: the "
            + "assembly name defaults from the file name, so the two silently diverge."
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders));
    }

    [Test]
    public void Every_root_namespace_matches_its_package_id_unless_documented()
    {
        var offenders = Packages()
            .Where(package => package.RootNamespace is not null)
            .Where(package => !string.Equals(package.RootNamespace, ExpectedRootNamespace(package), StringComparison.Ordinal))
            .Select(package =>
                $"{package.RelativePath}: <RootNamespace>{package.RootNamespace}</RootNamespace>, "
                + $"expected '{ExpectedRootNamespace(package)}'")
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            "renaming a published RootNamespace breaks every `using` in a consumer's code. Add a documented "
            + "a divergent RootNamespace only with a documented reason."
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders));
    }

    [Test]
    public void No_assembly_name_override_diverges_from_the_project_file_name()
    {
        // Setting it is fine as long as it agrees; the _content/ path and the
        // published assembly both follow it.
        var offenders = Packages()
            .Where(package => package.AssemblyName is not null)
            .Where(package => !string.Equals(package.AssemblyName, package.FileName, StringComparison.Ordinal))
            .Select(package => $"{package.RelativePath}: <AssemblyName>{package.AssemblyName}</AssemblyName>")
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            "an AssemblyName that disagrees with the project file name renames the assembly and moves every "
            + "_content/ static-web-asset path that referenced it."
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders));
    }

    [Test]
    public void Every_project_directory_holds_exactly_one_project()
    {
        var offenders = Packages()
            .GroupBy(package => package.Directory, StringComparer.OrdinalIgnoreCase)
            .Where(group => Directory.GetFiles(group.Key, "*.csproj").Length != 1)
            .Select(group => $"{Relative(group.Key)}: {Directory.GetFiles(group.Key, "*.csproj").Length} projects")
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            "a package move must move the project, not duplicate it: two projects in one directory compile the "
            + "same feature under two ids."
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders));
    }

    [Test]
    public void A_project_without_a_package_id_is_explicitly_not_a_package()
    {
        // Otherwise a new project ships to NuGet under its default id the first
        // time someone packs the solution.
        var repoRoot = HygieneRepository.FindRepoRoot();
        var offenders = ProjectFiles(repoRoot)
            .Select(path => new { Path = path, Text = File.ReadAllText(path) })
            .Where(project => !HasProperty(project.Text, "PackageId"))
            .Where(project => !IsExplicitlyNotPackable(project.Text))
            .Select(project => Relative(project.Path))
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            "a project with no <PackageId> must declare <IsPackable>false</IsPackable> or be an executable, so "
            + "it cannot become a package by accident."
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders));
    }

    [Test]
    public void Every_content_path_the_explorer_names_resolves_to_an_explorer_static_web_asset_root()
    {
        // A _content/ path is how a head reaches a package's static web assets, and
        // it is named after the ASSEMBLY. The Explorer names each package's asset
        // root in exactly one constant, so a rename that misses one fails here
        // instead of as a silent 404 and an unstyled console at runtime.
        var roots = StaticWebAssetRoots();
        var offenders = new List<string>();
        var scanned = 0;
        var repoRoot = HygieneRepository.FindRepoRoot();
        var sources = HygieneRepository.EnumerateFiles(Path.Combine(repoRoot, ExplorerRoot.Replace('/', Path.DirectorySeparatorChar)), "*.cs")
            .Concat(HygieneRepository.EnumerateFiles(Path.Combine(repoRoot, ExplorerRoot.Replace('/', Path.DirectorySeparatorChar)), "*.razor"));

        foreach (var path in sources)
        {
            var lines = File.ReadAllLines(path);
            for (var i = 0; i < lines.Length; i++)
            {
                foreach (Match reference in ContentRoot.Matches(lines[i]))
                {
                    scanned++;
                    if (!roots.Contains(reference.Groups["assembly"].Value))
                    {
                        offenders.Add($"{Relative(path)}:{i + 1}: _content/{reference.Groups["assembly"].Value}/");
                    }
                }
            }
        }

        Assert.That(scanned, Is.GreaterThanOrEqualTo(2), "the scan must reach the UI's and AppKit's asset-root constants");
        Assert.That(
            offenders,
            Is.Empty,
            "a _content/ path names an assembly no Explorer project ships static web assets under."
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders));
    }

    /// <summary>The assembly names of the Explorer projects that ship a <c>wwwroot</c>.</summary>
    private static HashSet<string> StaticWebAssetRoots()
    {
        var roots = new HashSet<string>(StringComparer.Ordinal);
        foreach (var project in ProjectFiles(HygieneRepository.FindRepoRoot()))
        {
            if (Directory.Exists(Path.Combine(Path.GetDirectoryName(project)!, "wwwroot")))
            {
                roots.Add(ReadProperty(File.ReadAllText(project), "AssemblyName") ?? Path.GetFileNameWithoutExtension(project));
            }
        }

        return roots;
    }
    private static string ExpectedRootNamespace(ExplorerPackage package) => package.FileName;

    private static IReadOnlyList<ExplorerPackage> Packages()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var packages = new List<ExplorerPackage>();

        foreach (var path in ProjectFiles(repoRoot))
        {
            var text = File.ReadAllText(path);
            if (IsExplicitlyNotPackable(text))
            {
                continue;
            }

            if (ReadProperty(text, "PackageId") is not { } packageId)
            {
                continue;
            }

            var directory = Path.GetDirectoryName(path)!;
            packages.Add(new ExplorerPackage(
                PackageId: packageId,
                FileName: Path.GetFileNameWithoutExtension(path),
                RootNamespace: ReadProperty(text, "RootNamespace"),
                AssemblyName: ReadProperty(text, "AssemblyName"),
                Directory: directory,
                RelativePath: Relative(path)));
        }

        return packages;
    }

    private static IEnumerable<string> ProjectFiles(string repoRoot) =>
        HygieneRepository.EnumerateFiles(
            Path.Combine(repoRoot, ExplorerRoot.Replace('/', Path.DirectorySeparatorChar)),
            "*.csproj");

    /// <summary>
    /// Whether the project opts out of packing, either explicitly or by being an
    /// application rather than a library.
    /// </summary>
    private static bool IsExplicitlyNotPackable(string projectText) =>
        Regex.IsMatch(projectText, @"<IsPackable>\s*false\s*</IsPackable>", RegexOptions.IgnoreCase)
        || Regex.IsMatch(projectText, @"<OutputType>\s*Exe\s*</OutputType>", RegexOptions.IgnoreCase);

    private static bool HasProperty(string projectText, string name) =>
        ReadProperty(projectText, name) is not null;

    private static string? ReadProperty(string projectText, string name)
    {
        var match = Regex.Match(
            projectText,
            $"<{Regex.Escape(name)}>(?<value>[^<]*)</{Regex.Escape(name)}>");

        return match.Success ? match.Groups["value"].Value.Trim() : null;
    }

    private static string Relative(string path) =>
        Path.GetRelativePath(HygieneRepository.FindRepoRoot(), path).Replace('\\', '/');

    /// <summary>One packable Explorer project, flattened to what this gate asserts on.</summary>
    private sealed record ExplorerPackage(
        string PackageId,
        string FileName,
        string? RootNamespace,
        string? AssemblyName,
        string Directory,
        string RelativePath);
}
