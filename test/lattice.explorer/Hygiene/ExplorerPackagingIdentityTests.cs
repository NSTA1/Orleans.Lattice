using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests;

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
/// <para>
/// It also catches a specific, recurring mistake. A session opens
/// <c>Plugins/</c>, sees <c>Access</c>, <c>Backup</c> and <c>Schema</c> named
/// <c>Orleans.Lattice.Explorer.&lt;Area&gt;</c>, and reasonably concludes that is
/// the convention. It is not: those three ids are <em>frozen</em> because they
/// have already shipped, while every genuinely new package takes the
/// <c>Plugins.</c> segment. Four separate sessions made that inference during the
/// plugin epic, each copying a visible sibling. None of them could have known;
/// this fixture tells the fifth one immediately.
/// </para>
/// </summary>
[TestFixture]
public sealed class ExplorerPackagingIdentityTests
{
    private const string ExplorerRoot = "src/lattice.explorer";

    /// <summary>The prefix every plugin package that has not already shipped must carry.</summary>
    private const string PluginPackagePrefix = "Orleans.Lattice.Explorer.Plugins.";

    /// <summary>
    /// The plugin packages allowed to sit under <c>Plugins/</c> <em>without</em>
    /// the <c>Plugins.</c> segment.
    /// <para>
    /// THESE ARE FROZEN, NOT EXEMPLARY. Each shipped release tags before the
    /// plugin conversion moved it into <c>Plugins/</c>, so its
    /// <c>PackageId</c> is a consumer contract: renaming it would break a
    /// consumer's <c>PackageReference</c> outright, with no type-forwarding
    /// possible, and renaming the matching <c>RootNamespace</c> would break every
    /// <c>using</c> in their code as well. The directory move already expresses
    /// the architecture internally; the package id does not have to mirror it.
    /// </para>
    /// <para>
    /// Do not copy this shape for a new plugin, and do not add to this list: a
    /// package that has never shipped has nothing to be compatible with, so it
    /// takes <see cref="PluginPackagePrefix"/> like every other new one.
    /// </para>
    /// </summary>
    private static readonly string[] FrozenPluginPackageIds =
    [
        "Orleans.Lattice.Explorer.Access",
        "Orleans.Lattice.Explorer.Backup",
        "Orleans.Lattice.Explorer.Schema",
    ];

    /// <summary>
    /// The one project whose <c>RootNamespace</c> deliberately differs from its
    /// package id: the plugin contract package publishes its types into
    /// <c>Orleans.Lattice.Explorer.Plugins</c>, the namespace the host and every
    /// plugin share, rather than into a namespace named after the package.
    /// </summary>
    private static readonly Dictionary<string, string> RootNamespaceExceptions = new(StringComparer.Ordinal)
    {
        ["Orleans.Lattice.Explorer.Plugins.Abstractions"] = "Orleans.Lattice.Explorer.Plugins",
    };

    private static readonly Regex ContentReference = new(
        @"_content/(?<assembly>[^/""]+)/(?<file>[^""]+)",
        RegexOptions.Compiled);

    /// <summary>The host documents that link packaged static web assets by <c>_content/</c> path.</summary>
    private static readonly string[] HostDocuments =
    [
        "src/lattice.explorer/WebHosting/Components/App.razor",
        "src/lattice.explorer/Maui/wwwroot/index.html",
    ];

    [Test]
    public void The_scan_finds_the_explorer_packages()
    {
        // Without this the whole fixture would pass vacuously if the layout moved.
        Assert.That(
            Packages(),
            Has.Count.GreaterThan(10),
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
            + "entry to RootNamespaceExceptions only when the divergence is deliberate."
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders));
    }

    [Test]
    public void Every_documented_root_namespace_exception_is_still_needed()
    {
        // An exception that no longer applies is a licence for the next drift.
        var packageIds = Packages().Select(package => package.PackageId).ToHashSet(StringComparer.Ordinal);

        Assert.That(
            RootNamespaceExceptions.Keys.Where(id => !packageIds.Contains(id)),
            Is.Empty,
            "these RootNamespace exceptions name packages that no longer exist");
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
    public void Every_new_plugin_package_carries_the_plugins_segment()
    {
        var offenders = Packages()
            .Where(package => package.IsUnderPluginsDirectory)
            .Where(package => !package.PackageId.StartsWith(PluginPackagePrefix, StringComparison.Ordinal))
            .Where(package => !FrozenPluginPackageIds.Contains(package.PackageId, StringComparer.Ordinal))
            .Select(package => $"{package.RelativePath}: '{package.PackageId}'")
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            $"a plugin package that has never shipped must be named '{PluginPackagePrefix}<Area>'. The "
            + "Access / Backup / Schema ids that lack the segment are FROZEN for consumer compatibility, not a "
            + "convention to copy - see FrozenPluginPackageIds."
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders));
    }

    [Test]
    public void Every_frozen_plugin_package_still_exists_under_the_plugins_directory()
    {
        // If one is renamed or retired, the allow-list must shrink with it -
        // otherwise it silently re-permits that id for a future package.
        var pluginPackageIds = Packages()
            .Where(package => package.IsUnderPluginsDirectory)
            .Select(package => package.PackageId)
            .ToHashSet(StringComparer.Ordinal);

        Assert.That(
            FrozenPluginPackageIds.Where(id => !pluginPackageIds.Contains(id)),
            Is.Empty,
            "these ids are exempted from the naming rule but no longer name a plugin package");
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
    public void Every_content_link_in_a_head_document_resolves_to_a_packaged_asset()
    {
        var assets = PackagedStaticWebAssets();
        var offenders = new List<string>();
        var scanned = 0;

        foreach (var document in HostDocuments)
        {
            var path = Path.Combine(
                HygieneRepository.FindRepoRoot(),
                document.Replace('/', Path.DirectorySeparatorChar));

            Assert.That(File.Exists(path), Is.True, "expected a head document at " + document);

            var lines = File.ReadAllLines(path);
            for (var i = 0; i < lines.Length; i++)
            {
                foreach (Match reference in ContentReference.Matches(lines[i]))
                {
                    scanned++;
                    var key = $"{reference.Groups["assembly"].Value}/{reference.Groups["file"].Value}";
                    if (!assets.Contains(key))
                    {
                        offenders.Add($"{document}:{i + 1}: _content/{key}");
                    }
                }
            }
        }

        Assert.That(scanned, Is.GreaterThan(5), "the scan must reach the head documents' asset links");

        Assert.That(
            offenders,
            Is.Empty,
            "a _content/ link names an assembly and a file that no Explorer project ships. It fails as a silent "
            + "404 at runtime - no build error, no test failure, just an unstyled panel - so nothing but this "
            + "catches a renamed assembly or a moved asset."
            + Environment.NewLine
            + string.Join(Environment.NewLine, offenders));
    }

    /// <summary>
    /// Every <c>{assemblyName}/{fileName}</c> an Explorer project publishes as a
    /// static web asset, which is exactly what a <c>_content/</c> path resolves
    /// against at runtime.
    /// </summary>
    private static HashSet<string> PackagedStaticWebAssets()
    {
        var assets = new HashSet<string>(StringComparer.Ordinal);

        foreach (var project in ProjectFiles(HygieneRepository.FindRepoRoot()))
        {
            var directory = Path.GetDirectoryName(project)!;
            var wwwroot = Path.Combine(directory, "wwwroot");
            if (!Directory.Exists(wwwroot))
            {
                continue;
            }

            var text = File.ReadAllText(project);
            var assemblyName = ReadProperty(text, "AssemblyName")
                ?? Path.GetFileNameWithoutExtension(project);

            foreach (var file in Directory.GetFiles(wwwroot, "*", SearchOption.AllDirectories))
            {
                assets.Add($"{assemblyName}/{Path.GetRelativePath(wwwroot, file).Replace('\\', '/')}");
            }
        }

        return assets;
    }

    private static string ExpectedRootNamespace(ExplorerPackage package) =>
        RootNamespaceExceptions.TryGetValue(package.PackageId, out var expected) ? expected : package.FileName;

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
                RelativePath: Relative(path),
                IsUnderPluginsDirectory: Relative(directory)
                    .StartsWith(ExplorerRoot + "/Plugins/", StringComparison.Ordinal)));
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
        string RelativePath,
        bool IsUnderPluginsDirectory);
}
