using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.Plugins.Schema;

/// <summary>
/// The packaging-identity guard for the Schema plugin.
/// <para>
/// <c>Orleans.Lattice.Explorer.Schema</c> has shipped tags through v9.4.0, so its
/// <c>PackageId</c>, its assembly name, and its <c>RootNamespace</c> are
/// consumer-facing contracts. The plugin conversion moved the project to
/// <c>src/lattice.explorer/Plugins/Schema/</c>, and the tempting follow-on is to
/// rename the package to mirror the folder - which would break a consumer's
/// <c>PackageReference</c> outright (no type-forwarding can help) and, if the
/// root namespace went too, every <c>using</c> in their code as well. The epic
/// rule is that a published package keeps all three through the move; only
/// genuinely new packages get a new id.
/// </para>
/// <para>
/// This fires on the rename rather than on its consequences, because the
/// consequences are invisible in this repository: everything here uses project
/// references, so a renamed package still builds and still passes every other
/// test. The break only appears downstream, in a consumer's restore.
/// </para>
/// </summary>
[TestFixture]
public sealed class SchemaPackagingIdentityGuardTests
{
    /// <summary>The project's new home. The move is expected; the rename is not.</summary>
    private const string ProjectPath =
        "src/lattice.explorer/Plugins/Schema/Orleans.Lattice.Explorer.Schema.csproj";

    /// <summary>The published identity, unchanged since before the plugin conversion.</summary>
    private const string PublishedIdentity = "Orleans.Lattice.Explorer.Schema";

    [Test]
    public void The_project_file_is_named_for_the_published_assembly()
    {
        // The assembly name defaults from the project file name, and the
        // _content/ static-web-asset path derives from the assembly name, so the
        // file name is load-bearing rather than cosmetic.
        Assert.That(
            Path.GetFileNameWithoutExtension(ProjectPath),
            Is.EqualTo(PublishedIdentity));
        Assert.That(File.Exists(ProjectFile()), Is.True, "expected the project at " + ProjectPath);
    }

    [Test]
    public void The_package_id_is_the_published_one()
    {
        Assert.That(
            ReadProperty("PackageId"),
            Is.EqualTo(PublishedIdentity),
            "renaming a published PackageId breaks a consumer's PackageReference with no type-forwarding possible");
    }

    [Test]
    public void The_root_namespace_is_the_published_one()
    {
        Assert.That(
            ReadProperty("RootNamespace"),
            Is.EqualTo(PublishedIdentity),
            "renaming the RootNamespace breaks every using in a consumer's code");
    }

    [Test]
    public void No_assembly_name_override_diverges_from_the_project_file_name()
    {
        // Setting it is allowed as long as it agrees; leaving it unset (the
        // Access plugin's shape) is what this project does.
        var assemblyName = TryReadProperty("AssemblyName");

        Assert.That(
            assemblyName is null || assemblyName == PublishedIdentity,
            Is.True,
            $"AssemblyName is '{assemblyName}', which would move the _content/ path and rename the assembly");
    }

    [Test]
    public void The_plugin_ships_no_second_package_for_the_same_feature()
    {
        var pluginDirectory = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            "src/lattice.explorer/Plugins/Schema".Replace('/', Path.DirectorySeparatorChar));

        var projects = Directory.GetFiles(pluginDirectory, "*.csproj", SearchOption.AllDirectories)
            .Select(Path.GetFileName)
            .ToArray();

        Assert.That(
            projects,
            Is.EqualTo(new[] { PublishedIdentity + ".csproj" }),
            "the conversion moves the published project; it must not leave a second one beside it");
    }

    [Test]
    public void The_retired_project_location_holds_no_project()
    {
        var retired = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            "src/lattice.explorer/Schema".Replace('/', Path.DirectorySeparatorChar));

        Assert.That(
            Directory.Exists(retired) && Directory.GetFiles(retired, "*.csproj", SearchOption.AllDirectories).Length > 0,
            Is.False,
            "the pre-conversion Schema project directory must be gone, not duplicated");
    }

    private static string ProjectFile() => Path.Combine(
        HygieneRepository.FindRepoRoot(),
        ProjectPath.Replace('/', Path.DirectorySeparatorChar));

    private static string ReadProperty(string name)
    {
        var value = TryReadProperty(name);
        Assert.That(value, Is.Not.Null, $"<{name}> must be declared in {ProjectPath}");
        return value!;
    }

    private static string? TryReadProperty(string name)
    {
        var match = Regex.Match(
            File.ReadAllText(ProjectFile()),
            $"<{Regex.Escape(name)}>(?<value>[^<]*)</{Regex.Escape(name)}>");

        return match.Success ? match.Groups["value"].Value.Trim() : null;
    }
}
