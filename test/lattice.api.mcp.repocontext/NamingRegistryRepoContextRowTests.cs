using System.Reflection;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Pins the <c>Orleans.Lattice.Api.Mcp.RepoContext</c> row of the naming
/// registry in <c>.github/skills/naming-conventions/SKILL.md</c> to the
/// package's compiled public surface (issue #2494). The row claims to list
/// every top-level public type of the namespace, so a reader can conclude a
/// name absent from it does not exist; these tests keep that claim true in
/// both directions by comparing the row against the assembly's exported types.
/// </summary>
[TestFixture]
public sealed class NamingRegistryRepoContextRowTests
{
    private const string RegistryRelativePath = ".github/skills/naming-conventions/SKILL.md";

    private const string Namespace = "Orleans.Lattice.Api.Mcp.RepoContext";

    private static readonly Regex BacktickedName = new("`([^`]+)`", RegexOptions.CultureInvariant);

    [Test]
    public void Registry_row_names_only_types_the_package_exports()
    {
        var listed = ReadRegistryRow(ReadRegistry());
        var exported = ExportedTypeNames();

        var bogus = listed.Where(name => !exported.Contains(name)).ToArray();

        Assert.That(
            bogus,
            Is.Empty,
            $"the {Namespace} registry row names types the package does not export as top-level public types: {string.Join(", ", bogus)}");
    }

    [Test]
    public void Registry_row_lists_every_type_the_package_exports()
    {
        var listed = ReadRegistryRow(ReadRegistry()).ToHashSet(StringComparer.Ordinal);
        var exported = ExportedTypeNames();

        var missing = exported.Where(name => !listed.Contains(name)).Order(StringComparer.Ordinal).ToArray();

        Assert.That(
            missing,
            Is.Empty,
            $"the {Namespace} registry row omits exported top-level public types; add them to {RegistryRelativePath}: {string.Join(", ", missing)}");
    }

    [Test]
    public void Registry_row_lists_each_type_once()
    {
        var listed = ReadRegistryRow(ReadRegistry());

        var duplicates = listed.GroupBy(name => name, StringComparer.Ordinal)
            .Where(group => group.Count() > 1)
            .Select(group => group.Key)
            .ToArray();

        Assert.That(duplicates, Is.Empty, $"the {Namespace} registry row repeats names: {string.Join(", ", duplicates)}");
    }

    [Test]
    public void Exported_type_scan_is_not_vacuous()
    {
        var exported = ExportedTypeNames();

        Assert.That(exported, Does.Contain(nameof(LatticeMcpRepoContextServiceCollectionExtensions)));
    }

    [Test]
    public void ReadRegistryRow_flags_a_planted_bogus_name()
    {
        var registry = ReadRegistry();
        var row = FindRowLine(registry);
        var planted = registry.Replace(row, row.TrimEnd().TrimEnd('|').TrimEnd() + ", `RepoContextBogusPlantedType` |", StringComparison.Ordinal);

        var listed = ReadRegistryRow(planted);

        Assert.That(listed.Where(name => !ExportedTypeNames().Contains(name)), Does.Contain("RepoContextBogusPlantedType"));
    }

    [Test]
    public void ReadRegistryRow_without_the_row_throws()
    {
        Assert.That(
            () => ReadRegistryRow("| Element | Convention | Example |\n|---|---|---|\n"),
            Throws.InstanceOf<AssertionException>());
    }

    private static string ReadRegistry()
    {
        var path = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            RegistryRelativePath.Replace('/', Path.DirectorySeparatorChar));
        return File.ReadAllText(path);
    }

    private static string FindRowLine(string registry)
    {
        var rows = registry
            .Split('\n')
            .Where(line => IsRepoContextRow(line))
            .ToArray();

        Assert.That(rows, Has.Length.EqualTo(1), $"expected exactly one {Namespace} row in {RegistryRelativePath}");
        return rows[0].TrimEnd('\r');
    }

    private static bool IsRepoContextRow(string line)
    {
        var cells = line.Split('|');
        return cells.Length >= 4 && string.Equals(cells[2].Trim(), $"`{Namespace}`", StringComparison.Ordinal);
    }

    private static IReadOnlyList<string> ReadRegistryRow(string registry)
    {
        var cells = FindRowLine(registry).Split('|');
        // A generic type is written `Name<T>` in the registry; compare on the bare name.
        return BacktickedName.Matches(cells[3]).Select(match => match.Groups[1].Value.Split('<')[0]).ToArray();
    }

    private static HashSet<string> ExportedTypeNames()
    {
        Assembly assembly = typeof(LatticeMcpRepoContextServiceCollectionExtensions).Assembly;
        return assembly.GetExportedTypes()
            .Where(type => !type.IsNested && string.Equals(type.Namespace, Namespace, StringComparison.Ordinal))
            .Select(type => type.Name.Split('`')[0])
            .ToHashSet(StringComparer.Ordinal);
    }
}
