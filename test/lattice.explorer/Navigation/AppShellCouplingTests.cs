using System.Reflection;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// The architectural guard this epic exists for: the shell must not name a
/// plugin. It enumerates <c>IExplorerPlugin</c> from the container and renders
/// the active one dynamically, so no feature type, no feature service, and no
/// per-plugin branch may appear in its source.
/// <para>
/// This scans the shell's own source rather than its compiled form, because the
/// coupling being guarded against - an <c>if</c> per area and an injected
/// per-area service - is a source-level shape, and reading the source is what
/// makes a regression legible in the failure message.
/// </para>
/// </summary>
[TestFixture]
public sealed class AppShellCouplingTests
{
    /// <summary>
    /// The feature types and services the retired shell named. None of them may
    /// reappear: a new area joins by being registered, not by editing this file.
    /// </summary>
    private static readonly string[] ForbiddenNames =
    [
        "BackupsPanel",
        "AccessPanel",
        "SchemaPanel",
        "IBackupCapabilityService",
        "IAuthAdminCapabilityService",
        "ISchemaAdminCapabilityService",
        "BackupsPluginKeys",
        "AccessPluginKeys",
        "SchemaPluginKeys",
        "BackupsAreaPlugin",
        "AccessAreaPlugin",
        "SchemaAreaPlugin",
        "orleans.lattice.backups",
        "orleans.lattice.access",
        "orleans.lattice.schema",
        "AppArea",
        "ExplorerCapabilities",
        "ExplorerNavigationOptions",
    ];

    private static string ShellSource()
    {
        var path = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            "src", "lattice.explorer", "UI", "Navigation", "AppShell.razor");

        Assert.That(File.Exists(path), Is.True, $"expected the shell at {path}");
        return File.ReadAllText(path);
    }

    [Test]
    public void The_shell_names_no_plugin_and_no_per_plugin_service()
    {
        var source = ShellSource();

        var offenders = ForbiddenNames
            .Where(name => source.Contains(name, StringComparison.Ordinal))
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            "the shell must reach every area through the plugin contract alone");
    }

    [Test]
    public void The_shell_renders_the_active_plugin_dynamically()
    {
        var source = ShellSource();

        Assert.Multiple(() =>
        {
            Assert.That(source, Does.Contain("DynamicComponent"));
            Assert.That(source, Does.Contain("IExplorerPluginCatalog"));
            Assert.That(source, Does.Contain("IExplorerPluginAccessStore"));
            Assert.That(source, Does.Contain("IExplorerPluginAccessRefresher"));
        });
    }

    [Test]
    public void The_shell_injects_no_feature_project_namespace()
    {
        var source = ShellSource();

        string[] featureNamespaces =
        [
            "Orleans.Lattice.Explorer.Backup",
            "Orleans.Lattice.Explorer.Access",
            "Orleans.Lattice.Explorer.Schema",
            "Orleans.Lattice.Explorer.UI.Backup",
            "Orleans.Lattice.Explorer.Access.Views",
            "Orleans.Lattice.Explorer.UI.Schema",
        ];

        var offenders = featureNamespaces
            .Where(name => source.Contains(name, StringComparison.Ordinal))
            .ToArray();

        Assert.That(offenders, Is.Empty);
    }

    [Test]
    public void The_compiled_shell_references_no_feature_type()
    {
        // The source scan is the legible guard; this is the one that cannot be
        // side-stepped by moving a name into a helper the shell calls.
        var shell = typeof(Orleans.Lattice.Explorer.UI.Navigation.AppShell);
        var referenced = shell
            .GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
            .Select(field => field.FieldType)
            .Concat(shell
                .GetProperties(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
                .Select(property => property.PropertyType))
            .Select(type => type.FullName ?? type.Name)
            .ToArray();

        var offenders = referenced
            .Where(name => name.Contains("Explorer.Backup", StringComparison.Ordinal)
                || name.Contains("Explorer.Access", StringComparison.Ordinal)
                || name.Contains("Explorer.Schema", StringComparison.Ordinal)
                || name.Contains("Explorer.UI.Backup", StringComparison.Ordinal)
                || name.Contains("Explorer.UI.Access", StringComparison.Ordinal)
                || name.Contains("Explorer.UI.Schema", StringComparison.Ordinal))
            .ToArray();

        Assert.That(offenders, Is.Empty);
    }
}
