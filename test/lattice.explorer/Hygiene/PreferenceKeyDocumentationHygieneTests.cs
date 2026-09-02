using System.Reflection;
using System.Text.RegularExpressions;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests;

/// <summary>
/// The page that documents what the Explorer remembers must list exactly the
/// preference keys the product declares.
/// </summary>
/// <remarks>
/// <para>
/// That page calls itself the contract and describes "a small, enumerated set",
/// so an omission is not a cosmetic gap: it is the page being untrue about its
/// own completeness. It drifted for exactly the reason a hand-maintained
/// enumeration always drifts - a plugin registers a key on mount without
/// touching the shell or the page, so nothing fails and nobody looks. It shipped
/// listing nine of the twelve keys.
/// </para>
/// <para>
/// The docs-agent fact-check cannot catch this, and structurally never could: it
/// verifies the claims a page makes against the source. It does not enumerate the
/// source to find claims the page omits.
/// </para>
/// <para>
/// Both directions are checked. A key that is declared and undocumented leaves a
/// reader unable to discover what is being remembered about them; a key that is
/// documented and no longer declared promises a control that does not exist.
/// </para>
/// </remarks>
[TestFixture]
public sealed class PreferenceKeyDocumentationHygieneTests
{
    private const string DocRelativePath = "docs/lattice.explorer/what-the-explorer-remembers.md";

    [Test]
    public void Every_declared_preference_key_is_documented_and_every_documented_key_is_declared()
    {
        var declared = DeclaredKeys();
        var documented = DocumentedKeys();

        Assert.That(
            declared,
            Is.Not.Empty,
            "The reflection sweep found no preference keys at all, so this gate would pass "
            + "vacuously. It is the sweep that is broken, not the documentation.");

        var undocumented = declared.Except(documented, StringComparer.Ordinal).Order().ToArray();
        var unknown = documented.Except(declared, StringComparer.Ordinal).Order().ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(
                undocumented,
                Is.Empty,
                $"These preference keys are declared but missing from {DocRelativePath}, which "
                + "presents itself as the complete contract. Add a row for each: "
                + string.Join(", ", undocumented));

            Assert.That(
                unknown,
                Is.Empty,
                $"These keys are documented in {DocRelativePath} but no longer declared anywhere, "
                + "so the page promises a remembered value that does not exist: "
                + string.Join(", ", unknown));
        });
    }

    [Test]
    public void The_gate_reports_a_key_that_is_declared_but_undocumented()
    {
        // A battery test, so a green run means something. Without it a broken
        // parser would silently agree with a broken sweep.
        var declared = new[] { "shell.area", "plugin.invented" };
        var documented = new[] { "shell.area" };

        var undocumented = declared.Except(documented, StringComparer.Ordinal).ToArray();

        Assert.That(undocumented, Is.EqualTo(new[] { "plugin.invented" }));
    }

    [Test]
    public void The_documentation_table_is_actually_parsed()
    {
        // Guards the parser itself: if the table's shape changes and the regex
        // stops matching, DocumentedKeys would return nothing and the real gate
        // would fail with a confusing "everything is undocumented" rather than
        // "the parser no longer understands the page".
        Assert.That(
            DocumentedKeys(),
            Does.Contain("shell.area"),
            $"The key column of the table in {DocRelativePath} could not be parsed. The gate reads "
            + "rows shaped `| `key` | description |`.");
    }

    /// <summary>
    /// Every preference key the product declares, found by reflecting over the
    /// loaded Explorer assemblies rather than from a hand-kept list, so a key
    /// added by a new plugin is picked up without editing this gate.
    /// </summary>
    private static IReadOnlyCollection<string> DeclaredKeys()
    {
        var keys = new SortedSet<string>(StringComparer.Ordinal);

        foreach (var assembly in ExplorerAssemblies())
        {
            foreach (var type in SafeTypes(assembly))
            {
                foreach (var member in type.GetMembers(
                    BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static))
                {
                    var value = member switch
                    {
                        PropertyInfo { CanRead: true } p when p.PropertyType == typeof(ExplorerPreferenceKey)
                            => TryRead(() => p.GetValue(null)),
                        FieldInfo f when f.FieldType == typeof(ExplorerPreferenceKey)
                            => TryRead(() => f.GetValue(null)),
                        _ => null,
                    };

                    if (value is ExplorerPreferenceKey key && !string.IsNullOrWhiteSpace(key.Name))
                    {
                        keys.Add(key.Name);
                    }
                }
            }
        }

        return keys;
    }

    private static object? TryRead(Func<object?> read)
    {
        try
        {
            return read();
        }
        catch (Exception)
        {
            // A static that throws on first access is not a declared key.
            return null;
        }
    }

    private static IEnumerable<Assembly> ExplorerAssemblies()
    {
        // Touch a type from each plugin package so its assembly is loaded before
        // the sweep; a package nothing has referenced yet would otherwise be
        // invisible to AppDomain.CurrentDomain.
        _ = typeof(ExplorerPreferenceKey);

        return AppDomain.CurrentDomain
            .GetAssemblies()
            .Where(a => a.GetName().Name is { } name
                && name.StartsWith("Orleans.Lattice.Explorer", StringComparison.Ordinal)
                // Test fixtures declare their own throwaway keys to exercise the
                // catalog; they are not product surface and must not be demanded
                // of the documentation.
                && !name.EndsWith(".Tests", StringComparison.Ordinal)
                && !name.EndsWith(".UiTests", StringComparison.Ordinal));
    }

    private static IEnumerable<Type> SafeTypes(Assembly assembly)
    {
        try
        {
            return assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            return ex.Types.Where(t => t is not null)!;
        }
    }

    private static IReadOnlyCollection<string> DocumentedKeys()
    {
        var path = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            DocRelativePath.Replace('/', Path.DirectorySeparatorChar));

        Assert.That(File.Exists(path), Is.True, $"{DocRelativePath} is missing.");

        var keys = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var line in File.ReadLines(path))
        {
            var match = Regex.Match(line, @"^\|\s*`([a-z0-9.\-]+)`\s*\|");
            if (match.Success)
            {
                keys.Add(match.Groups[1].Value);
            }
        }

        return keys;
    }
}
