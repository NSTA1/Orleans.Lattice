using System.Text.Json;
using NUnit.Framework.Interfaces;
using NUnit.Framework.Internal;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The TLC case census (contract C2 of #5148): every TLC base, variant and
/// mutation case, keyed by module, as
/// <c>{ "&lt;Module&gt;": ["&lt;FullyQualifiedName&gt;", ...] }</c>.
/// <para>
/// WHY NUNIT BUILDS THE NAMES. A consumer compares the census against test
/// results, so each name must be the exact <c>FullyQualifiedName</c> VSTest
/// reports. The census therefore runs every TLC gate's own
/// <c>[TestCaseSource]</c> through NUnit's case builder rather than formatting
/// names itself, and groups each built case by the
/// <see cref="TlcCiShard.ModuleCategory"/> it carries. A
/// <c>TestCategory=TlcModule_&lt;Module&gt;</c> filter and the census entry for
/// that module therefore select the same cases by construction.
/// </para>
/// <para>
/// TOOLCHAIN-FREE. Building the cases enumerates the three TLC sources and
/// reads <c>spec/</c>; nothing starts a JVM, so the census can be taken on any
/// runner. <see cref="ExportIfRequested"/> writes it when
/// <see cref="EnvironmentVariable"/> names a file.
/// </para>
/// </summary>
internal static class TlcCaseCensus
{
    /// <summary>The environment variable naming the file the census is written to.</summary>
    public const string EnvironmentVariable = "LATTICE_TLC_CENSUS";

    /// <summary>
    /// The environment variable naming the file each census case's test
    /// categories are written to, as
    /// <c>{ "&lt;FullyQualifiedName&gt;": ["&lt;Category&gt;", ...] }</c>. The CI
    /// matrix planner evaluates each TLC shard's filter over these to learn
    /// which shards a module's cases fall in (TLC verdict receipts, #5164).
    /// </summary>
    public const string CategoriesEnvironmentVariable = "LATTICE_TLC_CASE_CATEGORIES";

    /// <summary>
    /// Every TLC case, built by NUnit from the <c>[TestCaseSource]</c> of each
    /// gate that runs TLC, exactly as the test adapter discovers it.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// A TLC gate has no case source, or NUnit could not build one of its cases.
    /// </exception>
    public static IReadOnlyList<Test> Cases()
    {
        var cases = new List<Test>();
        foreach (var gate in SpecModuleGates.All().Where(gate => gate.RunsTlc))
        {
            var source = gate.Source
                ?? throw new InvalidOperationException($"TLC gate {gate.Name} has no [TestCaseSource] to take a census of.");
            var fixture = new TestFixture(new TypeWrapper(gate.Fixture));
            foreach (var test in source.BuildFrom(new MethodWrapper(gate.Fixture, gate.Method), fixture))
            {
                if (test.RunState == RunState.NotRunnable)
                {
                    throw new InvalidOperationException(
                        $"NUnit could not build TLC case {test.FullName}: {test.Properties.Get(PropertyNames.SkipReason)}");
                }

                cases.Add(test);
            }
        }

        return cases;
    }

    /// <summary>
    /// The census: each module name mapped to the fully qualified names of its
    /// TLC cases, both in ordinal order.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// A case does not carry exactly one module category, or two cases share a
    /// fully qualified name.
    /// </exception>
    public static IReadOnlyDictionary<string, IReadOnlyList<string>> Build()
    {
        var census = new SortedDictionary<string, List<string>>(StringComparer.Ordinal);
        foreach (var test in Cases())
        {
            var module = ModuleOf(test);
            if (!census.TryGetValue(module, out var names))
            {
                census[module] = names = [];
            }

            names.Add(test.FullName);
        }

        var duplicate = census.Values.SelectMany(names => names)
            .GroupBy(name => name, StringComparer.Ordinal)
            .FirstOrDefault(group => group.Count() > 1);
        if (duplicate is not null)
        {
            throw new InvalidOperationException($"Two TLC cases share the fully qualified name {duplicate.Key}.");
        }

        return census.ToDictionary(
            entry => entry.Key,
            entry => (IReadOnlyList<string>)entry.Value.Order(StringComparer.Ordinal).ToArray(),
            StringComparer.Ordinal);
    }

    /// <summary>
    /// The module named by the single <see cref="TlcCiShard.ModuleCategoryPrefix"/>
    /// category <paramref name="test"/> carries.
    /// </summary>
    /// <exception cref="InvalidOperationException">The case carries no module category, or more than one.</exception>
    public static string ModuleOf(Test test)
    {
        ArgumentNullException.ThrowIfNull(test);

        var modules = test.Properties[PropertyNames.Category].Cast<string>()
            .Where(category => category.StartsWith(TlcCiShard.ModuleCategoryPrefix, StringComparison.Ordinal))
            .ToArray();
        return modules is [var only]
            ? only[TlcCiShard.ModuleCategoryPrefix.Length..]
            : throw new InvalidOperationException(
                $"TLC case {test.FullName} carries {modules.Length} {TlcCiShard.ModuleCategoryPrefix} categories; it must carry exactly one.");
    }

    /// <summary>
    /// Serializes <paramref name="census"/> as an indented JSON object whose
    /// keys and name lists keep the census's ordinal order.
    /// </summary>
    public static string Serialize(IReadOnlyDictionary<string, IReadOnlyList<string>> census)
    {
        ArgumentNullException.ThrowIfNull(census);

        var ordered = new SortedDictionary<string, IReadOnlyList<string>>(
            census.ToDictionary(entry => entry.Key, entry => entry.Value, StringComparer.Ordinal),
            StringComparer.Ordinal);
        return JsonSerializer.Serialize(ordered, new JsonSerializerOptions { WriteIndented = true }) + "\n";
    }

    /// <summary>
    /// Every census case's test categories, as the test adapter reports them:
    /// the case's own categories plus those its fixture declares, ordinal-sorted
    /// and distinct, keyed by fully qualified name in ordinal order.
    /// </summary>
    public static IReadOnlyDictionary<string, IReadOnlyList<string>> BuildCategories()
    {
        var categories = new SortedDictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);
        foreach (var test in Cases())
        {
            var fixture = test.TypeInfo?.Type;
            var inherited = fixture is null
                ? []
                : fixture.GetCustomAttributes(typeof(CategoryAttribute), inherit: true)
                    .Cast<CategoryAttribute>().Select(attribute => attribute.Name);
            categories[test.FullName] = test.Properties[PropertyNames.Category].Cast<string>()
                .Concat(inherited)
                .Distinct(StringComparer.Ordinal)
                .Order(StringComparer.Ordinal)
                .ToArray();
        }

        return categories;
    }

    /// <summary>
    /// Writes <see cref="BuildCategories"/> to the file
    /// <see cref="CategoriesEnvironmentVariable"/> names, and returns that path;
    /// returns <see langword="null"/> and writes nothing when the variable is
    /// unset or blank.
    /// </summary>
    /// <exception cref="ArgumentException">The named path is not fully qualified.</exception>
    public static string? ExportCategoriesIfRequested()
    {
        var path = Environment.GetEnvironmentVariable(CategoriesEnvironmentVariable);
        if (string.IsNullOrWhiteSpace(path))
        {
            return null;
        }

        if (!Path.IsPathFullyQualified(path))
        {
            throw new ArgumentException(
                $"{CategoriesEnvironmentVariable} must name a fully qualified path; '{path}' is relative.", nameof(path));
        }

        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        File.WriteAllText(path, Serialize(BuildCategories()));
        return path;
    }

    /// <summary>Reads a census written by <see cref="Export"/>.</summary>
    public static IReadOnlyDictionary<string, IReadOnlyList<string>> Read(string path)
    {
        ArgumentNullException.ThrowIfNull(path);

        var census = JsonSerializer.Deserialize<Dictionary<string, string[]>>(File.ReadAllText(path))
            ?? throw new InvalidOperationException($"The census at {path} is empty.");
        return census.ToDictionary(entry => entry.Key, entry => (IReadOnlyList<string>)entry.Value, StringComparer.Ordinal);
    }

    /// <summary>Builds the census and writes it to <paramref name="path"/>, creating its directory.</summary>
    /// <exception cref="ArgumentException"><paramref name="path"/> is not fully qualified.</exception>
    public static void Export(string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        if (!Path.IsPathFullyQualified(path))
        {
            // The test host's working directory is the build output, not the
            // caller's, so a relative path would land somewhere nobody looks.
            throw new ArgumentException(
                $"{EnvironmentVariable} must name a fully qualified path; '{path}' is relative.", nameof(path));
        }

        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        File.WriteAllText(path, Serialize(Build()));
    }

    /// <summary>
    /// Writes the census to the file <see cref="EnvironmentVariable"/> names,
    /// and returns that path; returns <see langword="null"/> and writes nothing
    /// when the variable is unset or blank.
    /// </summary>
    public static string? ExportIfRequested()
    {
        var path = Environment.GetEnvironmentVariable(EnvironmentVariable);
        if (string.IsNullOrWhiteSpace(path))
        {
            return null;
        }

        Export(path);
        return path;
    }
}
