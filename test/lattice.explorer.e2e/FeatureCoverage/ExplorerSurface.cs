using System.Collections;
using System.Reflection;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.Web;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;

/// <summary>
/// Reflects the Explorer's user-facing surface: the reflected half of the feature catalogue
/// (<c>explorer-features.tsv</c>, #5361). A test fails when this finds a feature the catalogue
/// lacks, so a new route, tab, command, query key or hosting option cannot ship without a
/// journey owning it.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><b>route</b>: every <c>@page</c> template (<see cref="RouteAttribute"/>).</item>
/// <item><b>subroute</b>: every segment an area's catch-all router parses - each <c>*Segment</c>
/// constant, the cluster tree page's view words, the app page's sections (<c>AppPageTabs</c>)
/// and the telemetry boards.</item>
/// <item><b>tab</b>: every tab of a tree workspace - the <c>DataTabs</c> and <c>SchemaTabs</c>
/// values, and every literal <c>&lt;LtTab Id="..."&gt;</c> in the areas' markup (the cluster tree
/// page's tabs exist only there).</item>
/// <item><b>command</b>: every palette command id constant (a <c>*Id</c> constant whose value is
/// a dotted id, such as <c>data.refresh</c> or <c>go.home</c>).</item>
/// <item><b>query</b>: every query key an area parses (a <c>*Query</c> or <c>*QueryKey</c> constant).</item>
/// <item><b>option</b>: every settable <c>LatticeExplorerWebOptions</c> property.</item>
/// </list>
/// </remarks>
public static partial class ExplorerSurface
{
    private const BindingFlags Statics = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.DeclaredOnly;

    /// <summary>The Explorer assemblies the surface is read from: Core, UI and the web host.</summary>
    public static IReadOnlyList<Assembly> Assemblies { get; } =
    [
        typeof(ExplorerRouteSegments).Assembly,
        typeof(ExplorerPage).Assembly,
        typeof(LatticeExplorerWebOptions).Assembly,
    ];

    /// <summary>Every reflected feature, ordered by id, with duplicates (one value, two declarations) merged.</summary>
    /// <param name="repositoryRoot">The repository root, for the markup-only tabs.</param>
    public static IReadOnlyList<SurfaceFeature> Reflect(string repositoryRoot)
    {
        var found = new Dictionary<string, SurfaceFeature>(StringComparer.Ordinal);
        void Add(string id, string kind, string source)
        {
            found[id] = found.TryGetValue(id, out var existing)
                ? existing with { Source = string.Join("; ", existing.Source.Split("; ").Append(source).Distinct().Order(StringComparer.Ordinal)) }
                : new SurfaceFeature(id, kind, source);
        }

        foreach (var type in Assemblies.SelectMany(LoadableTypes))
        {
            foreach (var route in type.GetCustomAttributes<RouteAttribute>(inherit: false))
            {
                Add("route:" + route.Template, FeatureKinds.Route, type.Name);
            }

            foreach (var field in type.GetFields(Statics).Where(field => field is { IsLiteral: true } && field.FieldType == typeof(string)))
            {
                ClassifyConstant(type, field, Add);
            }

            if (type.Name == "ClusterAddresses" && type.GetField("ViewWords", Statics)?.GetValue(null) is IDictionary words)
            {
                foreach (var word in words.Keys.Cast<string>())
                {
                    Add($"subroute:cluster/trees/{{tree}}/{word}", FeatureKinds.Subroute, "ClusterAddresses.ViewWords");
                }
            }

            if (type.Name == "TelemetryBoards")
            {
                foreach (var key in TelemetryBoardKeys(type))
                {
                    Add($"subroute:telemetry/{key}", FeatureKinds.Subroute, "TelemetryBoards");
                }
            }
        }

        foreach (var property in typeof(LatticeExplorerWebOptions).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                     .Where(property => property.SetMethod is { IsPublic: true }))
        {
            Add("option:" + property.Name, FeatureKinds.Option, nameof(LatticeExplorerWebOptions) + "." + property.Name);
        }

        var areas = Path.Combine(repositoryRoot, "src", "lattice.explorer", "UI", "Areas");
        foreach (var markup in Directory.EnumerateFiles(areas, "*.razor", SearchOption.AllDirectories))
        {
            var area = Path.GetRelativePath(areas, markup).Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)[0].ToLowerInvariant();
            foreach (Match tab in LiteralTab().Matches(File.ReadAllText(markup)))
            {
                Add($"tab:{area}/{tab.Groups[1].Value}", FeatureKinds.Tab, Path.GetFileName(markup));
            }
        }

        return [.. found.Values.OrderBy(feature => feature.Id, StringComparer.Ordinal)];
    }

    /// <summary>
    /// The area a declaring type belongs to: the namespace segment after <c>Areas</c>, lower-cased
    /// (<c>...UI.Areas.Data</c> is <c>data</c>), or <c>shell</c> for the chrome and navigation.
    /// </summary>
    /// <param name="type">The declaring type.</param>
    public static string AreaOf(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        var parts = (type.Namespace ?? string.Empty).Split('.');
        var index = Array.IndexOf(parts, "Areas");
        return index >= 0 && index + 1 < parts.Length ? parts[index + 1].ToLowerInvariant() : "shell";
    }

    private static void ClassifyConstant(Type type, FieldInfo field, Action<string, string, string> add)
    {
        var value = (string)field.GetRawConstantValue()!;
        var name = field.Name;
        var source = $"{type.Name}.{name}";
        var area = AreaOf(type);
        if (name.EndsWith("Id", StringComparison.Ordinal) && CommandIdShape().IsMatch(value))
        {
            add("command:" + value, FeatureKinds.Command, source);
        }
        else if ((name.EndsWith("Query", StringComparison.Ordinal) || name.EndsWith("QueryKey", StringComparison.Ordinal)) && WordShape().IsMatch(value))
        {
            add($"query:{area}.{value}", FeatureKinds.Query, source);
        }
        else if (name.EndsWith("Segment", StringComparison.Ordinal) && WordShape().IsMatch(value))
        {
            add($"subroute:{area}/{value}", FeatureKinds.Subroute, source);
        }
        else if (type.Name is "DataTabs" or "SchemaTabs" && WordShape().IsMatch(value) && !name.EndsWith("Value", StringComparison.Ordinal))
        {
            add($"tab:{area}/{value}", FeatureKinds.Tab, source);
        }
        else if (type.Name == "ClusterAddresses" && name.EndsWith("Tab", StringComparison.Ordinal))
        {
            add($"tab:{area}/{value}", FeatureKinds.Tab, source);
        }
        else if (type.Name == "AppPageTabs" && WordShape().IsMatch(value))
        {
            add($"subroute:{area}/{{app}}/{value}", FeatureKinds.Subroute, source);
        }
    }

    private static IEnumerable<string> TelemetryBoardKeys(Type type)
    {
        var boards = new List<object>();
        if (type.GetProperty("Curated", Statics)?.GetValue(null) is IEnumerable curated)
        {
            boards.AddRange(curated.Cast<object>());
        }

        if (type.GetProperty("Other", Statics)?.GetValue(null) is { } other)
        {
            boards.Add(other);
        }

        return boards.Select(board => (string)board.GetType().GetProperty("Key")!.GetValue(board)!);
    }

    private static IEnumerable<Type> LoadableTypes(Assembly assembly)
    {
        try
        {
            return assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            return ex.Types.Where(type => type is not null)!;
        }
    }

    [GeneratedRegex("^[a-z]+(\\.[a-z0-9-]+)+$")]
    private static partial Regex CommandIdShape();

    [GeneratedRegex("^[a-z][a-z0-9-]*$")]
    private static partial Regex WordShape();

    [GeneratedRegex("<LtTab\\b[^>]*?\\bId=\"([a-z][a-z0-9-]*)\"")]
    private static partial Regex LiteralTab();
}
