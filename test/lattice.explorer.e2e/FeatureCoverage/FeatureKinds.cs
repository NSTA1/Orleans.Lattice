namespace Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;

/// <summary>The kinds of row in the feature catalogue, and which half of it each belongs to.</summary>
public static class FeatureKinds
{
    /// <summary>An <c>@page</c> route template.</summary>
    public const string Route = "route";

    /// <summary>A segment an area's catch-all router parses.</summary>
    public const string Subroute = "subroute";

    /// <summary>A tab of a tree workspace.</summary>
    public const string Tab = "tab";

    /// <summary>A palette command id.</summary>
    public const string Command = "command";

    /// <summary>A query key an area parses.</summary>
    public const string Query = "query";

    /// <summary>A user-visible hosting option.</summary>
    public const string Option = "option";

    /// <summary>A documented use case.</summary>
    public const string UseCase = "use-case";

    /// <summary>A documented dialog or one of its variants.</summary>
    public const string Dialog = "dialog";

    /// <summary>A documented value of a reflected query key.</summary>
    public const string QueryValue = "query-value";

    /// <summary>The kinds the reflection test keeps complete against the assemblies.</summary>
    public static IReadOnlySet<string> Reflected { get; } = new HashSet<string>(StringComparer.Ordinal) { Route, Subroute, Tab, Command, Query, Option };

    /// <summary>The kinds curated from the Explorer docs, each row citing its doc anchor.</summary>
    public static IReadOnlySet<string> Curated { get; } = new HashSet<string>(StringComparer.Ordinal) { UseCase, Dialog, QueryValue };

    /// <summary>Whether <paramref name="kind"/> is a known kind.</summary>
    /// <param name="kind">The kind.</param>
    public static bool IsKnown(string kind) => Reflected.Contains(kind) || Curated.Contains(kind);
}
