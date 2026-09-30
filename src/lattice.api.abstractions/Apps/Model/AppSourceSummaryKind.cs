namespace Orleans.Lattice.Api.Apps;

/// <summary>An API-layer mirror of an app source's kind, independent of the app engine's source types.</summary>
public enum AppSourceSummaryKind
{
    /// <summary>The source offers a fixed set of apps registered when the silo starts, such as the in-image source.</summary>
    Static = 0,
    /// <summary>The source's offer can change while the cluster runs, such as a package feed or registry.</summary>
    Dynamic = 1,
}
