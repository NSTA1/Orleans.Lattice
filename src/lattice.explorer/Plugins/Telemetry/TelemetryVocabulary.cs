using Orleans.Lattice.Explorer.Core.Vocabulary;

namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The telemetry jargon this area puts in front of a reader, explained once here
/// and rendered at the point of use through the help disclosure.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a plugin-owned table rather than more entries in
/// <see cref="ExplorerGlossary"/>.</b> The shared glossary names the concepts the
/// <em>whole</em> Explorer shares. A metric catalogue, a query range and a
/// metrics backend are meaningful only inside this area, and the Explorer's
/// assembly graph puts the shared glossary in a package this one consumes rather
/// than owns. So the terms live with the surface that says them, and reuse the
/// shared <see cref="ExplorerTerm"/> shape so a help disclosure renders them
/// identically.
/// </para>
/// <para>
/// Every term is constructed once, in a static initialiser, so explaining one at
/// the point of use costs no allocation per render.
/// </para>
/// </remarks>
public static class TelemetryVocabulary
{
    /// <summary>The id of the <see cref="MetricCatalog"/> term.</summary>
    public const string MetricCatalogId = "metric-catalog";

    /// <summary>The id of the <see cref="QueryRange"/> term.</summary>
    public const string QueryRangeId = "query-range";

    /// <summary>The id of the <see cref="QueryStep"/> term.</summary>
    public const string QueryStepId = "query-step";

    /// <summary>The id of the <see cref="Backend"/> term.</summary>
    public const string BackendId = "telemetry-backend";

    /// <summary>The id of the <see cref="Scope"/> term.</summary>
    public const string ScopeId = "telemetry-scope";

    /// <summary>The list of panels the cluster itself publishes.</summary>
    public static ExplorerTerm MetricCatalog { get; } = new()
    {
        Id = MetricCatalogId,
        Label = "Metric catalogue",
        Explanation =
            "The catalogue is the list of panels the cluster publishes, written by the cluster rather than "
            + "by this screen. You choose from it; there is no way to compose a query of your own here, which "
            + "is what stops a panel describing something the cluster does not measure.",
        DocsLink = ExplorerDocsLinks.Telemetry,
    };

    /// <summary>How far back a panel reads.</summary>
    public static ExplorerTerm QueryRange { get; } = new()
    {
        Id = QueryRangeId,
        Label = "Range",
        Explanation =
            "The range is how far back the panel reads - the last hour, the last day. Only the ranges the "
            + "selected panel declares are offered, because a range it does not accept would be refused by "
            + "the cluster rather than drawn.",
        DocsLink = ExplorerDocsLinks.Telemetry,
    };

    /// <summary>How finely the range is divided.</summary>
    public static ExplorerTerm QueryStep { get; } = new()
    {
        Id = QueryStepId,
        Label = "Step",
        Explanation =
            "The step is how finely the range is divided: one point per step. A smaller step draws more "
            + "detail and reads more data. Leaving it at the cluster's default lets the cluster pick one it "
            + "is certain to accept.",
        DocsLink = ExplorerDocsLinks.Telemetry,
    };

    /// <summary>The store the cluster records its measurements in.</summary>
    public static ExplorerTerm Backend { get; } = new()
    {
        Id = BackendId,
        Label = "Telemetry backend",
        Explanation =
            "The backend is the store a cluster records its measurements in. Telemetry is an add-on: a "
            + "cluster configured without a backend publishes no catalogue, so this area has nothing to "
            + "offer and does not appear.",
        DocsLink = ExplorerDocsLinks.Telemetry,
    };

    /// <summary>Which part of the cluster an answer covered.</summary>
    public static ExplorerTerm Scope { get; } = new()
    {
        Id = ScopeId,
        Label = "Scope",
        Explanation =
            "The scope is the part of the cluster the answer covered - everything, or one tenant. The "
            + "cluster decides it and reports back what it applied, which can be narrower than what was "
            + "asked for.",
        DocsLink = ExplorerDocsLinks.Telemetry,
    };
}
