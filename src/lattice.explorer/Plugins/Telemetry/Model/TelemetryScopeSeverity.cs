namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// How a panel reports the tenant scope a result was <em>actually</em> served
/// under: the sentence it renders, and how loudly.
/// </summary>
/// <remarks>
/// <para>
/// <b>This exists because a silent downgrade is a correctness bug, not a
/// cosmetic one.</b> The facade fails closed: an <c>AllTenants</c> request it
/// cannot validate is served as the caller's own tenant instead of refused. A
/// chart that quietly renders that answer under the heading the operator asked
/// for presents one tenant's traffic as the whole cluster's - so the degrade is
/// reported as a warning the caller has to read, never folded into the ordinary
/// caption.
/// </para>
/// </remarks>
public enum TelemetryScopeSeverity
{
    /// <summary>
    /// The effective scope is the requested one. The caption is informational:
    /// it says what the figures cover, and nothing went wrong.
    /// </summary>
    Informational = 0,

    /// <summary>
    /// The facade served a narrower scope than the caller requested. The panel
    /// must say so where the caller will see it, because the chart underneath
    /// is not answering the question that was asked.
    /// </summary>
    Degraded = 1,
}
