using MultiSiteManufacturing.Host.Domain;

namespace MultiSiteManufacturing.Host.Dashboard;

/// <summary>
/// One row of the inventory grid, pushed to Blazor subscribers whenever
/// the underlying part state changes. Baseline + lattice states are both
/// carried so the UI can flag divergence inline without a second fetch.
/// </summary>
public sealed record PartSummaryUpdate
{
    /// <summary>Part serial number.</summary>
    public required PartSerialNumber Serial { get; init; }

    /// <summary>Owning family (inferred from the serial prefix).</summary>
    public required string Family { get; init; }

    /// <summary>Most recent <see cref="ProcessStage"/> the part reached, or <c>null</c> if no step has been recorded.</summary>
    public required ProcessStage? LatestStage { get; init; }

    /// <summary>Compliance state as computed by the baseline backend.</summary>
    public required ComplianceState BaselineState { get; init; }

    /// <summary>Compliance state as computed by the lattice backend.</summary>
    public required ComplianceState LatticeState { get; init; }

    /// <summary>Total number of facts recorded against the part in the lattice backend.</summary>
    public required int FactCount { get; init; }

    /// <summary>True when the baseline and lattice states disagree (drives the divergence feed).</summary>
    public bool Diverges => BaselineState != LatticeState;
}

/// <summary>
/// Aggregate snapshot of site + backend chaos configuration, pushed to
/// the dashboard chaos banner whenever anything changes. Shallow by
/// design: the banner only needs counts, not per-row detail.
/// </summary>
public sealed record ChaosOverview
{
    /// <summary>Sites currently paused (<see cref="Federation.SiteConfig.IsPaused"/>).</summary>
    public required int PausedSites { get; init; }

    /// <summary>Sites with a non-zero <see cref="Federation.SiteConfig.DelayMs"/>.</summary>
    public required int DelayedSites { get; init; }

    /// <summary>Sites with <see cref="Federation.SiteConfig.ReorderEnabled"/> on.</summary>
    public required int ReorderingSites { get; init; }

    /// <summary>Backends with any non-nominal chaos knob engaged.</summary>
    public required IReadOnlyList<string> FlakyBackends { get; init; }

    /// <summary>True if any chaos knob anywhere is engaged.</summary>
    public bool Any => PausedSites > 0
        || DelayedSites > 0
        || ReorderingSites > 0
        || FlakyBackends.Count > 0;
}

/// <summary>
/// One row of the divergence feed — pushed whenever a part's
/// baseline-vs-lattice agreement status flips (enters divergence, stays
/// divergent with new states, or resolves).
/// </summary>
public sealed record DivergenceEvent
{
    /// <summary>Part the event is reporting on.</summary>
    public required PartSerialNumber Serial { get; init; }

    /// <summary>Compliance state as computed by the baseline backend.</summary>
    public required ComplianceState BaselineState { get; init; }

    /// <summary>Compliance state as computed by the lattice backend.</summary>
    public required ComplianceState LatticeState { get; init; }

    /// <summary>
    /// <c>true</c> when the two backends used to disagree on this part
    /// but have now converged (<see cref="BaselineState"/> equals
    /// <see cref="LatticeState"/>). Subscribers can drop the row instead
    /// of re-rendering it.
    /// </summary>
    public required bool Resolved { get; init; }
}
