namespace MultiSiteManufacturing.Host.Domain;

/// <summary>
/// Ordered compliance states. Higher values dominate under the severity fold.
/// <see cref="Scrap"/> is terminal: no further dispositions demote it.
/// </summary>
public enum ComplianceState
{
    /// <summary>No defects recorded, or defects explicitly dispositioned as use-as-is.</summary>
    Nominal = 0,

    /// <summary>An inspection is in progress; outcome not yet recorded.</summary>
    UnderInspection = 1,

    /// <summary>A non-conformance or failed inspection requires human review.</summary>
    FlaggedForReview = 2,

    /// <summary>Part has been dispositioned for rework; awaiting rework + re-inspection.</summary>
    Rework = 3,

    /// <summary>Terminal state — part is scrapped.</summary>
    Scrap = 4,
}

/// <summary>Lifecycle stages a turbine part traverses.</summary>
public enum ProcessStage
{
    Forge,
    HeatTreat,
    Machining,
    NDT,
    MRB,
    FAI,
}

/// <summary>
/// Physical process sites. Enum values use PascalCase identifiers;
/// <see cref="ProcessSiteInfo.GetDisplayName"/> returns the human label.
/// </summary>
public enum ProcessSite
{
    OhioForge,
    NagoyaHeatTreat,
    StuttgartMachining,
    StuttgartCmmLab,
    ToulouseNdtLab,
    CincinnatiMrb,
    BristolFai,
}

/// <summary>Inspection technique applied to a part.</summary>
public enum Inspection
{
    CMM,
    FPI,
    EddyCurrent,
    XRay,
    Visual,
}

/// <summary>Outcome of a single inspection.</summary>
public enum InspectionOutcome
{
    Pass,
    Fail,
}

/// <summary>Severity classification attached to a non-conformance.</summary>
public enum NcSeverity
{
    Minor,
    Major,
    Critical,
}

/// <summary>MRB disposition choices for an outstanding non-conformance.</summary>
public enum MrbDispositionKind
{
    UseAsIs,
    Rework,
    Scrap,
    ReturnToVendor,
}
