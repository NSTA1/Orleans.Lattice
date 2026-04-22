using Orleans.Lattice.Primitives;
using System.Text.Json.Serialization;

namespace MultiSiteManufacturing.Host.Domain;

/// <summary>
/// Base fact envelope shared by every fact emitted through the federation
/// router. Concrete fact payloads inherit from this record.
/// </summary>
[GenerateSerializer]
[JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")]
[JsonDerivedType(typeof(ProcessStepCompleted), nameof(ProcessStepCompleted))]
[JsonDerivedType(typeof(InspectionRecorded), nameof(InspectionRecorded))]
[JsonDerivedType(typeof(NonConformanceRaised), nameof(NonConformanceRaised))]
[JsonDerivedType(typeof(MrbDisposition), nameof(MrbDisposition))]
[JsonDerivedType(typeof(ReworkCompleted), nameof(ReworkCompleted))]
[JsonDerivedType(typeof(FinalAcceptance), nameof(FinalAcceptance))]
public abstract record Fact
{
    /// <summary>Part the fact applies to.</summary>
    [Id(0)] public required PartSerialNumber Serial { get; init; }

    /// <summary>Unique fact identifier (used for idempotency under replay).</summary>
    [Id(1)] public required Guid FactId { get; init; }

    /// <summary>Hybrid logical clock timestamp assigned at the origin site.</summary>
    [Id(2)] public required HybridLogicalClock Hlc { get; init; }

    /// <summary>Origin site that produced the fact.</summary>
    [Id(3)] public required ProcessSite Site { get; init; }

    /// <summary>Operator responsible for the fact.</summary>
    [Id(4)] public required OperatorId Operator { get; init; }

    /// <summary>Human-readable description surfaced in the evidence trail.</summary>
    [Id(5)] public required string Description { get; init; }
}

/// <summary>A stage of the lifecycle completed successfully at the origin site.</summary>
[GenerateSerializer]
public sealed record ProcessStepCompleted : Fact
{
    [Id(0)] public required ProcessStage Stage { get; init; }
    [Id(1)] public string? HeatLot { get; init; }
    [Id(2)] public IReadOnlyDictionary<string, string> ProcessParameters { get; init; }
        = new Dictionary<string, string>();
}

/// <summary>An inspection record with pass/fail outcome.</summary>
[GenerateSerializer]
public sealed record InspectionRecorded : Fact
{
    [Id(0)] public required Inspection Inspection { get; init; }
    [Id(1)] public required InspectionOutcome Outcome { get; init; }
    [Id(2)] public IReadOnlyDictionary<string, string> Measurements { get; init; }
        = new Dictionary<string, string>();
    [Id(3)] public DateTimeOffset? InstrumentCalibrationDate { get; init; }
}

/// <summary>A non-conformance raised against a part.</summary>
[GenerateSerializer]
public sealed record NonConformanceRaised : Fact
{
    [Id(0)] public required string NcNumber { get; init; }
    [Id(1)] public required string DefectCode { get; init; }
    [Id(2)] public required NcSeverity Severity { get; init; }
}

/// <summary>MRB disposition against an outstanding non-conformance.</summary>
[GenerateSerializer]
public sealed record MrbDisposition : Fact
{
    [Id(0)] public required string NcNumber { get; init; }
    [Id(1)] public required MrbDispositionKind Disposition { get; init; }
}

/// <summary>A rework operation was completed; optionally accompanied by a retest result.</summary>
[GenerateSerializer]
public sealed record ReworkCompleted : Fact
{
    [Id(0)] public required string ReworkOperation { get; init; }
    [Id(1)] public required bool RetestPassed { get; init; }
}

/// <summary>Final-article inspection sign-off.</summary>
[GenerateSerializer]
public sealed record FinalAcceptance : Fact
{
    [Id(0)] public required string FaiReportId { get; init; }
    [Id(1)] public required string InspectorId { get; init; }
    [Id(2)] public required bool CertificateIssued { get; init; }
}
