using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using Orleans.Lattice.Primitives;
using V1 = MultiSiteManufacturing.Contracts.V1;

namespace MultiSiteManufacturing.Host.Grpc;

/// <summary>
/// Conversions between the generated gRPC contract types in
/// <see cref="MultiSiteManufacturing.Contracts.V1"/> and the domain /
/// federation types used internally. Kept in one place so every service
/// implementation agrees on how a <c>PROCESS_SITE_OHIO_FORGE</c> enum
/// value maps to <see cref="ProcessSite.OhioForge"/>, etc.
/// </summary>
internal static class ProtoMappings
{
    public static V1.ComplianceState ToProto(ComplianceState state) => state switch
    {
        ComplianceState.Nominal => V1.ComplianceState.Nominal,
        ComplianceState.UnderInspection => V1.ComplianceState.UnderInspection,
        ComplianceState.FlaggedForReview => V1.ComplianceState.FlaggedForReview,
        ComplianceState.Rework => V1.ComplianceState.Rework,
        ComplianceState.Scrap => V1.ComplianceState.Scrap,
        _ => V1.ComplianceState.Unspecified,
    };

    public static V1.ProcessStage ToProto(ProcessStage stage) => stage switch
    {
        ProcessStage.Forge => V1.ProcessStage.Forge,
        ProcessStage.HeatTreat => V1.ProcessStage.HeatTreat,
        ProcessStage.Machining => V1.ProcessStage.Machining,
        ProcessStage.NDT => V1.ProcessStage.Ndt,
        ProcessStage.MRB => V1.ProcessStage.Mrb,
        ProcessStage.FAI => V1.ProcessStage.Fai,
        _ => V1.ProcessStage.Unspecified,
    };

    public static ProcessStage FromProto(V1.ProcessStage stage) => stage switch
    {
        V1.ProcessStage.Forge => ProcessStage.Forge,
        V1.ProcessStage.HeatTreat => ProcessStage.HeatTreat,
        V1.ProcessStage.Machining => ProcessStage.Machining,
        V1.ProcessStage.Ndt => ProcessStage.NDT,
        V1.ProcessStage.Mrb => ProcessStage.MRB,
        V1.ProcessStage.Fai => ProcessStage.FAI,
        _ => throw new ArgumentOutOfRangeException(nameof(stage), stage, "Unspecified process stage"),
    };

    public static V1.ProcessSite ToProto(ProcessSite site) => site switch
    {
        ProcessSite.OhioForge => V1.ProcessSite.OhioForge,
        ProcessSite.NagoyaHeatTreat => V1.ProcessSite.NagoyaHeatTreat,
        ProcessSite.StuttgartMachining => V1.ProcessSite.StuttgartMachining,
        ProcessSite.StuttgartCmmLab => V1.ProcessSite.StuttgartCmmLab,
        ProcessSite.ToulouseNdtLab => V1.ProcessSite.ToulouseNdtLab,
        ProcessSite.CincinnatiMrb => V1.ProcessSite.CincinnatiMrb,
        ProcessSite.BristolFai => V1.ProcessSite.BristolFai,
        _ => V1.ProcessSite.Unspecified,
    };

    public static ProcessSite FromProto(V1.ProcessSite site) => site switch
    {
        V1.ProcessSite.OhioForge => ProcessSite.OhioForge,
        V1.ProcessSite.NagoyaHeatTreat => ProcessSite.NagoyaHeatTreat,
        V1.ProcessSite.StuttgartMachining => ProcessSite.StuttgartMachining,
        V1.ProcessSite.StuttgartCmmLab => ProcessSite.StuttgartCmmLab,
        V1.ProcessSite.ToulouseNdtLab => ProcessSite.ToulouseNdtLab,
        V1.ProcessSite.CincinnatiMrb => ProcessSite.CincinnatiMrb,
        V1.ProcessSite.BristolFai => ProcessSite.BristolFai,
        _ => throw new ArgumentOutOfRangeException(nameof(site), site, "Unspecified process site"),
    };

    public static V1.Inspection ToProto(Inspection inspection) => inspection switch
    {
        Inspection.CMM => V1.Inspection.Cmm,
        Inspection.FPI => V1.Inspection.Fpi,
        Inspection.EddyCurrent => V1.Inspection.EddyCurrent,
        Inspection.XRay => V1.Inspection.XRay,
        Inspection.Visual => V1.Inspection.Visual,
        _ => V1.Inspection.Unspecified,
    };

    public static Inspection FromProto(V1.Inspection inspection) => inspection switch
    {
        V1.Inspection.Cmm => Inspection.CMM,
        V1.Inspection.Fpi => Inspection.FPI,
        V1.Inspection.EddyCurrent => Inspection.EddyCurrent,
        V1.Inspection.XRay => Inspection.XRay,
        V1.Inspection.Visual => Inspection.Visual,
        _ => throw new ArgumentOutOfRangeException(nameof(inspection), inspection, "Unspecified inspection"),
    };

    public static InspectionOutcome FromProto(V1.InspectionOutcome outcome) => outcome switch
    {
        V1.InspectionOutcome.Pass => InspectionOutcome.Pass,
        V1.InspectionOutcome.Fail => InspectionOutcome.Fail,
        _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, "Unspecified outcome"),
    };

    public static NcSeverity FromProto(V1.NcSeverity severity) => severity switch
    {
        V1.NcSeverity.Minor => NcSeverity.Minor,
        V1.NcSeverity.Major => NcSeverity.Major,
        V1.NcSeverity.Critical => NcSeverity.Critical,
        _ => throw new ArgumentOutOfRangeException(nameof(severity), severity, "Unspecified severity"),
    };

    public static MrbDispositionKind FromProto(V1.MrbDispositionKind kind) => kind switch
    {
        V1.MrbDispositionKind.UseAsIs => MrbDispositionKind.UseAsIs,
        V1.MrbDispositionKind.Rework => MrbDispositionKind.Rework,
        V1.MrbDispositionKind.Scrap => MrbDispositionKind.Scrap,
        V1.MrbDispositionKind.ReturnToVendor => MrbDispositionKind.ReturnToVendor,
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unspecified disposition"),
    };

    public static ChaosPreset FromProto(V1.ChaosPreset preset) => preset switch
    {
        V1.ChaosPreset.ClearAll => ChaosPreset.ClearAll,
        V1.ChaosPreset.TransoceanicBackhaulOutage => ChaosPreset.TransoceanicBackhaulOutage,
        V1.ChaosPreset.CustomsHold => ChaosPreset.CustomsHold,
        V1.ChaosPreset.MrbWeekend => ChaosPreset.MrbWeekend,
        V1.ChaosPreset.LatticeStorageFlakes => ChaosPreset.LatticeStorageFlakes,
        V1.ChaosPreset.BaselineReorderStorm => ChaosPreset.BaselineReorderStorm,
        V1.ChaosPreset.ClusterSplit => ChaosPreset.ClusterSplit,
        V1.ChaosPreset.ReplicationDisconnect => ChaosPreset.ReplicationDisconnect,
        _ => throw new ArgumentOutOfRangeException(nameof(preset), preset, "Unspecified preset"),
    };

    public static V1.Hlc ToProto(HybridLogicalClock hlc) => new()
    {
        WallClockTicks = hlc.WallClockTicks,
        Counter = hlc.Counter,
    };

    public static HybridLogicalClock FromProto(V1.Hlc? hlc)
    {
        if (hlc is null)
        {
            return HybridLogicalClock.Zero;
        }
        return new HybridLogicalClock
        {
            WallClockTicks = hlc.WallClockTicks,
            Counter = hlc.Counter,
        };
    }

    public static V1.SiteConfig ToProto(SiteConfig config) => new()
    {
        IsPaused = config.IsPaused,
        DelayMs = config.DelayMs,
        ReorderEnabled = config.ReorderEnabled,
    };

    public static SiteConfig FromProto(V1.SiteConfig? config)
    {
        if (config is null)
        {
            return SiteConfig.Nominal;
        }
        return new SiteConfig
        {
            IsPaused = config.IsPaused,
            DelayMs = config.DelayMs,
            ReorderEnabled = config.ReorderEnabled,
        };
    }

    public static V1.SiteState ToProto(SiteState state) => new()
    {
        Site = ToProto(state.Site),
        Config = ToProto(state.Config),
        PendingCount = state.PendingCount,
        AdmittedCount = state.AdmittedCount,
    };

    public static V1.BackendChaosConfig ToProto(BackendChaosConfig config) => new()
    {
        JitterMsMin = config.JitterMsMin,
        JitterMsMax = config.JitterMsMax,
        TransientFailureRate = config.TransientFailureRate,
        WriteAmplificationRate = config.WriteAmplificationRate,
        ReorderWindowMs = config.ReorderWindowMs,
    };

    public static BackendChaosConfig FromProto(V1.BackendChaosConfig? config)
    {
        if (config is null)
        {
            return BackendChaosConfig.Nominal;
        }
        return new BackendChaosConfig
        {
            JitterMsMin = config.JitterMsMin,
            JitterMsMax = config.JitterMsMax,
            TransientFailureRate = config.TransientFailureRate,
            WriteAmplificationRate = config.WriteAmplificationRate,
            ReorderWindowMs = config.ReorderWindowMs,
        };
    }

    public static V1.BackendChaosState ToProto(BackendChaosState state) => new()
    {
        Name = state.Name,
        Config = ToProto(state.Config),
    };

    /// <summary>
    /// Maps a <see cref="V1.FactEnvelope"/> (over-the-wire form) to the
    /// internal <see cref="Fact"/> hierarchy via the oneof payload.
    /// </summary>
    public static Fact FromProto(V1.FactEnvelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        var factId = string.IsNullOrEmpty(envelope.FactId)
            ? Guid.NewGuid()
            : Guid.Parse(envelope.FactId);
        var serial = new PartSerialNumber(envelope.Serial);
        var hlc = FromProto(envelope.Hlc);
        var site = FromProto(envelope.Site);
        var op = new OperatorId(envelope.OperatorId);
        var description = envelope.Description ?? string.Empty;

        return envelope.PayloadCase switch
        {
            V1.FactEnvelope.PayloadOneofCase.ProcessStepCompleted => new ProcessStepCompleted
            {
                Serial = serial,
                FactId = factId,
                Hlc = hlc,
                Site = site,
                Operator = op,
                Description = description,
                Stage = FromProto(envelope.ProcessStepCompleted.Stage),
                HeatLot = string.IsNullOrEmpty(envelope.ProcessStepCompleted.HeatLot)
                    ? null
                    : envelope.ProcessStepCompleted.HeatLot,
                ProcessParameters = envelope.ProcessStepCompleted.ProcessParameters
                    .ToDictionary(kv => kv.Key, kv => kv.Value),
            },
            V1.FactEnvelope.PayloadOneofCase.InspectionRecorded => new InspectionRecorded
            {
                Serial = serial,
                FactId = factId,
                Hlc = hlc,
                Site = site,
                Operator = op,
                Description = description,
                Inspection = FromProto(envelope.InspectionRecorded.Inspection),
                Outcome = FromProto(envelope.InspectionRecorded.Outcome),
                Measurements = envelope.InspectionRecorded.Measurements
                    .ToDictionary(kv => kv.Key, kv => kv.Value),
                InstrumentCalibrationDate = envelope.InspectionRecorded.InstrumentCalibrationUnixSeconds > 0
                    ? DateTimeOffset.FromUnixTimeSeconds(envelope.InspectionRecorded.InstrumentCalibrationUnixSeconds)
                    : null,
            },
            V1.FactEnvelope.PayloadOneofCase.NonConformanceRaised => new NonConformanceRaised
            {
                Serial = serial,
                FactId = factId,
                Hlc = hlc,
                Site = site,
                Operator = op,
                Description = description,
                NcNumber = envelope.NonConformanceRaised.NcNumber,
                DefectCode = envelope.NonConformanceRaised.DefectCode,
                Severity = FromProto(envelope.NonConformanceRaised.Severity),
            },
            V1.FactEnvelope.PayloadOneofCase.MrbDisposition => new MrbDisposition
            {
                Serial = serial,
                FactId = factId,
                Hlc = hlc,
                Site = site,
                Operator = op,
                Description = description,
                NcNumber = envelope.MrbDisposition.NcNumber,
                Disposition = FromProto(envelope.MrbDisposition.Disposition),
            },
            V1.FactEnvelope.PayloadOneofCase.ReworkCompleted => new ReworkCompleted
            {
                Serial = serial,
                FactId = factId,
                Hlc = hlc,
                Site = site,
                Operator = op,
                Description = description,
                ReworkOperation = envelope.ReworkCompleted.ReworkOperation,
                RetestPassed = envelope.ReworkCompleted.RetestPassed,
            },
            V1.FactEnvelope.PayloadOneofCase.FinalAcceptance => new FinalAcceptance
            {
                Serial = serial,
                FactId = factId,
                Hlc = hlc,
                Site = site,
                Operator = op,
                Description = description,
                FaiReportId = envelope.FinalAcceptance.FaiReportId,
                InspectorId = envelope.FinalAcceptance.InspectorId,
                CertificateIssued = envelope.FinalAcceptance.CertificateIssued,
            },
            _ => throw new ArgumentException(
                $"FactEnvelope has no payload ({envelope.PayloadCase}).",
                nameof(envelope)),
        };
    }

    /// <summary>Lightweight summary of a fact for the <see cref="V1.PartView"/> evidence trail.</summary>
    public static V1.FactSummary ToSummary(Fact fact) => new()
    {
        FactId = fact.FactId.ToString(),
        Hlc = ToProto(fact.Hlc),
        Site = ToProto(fact.Site),
        Kind = fact.GetType().Name,
        Description = fact.Description,
    };
}
