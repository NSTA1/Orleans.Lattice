using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Auth;

/// <summary>
/// Telemetry naming conventions and <see cref="System.Diagnostics.Metrics"/>
/// instruments for <c>Orleans.Lattice.Auth</c>. Every authorization instrument
/// is published on a single <see cref="Meter"/> named <see cref="MeterName"/> so
/// an OpenTelemetry pipeline can subscribe once and receive every authorization
/// metric. Mirrors the structure of <c>Orleans.Lattice.LatticeMetrics</c> and
/// <c>Orleans.Lattice.Replication.LatticeReplicationMetrics</c>.
/// </summary>
/// <remarks>
/// <para>
/// The decision counters and the decision-latency histogram are recorded by the
/// enforcement gate <b>after</b> a decision is computed, so they never influence
/// the decision. Recording is guarded by each instrument's
/// <see cref="Instrument.Enabled"/> flag, so when no OpenTelemetry listener is
/// attached the gate builds no tag list and does no measurement work: the meter
/// is zero-cost on the hot path when nobody is listening.
/// </para>
/// <para>
/// The compiled-snapshot <c>epoch</c> and <c>age</c> gauges are
/// <see cref="ObservableGauge{T}"/> instruments backed by the live snapshot
/// maintainers (see <see cref="AuthSnapshotGaugeRegistry"/>); their measurement
/// callbacks run only on scrape.
/// </para>
/// </remarks>
public static class LatticeAuthMetrics
{
    /// <summary>
    /// The root meter name for all <c>Orleans.Lattice.Auth</c> telemetry.
    /// Internal telemetry hooks and external subscribers must reference this
    /// constant rather than hard-coding the string.
    /// </summary>
    public const string MeterName = "orleans.lattice.auth";

    /// <summary>Tag key for the authorized <see cref="LatticeOperation"/>.</summary>
    public const string TagOperation = "operation";

    /// <summary>Tag key for the target tree id.</summary>
    public const string TagTree = "tree";

    /// <summary>Tag key for the decided effect (<see cref="EffectAllow"/> / <see cref="EffectDeny"/>).</summary>
    public const string TagEffect = "effect";

    /// <summary><see cref="TagEffect"/> value for an allowed decision.</summary>
    public const string EffectAllow = "allow";

    /// <summary><see cref="TagEffect"/> value for a denied decision.</summary>
    public const string EffectDeny = "deny";

    /// <summary>Canonical name of the <see cref="Decisions"/> counter.</summary>
    public const string DecisionsName = "orleans.lattice.auth.decisions";

    /// <summary>Canonical name of the <see cref="DecisionDuration"/> histogram.</summary>
    public const string DecisionDurationName = "orleans.lattice.auth.decision.duration";

    /// <summary>Canonical name of the <see cref="SnapshotRebuilds"/> counter.</summary>
    public const string SnapshotRebuildsName = "orleans.lattice.auth.snapshot.rebuilds";

    /// <summary>Canonical name of the compiled-snapshot epoch observable gauge.</summary>
    public const string SnapshotEpochName = "orleans.lattice.auth.snapshot.epoch";

    /// <summary>Canonical name of the compiled-snapshot age observable gauge.</summary>
    public const string SnapshotAgeName = "orleans.lattice.auth.snapshot.age";

    /// <summary>Canonical name of the <see cref="SubjectResolutionCacheHits"/> counter.</summary>
    public const string SubjectResolutionCacheHitsName = "orleans.lattice.auth.subject_cache.hits";

    /// <summary>Canonical name of the <see cref="SubjectResolutionCacheMisses"/> counter.</summary>
    public const string SubjectResolutionCacheMissesName = "orleans.lattice.auth.subject_cache.misses";

    /// <summary>
    /// The meter that owns every authorization instrument. Exposed publicly so
    /// integration tests and custom OpenTelemetry exporters can subscribe by
    /// reference rather than by name.
    /// </summary>
    public static readonly Meter Meter = new(MeterName);

    /// <summary>
    /// Counter of authorization decisions the enforcement gate produced, tagged
    /// by <see cref="TagOperation"/>, <see cref="TagTree"/>, and
    /// <see cref="TagEffect"/>. Incremented once per gated (user-originated)
    /// decision - allow or deny - including bootstrap-admin bypasses and
    /// strict-consistency fence denials.
    /// </summary>
    public static readonly Counter<long> Decisions =
        Meter.CreateCounter<long>(DecisionsName, unit: "{decision}",
            description: "Authorization decisions produced by the enforcement gate, tagged by operation, tree and effect.");

    /// <summary>
    /// Histogram of enforcement-gate decision latency in milliseconds, tagged by
    /// <see cref="TagOperation"/>, <see cref="TagTree"/>, and
    /// <see cref="TagEffect"/>. Measured from gate entry to the returned decision.
    /// </summary>
    public static readonly Histogram<double> DecisionDuration =
        Meter.CreateHistogram<double>(DecisionDurationName, unit: "ms",
            description: "Enforcement-gate decision latency, tagged by operation, tree and effect.");

    /// <summary>
    /// Counter of compiled authorization snapshot rebuilds. Incremented once per
    /// successful rebuild of the in-memory decision snapshot from the policy tree.
    /// </summary>
    public static readonly Counter<long> SnapshotRebuilds =
        Meter.CreateCounter<long>(SnapshotRebuildsName, unit: "{rebuild}",
            description: "Compiled authorization policy snapshot rebuilds.");

    /// <summary>
    /// Counter of subject-resolution cache hits, recorded through
    /// <see cref="RecordSubjectResolutionCacheHit"/>.
    /// <para>
    /// <b>Reserved seam; not emitted in this version.</b> The instrument and its
    /// <see cref="RecordSubjectResolutionCacheHit"/> entry point are published so
    /// operators and custom exporters can bind to a stable name and so a future
    /// version can begin emitting without a surface change, but nothing in the
    /// shipped resolution pipeline calls it, so no value is currently reported.
    /// The resolution cache that would source the signal lives in the
    /// <c>Orleans.Lattice.Membership</c> package, which does not (and, to keep
    /// the layering acyclic, must not) reference this authorization meter;
    /// wiring it therefore awaits a dedicated cross-package instrumentation seam
    /// rather than a backward package dependency. Until then this counter reads
    /// zero.
    /// </para>
    /// </summary>
    public static readonly Counter<long> SubjectResolutionCacheHits =
        Meter.CreateCounter<long>(SubjectResolutionCacheHitsName, unit: "{lookup}",
            description: "Subject-resolution cache hits (reserved seam; not emitted in this version).");

    /// <summary>
    /// Counter of subject-resolution cache misses, recorded through
    /// <see cref="RecordSubjectResolutionCacheMiss"/>. Like
    /// <see cref="SubjectResolutionCacheHits"/> this is a <b>reserved seam not
    /// emitted in this version</b>: the entry point is published for forward
    /// compatibility but the shipped resolution pipeline never calls it, so the
    /// counter reads zero. See <see cref="SubjectResolutionCacheHits"/> for why
    /// the wiring awaits a cross-package instrumentation seam.
    /// </summary>
    public static readonly Counter<long> SubjectResolutionCacheMisses =
        Meter.CreateCounter<long>(SubjectResolutionCacheMissesName, unit: "{lookup}",
            description: "Subject-resolution cache misses (reserved seam; not emitted in this version).");

    /// <summary>
    /// Records a subject-resolution cache hit on
    /// <see cref="SubjectResolutionCacheHits"/>. Cheap no-op when no listener is
    /// attached. This is the published entry point of a <b>reserved seam</b>: it
    /// is not called by the shipped caller-identity resolution pipeline in this
    /// version (see <see cref="SubjectResolutionCacheHits"/>), so the hits
    /// counter reads zero until a future cross-package instrumentation seam
    /// begins reporting through it.
    /// </summary>
    public static void RecordSubjectResolutionCacheHit()
    {
        if (SubjectResolutionCacheHits.Enabled)
        {
            SubjectResolutionCacheHits.Add(1);
        }
    }

    /// <summary>
    /// Records a subject-resolution cache miss on
    /// <see cref="SubjectResolutionCacheMisses"/>. Cheap no-op when no listener
    /// is attached. Like <see cref="RecordSubjectResolutionCacheHit"/> this is
    /// the published entry point of a <b>reserved seam</b> that the shipped
    /// resolution pipeline does not call in this version, so the misses counter
    /// reads zero.
    /// </summary>
    public static void RecordSubjectResolutionCacheMiss()
    {
        if (SubjectResolutionCacheMisses.Enabled)
        {
            SubjectResolutionCacheMisses.Add(1);
        }
    }

    /// <summary>
    /// The <see cref="TagEffect"/> value for a decision: <see cref="EffectAllow"/>
    /// when allowed, otherwise <see cref="EffectDeny"/>.
    /// </summary>
    /// <param name="allowed">Whether the decision allowed the request.</param>
    /// <returns>The effect tag value.</returns>
    public static string EffectTag(bool allowed) => allowed ? EffectAllow : EffectDeny;
}
