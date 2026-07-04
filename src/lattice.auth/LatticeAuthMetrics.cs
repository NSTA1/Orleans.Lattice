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
/// The compiled-snapshot <c>epoch</c>, <c>age</c> and <c>subjects</c> gauges are
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

    /// <summary>
    /// Canonical name of the compiled-snapshot subjects observable gauge - the
    /// number of distinct members (users and groups) for which an authorization
    /// policy is configured.
    /// </summary>
    public const string SnapshotSubjectsName = "orleans.lattice.auth.snapshot.subjects";

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
    /// The <see cref="TagEffect"/> value for a decision: <see cref="EffectAllow"/>
    /// when allowed, otherwise <see cref="EffectDeny"/>.
    /// </summary>
    /// <param name="allowed">Whether the decision allowed the request.</param>
    /// <returns>The effect tag value.</returns>
    public static string EffectTag(bool allowed) => allowed ? EffectAllow : EffectDeny;
}
