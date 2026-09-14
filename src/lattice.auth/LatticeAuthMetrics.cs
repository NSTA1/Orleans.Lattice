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
    /// <remarks>
    /// Must stay above every instrument declared below it, and every instrument must be
    /// constructed from it. Static field initialisers execute in declaration order, so a
    /// listener matching <c>ReferenceEquals(instrument.Meter, LatticeAuthMetrics.Meter)</c>
    /// that is the first code in the process to touch this class would compare against
    /// <see langword="null"/> while an instrument declared higher up is published, never
    /// enable it, and silently record nothing. Enforced by
    /// <c>MeterFieldDeclarationOrderTests</c>; demonstrated by <c>MeterListeningTests</c>.
    /// See the Metrics section of <c>.github/copilot-instructions.md</c>.
    /// </remarks>
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
            description: "Authorization decisions produced by the enforcement gate, tagged by operation, tree and effect. Both effect arms are zero-primed the first time the gate decides a given operation/tree pair, so a flat zero on deny is a measured zero rather than an absent series.");

    /// <summary>
    /// The <see cref="TagEffect"/> tag for an allowed decision, prebuilt so that
    /// <see cref="PrimeDecisions"/> can pass it as a literal at the priming site.
    /// </summary>
    public static readonly KeyValuePair<string, object?> EffectAllowTag =
        new(TagEffect, EffectAllow);

    /// <summary>
    /// The <see cref="TagEffect"/> tag for a denied decision, prebuilt so that
    /// <see cref="PrimeDecisions"/> can pass it as a literal at the priming site.
    /// </summary>
    public static readonly KeyValuePair<string, object?> EffectDenyTag =
        new(TagEffect, EffectDeny);

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

    /// <summary>
    /// Zero-primes both <see cref="TagEffect"/> arms of <see cref="Decisions"/> for
    /// one operation/tree pair, at the moment the gate first decides that pair and
    /// so before either effect is known to occur for it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Priming both arms together is what makes the absence of a series meaningful.
    /// After this call, a flat zero on <see cref="EffectDeny"/> means the gate
    /// evaluated this operation/tree pair and denied nothing, whereas no series at
    /// all means the gate was never asked about that pair. Those are different facts
    /// and, on a security surface, they have opposite readings: the first says the
    /// policy allowed everything it was asked about, the second is equally
    /// consistent with a gate that never ran, was never registered, or is fail-open.
    /// Before this priming existed, only the second shape was ever emitted.
    /// </para>
    /// <para>
    /// <b>Boundary - this primes per pair, not at startup.</b> The tag set is
    /// {operation, tree, tenant, effect} and the operation/tree cross product is not
    /// known until requests arrive, so there is no startup-time set to prime without
    /// either manufacturing cardinality or priming a sentinel that corresponds to no
    /// real gate. The guarantee is therefore "from the first decision on a pair
    /// onward", not "from process start": a pair the gate has never been asked about
    /// still has no series, which remains correct - nothing measured it.
    /// </para>
    /// <para>
    /// <b><see cref="DecisionDuration"/> is deliberately NOT primed</b>, although it
    /// carries the identical tag set and has the identical absent-arm behaviour. A
    /// counter is primed by adding zero, which leaves the value untouched and is not
    /// an observation. Priming a histogram would mean recording a 0 ms sample, which
    /// is a real observation: it would report that one decision took zero
    /// milliseconds, moving count, sum, and every bucket boundary below the first.
    /// That corrupts the latency distribution it exists to measure, so the remedy for
    /// a counter is a defect when transplanted to a histogram. Its absent deny arm is
    /// a genuine and separate limitation, recorded rather than silently mis-fixed.
    /// </para>
    /// </remarks>
    /// <param name="operation">The resolved <see cref="TagOperation"/> tag value.</param>
    /// <param name="treeId">The target tree id, as the recording site tags it.</param>
    /// <exception cref="ArgumentNullException"><paramref name="operation"/> or <paramref name="treeId"/> is <see langword="null"/>.</exception>
    public static void PrimeDecisions(string operation, string treeId)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(treeId);

        var operationTag = new KeyValuePair<string, object?>(TagOperation, operation);
        var treeTag = new KeyValuePair<string, object?>(TagTree, treeId);
        var tenantTag = LatticeTenantLabel.ForTree(treeId);

        // Written as two literal calls rather than through a shared helper or a
        // TagList: the priming-enrolment gate resolves a prime by reading args[0] of
        // the Add call and each tag pair at the call site, and discards anything it
        // cannot resolve to a literal. A helper would prime correctly at run time and
        // present nothing to the gate, so the instrument would keep its unprimed
        // enrolment while appearing fixed - a failure whose symptom is a green build.
        Decisions.Add(0, operationTag, treeTag, tenantTag, EffectAllowTag);
        Decisions.Add(0, operationTag, treeTag, tenantTag, EffectDenyTag);
    }
}
