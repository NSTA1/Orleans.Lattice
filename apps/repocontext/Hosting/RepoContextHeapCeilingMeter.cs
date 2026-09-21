using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Publishes the collector's own memory ceiling and the commitment measured against
/// it, so adherence to the heap limit is a number this container reports rather than
/// an inference drawn from outside it.
/// </summary>
/// <remarks>
/// <para>
/// Issue #2543, and the gap it leaves once the BCL's <c>System.Runtime</c> meter is
/// subscribed. That meter carries what the process is <i>using</i> - working set,
/// last-collection heap size, committed size, allocation total, collections by
/// generation - and carries no ceiling at all. A usage figure without the limit it
/// is measured against cannot answer the question this reliability epic keeps
/// asking: two merged fixes (#2765, #2767) claim heap-ceiling adherence as their
/// primary effect, and neither was measurable from inside the container, because
/// the ceiling itself was never exported.
/// </para>
/// <para>
/// <b>The ceiling is read from the runtime, never configured here.</b>
/// <see cref="GCMemoryInfo.TotalAvailableMemoryBytes"/> is the memory the collector
/// itself believes it may use: a container memory limit, a configured GC heap hard
/// limit, or physical memory, whichever actually binds, resolved by the runtime at
/// the moment of the read. Nothing in this type knows a byte count, a core count or
/// a fraction, so it reports the truth on a 12 GiB container and on a 55 GiB
/// developer machine without being told which it is on. That is deliberate: the
/// resource knobs on this image have already been found to be transcriptions of one
/// machine (issue #2779), and a hard-coded ceiling here would be another one, with
/// the added defect that it would look like a measurement.
/// </para>
/// <para>
/// <b>Committed bytes, not heap size, is what the ceiling bounds.</b> A GC hard
/// limit is enforced against committed memory, so
/// <see cref="GCMemoryInfo.TotalCommittedBytes"/> over
/// <see cref="GCMemoryInfo.TotalAvailableMemoryBytes"/> is the adherence ratio
/// directly, and the two are published as separate series rather than as a
/// pre-divided one so a query can read either alone and neither can drift from the
/// other.
/// </para>
/// <para>
/// <b>The high-load threshold does NOT sit between them, and issue #3133 is what
/// that mistake cost.</b> This type previously claimed - in these remarks and in the
/// threshold gauge's own exported description - that the threshold sits below the
/// limit, as an early-warning point crossed before the ceiling is reached. It is
/// false by construction, because the runtime computes the two against
/// <i>different denominators</i>:
/// <list type="bullet">
/// <item><description>
/// <see cref="GCMemoryInfo.HighMemoryLoadThresholdBytes"/> is a fraction (90% by
/// default) of the <i>total physical or cgroup memory</i> the runtime can see.
/// </description></item>
/// <item><description>
/// <see cref="GCMemoryInfo.TotalAvailableMemoryBytes"/> is the <i>GC hard limit</i>,
/// which in a container defaults to 75% of that same cgroup limit.
/// </description></item>
/// </list>
/// So the promised ordering holds only where the hard-limit percentage exceeds 90.
/// At the container default of 75 the threshold sits <i>above</i> the limit - on the
/// 12 GiB deployment, 10.80 GiB against 9.00 GiB - and the hard limit therefore binds
/// first. The threshold can never be crossed: the process OOMs with the signal
/// reading "not under pressure", which is precisely backwards, and any alert written
/// against the crossing is silently dead rather than merely quiet.
/// </para>
/// <para>
/// <b>The relationship is published, not asserted.</b> Because the ordering depends
/// on a runtime configuration this type cannot see, stating it in prose can only ever
/// be a guess that is right on some deployments. <see cref="ReachableGaugeName"/>
/// derives the comparison at scrape time from the same reading, so an operator reads
/// whether the signal can fire instead of recomputing two percentages against a
/// cgroup limit to discover that it cannot. Note this makes the honest claim on a
/// developer machine too, where no hard limit is configured, the two denominators
/// coincide, and the threshold genuinely does sit below the limit - which is exactly
/// why a test asserting the ordering passed locally for as long as it did.
/// </para>
/// <para>
/// <b>Zero semantics, stated because two of these read zero for different reasons.</b>
/// All three are observable gauges, sampled at scrape time and published from process
/// start, so an <i>absent</i> series means the host did not construct this meter or
/// the collector refused it at a ceiling - never that memory is unbounded. The limit
/// and the threshold are populated by the runtime before any collection has run, so
/// a zero on either is a fault to investigate rather than a reading. Committed bytes
/// is carried by the last collection's figures, so it genuinely is zero until the
/// first collection: read it against
/// <c>lattice_repocontext_gc_collections_total</c>, exactly as the pause total is
/// read, and a zero beside a rising collection count is the only form that means the
/// process has committed nothing.
/// </para>
/// <para>
/// <b>No tenant dimension.</b> A heap ceiling is a property of the host process and
/// belongs to no tenant's traffic. As with
/// <see cref="RepoContextGarbageCollectionMeter"/>, the
/// <c>PlatformSentinelInstruments</c> registry does not apply: it and
/// <c>MetricEmissionScanner</c> enumerate <c>src/</c> only and this type lives under
/// <c>apps/</c>, so there is no entry to add and adding one would claim coverage the
/// scan does not perform.
/// </para>
/// </remarks>
public sealed class RepoContextHeapCeilingMeter : IDisposable
{
    /// <summary>
    /// The memory the garbage collector believes it may use, in bytes: the binding
    /// container limit, configured hard limit, or physical memory, as the runtime
    /// resolves it.
    /// </summary>
    public const string LimitBytesGaugeName = "lattice_repocontext_heap_limit_bytes";

    /// <summary>
    /// Memory committed by the garbage collector as of the last collection, in bytes.
    /// This is the quantity a heap hard limit is enforced against, so it is the
    /// numerator of ceiling adherence.
    /// </summary>
    public const string CommittedBytesGaugeName = "lattice_repocontext_heap_committed_bytes";

    /// <summary>
    /// The commitment at which the garbage collector begins treating memory as under
    /// pressure and changes its own behaviour, in bytes.
    /// </summary>
    public const string HighLoadThresholdBytesGaugeName =
        "lattice_repocontext_heap_high_load_threshold_bytes";

    /// <summary>
    /// Whether the high-load threshold is reachable before the heap limit binds:
    /// <c>1</c> when it sits at or below <see cref="LimitBytesGaugeName"/> and can
    /// therefore fire, <c>0</c> when it sits above and is dead.
    /// </summary>
    /// <remarks>
    /// Published because the ordering is a property of the deployment's GC
    /// configuration and not of this code, so it can only be measured, never
    /// asserted. See the remarks on this type and issue #3133.
    /// </remarks>
    public const string ReachableGaugeName =
        "lattice_repocontext_heap_high_load_threshold_reachable";

    // Declared above the instruments it constructs, and all three are built from this
    // field, so reordering throws at initialisation rather than publishing an
    // instrument against a null meter. See the metrics conventions in
    // .github/copilot-instructions.md.
    private readonly Meter _meter;

    private readonly Func<RepoContextHeapCeiling> _read;

    /// <summary>
    /// Creates the meter and publishes all three instruments.
    /// </summary>
    /// <param name="read">
    /// Reads the current ceiling figures. Defaults to
    /// <see cref="GC.GetGCMemoryInfo(GCKind)"/>; a test substitutes it.
    /// </param>
    public RepoContextHeapCeilingMeter(Func<RepoContextHeapCeiling>? read = null)
    {
        _read = read ?? ReadFromRuntime;

        _meter = new Meter(RepoContextHostMeter.Name);
        _meter.CreateObservableGauge(
            LimitBytesGaugeName,
            () => (double)_read().LimitBytes,
            unit: "By",
            description:
                "Memory the garbage collector believes it may use, read from the runtime at scrape time "
                + "so it reflects whichever ceiling actually binds - a container memory limit, a "
                + "configured heap hard limit, or physical memory. Divide "
                + CommittedBytesGaugeName
                + " by this to read heap-ceiling adherence directly. It is populated before any "
                + "collection has run, so a zero here is a fault to investigate and never a measurement.");
        _meter.CreateObservableGauge(
            CommittedBytesGaugeName,
            () => (double)_read().CommittedBytes,
            unit: "By",
            description:
                "Memory committed by the garbage collector as of its last collection. This is the "
                + "quantity a heap hard limit is enforced against, so it is the numerator of ceiling "
                + "adherence against "
                + LimitBytesGaugeName
                + ". It is carried by the last collection's figures, so read a zero against "
                + "lattice_repocontext_gc_collections_total: zero beside a rising collection count is a "
                + "measured absence of commitment, and both at zero means no collection has happened yet.");
        _meter.CreateObservableGauge(
            HighLoadThresholdBytesGaugeName,
            () => (double)_read().HighLoadThresholdBytes,
            unit: "By",
            description:
                "Commitment at which the garbage collector begins treating memory as under pressure and "
                + "changes its own behaviour. It is NOT an early-warning point below "
                + LimitBytesGaugeName
                + ": the runtime computes the two against different denominators - this is a fraction "
                + "(90% by default) of total physical or cgroup memory, while the limit is the GC hard "
                + "limit, which in a container defaults to 75% of that same figure - so it frequently "
                + "sits ABOVE the limit, and where it does the limit binds first and this threshold can "
                + "never be crossed. Read "
                + ReachableGaugeName
                + " to learn which case a deployment is in rather than recomputing the percentages. "
                + "Like the limit, it is populated before any collection has run.");
        _meter.CreateObservableGauge(
            ReachableGaugeName,
            () =>
            {
                // One reading for both sides, so the comparison can never be drawn
                // across two samples taken either side of a collection.
                var ceiling = _read();
                return ceiling.HighLoadThresholdBytes <= ceiling.LimitBytes ? 1d : 0d;
            },
            description:
                "Whether the pressure threshold can fire before the heap limit binds: 1 when "
                + HighLoadThresholdBytesGaugeName
                + " sits at or below "
                + LimitBytesGaugeName
                + ", 0 when it sits above and is therefore unreachable. A 0 means any alert on the "
                + "threshold crossing is dead - it returns no data rather than erroring - and that the "
                + "process will reach its hard limit with the pressure signal still reading as uncrossed. "
                + "Derived at scrape time from a single reading rather than asserted here, because the "
                + "ordering depends on the deployment's GC hard-limit percentage, which this process "
                + "cannot know in advance.");
    }

    /// <inheritdoc />
    public void Dispose() => _meter.Dispose();

    private static RepoContextHeapCeiling ReadFromRuntime()
    {
        var info = GC.GetGCMemoryInfo();
        return new RepoContextHeapCeiling(
            info.TotalAvailableMemoryBytes,
            info.TotalCommittedBytes,
            info.HighMemoryLoadThresholdBytes);
    }
}
