namespace VehicleFleetSimulator.AzureThroughput.Engine;

/// <summary>
/// The slice of WAL-saturation state the shared ingest engine consults
/// on its shutdown path, abstracted so the engine can run in both the
/// in-silo host and a separate Orleans-client producer.
/// </summary>
/// <remarks>
/// <para>
/// The engine reads saturation in exactly two places, both on the
/// producer-stop boundary and both purely for accounting:
/// </para>
/// <list type="bullet">
/// <item><description>FX-029 - abandon the residual ingest-channel batch
/// rather than dispatch it into a storage account that is still
/// back-pressured, where it would trip <c>WalAppendDispatchTimeout</c>
/// 30 s later and surface as <c>failed=N</c> on FINAL.</description></item>
/// <item><description>FX-032 / FX-038 - quiesce the in-flight tail
/// before releasing it, so batches already dispatched get a chance to
/// settle against a recovered account.</description></item>
/// </list>
/// <para>
/// Both are guarded by a recency check, so on a tree that never
/// saturated <see cref="LastSaturatedUtc"/> returns <c>null</c>, both
/// guards are false, and the engine's behaviour is identical whichever
/// implementation is installed. The implementations diverge only in a
/// run that actually saturated.
/// </para>
/// <para>
/// <b>Why this is an abstraction rather than the real signal.</b>
/// <c>IWalSaturationSignal</c> is silo-scoped and in-process: it reports
/// what <i>this</i> silo observes about its own WAL writers. A client
/// process has no such signal, and in a multi-silo cluster the tree
/// spans every silo, so there is no single well-defined value for a
/// client to read. Polling one replica would yield a confident,
/// plausible, wrong answer. The client implementation therefore declines
/// to answer (see <see cref="NoOpBenchSaturationGate"/>) rather than
/// fabricating one, and saturation is instead reported out-of-band from
/// the silos' own <c>[silo:saturation]</c> log lines, which keep being
/// emitted on every replica because the observer that writes them still
/// runs in-silo.
/// </para>
/// </remarks>
internal interface IBenchSaturationGate
{
    /// <summary>
    /// Wall-clock UTC of the most recently observed transition into
    /// <c>Saturated</c> for <paramref name="treeId"/>, or <c>null</c>
    /// when no saturation has been observed (or when the host cannot
    /// observe saturation at all).
    /// </summary>
    /// <param name="treeId">Tree id whose saturation history to read.</param>
    DateTimeOffset? LastSaturatedUtc(string treeId);

    /// <summary>
    /// Completes when <paramref name="treeId"/> is observed to return to
    /// a healthy regime, or when <paramref name="cancellationToken"/>
    /// fires. A host that cannot observe saturation completes
    /// immediately.
    /// </summary>
    /// <param name="treeId">Tree id to wait on.</param>
    /// <param name="cancellationToken">Bounds the wait; the engine always
    /// supplies a budget so this cannot consume the shutdown window.</param>
    Task WaitForHealthyAsync(string treeId, CancellationToken cancellationToken = default);
}
