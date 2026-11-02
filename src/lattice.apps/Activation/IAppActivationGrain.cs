using Orleans.Concurrency;

namespace Orleans.Lattice.Apps;

/// <summary>
/// Serializes activation runs for one tenant app cluster-wide. Keyed <c>{tenant}/{slug}</c>
/// (the registry key); the grain is non-reentrant, so concurrent Enable / Disable / Uninstall /
/// Reconcile runs for the same app - including every silo's startup reconcile - execute one at
/// a time and each diffs against the rules its predecessor left.
/// </summary>
[Alias(AppActivationTypeAliases.IAppActivationGrain)]
internal interface IAppActivationGrain : IGrainWithStringKey
{
    /// <summary>
    /// The response timeout of <see cref="ExecuteAsync"/>, in <see cref="TimeSpan.Parse(string)"/>
    /// syntax. A run is a sequence of grain calls - rule withdrawal, replication enrolment, a
    /// soft-delete of every owned tree, the registry transition and the recorded outcome - and
    /// each of those carries the default response timeout and maps its own failure to a recorded
    /// <see cref="AppActivationOutcome"/>. So the run is already bounded by its steps; this bound
    /// only has to outlast their sum. Under the default (30 s) a slow but progressing run on a
    /// loaded estate was abandoned part-way, its outcome never recorded, and the caller saw an
    /// opaque timeout (issue #5482).
    /// </summary>
    internal const string RunResponseTimeout = "00:10:00";

    /// <summary>Runs <paramref name="operation"/> for the app and returns its outcome.</summary>
    [ResponseTimeout(RunResponseTimeout)]
    Task<AppActivationOutcome> ExecuteAsync(
        AppActivationOperation operation,
        TenantId tenant,
        AppSlug slug,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// The operation of the run executing on this activation right now, or <c>null</c> when none is.
    /// Interleaves with a run in flight, and is held only in memory, so a run whose silo died is
    /// never reported as still running (issue #5482).
    /// </summary>
    [AlwaysInterleave]
    Task<AppActivationOperation?> GetRunningOperationAsync();
}
