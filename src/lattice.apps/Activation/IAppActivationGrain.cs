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
    /// <summary>Runs <paramref name="operation"/> for the app and returns its outcome.</summary>
    Task<AppActivationOutcome> ExecuteAsync(
        AppActivationOperation operation,
        TenantId tenant,
        AppSlug slug,
        CancellationToken cancellationToken = default);
}
