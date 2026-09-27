namespace Orleans.Lattice.Apps;

/// <summary>
/// Durable storage for the <see cref="AppActivationStatus"/> recorded against each tenant app.
/// </summary>
internal interface IAppActivationStatusStore
{
    /// <summary>Reads the status recorded for an app, or <c>null</c> when none was recorded.</summary>
    Task<AppActivationStatus?> GetAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken);

    /// <summary>Records (replaces) the status for the app it names.</summary>
    Task SetAsync(AppActivationStatus status, CancellationToken cancellationToken);
}
