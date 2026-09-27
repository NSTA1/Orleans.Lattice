namespace Orleans.Lattice.Apps;

/// <summary>
/// App-supplied handler for one manifest-declared change-feed subscription, registered per
/// (app slug, subscription name) with
/// <see cref="LatticeAppsServiceCollectionExtensions.AddLatticeAppSubscriptionHandler{THandler}(Microsoft.Extensions.DependencyInjection.IServiceCollection, AppSlug, string)"/>.
/// One handler instance serves every tenant that installs and enables the app;
/// <see cref="AppSubscriptionContext.Tenant"/> tells them apart.
/// </summary>
/// <remarks>
/// <para>
/// Delivery rides the core <see cref="IMutationObserver"/> hook, so <see cref="HandleAsync"/> runs
/// inline on the committing grain's write path with the same constraints: return quickly, do not
/// perform blocking or network I/O, and enqueue follow-on work for a background drain. A thrown
/// exception or faulted task is logged and suppressed; it never fails the write or another app's
/// delivery.
/// </para>
/// <para>
/// Only user writes are delivered; library maintenance writes
/// (<see cref="MutationCategory.Maintenance"/>) are not. Delivery is at-most-once and eventual: a
/// subscription starts receiving shortly after its app is enabled and stops once it is disabled or
/// uninstalled.
/// </para>
/// </remarks>
public interface IAppChangeFeedHandler
{
    /// <summary>Handles one committed mutation observed by <paramref name="subscription"/>.</summary>
    /// <param name="subscription">The activated subscription the mutation matched.</param>
    /// <param name="mutation">The committed mutation; treat as immutable.</param>
    /// <param name="cancellationToken">The write path's cancellation signal.</param>
    /// <returns>A task that completes when the handler has taken the change.</returns>
    Task HandleAsync(AppSubscriptionContext subscription, LatticeMutation mutation, CancellationToken cancellationToken);
}
