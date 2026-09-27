namespace Orleans.Lattice.Apps;

/// <summary>
/// A deferred handle to the code of a resolved app. Obtaining the handle performs no activation;
/// code becomes available only when <see cref="ActivateAsync"/> is invoked, after consent.
/// </summary>
public interface IAppActivationHandle
{
    /// <summary>The identity of the manifest this handle activates.</summary>
    AppIdentity Identity { get; }

    /// <summary>
    /// Makes the app's code available. Idempotent: repeated calls report the same outcome.
    /// Never throws for an activation failure; failures are returned as <see cref="AppActivationResult"/>.
    /// </summary>
    /// <param name="cancellationToken">Cancels a source that performs asynchronous work.</param>
    ValueTask<AppActivationResult> ActivateAsync(CancellationToken cancellationToken = default);
}
