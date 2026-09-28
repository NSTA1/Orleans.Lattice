namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// Transport-independent data access for an untrusted app UI, addressed only by app
/// slug, install revision and app-local tree name.
/// </summary>
/// <remarks>
/// <para>
/// Implementations authorize each call against only the app-owned compiled rules for
/// the target slug that the caller matches, intersected with the bridge operations the
/// installed version was consented for, resolve the logical tree to its effective id
/// server-side, and then execute under the caller's own identity so ordinary
/// authorization also applies. The effective right is therefore the caller's app-role
/// grants intersected with the caller's own rights: an app UI can never reach a tree
/// its app does not own, nor exceed what its signed-in user may do.
/// </para>
/// <para>
/// There is deliberately no overload that accepts a physical tree id. A target whose
/// <see cref="AppBridgeTarget.InstallRevision"/> no longer matches the enabled install
/// is refused, so a frame launched before an upgrade or disable cannot keep operating.
/// Every failure is an <see cref="AppBridgeException"/> carrying a closed
/// <see cref="AppBridgeFailure"/> code and a sanitised message.
/// </para>
/// </remarks>
public interface ILatticeAppBridge
{
    /// <summary>Reads one key from an app-owned tree.</summary>
    /// <param name="target">The non-null app, install revision and app-local tree.</param>
    /// <param name="key">The non-null key to read.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The stored entry, or null when the key has no live value.</returns>
    /// <exception cref="AppBridgeException">The request was refused or could not be served.</exception>
    Task<AppBridgeValue?> GetAsync(AppBridgeTarget target, string key, CancellationToken cancellationToken = default);

    /// <summary>Reads one page of keys under a prefix of an app-owned tree, in ordinal key order.</summary>
    /// <param name="target">The non-null app, install revision and app-local tree.</param>
    /// <param name="prefix">The non-null key prefix; empty scans the whole tree.</param>
    /// <param name="pageSize">The requested page size; implementations clamp it to their bound.</param>
    /// <param name="continuation">The opaque continuation from the previous page, or null to start.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The page of entries and the continuation for the next page, null on the last page.</returns>
    /// <exception cref="AppBridgeException">The request was refused or could not be served.</exception>
    Task<AppBridgePage> ScanAsync(
        AppBridgeTarget target,
        string prefix,
        int pageSize,
        string? continuation = null,
        CancellationToken cancellationToken = default);

    /// <summary>Writes one key in an app-owned tree.</summary>
    /// <param name="target">The non-null app, install revision and app-local tree.</param>
    /// <param name="key">The non-null key to write.</param>
    /// <param name="value">The value bytes; implementations bound their size.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes once the write is durable.</returns>
    /// <exception cref="AppBridgeException">The request was refused or could not be served.</exception>
    Task SetAsync(
        AppBridgeTarget target,
        string key,
        ReadOnlyMemory<byte> value,
        CancellationToken cancellationToken = default);

    /// <summary>Deletes one key from an app-owned tree.</summary>
    /// <param name="target">The non-null app, install revision and app-local tree.</param>
    /// <param name="key">The non-null key to delete.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True when a live value was deleted; false when the key had none.</returns>
    /// <exception cref="AppBridgeException">The request was refused or could not be served.</exception>
    Task<bool> DeleteAsync(AppBridgeTarget target, string key, CancellationToken cancellationToken = default);
}
