using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// Transport-independent, administrative view of the app sources a cluster offers and
/// the apps each one makes available, before anything is installed.
/// </summary>
/// <remarks>
/// Except for the advisory capability probe, implementations authorize every operation
/// with <see cref="LatticeOperation.AppInstall"/> over the cluster-wide scope before
/// touching any source or the registry, exactly as <see cref="ILatticeAppsControl"/>
/// does, so a caller without that authority learns nothing about what exists. Sources
/// are cluster-wide; installed version and lifecycle state are joined against the
/// caller's active tenant. Describing or reading an icon never loads or activates app
/// code, and no response carries a composed physical tree id.
/// </remarks>
public interface ILatticeAppCatalog
{
    /// <summary>Lists every configured app source in its registration order.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>One summary per configured source; empty when none is configured.</returns>
    Task<ImmutableArray<AppSourceSummary>> ListSourcesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists one page of the apps the selected sources offer, merged deterministically by
    /// slug and then source key, each joined with its installation in the active tenant.
    /// </summary>
    /// <param name="query">The non-null source selection, filters, page size and continuation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The page of available apps and an opaque continuation, null on the last page.</returns>
    Task<AvailableAppPage> ListAvailableAsync(AvailableAppQuery query, CancellationToken cancellationToken = default);

    /// <summary>
    /// Describes an app version as the named source offers it, without activation, for
    /// consent review before install. The descriptor's installation members reflect the
    /// active tenant.
    /// </summary>
    /// <param name="sourceKey">The non-empty key of the source to describe from.</param>
    /// <param name="appSlug">The non-empty app slug.</param>
    /// <param name="version">An exact source version, or null for the source's newest version.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The pre-install description, or null when the source, app or version is unknown.</returns>
    Task<AppDescriptor?> DescribeFromSourceAsync(
        string sourceKey,
        string appSlug,
        string? version = null,
        CancellationToken cancellationToken = default);

    /// <summary>Reads an app version's declared presentation icon from the named source, verified against its manifest digest.</summary>
    /// <param name="sourceKey">The non-empty key of the source to read from.</param>
    /// <param name="appSlug">The non-empty app slug.</param>
    /// <param name="version">An exact source version, or null for the source's newest version.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The verified icon, or null when unknown, undeclared or failing digest verification.</returns>
    Task<AppIconAsset?> GetIconAsync(
        string sourceKey,
        string appSlug,
        string? version = null,
        CancellationToken cancellationToken = default);

    /// <summary>Probes caller access without touching any source or the registry; all permissions default to denied.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Advisory permissions; every actual operation still authorizes independently.</returns>
    Task<LatticeAppCatalogCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default);
}
