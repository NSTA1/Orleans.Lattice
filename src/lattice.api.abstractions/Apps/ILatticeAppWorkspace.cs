using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// Transport-independent, per-user view of the enabled apps in which the caller holds a
/// role in the active tenant, the sanitised description any role holder may see, and the
/// installed version's presentation and UI assets.
/// </summary>
/// <remarks>
/// Implementations gate every operation on the caller matching at least one app-owned
/// compiled rule of an enabled install in the active tenant, and fail closed on a
/// missing membership context or an unresolved tenant. A caller without a grant receives
/// exactly the result for an app that does not exist: no list entry, or null. Consent,
/// capability ceilings, approved exception scopes and role-to-group bindings are never
/// exposed here; they stay behind <see cref="LatticeOperation.AppInstall"/> through
/// <see cref="ILatticeAppsControl"/>.
/// </remarks>
public interface ILatticeAppWorkspace
{
    /// <summary>Lists the enabled apps in which the caller holds at least one role, in slug order.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The caller's apps; empty when the caller holds no app role.</returns>
    Task<ImmutableArray<WorkspaceAppSummary>> ListMyAppsAsync(CancellationToken cancellationToken = default);

    /// <summary>Describes one of the caller's apps with the sanitised, non-administrative projection.</summary>
    /// <param name="appSlug">The non-empty app slug.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The description, or null when the app does not exist or the caller holds no role in it.</returns>
    Task<WorkspaceAppDescriptor?> DescribeMyAppAsync(string appSlug, CancellationToken cancellationToken = default);

    /// <summary>Reads the installed version's presentation icon, verified against its manifest digest.</summary>
    /// <param name="appSlug">The non-empty app slug.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The verified icon, or null when absent, undeclared, unverifiable or not granted.</returns>
    Task<AppIconAsset?> GetIconAsync(string appSlug, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads one UI bundle asset of the <b>installed</b> version, verified against its
    /// manifest digest. The caller cannot choose a version, so a UI is never served from a
    /// version the tenant has not consented to.
    /// </summary>
    /// <param name="appSlug">The non-empty app slug.</param>
    /// <param name="path">The non-empty, normalised bundle-relative asset path.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The verified asset, or null when absent, undeclared, unverifiable or not granted.</returns>
    Task<AppUiAsset?> GetUiAssetAsync(string appSlug, string path, CancellationToken cancellationToken = default);
}
