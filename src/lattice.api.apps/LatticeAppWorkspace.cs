using System.Collections.Immutable;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// The in-process implementation of <see cref="ILatticeAppWorkspace"/>: the per-user view of the enabled apps in
/// which the caller holds a role in the active tenant, with the sanitised description, icon and installed UI
/// bundle assets any role holder may read.
/// </summary>
/// <remarks>
/// <para>
/// <b>Gate.</b> Every verb evaluates the caller through the shared <see cref="AppRoleGrantEvaluator"/> - the
/// evaluation that gates the app MCP tools and that the app bridge derives from - against the install recorded
/// in the current registry snapshot, and serves nothing unless the caller holds at least one role of that
/// enabled install. A role is held by binding (membership of a group bound to it), never by rights the caller
/// holds outside the app's own rules, so the roles reported here are exactly the roles the bridge honours. The
/// evaluation happens before any source is read.
/// </para>
/// <para>
/// <b>Fail closed, and a denial looks like absence.</b> A missing membership context, a missing collaborator, a
/// refused or unresolved tenant, a disabled or foreign-tenant install and a caller without a role all produce
/// exactly what an app that does not exist produces: no list entry, or null.
/// </para>
/// <para>
/// <b>Installed version only.</b> Descriptions and assets always come from the installed version, resolved and
/// opened through the source its provenance names, with every asset verified against its manifest digest.
/// </para>
/// </remarks>
internal sealed class LatticeAppWorkspace : ILatticeAppWorkspace
{
    private readonly AppRoleGrantEvaluator _evaluator;
    private readonly AppSourceSet? _sources;
    private readonly ITenantContextResolver? _tenants;
    private readonly ILatticeMembershipContext? _membership;
    private readonly IAppActivationPipeline? _pipeline;
    private readonly ILogger _logger;

    /// <summary>Initializes a new <see cref="LatticeAppWorkspace"/>.</summary>
    /// <param name="evaluator">The shared app-role evaluation.</param>
    /// <param name="sources">The composed app sources assets are opened through, or null.</param>
    /// <param name="tenants">The active-tenant resolver, or null (every verb then fails closed).</param>
    /// <param name="membership">The membership context resolving the caller, or null (every verb then fails closed).</param>
    /// <param name="pipeline">The activation pipeline whose recorded status marks a failed install, or null.</param>
    /// <param name="logger">The logger digest failures are reported to, or null.</param>
    /// <exception cref="ArgumentNullException"><paramref name="evaluator"/> is null.</exception>
    public LatticeAppWorkspace(
        AppRoleGrantEvaluator evaluator,
        AppSourceSet? sources,
        ITenantContextResolver? tenants,
        ILatticeMembershipContext? membership,
        IAppActivationPipeline? pipeline = null,
        ILogger<LatticeAppWorkspace>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(evaluator);
        _evaluator = evaluator;
        _sources = sources;
        _tenants = tenants;
        _membership = membership;
        _pipeline = pipeline;
        _logger = logger ?? NullLogger<LatticeAppWorkspace>.Instance;
    }

    /// <inheritdoc />
    public async Task<ImmutableArray<WorkspaceAppSummary>> ListMyAppsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            if (await ResolveCallerAsync(cancellationToken).ConfigureAwait(false) is not { } caller)
            {
                return [];
            }

            var snapshot = await _evaluator.GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
            var enabled = snapshot.GetEnabledTenantApps(caller.Tenant);
            ImmutableArray<WorkspaceAppSummary>.Builder? apps = null;
            foreach (var record in enabled)
            {
                if (record.Tenant != caller.Tenant
                    || await _evaluator.GetInstallAsync(record, cancellationToken).ConfigureAwait(false) is not { } install)
                {
                    continue;
                }

                var roles = AppRoleGrantEvaluator.Evaluate(install, caller.Subject);
                if (roles.IsEmpty)
                {
                    continue;
                }

                (apps ??= ImmutableArray.CreateBuilder<WorkspaceAppSummary>()).Add(new WorkspaceAppSummary
                {
                    Slug = record.Slug.Value,
                    Version = record.Version.Value,
                    InstallRevision = record.Revision,
                    Presentation = AppsPresentationMapping.ToWirePresentation(install.Manifest.Presentation),
                    HasUi = install.Manifest.Ui is not null,
                    Roles = roles,
                });
            }

            return apps is null ? [] : apps.ToImmutable();
        }
        catch (Exception ex) when (AppsControlExceptionSanitizer.TryRewrite(ex, ownApp: null, out var sanitized))
        {
            throw sanitized;
        }
    }

    /// <inheritdoc />
    public async Task<WorkspaceAppDescriptor?> DescribeMyAppAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        try
        {
            var slug = AppsControlMapping.ParseSlug(appSlug, nameof(appSlug));
            if (await EvaluateAsync(slug, cancellationToken).ConfigureAwait(false) is not { } granted)
            {
                return null;
            }

            var record = granted.Install.Record;
            var status = _pipeline is null
                ? null
                : await _pipeline.GetStatusAsync(record.Tenant, slug, cancellationToken).ConfigureAwait(false);
            return AppsWorkspaceMapping.ToDescriptor(granted, AppsControlMapping.ToWireState(record.State, status));
        }
        catch (Exception ex) when (AppsControlExceptionSanitizer.TryRewrite(ex, appSlug, out var sanitized))
        {
            throw sanitized;
        }
    }

    /// <inheritdoc />
    public async Task<AppIconAsset?> GetIconAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        try
        {
            var slug = AppsControlMapping.ParseSlug(appSlug, nameof(appSlug));
            if (await EvaluateAsync(slug, cancellationToken).ConfigureAwait(false) is not { } granted
                || granted.Install.Manifest.Presentation?.Icon is not { } icon
                || await OpenAsync(granted.Install.Record, icon.Path, icon.Digest, isEntry: false, cancellationToken).ConfigureAwait(false) is not { } asset)
            {
                return null;
            }

            return new AppIconAsset { Bytes = asset.Bytes, MediaType = asset.MediaType, Sha256 = icon.Digest };
        }
        catch (Exception ex) when (AppsControlExceptionSanitizer.TryRewrite(ex, appSlug, out var sanitized))
        {
            throw sanitized;
        }
    }

    /// <inheritdoc />
    public async Task<AppUiAsset?> GetUiAssetAsync(string appSlug, string path, CancellationToken cancellationToken = default)
    {
        try
        {
            var slug = AppsControlMapping.ParseSlug(appSlug, nameof(appSlug));
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("An asset path is required.", nameof(path));
            }

            if (await EvaluateAsync(slug, cancellationToken).ConfigureAwait(false) is not { } granted
                || granted.Install.Manifest.Ui is not { } ui
                || FindAsset(ui, path) is not { } declared)
            {
                return null;
            }

            var isEntry = string.Equals(ui.Entry, declared.Path, StringComparison.Ordinal);
            if (await OpenAsync(granted.Install.Record, declared.Path, declared.Digest, isEntry, cancellationToken).ConfigureAwait(false) is not { } asset)
            {
                return null;
            }

            return new AppUiAsset
            {
                Path = declared.Path,
                Bytes = asset.Bytes,
                MediaType = declared.MediaType,
                Sha256 = declared.Digest,
            };
        }
        catch (Exception ex) when (AppsControlExceptionSanitizer.TryRewrite(ex, appSlug, out var sanitized))
        {
            throw sanitized;
        }
    }

    private static Orleans.Lattice.Apps.AppUiAsset? FindAsset(AppUiDeclaration ui, string path)
    {
        foreach (var asset in ui.Assets ?? [])
        {
            if (asset is not null && string.Equals(asset.Path, path, StringComparison.Ordinal))
            {
                return asset;
            }
        }

        return null;
    }

    private async ValueTask<AppsAssetReader.VerifiedAsset?> OpenAsync(
        AppRegistryRecord record,
        string path,
        string digest,
        bool isEntry,
        CancellationToken cancellationToken)
    {
        if (_sources is null || !_sources.TryGet(record.Provenance.Source, out var source))
        {
            return null;
        }

        return await AppsAssetReader.OpenAsync(source, record.Slug, record.Version, path, digest, _logger, cancellationToken, isEntry)
            .ConfigureAwait(false);
    }

    /// <summary>Evaluates the caller against one app; null unless the caller holds a role of its enabled install.</summary>
    private async ValueTask<AppRoleGrantEvaluation?> EvaluateAsync(AppSlug slug, CancellationToken cancellationToken)
    {
        if (await ResolveCallerAsync(cancellationToken).ConfigureAwait(false) is not { } caller)
        {
            return null;
        }

        var evaluation = await _evaluator.EvaluateAsync(caller.Tenant, slug, caller.Subject, cancellationToken).ConfigureAwait(false);
        return evaluation is { HasGrant: true } ? evaluation : null;
    }

    /// <summary>
    /// Resolves the caller's tenant and subject, or null when any part of the chain is missing or refuses:
    /// no membership context (or only the anonymous null context), no tenant resolver, an evaluator that cannot serve,
    /// or a denied tenant.
    /// </summary>
    private async ValueTask<Caller?> ResolveCallerAsync(CancellationToken cancellationToken)
    {
        if (_membership is null or NullLatticeMembershipContext || _tenants is null || !_evaluator.CanServe)
        {
            return null;
        }

        TenantId tenant;
        try
        {
            tenant = await AppsFacadeAccess.ResolveTenantAsync(_tenants, cancellationToken).ConfigureAwait(false);
        }
        catch (LatticeTenantAccessDeniedException)
        {
            return null;
        }

        var subject = await LatticeAccessGateSubjectResolver.ResolveAsync(_membership, cancellationToken).ConfigureAwait(false);
        return new Caller(tenant, subject);
    }

    private readonly record struct Caller(TenantId Tenant, LatticeSubject Subject);
}
