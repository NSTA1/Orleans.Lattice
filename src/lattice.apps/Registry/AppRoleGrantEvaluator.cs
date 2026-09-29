using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Runtime.InteropServices;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The shared app-role evaluation: which roles of an enabled install a caller holds, for the install recorded in
/// the current registry snapshot. A role is held by <em>binding</em>: the caller is a member of a membership group
/// the install binds to the role, and the role's app-owned <c>app:{slug}:</c> rules confer something (see
/// <see cref="AppRoleGate"/>). The app workspace, the app MCP tool surface and the app bridge all derive from the
/// roles compiled here, so the three surfaces can never disagree about who holds a role.
/// </summary>
/// <remarks>
/// <para>
/// <b>Only app-owned rules.</b> The evaluation never consults the caller's other rules, so broad rights a caller
/// holds of its own never make it hold an app role, exactly as the bridge never lets them flow into the app.
/// </para>
/// <para>
/// <b>Which installs count.</b> Only an <see cref="AppRegistryLifecycleState.Enabled"/> install whose
/// ceiling is pinned to its version, in the tenant asked about, is ever evaluated. Its manifest is resolved
/// from the source its provenance names (see <see cref="AppSourceResolution"/>), so a second source offering
/// the same slug cannot substitute the roles.
/// </para>
/// <para>
/// <b>Fail closed.</b> A missing projection or source, an install whose manifest does not resolve to exactly the
/// installed slug and version, or a source fault, evaluates to no install at all. A caller with no resolved
/// identity or group closure holds no role.
/// </para>
/// <para>
/// <b>Cost.</b> Each install's roles, with their bound groups, are compiled once per registry record revision and
/// cached, so a re-binding (which writes a new revision) moves the role on the next evaluation. A warm evaluation
/// resolves no manifest, consults no gate and allocates only the list of held role names.
/// </para>
/// </remarks>
internal sealed class AppRoleGrantEvaluator
{
    private readonly IAppRegistryProjection? _projection;
    private readonly IAppSource? _source;
    private readonly ConcurrentDictionary<(TenantId Tenant, AppSlug Slug), AppRoleGrantInstall> _installs = new();

    /// <summary>Initializes a new <see cref="AppRoleGrantEvaluator"/>.</summary>
    /// <param name="projection">The app registry projection, or null when none is registered.</param>
    /// <param name="source">The app source manifests resolve through, or null when none is registered.</param>
    public AppRoleGrantEvaluator(IAppRegistryProjection? projection, IAppSource? source)
    {
        _projection = projection;
        _source = source;
    }

    /// <summary>Whether every collaborator is present; when false, every evaluation reports no install.</summary>
    public bool CanServe => _projection is not null && _source is not null;

    /// <summary>
    /// Compiles the roles of <paramref name="manifest"/> for the install <paramref name="record"/>, in manifest
    /// order: each role's operations intersected with the install's ceiling, its scopes resolved for the install's
    /// tenant, and the groups the install binds to it.
    /// </summary>
    /// <param name="record">The install record.</param>
    /// <param name="manifest">The installed manifest.</param>
    /// <returns>One gate per declared role.</returns>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public static AppRoleGate[] CompileRoles(AppRegistryRecord record, AppManifest manifest)
    {
        ArgumentNullException.ThrowIfNull(record);
        ArgumentNullException.ThrowIfNull(manifest);
        var declared = manifest.Roles ?? [];
        var ceiling = (record.Ceiling?.AllowedOperations ?? LatticeOperation.None) & AppManifestValidator.RoleOperations;
        var roles = new AppRoleGate[declared.Length];
        for (var i = 0; i < roles.Length; i++)
        {
            var role = declared[i];
            roles[i] = role is null
                ? new AppRoleGate(LatticeOperation.None, [], [])
                : new AppRoleGate(
                    role.Operations & ceiling,
                    AppRoleScopeResolver.Resolve(record.Slug, role, manifest.Trees ?? [], record.Tenant),
                    BoundGroups(record, role.Name));
        }

        return roles;
    }

    /// <summary>Returns whether <paramref name="record"/> is an install the evaluation considers.</summary>
    /// <param name="record">The record.</param>
    /// <returns><c>true</c> for an enabled install whose ceiling is pinned to its version.</returns>
    public static bool IsEvaluated(AppRegistryRecord record) =>
        record is { State: AppRegistryLifecycleState.Enabled, IsCeilingPinnedToVersion: true };

    /// <summary>Returns the current registry snapshot, warming the projection when it is still cold.</summary>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns>The current snapshot, or <see cref="CompiledAppRegistrySnapshot.Empty"/> when no projection is registered.</returns>
    public async ValueTask<CompiledAppRegistrySnapshot> GetSnapshotAsync(CancellationToken cancellationToken)
    {
        if (_projection is null)
            return CompiledAppRegistrySnapshot.Empty;
        if (_projection.CurrentEpoch == 0)
            await _projection.EnsureWarmAsync(cancellationToken).ConfigureAwait(false);
        return _projection.Current;
    }

    /// <summary>
    /// Returns the compiled install for <paramref name="record"/>, resolving and compiling its manifest on
    /// the first call for the record's revision.
    /// </summary>
    /// <param name="record">The install record, taken from the current snapshot.</param>
    /// <param name="cancellationToken">Cancels the resolution.</param>
    /// <returns>The compiled install, or null when the record is not evaluated or its manifest does not resolve.</returns>
    public async ValueTask<AppRoleGrantInstall?> GetInstallAsync(AppRegistryRecord record, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(record);
        if (!CanServe || !IsEvaluated(record))
            return null;

        var key = (record.Tenant, record.Slug);
        if (_installs.TryGetValue(key, out var cached) && cached.Record.Revision == record.Revision && cached.Record.Version == record.Version)
            return cached;

        AppSourceResult resolved;
        try
        {
            resolved = await _source!.ResolveInstalledAsync(record, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }

        if (!resolved.IsResolved
            || resolved.Manifest is not { } manifest
            || manifest.Identity.Slug != record.Slug
            || manifest.Identity.Version != record.Version)
        {
            return null;
        }

        var install = new AppRoleGrantInstall(record, manifest, CompileRoles(record, manifest));
        _installs[key] = install;
        return install;
    }

    /// <summary>
    /// Evaluates which roles of the enabled install of <paramref name="slug"/> in <paramref name="tenant"/>
    /// <paramref name="subject"/> holds.
    /// </summary>
    /// <param name="tenant">The caller's active tenant.</param>
    /// <param name="slug">The app slug.</param>
    /// <param name="subject">The resolved caller.</param>
    /// <param name="cancellationToken">Cancels the evaluation.</param>
    /// <returns>The evaluation, or null when the tenant has no evaluated install of the app.</returns>
    public async ValueTask<AppRoleGrantEvaluation?> EvaluateAsync(
        TenantId tenant,
        AppSlug slug,
        LatticeSubject subject,
        CancellationToken cancellationToken)
    {
        if (!CanServe || tenant.Value is null || slug.Value is null)
            return null;

        var snapshot = await GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
        if (!snapshot.TryGet(tenant, slug, out var record) || record.Tenant != tenant)
            return null;

        var install = await GetInstallAsync(record, cancellationToken).ConfigureAwait(false);
        if (install is null)
            return null;

        return new AppRoleGrantEvaluation(install, Evaluate(install, subject));
    }

    /// <summary>Evaluates which of an install's roles <paramref name="subject"/> holds by binding.</summary>
    /// <param name="install">The compiled install.</param>
    /// <param name="subject">The resolved caller.</param>
    /// <returns>The names of the held roles, in manifest order; empty when none is held.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="install"/> is null.</exception>
    public static ImmutableArray<string> Evaluate(AppRoleGrantInstall install, in LatticeSubject subject)
    {
        ArgumentNullException.ThrowIfNull(install);
        var roles = install.Roles;
        var declared = install.Manifest.Roles ?? [];
        string[]? held = null;
        var count = 0;
        for (var i = 0; i < roles.Length && i < declared.Length; i++)
        {
            if (!roles[i].IsHeld(subject))
                continue;
            held ??= new string[roles.Length - i];
            held[count++] = declared[i].Name;
        }

        if (held is null)
            return [];
        if (count != held.Length)
            Array.Resize(ref held, count);
        return ImmutableCollectionsMarshal.AsImmutableArray(held);
    }

    /// <summary>The distinct, non-empty group ids <paramref name="record"/> binds to <paramref name="roleName"/>.</summary>
    private static string[] BoundGroups(AppRegistryRecord record, string? roleName)
    {
        if (string.IsNullOrEmpty(roleName) || record.RoleBindings is not { Count: > 0 } bindings)
            return [];

        List<string>? groups = null;
        foreach (var binding in bindings)
        {
            if (binding is null
                || string.IsNullOrEmpty(binding.GroupId)
                || !string.Equals(binding.RoleName, roleName, StringComparison.Ordinal)
                || (groups is not null && groups.Contains(binding.GroupId, StringComparer.Ordinal)))
            {
                continue;
            }

            (groups ??= []).Add(binding.GroupId);
        }

        return groups is null ? [] : groups.ToArray();
    }
}
