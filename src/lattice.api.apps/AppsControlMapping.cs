using System.Collections.Immutable;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// Validates caller-supplied app-control wire values and maps them to and from
/// the apps engine types. Every rejection is an <see cref="ArgumentException"/>
/// raised before any mutation, whose message names only slugs, indices and
/// app-local names.
/// </summary>
internal static class AppsControlMapping
{
    private const int MaxTreeNameLength = 128;

    /// <summary>Parses a caller-supplied app slug.</summary>
    /// <param name="value">The slug text.</param>
    /// <param name="parameterName">The parameter the slug came from.</param>
    /// <returns>The parsed slug.</returns>
    /// <exception cref="ArgumentException">The slug is missing or malformed.</exception>
    public static AppSlug ParseSlug(string? value, string parameterName)
    {
        if (!AppSlug.TryParse(value, out var slug))
        {
            throw new ArgumentException(
                "An app slug must be 2-31 lowercase ASCII letters, digits or hyphens, starting with a letter.",
                parameterName);
        }

        return slug;
    }

    /// <summary>Parses a caller-supplied semantic version.</summary>
    /// <param name="value">The version text.</param>
    /// <param name="parameterName">The parameter the version came from.</param>
    /// <returns>The parsed version.</returns>
    /// <exception cref="ArgumentException">The version is missing or malformed.</exception>
    public static AppVersion ParseVersion(string? value, string parameterName)
    {
        if (!AppVersion.TryParse(value, out var version))
        {
            throw new ArgumentException("An app version must be a semantic version.", parameterName);
        }

        return version;
    }

    /// <summary>
    /// Validates role bindings: each non-null, naming a non-empty role and group,
    /// and no role bound twice.
    /// </summary>
    /// <param name="bindings">The wire bindings; a default array is treated as empty.</param>
    /// <returns>The engine bindings.</returns>
    /// <exception cref="ArgumentException">A binding is invalid or duplicated.</exception>
    public static AppRoleBinding[] ToEngineBindings(ImmutableArray<AppRoleBindingDescriptor> bindings)
    {
        if (bindings.IsDefaultOrEmpty)
        {
            return [];
        }

        var result = new AppRoleBinding[bindings.Length];
        for (var i = 0; i < result.Length; i++)
        {
            var binding = bindings[i];
            if (binding is null || string.IsNullOrEmpty(binding.RoleName) || string.IsNullOrEmpty(binding.GroupId))
            {
                throw new ArgumentException(
                    $"Role binding {i} must name a non-empty role and membership group.", "request");
            }

            for (var j = 0; j < i; j++)
            {
                if (string.Equals(result[j].RoleName, binding.RoleName, StringComparison.Ordinal))
                {
                    throw new ArgumentException(
                        $"Role binding {i} binds a role that is already bound; each role binds to exactly one group.",
                        "request");
                }
            }

            result[i] = new AppRoleBinding { RoleName = binding.RoleName, GroupId = binding.GroupId };
        }

        return result;
    }

    /// <summary>
    /// Validates a wire ceiling and converts it to the engine ceiling. Each
    /// approved exception scope is validated, composed under
    /// <paramref name="tenant"/> and checked against the reserved namespaces at
    /// entry, then stored in its tenant-local (pre-composition) form, which is the
    /// form the role compiler compares exceptions in.
    /// </summary>
    /// <param name="ceiling">The wire ceiling.</param>
    /// <param name="tenant">The caller's active tenant.</param>
    /// <returns>The engine ceiling.</returns>
    /// <exception cref="ArgumentException">The ceiling or one of its scopes is invalid.</exception>
    public static AppCapabilityCeiling ToEngineCeiling(AppCapabilityCeilingDescriptor? ceiling, TenantId tenant)
    {
        if (ceiling is null)
        {
            throw new ArgumentException("A capability ceiling is required.", "request");
        }

        var scopes = ceiling.ApprovedExceptionScopes;
        if (scopes.IsDefaultOrEmpty)
        {
            return new AppCapabilityCeiling { AllowedOperations = ceiling.AllowedOperations };
        }

        var engineScopes = new LatticeScope[scopes.Length];
        for (var i = 0; i < engineScopes.Length; i++)
        {
            engineScopes[i] = ToEngineScope(scopes[i], tenant, i);
        }

        return new AppCapabilityCeiling
        {
            AllowedOperations = ceiling.AllowedOperations,
            ApprovedExceptionScopes = engineScopes,
        };
    }

    /// <summary>Maps an engine ceiling to its wire form, echoing app-local names only.</summary>
    /// <param name="ceiling">The engine ceiling.</param>
    /// <returns>The wire ceiling.</returns>
    public static AppCapabilityCeilingDescriptor ToWireCeiling(AppCapabilityCeiling ceiling)
    {
        var scopes = ceiling.ApprovedExceptionScopes;
        if (scopes is null || scopes.Count == 0)
        {
            return new AppCapabilityCeilingDescriptor { AllowedOperations = ceiling.AllowedOperations };
        }

        var builder = ImmutableArray.CreateBuilder<AppExceptionScope>(scopes.Count);
        for (var i = 0; i < scopes.Count; i++)
        {
            builder.Add(ToWireScope(scopes[i]));
        }

        return new AppCapabilityCeilingDescriptor
        {
            AllowedOperations = ceiling.AllowedOperations,
            ApprovedExceptionScopes = builder.MoveToImmutable(),
        };
    }

    /// <summary>Maps engine role bindings to their wire form.</summary>
    /// <param name="bindings">The engine bindings.</param>
    /// <returns>The wire bindings.</returns>
    public static ImmutableArray<AppRoleBindingDescriptor> ToWireBindings(IReadOnlyList<AppRoleBinding>? bindings)
    {
        if (bindings is null || bindings.Count == 0)
        {
            return [];
        }

        var builder = ImmutableArray.CreateBuilder<AppRoleBindingDescriptor>(bindings.Count);
        for (var i = 0; i < bindings.Count; i++)
        {
            builder.Add(new AppRoleBindingDescriptor { RoleName = bindings[i].RoleName, GroupId = bindings[i].GroupId });
        }

        return builder.MoveToImmutable();
    }

    /// <summary>Maps engine provenance to its wire form.</summary>
    /// <param name="provenance">The engine provenance.</param>
    /// <returns>The wire provenance.</returns>
    public static AppProvenanceDescriptor ToWireProvenance(AppProvenance provenance) =>
        new() { Source = provenance.Source, Publisher = provenance.Publisher, Reference = provenance.Reference };

    /// <summary>
    /// Maps a registry state to the wire lifecycle state, reporting
    /// <see cref="AppLifecycleState.Failed"/> only when the recorded activation
    /// status of a live app shows a failed last run. Caller errors (not installed,
    /// invalid transition) are not activation failures, and a disabled registry
    /// state is never interpreted as one.
    /// </summary>
    /// <param name="state">The registry state.</param>
    /// <param name="status">The recorded activation status, or null.</param>
    /// <returns>The wire state.</returns>
    public static AppLifecycleState ToWireState(AppRegistryLifecycleState state, AppActivationStatus? status)
    {
        if (state != AppRegistryLifecycleState.Uninstalled
            && status?.LastOutcome is { Succeeded: false } last
            && last.Failure is not (AppActivationFailure.NotInstalled or AppActivationFailure.InvalidTransition))
        {
            return AppLifecycleState.Failed;
        }

        return ToWireState(state);
    }

    /// <summary>Maps a registry state to the wire lifecycle state.</summary>
    /// <param name="state">The registry state.</param>
    /// <returns>The wire state.</returns>
    public static AppLifecycleState ToWireState(AppRegistryLifecycleState state) => state switch
    {
        AppRegistryLifecycleState.Installed => AppLifecycleState.Installed,
        AppRegistryLifecycleState.Enabled => AppLifecycleState.Enabled,
        AppRegistryLifecycleState.Disabled => AppLifecycleState.Disabled,
        AppRegistryLifecycleState.Uninstalled => AppLifecycleState.Uninstalled,
        _ => throw new InvalidOperationException("The app registry reported an unknown lifecycle state."),
    };

    /// <summary>
    /// The provenance whose publisher the tree ownership probe claims as. The version a live install
    /// holds is judged as that install, because its claims (taken at install and re-verified at every
    /// activation) are held under the provenance its record carries; any other version is judged as
    /// the publisher the source vouches for, which is who an install or upgrade of it would claim as.
    /// </summary>
    /// <param name="liveRecord">The live registry record of the described version, or null when no live install holds it.</param>
    /// <param name="sourceProvenance">The provenance the source reported.</param>
    /// <returns>The provenance to probe ownership as.</returns>
    public static AppProvenance OwnershipProbeProvenance(AppRegistryRecord? liveRecord, AppProvenance sourceProvenance) =>
        liveRecord?.Provenance ?? sourceProvenance;

    /// <summary>
    /// Builds the wire descriptor of a manifest. Every tree reference in a
    /// manifest is already app-local; a reference to this app is echoed without
    /// an app qualifier.
    /// </summary>
    /// <param name="manifest">The source manifest.</param>
    /// <param name="provenance">The provenance the source reported.</param>
    /// <param name="state">The matching installation state.</param>
    /// <param name="record">The matching live registry record, or null when none applies.</param>
    /// <param name="conflicts">The tree ownership conflicts an install of this manifest would hit; null or empty when none.</param>
    /// <returns>The wire descriptor.</returns>
    public static AppDescriptor ToDescriptor(
        AppManifest manifest,
        AppProvenance provenance,
        AppLifecycleState state,
        AppRegistryRecord? record,
        IReadOnlyList<AppTreeOwnershipConflict>? conflicts = null)
    {
        var slug = manifest.Identity.Slug;
        return new AppDescriptor
        {
            Slug = slug.Value,
            Version = manifest.Identity.Version.Value,
            Provenance = ToWireProvenance(provenance),
            State = state,
            Ceiling = record is null ? null : ToWireCeiling(record.Ceiling),
            RoleBindings = record is null ? [] : ToWireBindings(record.RoleBindings),
            Trees = Map(manifest.Trees, t => new AppTreeDescriptor
            {
                Name = t.Name,
                Rebuildable = t.Rebuildable,
                AdoptedTreeId = t.AdoptedTreeId,
                ShardCount = t.ShardCount,
                VirtualShardCount = t.VirtualShardCount,
                MaxLeafKeys = t.MaxLeafKeys,
                MaxInternalChildren = t.MaxInternalChildren,
                WalPartitions = t.WalPartitions,
                SoftDeleteDuration = t.SoftDeleteDuration,
                OwnershipConflict = FindConflict(conflicts, t.Name, slug),
            }),
            Roles = MapRoles(manifest.Roles, slug),
            Subscriptions = MapSubscriptions(manifest.Subscriptions, slug),
            McpTools = Map(manifest.McpTools, static t => new AppMcpToolDescriptor
            {
                Name = t.Name,
                Description = t.Description,
                Role = t.Role,
            }),
            Replication = Map(manifest.Replication, static r => new AppReplicationDescriptor
            {
                Tree = r.Tree,
                MergeMode = r.MergeMode,
            }),
            Schema = Map(manifest.Schema, static s => new AppSchemaDescriptor
            {
                Tree = s.Tree,
                Family = s.Family,
                Version = s.Version,
                StrictIngest = s.StrictIngest,
            }),
            Presentation = AppsPresentationMapping.ToWirePresentation(manifest.Presentation),
            Ui = AppsPresentationMapping.ToWireUi(manifest),
            SourceKey = provenance.Source,
            ManifestDigest = AppManifestDigest.Compute(manifest, provenance),
        };
    }

    /// <summary>Reports whether a tree name matches the manifest's local tree-name grammar.</summary>
    /// <param name="value">The candidate name.</param>
    /// <returns><c>true</c> when the name is a valid local tree name.</returns>
    public static bool IsLocalTreeName(string? value)
    {
        if (string.IsNullOrEmpty(value) || value.Length > MaxTreeNameLength || value[0] is < 'a' or > 'z')
        {
            return false;
        }

        foreach (var c in value)
        {
            if (c is not (>= 'a' and <= 'z') and not (>= '0' and <= '9') and not '-' and not '_')
            {
                return false;
            }
        }

        return true;
    }

    private static LatticeScope ToEngineScope(AppExceptionScope? scope, TenantId tenant, int index)
    {
        if (scope is null)
        {
            throw new ArgumentException($"Approved exception scope {index} is null.", "request");
        }

        if (!Enum.IsDefined(scope.Kind))
        {
            throw new ArgumentException($"Approved exception scope {index} has an unknown kind.", "request");
        }

        var namesAppTree = scope.App is not null || scope.Tree is not null;
        var namesAdopted = scope.AdoptedTreeId is not null;
        if (namesAppTree == namesAdopted)
        {
            throw new ArgumentException(
                $"Approved exception scope {index} must name either an app tree (App and Tree) or an adopted tree id, "
                + "not both and not neither.",
                "request");
        }

        string localTreeId;
        if (namesAppTree)
        {
            if (!AppSlug.TryParse(scope.App, out var app) || !IsLocalTreeName(scope.Tree))
            {
                throw new ArgumentException(
                    $"Approved exception scope {index} must name a valid app slug and a local tree name.", "request");
            }

            localTreeId = string.Concat(LatticeConstants.AppTreePrefix, app.Value, "/", scope.Tree);
        }
        else
        {
            if (!IsAdoptableTreeId(scope.AdoptedTreeId))
            {
                throw new ArgumentException(
                    $"Approved exception scope {index} must name a pre-app tree id outside the app, tenant and reserved "
                    + "namespaces.",
                    "request");
            }

            localTreeId = scope.AdoptedTreeId!;
        }

        switch (scope.Kind)
        {
            case LatticeScopeKind.Tree when scope.KeyOrPrefix is not null:
                throw new ArgumentException(
                    $"Approved exception scope {index} is a tree scope and must not carry a key or prefix.", "request");
            case LatticeScopeKind.Key or LatticeScopeKind.Prefix when string.IsNullOrEmpty(scope.KeyOrPrefix):
                throw new ArgumentException(
                    $"Approved exception scope {index} is a key or prefix scope and requires a non-empty key or prefix.",
                    "request");
        }

        // Compose at entry, before any authorization, and guard the single effective id.
        // The stored scope stays tenant-local because the role compiler composes it with
        // the same tenant when it compiles rules.
        var effectiveTreeId = LatticeTenantResolution.ComposeEffectiveTreeId(tenant, localTreeId);
        ThrowIfOutsideTenant(effectiveTreeId, tenant, index);

        return new LatticeScope(scope.Kind, localTreeId, scope.KeyOrPrefix);
    }

    private static void ThrowIfOutsideTenant(string effectiveTreeId, TenantId tenant, int index)
    {
        var reserved = effectiveTreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal)
            || effectiveTreeId.StartsWith(LatticeConstants.SystemDataTreePrefix, StringComparison.Ordinal);
        var foreign = !tenant.IsDefault && LatticeTenantTrees.GetOwner(effectiveTreeId).Tenant != tenant;
        if (reserved || foreign)
        {
            throw new ArgumentException(
                $"Approved exception scope {index} resolves outside the caller's tenant namespace.", "request");
        }
    }

    private static bool IsAdoptableTreeId(string? treeId) =>
        !string.IsNullOrWhiteSpace(treeId)
        && !string.Equals(treeId, LatticeScope.ClusterWideTreeId, StringComparison.Ordinal)
        && !treeId.StartsWith(LatticeConstants.AppTreePrefix, StringComparison.Ordinal)
        && !treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal)
        && !treeId.StartsWith(LatticeConstants.SystemDataTreePrefix, StringComparison.Ordinal)
        && !treeId.StartsWith(LatticeTenantTrees.SegmentPrefix, StringComparison.Ordinal);

    private static AppExceptionScope ToWireScope(LatticeScope scope)
    {
        var treeId = scope.TreeId;
        if (treeId.StartsWith(LatticeConstants.AppTreePrefix, StringComparison.Ordinal))
        {
            var rest = treeId.AsSpan(LatticeConstants.AppTreePrefix.Length);
            var separator = rest.IndexOf('/');
            if (separator > 0 && separator < rest.Length - 1)
            {
                return new AppExceptionScope
                {
                    Kind = scope.Kind,
                    App = rest[..separator].ToString(),
                    Tree = rest[(separator + 1)..].ToString(),
                    KeyOrPrefix = scope.KeyOrPrefix,
                };
            }
        }

        return new AppExceptionScope
        {
            Kind = scope.Kind,
            AdoptedTreeId = AppsControlExceptionSanitizer.SanitizeText(treeId, ownApp: null),
            KeyOrPrefix = scope.KeyOrPrefix,
        };
    }

    internal static ImmutableArray<AppRoleDescriptor> MapRoles(AppRoleDeclaration[]? roles, AppSlug self)
    {
        if (roles is null || roles.Length == 0)
        {
            return [];
        }

        var builder = ImmutableArray.CreateBuilder<AppRoleDescriptor>(roles.Length);
        foreach (var role in roles)
        {
            var scopes = role.Scopes ?? [];
            var scopeBuilder = ImmutableArray.CreateBuilder<AppRoleScope>(scopes.Length);
            foreach (var template in scopes)
            {
                scopeBuilder.Add(new AppRoleScope
                {
                    Tree = template.Tree,
                    App = ForeignApp(template.App, self),
                    Kind = template.Kind,
                    KeyOrPrefix = template.KeyOrPrefix,
                });
            }

            builder.Add(new AppRoleDescriptor
            {
                Name = role.Name,
                Operations = role.Operations,
                Scopes = scopeBuilder.MoveToImmutable(),
            });
        }

        return builder.MoveToImmutable();
    }

    internal static ImmutableArray<AppSubscriptionDescriptor> MapSubscriptions(
        AppSubscriptionDeclaration[]? subscriptions,
        AppSlug self)
    {
        if (subscriptions is null || subscriptions.Length == 0)
        {
            return [];
        }

        var builder = ImmutableArray.CreateBuilder<AppSubscriptionDescriptor>(subscriptions.Length);
        foreach (var subscription in subscriptions)
        {
            builder.Add(new AppSubscriptionDescriptor
            {
                Name = subscription.Name,
                Tree = subscription.Tree,
                App = ForeignApp(subscription.App, self),
                KeyPrefix = subscription.KeyPrefix,
            });
        }

        return builder.MoveToImmutable();
    }

    private static string? ForeignApp(AppSlug? app, AppSlug self) =>
        app is { } other && other != self ? other.Value : null;

    private static string? FindConflict(IReadOnlyList<AppTreeOwnershipConflict>? conflicts, string treeName, AppSlug slug)
    {
        if (conflicts is null)
        {
            return null;
        }

        foreach (var conflict in conflicts)
        {
            if (string.Equals(conflict.TreeName, treeName, StringComparison.Ordinal))
            {
                return AppsControlExceptionSanitizer.SanitizeText(conflict.Message, slug.Value);
            }
        }

        return null;
    }

    internal static ImmutableArray<TOut> Map<TIn, TOut>(TIn[]? source, Func<TIn, TOut> map)
    {
        if (source is null || source.Length == 0)
        {
            return [];
        }

        var builder = ImmutableArray.CreateBuilder<TOut>(source.Length);
        foreach (var item in source)
        {
            builder.Add(map(item));
        }

        return builder.MoveToImmutable();
    }
}
