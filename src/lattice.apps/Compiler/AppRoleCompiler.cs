using System.Buffers;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Apps;

/// <summary>
/// Compiles an app manifest's flat roles into ordinary <see cref="LatticeAuthorizationRule"/>
/// records. A pure function with no I/O: persisting the compiled set, inside a system-origin
/// scope, is the caller's concern. Enforcement stays with the existing access gate.
/// </summary>
/// <remarks>
/// <para>
/// <b>Scope resolution.</b> Each role scope template names an app-local tree and resolves, in
/// the tenant-local vocabulary, to: <c>a/{otherApp}/{tree}</c> when the template names another
/// app; otherwise the declaration's <see cref="AppTreeDeclaration.AdoptedTreeId"/> when set;
/// otherwise the structural <c>a/{app}/{tree}</c>. The resolved id is then composed with the
/// tenant through the core tenant-resolution seam (tenant is the outer axis), yielding the
/// bare id when tenancy is off and <c>t/{tenant}/...</c> when it is on.
/// </para>
/// <para>
/// <b>Ceiling.</b> Every role's operations must be a subset of
/// <see cref="AppCapabilityCeiling.AllowedOperations"/>, whether or not the role is bound.
/// Structural scopes in the app's own namespace are inside the ceiling by construction. An
/// adopted tree or another app's tree is allowed only when an
/// <see cref="AppCapabilityCeiling.ApprovedExceptionScopes"/> entry covers it. Exception scopes
/// are written in the tenant-local vocabulary, before tenant composition, and are compared
/// against the tenant-local resolved scope, so one consent applies to whichever tenant installs
/// the app; an exception written tenant-qualified (<c>t/...</c>) never matches. An exception
/// covers a requested scope when both name the same tree (ordinal) and: a tree exception covers
/// any scope; a prefix exception covers a key or prefix scope that starts with it; a key
/// exception covers only the identical key scope. <see cref="LatticeScope.ClusterWideTreeId"/>
/// is not a wildcard here. Any excess fails the whole compilation; nothing is clamped.
/// </para>
/// <para>
/// <b>Subjects.</b> Each binding emits <see cref="LatticeSubjectSelector.Group(string)"/> for its
/// group, never a user selector. A declared role with no binding emits nothing and is reported in
/// <see cref="AppRuleCompilation.UnboundRoles"/>; a binding naming an undeclared role fails
/// compilation and is reported in <see cref="AppRuleCompilation.UnknownRoleBindings"/>.
/// </para>
/// <para>
/// <b>Rule ids.</b> Every id is <c>app:{slug}:{role}:{hash}</c>, where <c>{hash}</c> is the
/// lowercase hex of the first 16 bytes of SHA-256 over the canonical encoding of the fields
/// <c>app-rule-id/v1</c>, slug, role, group id, scope kind name, effective (tenant-composed) tree
/// id and key or prefix. Each field is its UTF-8 bytes preceded by a little-endian 32-bit byte
/// length, with length -1 and no bytes for an absent key or prefix. Ids are stable across runs
/// and processes, distinct per role, group and scope, and differ per tenant when tenancy is on
/// because the effective tree id does. They start with <see cref="GetOwnedRuleIdPrefix(AppSlug)"/>.
/// </para>
/// </remarks>
public static class AppRoleCompiler
{
    private const string DerivationVersion = "app-rule-id/v1";
    private const int RuleIdHashBytes = 16;
    private const int StackBufferBytes = 512;

    /// <summary>
    /// Returns the rule-id prefix <c>app:{slug}:</c> owned by <paramref name="slug"/>. Every rule the
    /// compiler emits for the app starts with it, and no other app's ids do.
    /// </summary>
    /// <param name="slug">The app slug. Must not be the uninitialised value.</param>
    /// <returns>The owned rule-id prefix.</returns>
    /// <exception cref="ArgumentException"><paramref name="slug"/> is the uninitialised value.</exception>
    public static string GetOwnedRuleIdPrefix(AppSlug slug)
    {
        if (slug.Value is null)
            throw new ArgumentException("The app slug is uninitialised.", nameof(slug));
        return string.Concat(LatticeAppRuleIds.Prefix, slug.Value, ":");
    }

    /// <summary>
    /// Compiles <paramref name="manifest"/>'s roles for one install into the complete owned rule set,
    /// or an activation failure listing every excess. Compiling the same input twice yields an
    /// identical, identically ordered set.
    /// </summary>
    /// <param name="manifest">A manifest that passed <see cref="AppManifestValidator.Validate"/>; it is not re-validated.</param>
    /// <param name="tenant">The install's tenant; <see cref="TenantId.Default"/> when tenancy is off.</param>
    /// <param name="bindings">The install's role-to-group bindings.</param>
    /// <param name="ceiling">The install's pinned capability ceiling.</param>
    /// <returns>The compilation result.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="manifest"/>, <paramref name="bindings"/> or <paramref name="ceiling"/> is <c>null</c>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="tenant"/> is the uninitialised "no tenant" value, the manifest has no valid slug,
    /// or a binding is <c>null</c> or has an empty group id.
    /// </exception>
    public static AppRuleCompilation Compile(
        AppManifest manifest,
        TenantId tenant,
        IReadOnlyList<AppRoleBinding> bindings,
        AppCapabilityCeiling ceiling)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        ArgumentNullException.ThrowIfNull(bindings);
        ArgumentNullException.ThrowIfNull(ceiling);
        if (tenant.Value is null)
            throw new ArgumentException("An install must be attributed to a tenant.", nameof(tenant));
        var slug = manifest.Identity?.Slug ?? default;
        if (slug.Value is null)
            throw new ArgumentException("The manifest has no valid app slug.", nameof(manifest));

        var ownedPrefix = GetOwnedRuleIdPrefix(slug);
        var exceptions = ceiling.ApprovedExceptionScopes ?? Array.Empty<LatticeScope>();
        var adopted = AdoptedTrees(manifest.Trees);
        var roles = manifest.Roles;

        var compiledRoles = new Dictionary<string, CompiledRole>(roles.Length, StringComparer.Ordinal);
        List<AppCeilingExcess>? excesses = null;
        HashSet<LatticeScope>? reportedScopes = null;
        foreach (var role in roles)
        {
            var excessOperations = role.Operations & ~ceiling.AllowedOperations;
            if (excessOperations != LatticeOperation.None)
                (excesses ??= []).Add(new(role.Name, AppCeilingExcessKind.Operations, excessOperations, null));

            reportedScopes?.Clear();
            var scopes = new LatticeScope[role.Scopes.Length];
            for (var i = 0; i < scopes.Length; i++)
            {
                var local = ResolveLocalScope(slug, role.Scopes[i], adopted, out var structural);
                if (!structural && !IsCovered(local, exceptions) && (reportedScopes ??= []).Add(local))
                    (excesses ??= []).Add(new(role.Name, AppCeilingExcessKind.Scope, role.Operations, local));

                var effectiveTree = LatticeTenantResolution.ComposeEffectiveTreeId(tenant, local.TreeId);
                scopes[i] = ReferenceEquals(effectiveTree, local.TreeId) ? local : local with { TreeId = effectiveTree };
            }

            compiledRoles.TryAdd(role.Name, new(role.Operations, scopes));
        }

        HashSet<string>? boundRoles = null;
        List<AppRoleBinding>? unknownBindings = null;
        foreach (var binding in bindings)
        {
            if (binding is null)
                throw new ArgumentException("A role binding cannot be null.", nameof(bindings));
            if (string.IsNullOrEmpty(binding.GroupId))
                throw new ArgumentException($"The binding for role '{binding.RoleName}' has no group id.", nameof(bindings));
            if (compiledRoles.ContainsKey(binding.RoleName))
                (boundRoles ??= new(StringComparer.Ordinal)).Add(binding.RoleName);
            else
                (unknownBindings ??= []).Add(binding);
        }

        List<string>? unboundRoles = null;
        foreach (var role in roles)
            if (boundRoles is null || !boundRoles.Contains(role.Name))
                (unboundRoles ??= []).Add(role.Name);

        if (excesses is not null || unknownBindings is not null)
            return new(Array.Empty<LatticeAuthorizationRule>(), OrEmpty(excesses), OrEmpty(unknownBindings), OrEmpty(unboundRoles));

        var rules = new List<LatticeAuthorizationRule>();
        var ruleIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (var binding in bindings)
        {
            var role = compiledRoles[binding.RoleName];
            var subject = LatticeSubjectSelector.Group(binding.GroupId);
            foreach (var scope in role.Scopes)
            {
                var ruleId = ComputeRuleId(ownedPrefix, slug.Value, binding.RoleName, binding.GroupId, scope);
                if (ruleIds.Add(ruleId))
                    rules.Add(new(ruleId, subject, scope, role.Operations, LatticeEffect.Allow));
            }
        }

        rules.Sort(static (x, y) => string.CompareOrdinal(x.RuleId, y.RuleId));
        return new(rules, Array.Empty<AppCeilingExcess>(), Array.Empty<AppRoleBinding>(), OrEmpty(unboundRoles));
    }

    /// <summary>
    /// Computes the writes that replace the stored owned set of <paramref name="slug"/> with
    /// <paramref name="compiled"/>. Stored rules outside the app's owned id prefix, including
    /// operator rules and other apps' rules, are ignored. Diffing a set against itself is empty.
    /// </summary>
    /// <param name="slug">The app whose owned set is being reconciled.</param>
    /// <param name="compiled">The freshly compiled rule set, normally <see cref="AppRuleCompilation.Rules"/>.</param>
    /// <param name="stored">The currently stored rules; may include rules the app does not own.</param>
    /// <returns>The rules to put and the stored owned rules to remove.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="compiled"/> or <paramref name="stored"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="slug"/> is uninitialised, either set contains a <c>null</c> rule, or
    /// <paramref name="compiled"/> contains a rule outside the owned prefix or a duplicate id.
    /// </exception>
    public static AppRuleSetDiff ComputeDiff(
        AppSlug slug,
        IReadOnlyList<LatticeAuthorizationRule> compiled,
        IEnumerable<LatticeAuthorizationRule> stored)
    {
        ArgumentNullException.ThrowIfNull(compiled);
        ArgumentNullException.ThrowIfNull(stored);
        var prefix = GetOwnedRuleIdPrefix(slug);

        var desired = new Dictionary<string, LatticeAuthorizationRule>(compiled.Count, StringComparer.Ordinal);
        foreach (var rule in compiled)
        {
            if (rule is null)
                throw new ArgumentException("A compiled rule cannot be null.", nameof(compiled));
            if (!rule.RuleId.StartsWith(prefix, StringComparison.Ordinal))
                throw new ArgumentException($"Compiled rule '{rule.RuleId}' is not owned by app '{slug}'.", nameof(compiled));
            if (!desired.TryAdd(rule.RuleId, rule))
                throw new ArgumentException($"Compiled rule id '{rule.RuleId}' is duplicated.", nameof(compiled));
        }

        var satisfied = new HashSet<string>(StringComparer.Ordinal);
        var toDelete = new List<LatticeAuthorizationRule>();
        foreach (var rule in stored)
        {
            if (rule is null)
                throw new ArgumentException("A stored rule cannot be null.", nameof(stored));
            if (!rule.RuleId.StartsWith(prefix, StringComparison.Ordinal))
                continue;
            if (!desired.TryGetValue(rule.RuleId, out var wanted))
                toDelete.Add(rule);
            else if (wanted == rule)
                satisfied.Add(rule.RuleId);
            else if (!string.Equals(wanted.Scope.TreeId, rule.Scope.TreeId, StringComparison.Ordinal))
                toDelete.Add(rule);
        }

        var toUpsert = new List<LatticeAuthorizationRule>();
        foreach (var rule in compiled)
            if (!satisfied.Contains(rule.RuleId))
                toUpsert.Add(rule);

        toUpsert.Sort(static (x, y) => string.CompareOrdinal(x.RuleId, y.RuleId));
        toDelete.Sort(static (x, y) =>
        {
            var order = string.CompareOrdinal(x.RuleId, y.RuleId);
            return order != 0 ? order : string.CompareOrdinal(x.Scope.TreeId, y.Scope.TreeId);
        });
        return new(toUpsert, toDelete);
    }

    internal static string ComputeRuleId(string ownedPrefix, string slug, string role, string groupId, LatticeScope scope)
    {
        var kind = scope.Kind switch
        {
            LatticeScopeKind.Tree => "Tree",
            LatticeScopeKind.Key => "Key",
            LatticeScopeKind.Prefix => "Prefix",
            _ => throw new ArgumentException("Unknown scope kind.", nameof(scope)),
        };

        var length = FieldLength(DerivationVersion) + FieldLength(slug) + FieldLength(role) + FieldLength(groupId)
            + FieldLength(kind) + FieldLength(scope.TreeId) + FieldLength(scope.KeyOrPrefix);
        byte[]? rented = null;
        Span<byte> buffer = length <= StackBufferBytes
            ? stackalloc byte[StackBufferBytes]
            : (rented = ArrayPool<byte>.Shared.Rent(length));
        try
        {
            var written = 0;
            written += WriteField(buffer[written..], DerivationVersion);
            written += WriteField(buffer[written..], slug);
            written += WriteField(buffer[written..], role);
            written += WriteField(buffer[written..], groupId);
            written += WriteField(buffer[written..], kind);
            written += WriteField(buffer[written..], scope.TreeId);
            written += WriteField(buffer[written..], scope.KeyOrPrefix);

            Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
            SHA256.HashData(buffer[..written], hash);
            Span<char> hex = stackalloc char[RuleIdHashBytes * 2];
            Convert.TryToHexStringLower(hash[..RuleIdHashBytes], hex, out _);
            return string.Concat(ownedPrefix.AsSpan(), role.AsSpan(), ":".AsSpan(), hex);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static IReadOnlyList<T> OrEmpty<T>(List<T>? list) => list is null ? Array.Empty<T>() : list;

    private static int FieldLength(string? value) => sizeof(int) + (value is null ? 0 : Encoding.UTF8.GetByteCount(value));

    private static int WriteField(Span<byte> destination, string? value)
    {
        if (value is null)
        {
            BinaryPrimitives.WriteInt32LittleEndian(destination, -1);
            return sizeof(int);
        }

        var count = Encoding.UTF8.GetBytes(value, destination[sizeof(int)..]);
        BinaryPrimitives.WriteInt32LittleEndian(destination, count);
        return sizeof(int) + count;
    }

    private static Dictionary<string, string>? AdoptedTrees(AppTreeDeclaration[] trees)
    {
        Dictionary<string, string>? adopted = null;
        foreach (var tree in trees)
            if (tree.AdoptedTreeId is { } physical)
                (adopted ??= new(StringComparer.Ordinal)).TryAdd(tree.Name, physical);
        return adopted;
    }

    private static LatticeScope ResolveLocalScope(
        AppSlug slug,
        AppScopeTemplate template,
        Dictionary<string, string>? adopted,
        out bool structural)
    {
        string treeId;
        if (template.App is { } app && app != slug)
        {
            treeId = string.Concat(LatticeConstants.AppTreePrefix, app.Value, "/", template.Tree);
            structural = false;
        }
        else if (adopted is not null && adopted.TryGetValue(template.Tree, out var physical))
        {
            treeId = physical;
            structural = false;
        }
        else
        {
            treeId = string.Concat(LatticeConstants.AppTreePrefix, slug.Value, "/", template.Tree);
            structural = true;
        }

        return new(template.Kind, treeId, template.KeyOrPrefix);
    }

    private static bool IsCovered(LatticeScope requested, IReadOnlyList<LatticeScope> exceptions)
    {
        foreach (var exception in exceptions)
        {
            if (exception is null || !string.Equals(exception.TreeId, requested.TreeId, StringComparison.Ordinal))
                continue;
            var covered = exception.Kind switch
            {
                LatticeScopeKind.Tree => true,
                LatticeScopeKind.Prefix => requested.Kind != LatticeScopeKind.Tree
                    && requested.KeyOrPrefix!.StartsWith(exception.KeyOrPrefix!, StringComparison.Ordinal),
                LatticeScopeKind.Key => requested.Kind == LatticeScopeKind.Key
                    && string.Equals(requested.KeyOrPrefix, exception.KeyOrPrefix, StringComparison.Ordinal),
                _ => false,
            };
            if (covered)
                return true;
        }

        return false;
    }

    private readonly record struct CompiledRole(LatticeOperation Operations, LatticeScope[] Scopes);
}
