using System.Text.Json;
using System.Text.Json.Serialization;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.ReferenceArchitecture.Silo;

/// <summary>
/// The parsed local-dev identity model (identities, their groups, and each
/// group's authorization grants) loaded from the JSON file mounted into the silo
/// (see <c>reference-architecture/local-dev/identities.json</c>). It is the
/// single source of truth the harness's two dev seams consume: the
/// <see cref="DevIdentityCredentialAuthenticator"/> reads the identity to groups
/// map to attach asserted groups to a resolved principal, and the
/// <see cref="LocalDevIdentitySeeder"/> reads the groups and their grants to seed
/// the durable membership directory and author the authorization policy.
/// </summary>
/// <remarks>
/// This type exists only in the local-dev harness. It is a development convenience
/// for standing up differentiated identities WITHOUT an Entra tenant; it never
/// ships in, and is never referenced by, a real deployment host.
/// </remarks>
internal sealed class LocalDevIdentityModel
{
    /// <summary>The default identities-file path baked into the image; overridable via the <c>IDENTITIES_FILE</c> environment variable.</summary>
    public const string DefaultPath = "/config/identities.json";

    /// <summary>The declared identities, keyed by subject id.</summary>
    public IReadOnlyDictionary<string, LocalDevIdentity> Identities { get; }

    /// <summary>The declared groups, keyed by group id.</summary>
    public IReadOnlyDictionary<string, LocalDevGroup> Groups { get; }

    /// <summary>
    /// The declared demo tenants (only consumed when tenancy is enabled). Each
    /// names a tenant id, an optional display name, and the subject ids that are
    /// its tenant administrators; the <see cref="TenantSeeder"/> writes these into
    /// the tenant registry so <c>lattice_tenant_list</c> / <c>_get</c> resolve
    /// differentiated results per identity.
    /// </summary>
    public IReadOnlyList<LocalDevTenant> Tenants { get; }

    private LocalDevIdentityModel(
        IReadOnlyDictionary<string, LocalDevIdentity> identities,
        IReadOnlyDictionary<string, LocalDevGroup> groups,
        IReadOnlyList<LocalDevTenant> tenants)
    {
        Identities = identities;
        Groups = groups;
        Tenants = tenants;
    }

    /// <summary>An empty model (no identities, no groups, no tenants); the safe default when no file is configured.</summary>
    public static LocalDevIdentityModel Empty { get; } = new(
        new Dictionary<string, LocalDevIdentity>(StringComparer.Ordinal),
        new Dictionary<string, LocalDevGroup>(StringComparer.Ordinal),
        []);

    /// <summary>
    /// Loads and validates the model from <paramref name="path"/>. A missing path
    /// yields <see cref="Empty"/> (the harness simply has no differentiated
    /// identities); a present-but-malformed file throws so the misconfiguration
    /// surfaces immediately at silo startup rather than silently disabling
    /// enforcement.
    /// </summary>
    /// <param name="path">The identities-file path, or <c>null</c>/empty to load nothing.</param>
    /// <returns>The parsed, validated model.</returns>
    /// <exception cref="InvalidOperationException">The file is present but invalid.</exception>
    public static LocalDevIdentityModel Load(string? path)
    {
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
        {
            return Empty;
        }

        Document? document;
        try
        {
            using var stream = File.OpenRead(path);
            document = JsonSerializer.Deserialize(stream, LocalDevIdentityJsonContext.Default.Document);
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"The local-dev identities file '{path}' is not valid JSON.", ex);
        }

        if (document is null)
        {
            return Empty;
        }

        var groups = new Dictionary<string, LocalDevGroup>(StringComparer.Ordinal);
        foreach (var group in document.Groups)
        {
            if (string.IsNullOrWhiteSpace(group.Id))
            {
                throw new InvalidOperationException($"A group in '{path}' has no id.");
            }

            var grants = new List<LocalDevGrant>();
            foreach (var grant in group.Grants)
            {
                grants.Add(LocalDevGrant.Parse(group.Id, grant, path));
            }

            groups[group.Id] = new LocalDevGroup(group.Id, group.DisplayName, grants);
        }

        var identities = new Dictionary<string, LocalDevIdentity>(StringComparer.Ordinal);
        foreach (var identity in document.Identities)
        {
            if (string.IsNullOrWhiteSpace(identity.Id))
            {
                throw new InvalidOperationException($"An identity in '{path}' has no id.");
            }

            var identityGroups = (identity.Groups ?? []).Where(g => !string.IsNullOrWhiteSpace(g)).ToArray();
            foreach (var groupId in identityGroups)
            {
                if (!groups.ContainsKey(groupId))
                {
                    throw new InvalidOperationException(
                        $"Identity '{identity.Id}' in '{path}' references unknown group '{groupId}'.");
                }
            }

            identities[identity.Id] = new LocalDevIdentity(identity.Id, identity.Description, identityGroups);
        }

        var tenants = new List<LocalDevTenant>();
        var seenTenantIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (var tenant in document.Tenants)
        {
            if (string.IsNullOrWhiteSpace(tenant.Id))
            {
                throw new InvalidOperationException($"A tenant in '{path}' has no id.");
            }

            if (!seenTenantIds.Add(tenant.Id))
            {
                throw new InvalidOperationException($"Tenant '{tenant.Id}' in '{path}' is declared more than once.");
            }

            var adminSubjects = (tenant.AdminSubjects ?? [])
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .Distinct(StringComparer.Ordinal)
                .ToArray();
            foreach (var subjectId in adminSubjects)
            {
                if (!identities.ContainsKey(subjectId))
                {
                    throw new InvalidOperationException(
                        $"Tenant '{tenant.Id}' in '{path}' names unknown admin subject '{subjectId}'.");
                }
            }

            tenants.Add(new LocalDevTenant(tenant.Id, tenant.DisplayName, adminSubjects));
        }

        return new LocalDevIdentityModel(identities, groups, tenants);
    }

    /// <summary>The raw JSON document shape.</summary>
    internal sealed class Document
    {
        [JsonPropertyName("identities")]
        public IReadOnlyList<IdentityEntry> Identities { get; init; } = [];

        [JsonPropertyName("groups")]
        public IReadOnlyList<GroupEntry> Groups { get; init; } = [];

        [JsonPropertyName("tenants")]
        public IReadOnlyList<TenantEntry> Tenants { get; init; } = [];
    }

    /// <summary>The raw JSON identity entry.</summary>
    internal sealed class IdentityEntry
    {
        [JsonPropertyName("id")]
        public string? Id { get; init; }

        [JsonPropertyName("description")]
        public string? Description { get; init; }

        [JsonPropertyName("groups")]
        public IReadOnlyList<string>? Groups { get; init; }
    }

    /// <summary>The raw JSON group entry.</summary>
    internal sealed class GroupEntry
    {
        [JsonPropertyName("id")]
        public string? Id { get; init; }

        [JsonPropertyName("displayName")]
        public string? DisplayName { get; init; }

        [JsonPropertyName("grants")]
        public IReadOnlyList<GrantEntry> Grants { get; init; } = [];
    }

    /// <summary>The raw JSON grant entry.</summary>
    internal sealed class GrantEntry
    {
        [JsonPropertyName("scope")]
        public string? Scope { get; init; }

        [JsonPropertyName("effect")]
        public string? Effect { get; init; }

        [JsonPropertyName("operations")]
        public IReadOnlyList<string> Operations { get; init; } = [];
    }

    /// <summary>The raw JSON tenant entry.</summary>
    internal sealed class TenantEntry
    {
        [JsonPropertyName("id")]
        public string? Id { get; init; }

        [JsonPropertyName("displayName")]
        public string? DisplayName { get; init; }

        [JsonPropertyName("adminSubjects")]
        public IReadOnlyList<string>? AdminSubjects { get; init; }
    }
}

/// <summary>A resolved identity: its subject id, optional description, and the group ids it belongs to.</summary>
internal sealed record LocalDevIdentity(string Id, string? Description, IReadOnlyList<string> Groups);

/// <summary>A resolved group: its id, optional display name, and the authorization grants it confers on its members.</summary>
internal sealed record LocalDevGroup(string Id, string? DisplayName, IReadOnlyList<LocalDevGrant> Grants);

/// <summary>A resolved demo tenant: its id, optional display name, and the subject ids that administer it.</summary>
internal sealed record LocalDevTenant(string Id, string? DisplayName, IReadOnlyList<string> AdminSubjects);

/// <summary>
/// A resolved authorization grant conferred on a group's members: the parsed
/// operation mask, the scope (cluster-wide or a single named tree), and the
/// effect. Built from a validated JSON grant entry.
/// </summary>
internal sealed record LocalDevGrant(LatticeOperation Operations, LocalDevScopeKind ScopeKind, string? TreeId, LatticeEffect Effect)
{
    /// <summary>The "all trees" token for a cluster-wide scope.</summary>
    private const string ClusterScope = "cluster";

    /// <summary>The "tree:&lt;id&gt;" scope prefix for a single-tree scope.</summary>
    private const string TreeScopePrefix = "tree:";

    /// <summary>Materialises the parsed scope into a concrete <see cref="LatticeScope"/>.</summary>
    public LatticeScope ToScope() => ScopeKind == LocalDevScopeKind.Cluster
        ? LatticeScope.ClusterWide()
        : LatticeScope.Tree(TreeId!);

    /// <summary>Parses and validates a single JSON grant entry for <paramref name="groupId"/>.</summary>
    public static LocalDevGrant Parse(string groupId, LocalDevIdentityModel.GrantEntry entry, string path)
    {
        var operations = LatticeOperation.None;
        foreach (var token in entry.Operations)
        {
            if (string.IsNullOrWhiteSpace(token))
            {
                continue;
            }

            operations |= ParseOperation(token.Trim(), groupId, path);
        }

        if (operations == LatticeOperation.None)
        {
            throw new InvalidOperationException(
                $"Group '{groupId}' in '{path}' has a grant with no operations.");
        }

        var (scopeKind, treeId) = ParseScope(entry.Scope, groupId, path);
        var effect = ParseEffect(entry.Effect, groupId, path);
        return new LocalDevGrant(operations, scopeKind, treeId, effect);
    }

    private static LatticeOperation ParseOperation(string token, string groupId, string path)
    {
        // "All" is a convenience for the whole data-plane aggregate; the scopeless
        // capabilities (Telemetry / Replication) and the whole-tree lifecycle bit
        // must be named explicitly, mirroring LatticeAuthOperations.All.
        if (string.Equals(token, "All", StringComparison.OrdinalIgnoreCase))
        {
            return LatticeAuthOperations.All;
        }

        if (Enum.TryParse<LatticeOperation>(token, ignoreCase: true, out var operation)
            && operation != LatticeOperation.None)
        {
            return operation;
        }

        throw new InvalidOperationException(
            $"Group '{groupId}' in '{path}' names unknown operation '{token}'.");
    }

    private static (LocalDevScopeKind Kind, string? TreeId) ParseScope(string? scope, string groupId, string path)
    {
        var value = string.IsNullOrWhiteSpace(scope) ? ClusterScope : scope.Trim();
        if (string.Equals(value, ClusterScope, StringComparison.OrdinalIgnoreCase))
        {
            return (LocalDevScopeKind.Cluster, null);
        }

        if (value.StartsWith(TreeScopePrefix, StringComparison.OrdinalIgnoreCase))
        {
            var treeId = value[TreeScopePrefix.Length..].Trim();
            if (string.IsNullOrWhiteSpace(treeId))
            {
                throw new InvalidOperationException(
                    $"Group '{groupId}' in '{path}' has a 'tree:' scope with no tree id.");
            }

            return (LocalDevScopeKind.Tree, treeId);
        }

        throw new InvalidOperationException(
            $"Group '{groupId}' in '{path}' has unknown scope '{value}' (expected 'cluster' or 'tree:<id>').");
    }

    private static LatticeEffect ParseEffect(string? effect, string groupId, string path)
    {
        if (string.IsNullOrWhiteSpace(effect) || string.Equals(effect, "Allow", StringComparison.OrdinalIgnoreCase))
        {
            return LatticeEffect.Allow;
        }

        if (string.Equals(effect, "Deny", StringComparison.OrdinalIgnoreCase))
        {
            return LatticeEffect.Deny;
        }

        throw new InvalidOperationException(
            $"Group '{groupId}' in '{path}' has unknown effect '{effect}' (expected 'Allow' or 'Deny').");
    }
}

/// <summary>Whether a grant's scope is the cluster-wide (all-trees) sentinel or a single named tree.</summary>
internal enum LocalDevScopeKind
{
    /// <summary>The cluster-wide (all-trees) scope, and the home of the scopeless capabilities.</summary>
    Cluster,

    /// <summary>A single named tree.</summary>
    Tree,
}

/// <summary>The source-generated JSON context for the identity model (trim/AOT-friendly, InvariantGlobalization-safe).</summary>
[JsonSourceGenerationOptions(PropertyNameCaseInsensitive = true)]
[JsonSerializable(typeof(LocalDevIdentityModel.Document))]
internal sealed partial class LocalDevIdentityJsonContext : JsonSerializerContext
{
}
