using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps;

/// <summary>Pure manifest validation; invalid declarations become diagnostics, never startup exceptions.</summary>
public static class AppManifestValidator
{
    internal static readonly LatticeOperation RoleOperations =
        Enum.GetValues<LatticeOperation>()
            .Where(static value => value != LatticeOperation.Telemetry && Enum.GetName(value) != "AppInstall")
            .Aggregate(LatticeOperation.None, static (mask, value) => mask | value);

    /// <summary>
    /// Checks declarations and references, optionally rejecting changes to existing virtual shard pins.
    /// Null manifests and malformed programmatically constructed records return diagnostics.
    /// </summary>
    public static AppManifestResult Validate(AppManifest? manifest, AppManifest? previous = null)
    {
        if (manifest is null)
            return AppManifestResult.Failure("required", "$", "A manifest is required.");

        List<AppManifestError> errors = [];
        void Error(string code, string path, string message) => errors.Add(new(code, path, message));
        void RequiredText(string? value, string path)
        {
            if (string.IsNullOrWhiteSpace(value))
                Error("required", path, "Non-empty text is required.");
        }

        var identity = manifest.Identity;
        if (identity is null)
            Error("required", "$.identity", "Identity is required.");
        else
        {
            if (!AppSlug.TryParse(identity.Slug.Value, out _))
                Error("slug", "$.identity.slug", "Expected 2-31 lowercase ASCII slug characters, without underscores.");
            if (!AppVersion.TryParse(identity.Version.Value, out _))
                Error("version", "$.identity.version", "Expected a Semantic Version 2.0 string.");
            if (identity.Provenance is null)
                Error("required", "$.identity.provenance", "Provenance cannot be null.");
            else
            {
                RequiredText(identity.Provenance.Source, "$.identity.provenance.source");
                RequiredText(identity.Provenance.Publisher, "$.identity.provenance.publisher");
                if (identity.Provenance.Reference is not null)
                    RequiredText(identity.Provenance.Reference, "$.identity.provenance.reference");
            }
        }

        var trees = Names(manifest.Trees, static t => t.Name, "$.trees");
        var roles = Names(manifest.Roles, static r => r.Name, "$.roles");
        Names(manifest.Subscriptions, static s => s.Name, "$.subscriptions");
        Names(manifest.McpTools, static t => t.Name, "$.mcpTools", 96);

        HashSet<string> Names<T>(T[]? declarations, Func<T, string> name, string path, int maxLength = 128) where T : class
        {
            HashSet<string> names = new(StringComparer.Ordinal);
            if (declarations is null)
                Error("required", path, "A declaration array is required.");
            else
                for (var i = 0; i < declarations.Length; i++)
                {
                    var itemPath = $"{path}[{i}]";
                    if (declarations[i] is not { } item)
                    {
                        Error("required", itemPath, "A declaration cannot be null.");
                        continue;
                    }
                    var value = name(item);
                    if (!IsName(value, maxLength))
                        Error("name", itemPath + ".name", $"Expected a lowercase ASCII name of 1-{maxLength} characters.");
                    else if (!names.Add(value))
                        Error("duplicate", itemPath + ".name", "Names must be unique within a section.");
                }
            return names;
        }

        void TreeReference(string? tree, AppSlug? app, string path)
        {
            if (!IsName(tree))
                Error("name", path + ".tree", "Expected a local tree name, not a path or wildcard.");
            if (app is { } source && !AppSlug.TryParse(source.Value, out _))
                Error("slug", path + ".app", "Invalid source app slug.");
            if ((app is null || app == identity?.Slug) && (tree is null || !trees.Contains(tree)))
                Error("reference", path + ".tree", "The local tree must be declared by this app.");
        }

        HashSet<string>? adoptedTrees = null;
        if (manifest.Trees is not null)
            for (var i = 0; i < manifest.Trees.Length; i++)
            {
                if (manifest.Trees[i] is not { } tree) continue;
                var path = $"$.trees[{i}]";
                if (tree.AdoptedTreeId is { } adopted)
                {
                    if (string.IsNullOrWhiteSpace(adopted) ||
                        adopted.StartsWith("a/", StringComparison.Ordinal) ||
                        adopted.StartsWith("_lattice_", StringComparison.Ordinal) ||
                        adopted.StartsWith("sys-", StringComparison.Ordinal) ||
                        adopted.StartsWith("t/", StringComparison.Ordinal))
                        Error("adoption", path + ".adoptedTreeId", "Expected a non-empty pre-app physical tree id outside structural and reserved namespaces.");
                    else if (!(adoptedTrees ??= new(StringComparer.Ordinal)).Add(adopted))
                        Error("duplicate", path + ".adoptedTreeId", "A physical tree may be adopted only once per manifest.");
                }
                if (tree.ShardCount is < 1 or > 4096)
                    Error("shape", path + ".shardCount", "Physical shard count must be between 1 and 4096.");
                if (tree.VirtualShardCount is < 1)
                    Error("shape", path + ".virtualShardCount", "Virtual shard count must be positive.");
                if (tree.ShardCount > tree.VirtualShardCount)
                    Error("shape", path + ".shardCount", "Physical shards cannot exceed virtual slots.");
                if (tree.MaxLeafKeys is < 2)
                    Error("shape", path + ".maxLeafKeys", "Leaf capacity must be at least two.");
                if (tree.MaxInternalChildren is < 3)
                    Error("shape", path + ".maxInternalChildren", "Internal capacity must be at least three.");
                if (tree.WalPartitions is < 1)
                    Error("shape", path + ".walPartitions", "WAL partition count must be positive.");
                if (tree.SoftDeleteDuration is { } retention && retention <= TimeSpan.Zero)
                    Error("retention", path + ".softDeleteDuration", "Soft-delete retention must be positive.");
            }

        if (manifest.Roles is not null)
            for (var i = 0; i < manifest.Roles.Length; i++)
            {
                if (manifest.Roles[i] is not { } role) continue;
                var path = $"$.roles[{i}]";
                if (role.Operations == LatticeOperation.None || (role.Operations & ~RoleOperations) != 0)
                    Error("operations", path + ".operations", "Expected a non-empty mask of known tree-scoped operations.");
                if (role.Scopes is null || role.Scopes.Length == 0)
                    Error("required", path + ".scopes", "At least one scope is required.");
                else
                    for (var s = 0; s < role.Scopes.Length; s++)
                    {
                        var scopePath = $"{path}.scopes[{s}]";
                        if (role.Scopes[s] is not { } scope)
                        {
                            Error("required", scopePath, "A scope cannot be null.");
                            continue;
                        }
                        TreeReference(scope.Tree, scope.App, scopePath);
                        if (!Enum.IsDefined(scope.Kind))
                            Error("scope", scopePath + ".kind", "Unknown scope kind.");
                        else if (scope.Kind == LatticeScopeKind.Tree ? scope.KeyOrPrefix is not null : string.IsNullOrEmpty(scope.KeyOrPrefix))
                            Error("scope", scopePath + ".keyOrPrefix", "Only key and prefix scopes require a non-empty keyOrPrefix.");
                    }
            }

        if (manifest.Replication is not null)
        {
            HashSet<string> enrolled = new(StringComparer.Ordinal);
            for (var i = 0; i < manifest.Replication.Length; i++)
            {
                var path = $"$.replication[{i}]";
                if (manifest.Replication[i] is not { } replication)
                {
                    Error("required", path, "A replication declaration cannot be null.");
                    continue;
                }
                TreeReference(replication.Tree, null, path);
                if (replication.Tree is not null && !enrolled.Add(replication.Tree))
                    Error("duplicate", path + ".tree", "A tree may have only one replication mode.");
                if (!Enum.IsDefined(replication.MergeMode))
                    Error("mergeMode", path + ".mergeMode", "Unknown replication merge mode.");
            }
        }

        if (manifest.Schema is not null)
        {
            HashSet<string> bound = new(StringComparer.Ordinal);
            for (var i = 0; i < manifest.Schema.Length; i++)
            {
                var path = $"$.schema[{i}]";
                if (manifest.Schema[i] is not { } schema)
                {
                    Error("required", path, "A schema declaration cannot be null.");
                    continue;
                }
                TreeReference(schema.Tree, null, path);
                if (schema.Tree is not null && !bound.Add(schema.Tree))
                    Error("duplicate", path + ".tree", "A tree may have only one schema binding.");
                RequiredText(schema.Family, path + ".family");
                if (schema.Version < 1)
                    Error("version", path + ".version", "Schema envelope versions must be positive.");
            }
        }

        if (manifest.Subscriptions is not null)
            for (var i = 0; i < manifest.Subscriptions.Length; i++)
            {
                if (manifest.Subscriptions[i] is not { } subscription) continue;
                var path = $"$.subscriptions[{i}]";
                TreeReference(subscription.Tree, subscription.App, path);
                if (subscription.KeyPrefix is { Length: 0 })
                    Error("scope", path + ".keyPrefix", "Omit the key prefix to observe the whole tree.");
            }

        if (manifest.McpTools is not null)
            for (var i = 0; i < manifest.McpTools.Length; i++)
            {
                if (manifest.McpTools[i] is not { } tool) continue;
                var path = $"$.mcpTools[{i}]";
                RequiredText(tool.Description, path + ".description");
                if (tool.Role is null || !roles.Contains(tool.Role))
                    Error("reference", path + ".role", "The tool's role must be declared by this app.");
            }

        if (previous is not null)
        {
            if (!Validate(previous).IsValid)
                Error("previous", "$", "The previous manifest must be valid before checking an upgrade.");
            else
            {
                if (identity?.Slug != previous.Identity.Slug)
                    Error("identity", "$.identity.slug", "An upgrade cannot change the app slug.");
                if (manifest.Trees is not null)
                    foreach (var tree in manifest.Trees)
                    {
                        if (tree is null) continue;
                        foreach (var oldTree in previous.Trees)
                            if (tree.Name == oldTree.Name && tree.VirtualShardCount != oldTree.VirtualShardCount)
                                Error("immutable", "$.trees", "An existing tree's virtual shard pin cannot change, including omission.");
                    }
            }
        }
        return new(manifest, errors);
    }

    internal static bool IsName(string? value, int maxLength = 128)
    {
        if (string.IsNullOrEmpty(value) || value.Length > maxLength || value[0] is < 'a' or > 'z')
            return false;
        foreach (var c in value)
            if (c is not (>= 'a' and <= 'z') and not (>= '0' and <= '9') and not '-' and not '_')
                return false;
        return true;
    }
}
