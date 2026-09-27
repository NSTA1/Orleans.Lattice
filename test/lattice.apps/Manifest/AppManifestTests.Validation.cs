using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

public sealed partial class AppManifestTests
{
    [TestCase("null")]
    [TestCase("identity")]
    [TestCase("slug")]
    [TestCase("version")]
    [TestCase("provenance")]
    [TestCase("source")]
    [TestCase("publisher")]
    [TestCase("reference")]
    [TestCase("trees")]
    [TestCase("roles")]
    [TestCase("subscriptions")]
    [TestCase("tools")]
    [TestCase("tree-entry")]
    [TestCase("role-entry")]
    [TestCase("subscription-entry")]
    [TestCase("tool-entry")]
    [TestCase("replication-entry")]
    [TestCase("schema-entry")]
    [TestCase("scope-entry")]
    [TestCase("scopes")]
    [TestCase("empty-scopes")]
    [TestCase("empty-mask")]
    [TestCase("unknown-mask")]
    [TestCase("invalid-tree")]
    [TestCase("missing-tree")]
    [TestCase("invalid-app")]
    [TestCase("invalid-kind")]
    [TestCase("tree-key")]
    [TestCase("key-empty")]
    [TestCase("prefix-empty")]
    [TestCase("missing-replication-tree")]
    [TestCase("invalid-mode")]
    [TestCase("missing-schema-tree")]
    [TestCase("schema-family")]
    [TestCase("schema-version")]
    [TestCase("subscription-tree")]
    [TestCase("subscription-prefix")]
    [TestCase("tool-description")]
    [TestCase("tool-role")]
    public void Validate_malformed_programmatic_records_return_errors(string mutation)
    {
        var m = Manifest;
        var role = m.Roles[0];
        var scope = role.Scopes[0];
        AppManifest? invalid = mutation switch
        {
            "null" => null,
            "identity" => m with { Identity = null! },
            "slug" => m with { Identity = m.Identity with { Slug = default } },
            "version" => m with { Identity = m.Identity with { Version = default } },
            "provenance" => m with { Identity = m.Identity with { Provenance = null! } },
            "source" => m with { Identity = m.Identity with { Provenance = new() { Source = null! } } },
            "publisher" => m with { Identity = m.Identity with { Provenance = new() { Publisher = " " } } },
            "reference" => m with { Identity = m.Identity with { Provenance = new() { Reference = "" } } },
            "trees" => m with { Trees = null! },
            "roles" => m with { Roles = null! },
            "subscriptions" => m with { Subscriptions = null! },
            "tools" => m with { McpTools = null! },
            "tree-entry" => m with { Trees = [null!] },
            "role-entry" => m with { Roles = [null!] },
            "subscription-entry" => m with { Subscriptions = [null!] },
            "tool-entry" => m with { McpTools = [null!] },
            "replication-entry" => m with { Replication = [null!] },
            "schema-entry" => m with { Schema = [null!] },
            "scope-entry" => m with { Roles = [role with { Scopes = [null!] }] },
            "scopes" => m with { Roles = [role with { Scopes = null! }] },
            "empty-scopes" => m with { Roles = [role with { Scopes = [] }] },
            "empty-mask" => m with { Roles = [role with { Operations = LatticeOperation.None }] },
            "unknown-mask" => m with { Roles = [role with { Operations = (LatticeOperation)(1 << 29) }] },
            "invalid-tree" => m with { Roles = [role with { Scopes = [scope with { Tree = "../escape" }] }] },
            "missing-tree" => m with { Roles = [role with { Scopes = [scope with { Tree = "missing" }] }] },
            "invalid-app" => m with { Roles = [role with { Scopes = [scope with { App = default(AppSlug) }] }] },
            "invalid-kind" => m with { Roles = [role with { Scopes = [scope with { Kind = (LatticeScopeKind)99 }] }] },
            "tree-key" => m with { Roles = [role with { Scopes = [scope with { KeyOrPrefix = "key" }] }] },
            "key-empty" => m with { Roles = [role with { Scopes = [scope with { Kind = LatticeScopeKind.Key }] }] },
            "prefix-empty" => m with { Roles = [role with { Scopes = [scope with { Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "" }] }] },
            "missing-replication-tree" => m with { Replication = [m.Replication![0] with { Tree = null! }] },
            "invalid-mode" => m with { Replication = [m.Replication![0] with { MergeMode = (LatticeMergeMode)999 }] },
            "missing-schema-tree" => m with { Schema = [m.Schema![0] with { Tree = null! }] },
            "schema-family" => m with { Schema = [m.Schema![0] with { Family = null! }] },
            "schema-version" => m with { Schema = [m.Schema![0] with { Version = 0 }] },
            "subscription-tree" => m with { Subscriptions = [m.Subscriptions[0] with { Tree = "missing", App = null }] },
            "subscription-prefix" => m with { Subscriptions = [m.Subscriptions[0] with { KeyPrefix = "" }] },
            "tool-description" => m with { McpTools = [m.McpTools[0] with { Description = null! }] },
            "tool-role" => m with { McpTools = [m.McpTools[0] with { Role = null! }] },
            _ => throw new ArgumentOutOfRangeException(nameof(mutation)),
        };
        var result = AppManifestValidator.Validate(invalid);
        Assert.That(result.IsValid, Is.False);
        Assert.That(result.Manifest, Is.Null);
        Assert.That(result.Errors, Is.Not.Empty);
        Assert.That(result.Errors.All(e => e.Path.StartsWith('$') && e.Message.Length > 0 && e.Code.Length > 0), Is.True);
    }

    [TestCase("trees")]
    [TestCase("roles")]
    [TestCase("subscriptions")]
    [TestCase("tools")]
    [TestCase("replication")]
    [TestCase("schema")]
    public void Validate_duplicate_declarations_fail_activation(string section)
    {
        var m = Manifest;
        var invalid = section switch
        {
            "trees" => m with { Trees = [m.Trees[0], m.Trees[0]] },
            "roles" => m with { Roles = [m.Roles[0], m.Roles[0]] },
            "subscriptions" => m with { Subscriptions = [m.Subscriptions[0], m.Subscriptions[0]] },
            "tools" => m with { McpTools = [m.McpTools[0], m.McpTools[0]] },
            "replication" => m with { Replication = [m.Replication![0], m.Replication[0]] },
            "schema" => m with { Schema = [m.Schema![0], m.Schema[0]] },
            _ => throw new ArgumentOutOfRangeException(nameof(section)),
        };
        Assert.That(AppManifestValidator.Validate(invalid).Errors.Any(e => e.Code == "duplicate"), Is.True);
    }

    [TestCase("shards", 0, false)]
    [TestCase("shards", 1, true)]
    [TestCase("shards", 4096, true)]
    [TestCase("shards", 4097, false)]
    [TestCase("virtual", 0, false)]
    [TestCase("virtual", 1, false)]
    [TestCase("virtual", 2, true)]
    [TestCase("leaf", 1, false)]
    [TestCase("leaf", 2, true)]
    [TestCase("internal", 2, false)]
    [TestCase("internal", 3, true)]
    [TestCase("wal", 0, false)]
    [TestCase("wal", 1, true)]
    [TestCase("retention", -1, false)]
    [TestCase("retention", 0, false)]
    [TestCase("retention", 1, true)]
    public void Validate_physical_shape_enforces_boundaries(string field, int value, bool valid)
    {
        var m = Manifest;
        var t = m.Trees[0];
        t = field switch
        {
            "shards" => t with { ShardCount = value, VirtualShardCount = 4096 },
            "virtual" => t with { VirtualShardCount = value },
            "leaf" => t with { MaxLeafKeys = value },
            "internal" => t with { MaxInternalChildren = value },
            "wal" => t with { WalPartitions = value },
            "retention" => t with { SoftDeleteDuration = TimeSpan.FromTicks(value) },
            _ => throw new ArgumentOutOfRangeException(nameof(field)),
        };
        Assert.That(AppManifestValidator.Validate(m with { Trees = [t] }).IsValid, Is.EqualTo(valid));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("../bad")]
    [TestCase("*")]
    [TestCase("bad/name")]
    [TestCase("Bad")]
    [TestCase("bad\n")]
    public void Validate_names_reject_paths_wildcards_and_noncanonical_text(string? name)
    {
        var m = Manifest;
        Assert.That(AppManifestValidator.Validate(m with { Trees = [m.Trees[0] with { Name = name! }] }).IsValid, Is.False);
    }

    [TestCase(96, true)]
    [TestCase(97, false)]
    public void Validate_tool_names_leave_room_for_slug_namespace(int length, bool valid)
    {
        var m = Manifest;
        Assert.That(AppManifestValidator.Validate(m with { McpTools = [m.McpTools[0] with { Name = new string('a', length) }] }).IsValid,
            Is.EqualTo(valid));
    }

    [Test]
    public void Validate_key_scope_and_self_subscriptions_are_valid()
    {
        var m = Manifest;
        var next = m with
        {
            Roles = [m.Roles[0] with { Scopes = [new() { Tree = "records", Kind = LatticeScopeKind.Key, KeyOrPrefix = "key" }] }],
            Subscriptions = [new() { Name = "self", Tree = "records", App = m.Identity.Slug }],
        };
        Assert.That(AppManifestValidator.Validate(next).IsValid, Is.True);
        Assert.That(AppManifestValidator.Validate(next with
        {
            Subscriptions = [next.Subscriptions[0] with { Tree = "missing" }],
        }).IsValid, Is.False);
    }

    [Test]
    public void Validate_upgrade_preserves_virtual_shard_pin_even_when_omitted()
    {
        var m = Manifest;
        var next = m with { Identity = m.Identity with { Version = AppVersion.Parse("2.0.0") } };
        Assert.That(AppManifestValidator.Validate(next, m).IsValid, Is.True);
        Assert.That(AppManifestValidator.Validate(next with { Trees = [m.Trees[0] with { VirtualShardCount = 32 }] }, m)
            .Errors.Any(e => e.Code == "immutable"), Is.True);
        Assert.That(AppManifestValidator.Validate(next with { Trees = [m.Trees[0] with { VirtualShardCount = null }] }, m)
            .Errors.Any(e => e.Code == "immutable"), Is.True);
        Assert.That(AppManifestValidator.Validate(next with { Identity = next.Identity with { Slug = AppSlug.Parse("another") } }, m)
            .Errors.Any(e => e.Code == "identity"), Is.True);
        Assert.That(AppManifestValidator.Validate(next, m with { Trees = null! }).Errors.Any(e => e.Code == "previous"), Is.True);
    }

    [Test]
    public void Validate_reports_multiple_errors_without_success_shaped_fallback()
    {
        var m = Manifest;
        var result = AppManifestValidator.Validate(m with { Identity = null!, Trees = null!, Roles = null! });
        Assert.That(result.Manifest, Is.Null);
        Assert.That(result.Errors.Count, Is.GreaterThanOrEqualTo(3));
        Assert.Throws<NotSupportedException>(() => ((IList<AppManifestError>)result.Errors).Clear());
    }
}
