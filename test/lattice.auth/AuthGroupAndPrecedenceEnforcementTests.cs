using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// End-to-end acceptance matrix for the two authorization behaviours the base
/// enforcement suite (<see cref="LatticeEnforcementIntegrationTests"/>) does not
/// exercise: <b>per-group</b> control of each operation at every scope tier, and
/// the rule-combination precedence the decision engine applies - most-specific
/// scope wins across tiers, and within a tier a user rule outranks a group rule
/// and a deny outranks an allow (deny-override). Runs against the live
/// <see cref="PolicyAccessGate"/> through <see cref="AuthClusterFixture"/> with a
/// fail-closed default-deny policy, so an allowed operation persists / observes
/// and a denied one throws (writes) or reports empty (reads).
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AuthGroupAndPrecedenceEnforcementTests
{
    private AuthClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    private static string? Str(byte[]? value) => value is null ? null : Encoding.UTF8.GetString(value);

    private async Task GrantAsync(params LatticeAuthorizationRule[] rules)
    {
        foreach (var rule in rules)
        {
            await _fixture.Store.PutRuleAsync(rule);
        }

        await _fixture.RebuildPolicyAsync();
    }

    private async Task SeedAsync(string treeId, params (string Key, string Value)[] entries)
    {
        using (AuthClusterFixture.AsSubject(AuthClusterFixture.BootstrapAdmin))
        {
            var lattice = _fixture.Lattice(treeId);
            foreach (var (key, value) in entries)
            {
                await lattice.SetAsync(key, Bytes(value));
            }
        }

        await _fixture.RebuildPolicyAsync();
    }

    private async Task<string?> ReadBackAsync(string treeId, string key)
    {
        using (AuthClusterFixture.AsSubject(AuthClusterFixture.BootstrapAdmin))
        {
            return Str(await _fixture.Lattice(treeId).GetAsync(key));
        }
    }

    // ---- Per-group control of each operation, at each scope tier ----

    [Test]
    public async Task Group_tree_write_grant_admits_members_and_refuses_non_members()
    {
        const string tree = "grp-tree-write";
        await GrantAsync(new LatticeAuthorizationRule(
            "editors-write", LatticeSubjectSelector.Group("editors"), LatticeScope.Tree(tree),
            LatticeOperation.Write, LatticeEffect.Allow));

        using (AuthClusterFixture.AsSubject("alice", "editors"))
        {
            await _fixture.Lattice(tree).SetAsync("k", Bytes("v"));
        }

        Assert.That(await ReadBackAsync(tree, "k"), Is.EqualTo("v"),
            "a member of the granted group inherits the group's write grant");

        using (AuthClusterFixture.AsSubject("mallory", "guests"))
        {
            Assert.That(
                async () => await _fixture.Lattice(tree).SetAsync("k2", Bytes("v2")),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "a caller who is not in the granted group is denied");
        }

        Assert.That(await ReadBackAsync(tree, "k2"), Is.Null);
    }

    [Test]
    public async Task Group_key_read_grant_admits_a_member_and_hides_the_key_from_non_members()
    {
        const string tree = "grp-key-read";
        await SeedAsync(tree, ("secret", "classified"));
        await GrantAsync(new LatticeAuthorizationRule(
            "analysts-read", LatticeSubjectSelector.Group("analysts"), LatticeScope.Key(tree, "secret"),
            LatticeOperation.Read, LatticeEffect.Allow));

        using (AuthClusterFixture.AsSubject("bob", "analysts"))
        {
            Assert.That(Str(await _fixture.Lattice(tree).GetAsync("secret")), Is.EqualTo("classified"),
                "a member of the key-scoped group may read the key");
        }

        using (AuthClusterFixture.AsSubject("carol", "interns"))
        {
            Assert.That(await _fixture.Lattice(tree).GetAsync("secret"), Is.Null,
                "a non-member observes the key as absent");
        }
    }

    [Test]
    public async Task Group_prefix_range_read_grant_filters_a_scan_to_the_authorized_prefix()
    {
        const string tree = "grp-prefix-range";
        await SeedAsync(tree, ("eu/1", "1"), ("eu/2", "2"), ("us/1", "3"));
        await GrantAsync(new LatticeAuthorizationRule(
            "eu-readers", LatticeSubjectSelector.Group("eu-readers"), LatticeScope.Prefix(tree, "eu/"),
            LatticeOperation.RangeRead, LatticeEffect.Allow));

        var observed = new List<string>();
        using (AuthClusterFixture.AsSubject("dave", "eu-readers"))
        {
            await foreach (var key in _fixture.Lattice(tree).KeysAsync())
            {
                observed.Add(key);
            }
        }

        Assert.That(observed, Is.EquivalentTo(new[] { "eu/1", "eu/2" }),
            "a group prefix RangeRead grant admits only keys under that prefix");
    }

    [Test]
    public async Task Group_delete_and_crdt_grants_are_per_operation_and_do_not_imply_each_other()
    {
        const string tree = "grp-delete-crdt";
        await SeedAsync(tree, ("d", "v"));
        await GrantAsync(new LatticeAuthorizationRule(
            "ops-delete", LatticeSubjectSelector.Group("ops"), LatticeScope.Tree(tree),
            LatticeOperation.Delete, LatticeEffect.Allow));

        var delta = JsonLatticeSerializer<PnCounterDelta>.Default.Serialize(new PnCounterDelta
        {
            Increments = new Dictionary<string, long> { ["site-a"] = 1 },
        });

        using (AuthClusterFixture.AsSubject("erin", "ops"))
        {
            Assert.That(await _fixture.Lattice(tree).DeleteAsync("d"), Is.True,
                "a group Delete grant permits the delete");

            Assert.That(
                async () => await _fixture.Lattice(tree).ApplyCrdtDeltaAsync("c", LatticeMergeMode.PnCounter, delta),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "a group Delete grant does not imply CrdtApply");
        }

        await GrantAsync(new LatticeAuthorizationRule(
            "ops-crdt", LatticeSubjectSelector.Group("ops"), LatticeScope.Tree(tree),
            LatticeOperation.CrdtApply, LatticeEffect.Allow));

        using (AuthClusterFixture.AsSubject("erin", "ops"))
        {
            await _fixture.Lattice(tree).ApplyCrdtDeltaAsync("c", LatticeMergeMode.PnCounter, delta);
        }
    }

    // ---- Precedence: most-specific scope wins across tiers ----

    [Test]
    public async Task Key_scope_allow_carves_an_exception_out_of_a_tree_scope_deny()
    {
        const string tree = "prec-key-over-tree";
        await GrantAsync(
            new LatticeAuthorizationRule(
                "tree-deny", LatticeSubjectSelector.Group("staff"), LatticeScope.Tree(tree),
                LatticeOperation.Write, LatticeEffect.Deny),
            new LatticeAuthorizationRule(
                "key-allow", LatticeSubjectSelector.Group("staff"), LatticeScope.Key(tree, "allowed"),
                LatticeOperation.Write, LatticeEffect.Allow));

        using (AuthClusterFixture.AsSubject("frank", "staff"))
        {
            await _fixture.Lattice(tree).SetAsync("allowed", Bytes("ok"));

            Assert.That(
                async () => await _fixture.Lattice(tree).SetAsync("other", Bytes("no")),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "the tree-scope deny still governs every key the key-scope allow does not name");
        }

        Assert.That(await ReadBackAsync(tree, "allowed"), Is.EqualTo("ok"),
            "the most-specific key-scope allow wins over the broader tree-scope deny");
        Assert.That(await ReadBackAsync(tree, "other"), Is.Null);
    }

    [Test]
    public async Task Key_scope_deny_overrides_a_tree_scope_allow_for_that_key_only()
    {
        const string tree = "prec-keydeny-over-treeallow";
        await GrantAsync(
            new LatticeAuthorizationRule(
                "tree-allow", LatticeSubjectSelector.Group("team"), LatticeScope.Tree(tree),
                LatticeOperation.Write, LatticeEffect.Allow),
            new LatticeAuthorizationRule(
                "key-deny", LatticeSubjectSelector.Group("team"), LatticeScope.Key(tree, "locked"),
                LatticeOperation.Write, LatticeEffect.Deny));

        using (AuthClusterFixture.AsSubject("grace", "team"))
        {
            await _fixture.Lattice(tree).SetAsync("free", Bytes("ok"));

            Assert.That(
                async () => await _fixture.Lattice(tree).SetAsync("locked", Bytes("no")),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "the most-specific key-scope deny wins over the broader tree-scope allow");
        }

        Assert.That(await ReadBackAsync(tree, "free"), Is.EqualTo("ok"));
        Assert.That(await ReadBackAsync(tree, "locked"), Is.Null);
    }

    // ---- Precedence: within a tier, deny overrides allow ----

    [Test]
    public async Task Deny_overrides_allow_within_the_same_scope_tier()
    {
        const string tree = "prec-deny-override";
        // Two group rules at the same (tree) tier: one allows Write, one denies it.
        // A subject in both groups must be denied - deny overrides allow at equal
        // specificity.
        await GrantAsync(
            new LatticeAuthorizationRule(
                "allow-writers", LatticeSubjectSelector.Group("writers"), LatticeScope.Tree(tree),
                LatticeOperation.Write, LatticeEffect.Allow),
            new LatticeAuthorizationRule(
                "deny-suspended", LatticeSubjectSelector.Group("suspended"), LatticeScope.Tree(tree),
                LatticeOperation.Write, LatticeEffect.Deny));

        using (AuthClusterFixture.AsSubject("heidi", "writers", "suspended"))
        {
            Assert.That(
                async () => await _fixture.Lattice(tree).SetAsync("k", Bytes("v")),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "at equal scope a group deny overrides a sibling group allow");
        }

        Assert.That(await ReadBackAsync(tree, "k"), Is.Null);

        // A subject in only the allowing group is unaffected by the deny.
        using (AuthClusterFixture.AsSubject("ivan", "writers"))
        {
            await _fixture.Lattice(tree).SetAsync("k2", Bytes("v2"));
        }

        Assert.That(await ReadBackAsync(tree, "k2"), Is.EqualTo("v2"),
            "a subject not in the denied group keeps the group allow");
    }

    [Test]
    public async Task User_rule_beats_group_rule_at_equal_scope()
    {
        const string tree = "prec-user-beats-group";
        // A group deny and a user allow at the same tree scope: the user-specific
        // rule is more specific and wins, so the named user may write even though
        // the user's group is denied.
        await GrantAsync(
            new LatticeAuthorizationRule(
                "group-deny", LatticeSubjectSelector.Group("contractors"), LatticeScope.Tree(tree),
                LatticeOperation.Write, LatticeEffect.Deny),
            new LatticeAuthorizationRule(
                "user-allow", LatticeSubjectSelector.User("judy"), LatticeScope.Tree(tree),
                LatticeOperation.Write, LatticeEffect.Allow));

        using (AuthClusterFixture.AsSubject("judy", "contractors"))
        {
            await _fixture.Lattice(tree).SetAsync("k", Bytes("v"));
        }

        Assert.That(await ReadBackAsync(tree, "k"), Is.EqualTo("v"),
            "a user-specific allow outranks a group-level deny at equal scope");

        // Another member of the denied group, with no user allow, stays denied.
        using (AuthClusterFixture.AsSubject("ken", "contractors"))
        {
            Assert.That(
                async () => await _fixture.Lattice(tree).SetAsync("k2", Bytes("v2")),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "the group deny still governs a member with no user-specific allow");
        }

        Assert.That(await ReadBackAsync(tree, "k2"), Is.Null);
    }
}
