using System.Text;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// End-to-end enforcement acceptance matrix for the authorization control point.
/// With <see cref="PolicyAccessGate"/> live (via <see cref="AuthClusterFixture"/>)
/// and a fail-closed default-deny policy, every user-originated operation class at
/// the data-plane <c>LatticeGrain</c> and the cross-tree coordinator is
/// authorized: allowed operations persist / observe, denied operations throw
/// <see cref="LatticeAuthorizationDeniedException"/> (writes) or report empty
/// (reads) and never leave partial state, atomic and cross-tree batches abort
/// wholesale on any denied leg, range deletes are all-or-nothing, and a bootstrap
/// administrator is the root-of-trust bypass.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeEnforcementIntegrationTests
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

    /// <summary>Authors one or more rules and forces a synchronous snapshot rebuild.</summary>
    private async Task GrantAsync(params LatticeAuthorizationRule[] rules)
    {
        foreach (var rule in rules)
        {
            await _fixture.Store.PutRuleAsync(rule);
        }

        await _fixture.RebuildPolicyAsync();
    }

    /// <summary>Reads a key back under the bootstrap administrator (which bypasses policy).</summary>
    private async Task<string?> ReadBackAsync(string treeId, string key)
    {
        using (AuthClusterFixture.AsSubject(AuthClusterFixture.BootstrapAdmin))
        {
            return Str(await _fixture.Lattice(treeId).GetAsync(key));
        }
    }

    [Test]
    public async Task Set_with_no_matching_rule_is_denied_and_does_not_persist()
    {
        const string tree = "enf-set-deny";
        await _fixture.RebuildPolicyAsync();

        using (AuthClusterFixture.AsSubject("mallory"))
        {
            Assert.That(
                async () => await _fixture.Lattice(tree).SetAsync("k", Bytes("v")),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "a fail-closed default-deny policy must reject a write with no granting rule");
        }

        Assert.That(await ReadBackAsync(tree, "k"), Is.Null, "the denied write must not have persisted");
    }

    [Test]
    public async Task Set_with_a_write_rule_is_allowed_and_persists()
    {
        const string tree = "enf-set-allow";
        await GrantAsync(new LatticeAuthorizationRule(
            "w", LatticeSubjectSelector.User("writer"), LatticeScope.Tree(tree),
            LatticeOperation.Write, LatticeEffect.Allow));

        using (AuthClusterFixture.AsSubject("writer"))
        {
            await _fixture.Lattice(tree).SetAsync("k", Bytes("v"));
        }

        Assert.That(await ReadBackAsync(tree, "k"), Is.EqualTo("v"), "an authorized write must persist");
    }

    [Test]
    public async Task Delete_requires_delete_authorization_distinct_from_write()
    {
        const string tree = "enf-delete";
        await GrantAsync(new LatticeAuthorizationRule(
            "w", LatticeSubjectSelector.User("del"), LatticeScope.Tree(tree),
            LatticeOperation.Write, LatticeEffect.Allow));

        using (AuthClusterFixture.AsSubject("del"))
        {
            await _fixture.Lattice(tree).SetAsync("k", Bytes("v"));

            Assert.That(
                async () => await _fixture.Lattice(tree).DeleteAsync("k"),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "a Write grant does not imply Delete");
        }

        Assert.That(await ReadBackAsync(tree, "k"), Is.EqualTo("v"), "the denied delete must not have removed the value");

        await GrantAsync(new LatticeAuthorizationRule(
            "d", LatticeSubjectSelector.User("del"), LatticeScope.Tree(tree),
            LatticeOperation.Delete, LatticeEffect.Allow));

        using (AuthClusterFixture.AsSubject("del"))
        {
            Assert.That(await _fixture.Lattice(tree).DeleteAsync("k"), Is.True, "a Delete grant permits the delete");
        }

        Assert.That(await ReadBackAsync(tree, "k"), Is.Null, "the authorized delete removed the value");
    }

    [Test]
    public async Task Point_read_of_a_denied_key_returns_empty_and_does_not_throw()
    {
        const string tree = "enf-read-deny";
        using (AuthClusterFixture.AsSubject(AuthClusterFixture.BootstrapAdmin))
        {
            await _fixture.Lattice(tree).SetAsync("k", Bytes("secret"));
        }

        await _fixture.RebuildPolicyAsync();

        using (AuthClusterFixture.AsSubject("nobody"))
        {
            var lattice = _fixture.Lattice(tree);
            Assert.That(await lattice.GetAsync("k"), Is.Null, "a denied point read reports the key as absent, not throwing");
            Assert.That(await lattice.ExistsAsync("k"), Is.False, "a denied existence probe reports absent");
        }
    }

    [Test]
    public async Task Range_scan_filters_to_the_authorized_prefix()
    {
        const string tree = "enf-range-prefix";
        using (AuthClusterFixture.AsSubject(AuthClusterFixture.BootstrapAdmin))
        {
            var seed = _fixture.Lattice(tree);
            await seed.SetAsync("a:1", Bytes("1"));
            await seed.SetAsync("a:2", Bytes("2"));
            await seed.SetAsync("b:1", Bytes("3"));
        }

        await GrantAsync(new LatticeAuthorizationRule(
            "ra", LatticeSubjectSelector.User("reader"), LatticeScope.Prefix(tree, "a:"),
            LatticeOperation.RangeRead, LatticeEffect.Allow));

        var observed = new List<string>();
        using (AuthClusterFixture.AsSubject("reader"))
        {
            await foreach (var key in _fixture.Lattice(tree).KeysAsync())
            {
                observed.Add(key);
            }
        }

        Assert.That(observed, Is.EquivalentTo(new[] { "a:1", "a:2" }),
            "a prefix-scoped RangeRead grant admits only the keys under that prefix");
    }

    [Test]
    public async Task Range_scan_with_no_matching_rule_yields_nothing()
    {
        const string tree = "enf-range-deny";
        using (AuthClusterFixture.AsSubject(AuthClusterFixture.BootstrapAdmin))
        {
            await _fixture.Lattice(tree).SetAsync("a:1", Bytes("1"));
        }

        await _fixture.RebuildPolicyAsync();

        var observed = new List<string>();
        using (AuthClusterFixture.AsSubject("nobody"))
        {
            await foreach (var key in _fixture.Lattice(tree).KeysAsync())
            {
                observed.Add(key);
            }
        }

        Assert.That(observed, Is.Empty, "fail-closed: a range scan with no granting rule observes nothing");
    }

    [Test]
    public async Task GetMany_filters_out_unauthorized_keys()
    {
        const string tree = "enf-getmany";
        using (AuthClusterFixture.AsSubject(AuthClusterFixture.BootstrapAdmin))
        {
            var seed = _fixture.Lattice(tree);
            await seed.SetAsync("a:1", Bytes("1"));
            await seed.SetAsync("b:1", Bytes("2"));
        }

        await GrantAsync(new LatticeAuthorizationRule(
            "ra", LatticeSubjectSelector.User("mreader"), LatticeScope.Prefix(tree, "a:"),
            LatticeOperation.Read, LatticeEffect.Allow));

        using (AuthClusterFixture.AsSubject("mreader"))
        {
            var got = await _fixture.Lattice(tree).GetManyAsync(new List<string> { "a:1", "b:1" });
            Assert.That(got.Keys, Is.EquivalentTo(new[] { "a:1" }),
                "GetMany prunes keys the caller is not authorized to read");
        }
    }

    [Test]
    public async Task Atomic_SetMany_aborts_wholesale_when_one_key_is_denied()
    {
        const string tree = "enf-atomic";
        // Grant Write only on the "ok" key, leaving "bad" denied.
        await GrantAsync(new LatticeAuthorizationRule(
            "ok", LatticeSubjectSelector.User("atom"), LatticeScope.Key(tree, "ok"),
            LatticeOperation.Write, LatticeEffect.Allow));

        using (AuthClusterFixture.AsSubject("atom"))
        {
            Assert.That(
                async () => await _fixture.Lattice(tree).SetManyAtomicAsync(new List<KeyValuePair<string, byte[]>>
                {
                    new("ok", Bytes("1")),
                    new("bad", Bytes("2")),
                }),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "an atomic batch with one denied key must abort before any leg is applied");
        }

        Assert.That(await ReadBackAsync(tree, "ok"), Is.Null, "the authorized leg must NOT persist when a sibling leg is denied");
        Assert.That(await ReadBackAsync(tree, "bad"), Is.Null, "the denied leg must not persist");
    }

    [Test]
    public async Task Nonatomic_SetMany_with_a_denied_key_aborts_before_applying_anything()
    {
        const string tree = "enf-setmany";
        await GrantAsync(new LatticeAuthorizationRule(
            "ok", LatticeSubjectSelector.User("multi"), LatticeScope.Key(tree, "ok"),
            LatticeOperation.Write, LatticeEffect.Allow));

        using (AuthClusterFixture.AsSubject("multi"))
        {
            Assert.That(
                async () => await _fixture.Lattice(tree).SetManyAsync(new List<KeyValuePair<string, byte[]>>
                {
                    new("ok", Bytes("1")),
                    new("bad", Bytes("2")),
                }),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "every key of a batch is authorized before any write is applied");
        }

        Assert.That(await ReadBackAsync(tree, "ok"), Is.Null, "no key of the batch persists when one is denied");
        Assert.That(await ReadBackAsync(tree, "bad"), Is.Null);
    }

    [Test]
    public async Task Cross_tree_saga_aborts_wholesale_when_one_legs_tree_is_denied()
    {
        const string treeA = "enf-xtree-a";
        const string treeB = "enf-xtree-b";
        // Authorize writes on tree A only; tree B has no rule (default-deny).
        await GrantAsync(new LatticeAuthorizationRule(
            "a", LatticeSubjectSelector.User("saga"), LatticeScope.Tree(treeA),
            LatticeOperation.Write, LatticeEffect.Allow));

        using (AuthClusterFixture.AsSubject("saga"))
        {
            var batches = new List<LatticeTreeBatch>
            {
                new(treeA, new List<KeyValuePair<string, byte[]>> { new("ka", Bytes("1")) }),
                new(treeB, new List<KeyValuePair<string, byte[]>> { new("kb", Bytes("2")) }),
            };

            Assert.That(
                async () => await _fixture.Cluster.Client.SetManyAtomicAsync(batches, operationId: "saga-op-1"),
                Throws.InstanceOf<LatticeAuthorizationDeniedException>(),
                "a cross-tree saga with one denied leg must abort by throwing the authorization denial");
        }

        Assert.That(await ReadBackAsync(treeA, "ka"), Is.Null, "the authorized tree's leg must not persist when a sibling tree's leg is denied");
        Assert.That(await ReadBackAsync(treeB, "kb"), Is.Null, "the denied tree's leg must not persist");
    }

    [Test]
    public async Task BootstrapAdministrator_bypasses_every_operation_class()
    {
        const string tree = "enf-bootstrap";
        await _fixture.RebuildPolicyAsync();

        using (AuthClusterFixture.AsSubject(AuthClusterFixture.BootstrapAdmin))
        {
            var lattice = _fixture.Lattice(tree);
            await lattice.SetAsync("k", Bytes("v"));
            Assert.That(Str(await lattice.GetAsync("k")), Is.EqualTo("v"), "bootstrap admin may write and read with no rules");

            var keys = new List<string>();
            await foreach (var key in lattice.KeysAsync())
            {
                keys.Add(key);
            }

            Assert.That(keys, Does.Contain("k"), "bootstrap admin observes all keys in a range scan");
            Assert.That(await lattice.DeleteAsync("k"), Is.True, "bootstrap admin may delete");
        }
    }

    [Test]
    public async Task RangeDelete_under_partial_authorization_is_hard_denied_and_deletes_nothing()
    {
        const string tree = "enf-rangedelete-partial";
        using (AuthClusterFixture.AsSubject(AuthClusterFixture.BootstrapAdmin))
        {
            var seed = _fixture.Lattice(tree);
            await seed.SetAsync("a:1", Bytes("1"));
            await seed.SetAsync("a:2", Bytes("2"));
            await seed.SetAsync("b:1", Bytes("3"));
        }

        // Only a prefix of the requested [a:, c:) range is authorized for delete.
        await GrantAsync(new LatticeAuthorizationRule(
            "rd", LatticeSubjectSelector.User("ranger"), LatticeScope.Prefix(tree, "a:"),
            LatticeOperation.RangeDelete, LatticeEffect.Allow));

        using (AuthClusterFixture.AsSubject("ranger"))
        {
            Assert.That(
                async () => await _fixture.Lattice(tree).DeleteRangeAsync("a:", "c:"),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "a range delete over a range only partially authorized is refused, never narrowed");
        }

        Assert.That(await ReadBackAsync(tree, "a:1"), Is.EqualTo("1"), "nothing may be deleted under a hard-denied range delete");
        Assert.That(await ReadBackAsync(tree, "a:2"), Is.EqualTo("2"));
        Assert.That(await ReadBackAsync(tree, "b:1"), Is.EqualTo("3"));
    }

    [Test]
    public async Task RangeDelete_over_a_fully_authorized_range_is_allowed()
    {
        const string tree = "enf-rangedelete-full";
        using (AuthClusterFixture.AsSubject(AuthClusterFixture.BootstrapAdmin))
        {
            var seed = _fixture.Lattice(tree);
            await seed.SetAsync("a:1", Bytes("1"));
            await seed.SetAsync("a:2", Bytes("2"));
        }

        await GrantAsync(new LatticeAuthorizationRule(
            "rd", LatticeSubjectSelector.User("fullranger"), LatticeScope.Tree(tree),
            LatticeOperation.RangeDelete, LatticeEffect.Allow));

        using (AuthClusterFixture.AsSubject("fullranger"))
        {
            var deleted = await _fixture.Lattice(tree).DeleteRangeAsync("a:", "c:");
            Assert.That(deleted, Is.EqualTo(2), "a whole-range authorized delete proceeds");
        }

        Assert.That(await ReadBackAsync(tree, "a:1"), Is.Null);
        Assert.That(await ReadBackAsync(tree, "a:2"), Is.Null);
    }

    [Test]
    public async Task CrdtApply_requires_crdt_authorization()
    {
        const string tree = "enf-crdt";
        await _fixture.RebuildPolicyAsync();

        using (AuthClusterFixture.AsSubject("crdt-user"))
        {
            Assert.That(
                async () => await _fixture.Lattice(tree).ApplyCrdtDeltaAsync("k", LatticeMergeMode.PnCounter, Bytes("1")),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "a CRDT apply with no CrdtApply grant is denied");
        }
    }

    [Test]
    public async Task BulkLoad_requires_bulkload_authorization()
    {
        const string tree = "enf-bulkload";
        await _fixture.RebuildPolicyAsync();

        using (AuthClusterFixture.AsSubject("bulk-user"))
        {
            Assert.That(
                async () => await _fixture.Lattice(tree).BulkLoadAsync(new List<KeyValuePair<string, byte[]>>
                {
                    new("k", Bytes("v")),
                }),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "a bulk load with no BulkLoad grant is denied");
        }
    }

    [Test]
    public async Task Admin_lifecycle_requires_admin_authorization()
    {
        const string tree = "enf-admin";
        using (AuthClusterFixture.AsSubject(AuthClusterFixture.BootstrapAdmin))
        {
            await _fixture.Lattice(tree).SetAsync("k", Bytes("v"));
        }

        await _fixture.RebuildPolicyAsync();

        using (AuthClusterFixture.AsSubject("admin-user"))
        {
            Assert.That(
                async () => await _fixture.Lattice(tree).DeleteTreeAsync(),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "a tree-lifecycle admin operation with no Admin grant is denied");
        }

        Assert.That(await ReadBackAsync(tree, "k"), Is.EqualTo("v"), "the denied admin operation left the tree intact");
    }

    [Test]
    public async Task History_read_of_a_denied_key_returns_an_empty_timeline()
    {
        const string tree = "enf-history";
        using (AuthClusterFixture.AsSubject(AuthClusterFixture.BootstrapAdmin))
        {
            var seed = _fixture.Lattice(tree);
            await seed.SetAsync("k", Bytes("v1"));
            await seed.SetAsync("k", Bytes("v2"));
        }

        await _fixture.RebuildPolicyAsync();

        using (AuthClusterFixture.AsSubject("hist-nobody"))
        {
            var page = await _fixture.Lattice(tree).ScanEntryHistoryAsync("k", null, null, 100, null);
            Assert.That(page.Revisions, Is.Empty, "a denied caller observes no change history for a key");
        }
    }
}
