using System.Text;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Enforcement coverage for the aggregate / structural reads that cannot be
/// narrowed by a per-key filter - the per-shard count and the leaf-projection
/// content digests. Unlike the per-key read surfaces (which prune to the
/// authorized subset), these reads leak structural information (the physical
/// shard count and key distribution, or a content-digest oracle) about keys the
/// caller may not read, so they are <b>hard-denied</b>: a caller with no rule or
/// only a partial (prefix) grant is refused with
/// <see cref="LatticeAuthorizationDeniedException"/>, and only a uniformly
/// authorized caller (or the bootstrap administrator) proceeds.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeAggregateReadEnforcementIntegrationTests
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

    private async Task GrantAsync(params LatticeAuthorizationRule[] rules)
    {
        foreach (var rule in rules)
        {
            await _fixture.Store.PutRuleAsync(rule);
        }

        await _fixture.RebuildPolicyAsync();
    }

    private async Task SeedAsync(string tree)
    {
        using (AuthClusterFixture.AsSubject(AuthClusterFixture.BootstrapAdmin))
        {
            var seed = _fixture.Lattice(tree);
            await seed.SetAsync("a:1", Bytes("1"));
            await seed.SetAsync("a:2", Bytes("2"));
            await seed.SetAsync("b:1", Bytes("3"));
        }
    }

    [Test]
    public async Task CountPerShard_with_no_matching_rule_is_hard_denied()
    {
        const string tree = "enf-cps-deny";
        await SeedAsync(tree);
        await _fixture.RebuildPolicyAsync();

        using (AuthClusterFixture.AsSubject("cps-nobody"))
        {
            Assert.That(
                async () => await _fixture.Lattice(tree).CountPerShardAsync(),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "a denied caller must not learn per-shard key counts");
        }
    }

    [Test]
    public async Task CountPerShard_under_partial_authorization_is_hard_denied()
    {
        const string tree = "enf-cps-partial";
        await SeedAsync(tree);

        // Only a prefix of the tree is authorized: a per-shard count cannot be
        // narrowed to that prefix, so it is refused rather than partially served.
        await GrantAsync(new LatticeAuthorizationRule(
            "ra", LatticeSubjectSelector.User("cps-partial"), LatticeScope.Prefix(tree, "a:"),
            LatticeOperation.RangeRead, LatticeEffect.Allow));

        using (AuthClusterFixture.AsSubject("cps-partial"))
        {
            Assert.That(
                async () => await _fixture.Lattice(tree).CountPerShardAsync(),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "a partial (prefix) grant must not narrow a per-shard count; it is hard-denied");
        }
    }

    [Test]
    public async Task CountPerShard_is_allowed_for_a_uniformly_authorized_caller()
    {
        const string tree = "enf-cps-allow";
        await SeedAsync(tree);
        await GrantAsync(new LatticeAuthorizationRule(
            "ra", LatticeSubjectSelector.User("cps-reader"), LatticeScope.Tree(tree),
            LatticeOperation.RangeRead, LatticeEffect.Allow));

        using (AuthClusterFixture.AsSubject("cps-reader"))
        {
            var perShard = await _fixture.Lattice(tree).CountPerShardAsync();
            Assert.That(perShard.Sum(), Is.EqualTo(3),
                "a whole-tree RangeRead grant permits the per-shard count aggregate");
        }
    }

    [Test]
    public async Task CountPerShard_is_allowed_for_the_bootstrap_administrator()
    {
        const string tree = "enf-cps-admin";
        await SeedAsync(tree);
        await _fixture.RebuildPolicyAsync();

        using (AuthClusterFixture.AsSubject(AuthClusterFixture.BootstrapAdmin))
        {
            var perShard = await _fixture.Lattice(tree).CountPerShardAsync();
            Assert.That(perShard.Sum(), Is.EqualTo(3), "the bootstrap admin bypasses the aggregate-read gate");
        }
    }

    [Test]
    public async Task LeafProjectionDigest_of_a_denied_tree_is_hard_denied_before_any_config_check()
    {
        const string tree = "enf-digest-deny";
        await SeedAsync(tree);
        await _fixture.RebuildPolicyAsync();

        // The denial must be an authorization denial (not an
        // InvalidOperationException about digest maintenance nor an
        // ArgumentOutOfRangeException about the shard index), proving the gate is
        // consulted before any routing / shard-index / options disclosure.
        using (AuthClusterFixture.AsSubject("digest-nobody"))
        {
            Assert.That(
                async () => await _fixture.Lattice(tree).GetLeafProjectionDigestAsync(0),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "a denied caller must not obtain a content digest oracle over restricted data");
        }
    }

    [Test]
    public async Task LeafProjectionDigestForRange_under_partial_authorization_is_hard_denied()
    {
        const string tree = "enf-digest-partial";
        await SeedAsync(tree);

        // Authorize only the "a:" prefix; the probed range [a:, c:) is only
        // partially covered, and a digest cannot be narrowed, so it is refused.
        await GrantAsync(new LatticeAuthorizationRule(
            "ra", LatticeSubjectSelector.User("digest-partial"), LatticeScope.Prefix(tree, "a:"),
            LatticeOperation.RangeRead, LatticeEffect.Allow));

        using (AuthClusterFixture.AsSubject("digest-partial"))
        {
            Assert.That(
                async () => await _fixture.Lattice(tree).GetLeafProjectionDigestForRangeAsync(0, "a:", "c:"),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "a partial grant must not narrow a range digest; it is hard-denied");
        }
    }
}
