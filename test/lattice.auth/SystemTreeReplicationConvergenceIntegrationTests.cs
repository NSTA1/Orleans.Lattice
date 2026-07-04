using System.Text;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Acceptance coverage for replicating the reserved auth policy tree across sites
/// (issue #982). A policy change authored on site A, once replicated to site B and
/// after site B's policy snapshot settles, changes site B's authorization
/// decisions. The replication apply also proves the system-origin apply bypass:
/// the shipped policy write lands on site B's default-deny policy tree even though
/// the applier has no user identity, because the receiver apply path runs under
/// the system-origin scope.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SystemTreeReplicationConvergenceIntegrationTests
{
    private AuthReplicationClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthReplicationClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    [Test]
    public async Task Policy_change_on_site_a_converges_to_site_b_and_flips_its_decision()
    {
        const string tree = "conv-app";
        const string writer = "conv-writer";

        // Site B starts with no granting rule: warm its (empty) snapshot and prove
        // the writer is denied there.
        await _fixture.RebuildBAsync();
        using (AuthReplicationClusterFixture.AsSubject(writer))
        {
            Assert.That(
                async () => await _fixture.LatticeB(tree).SetAsync("k", Bytes("v")),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "site B must deny the write before the policy has replicated");
        }

        // Author the grant on site A only.
        await _fixture.StoreA.PutRuleAsync(new LatticeAuthorizationRule(
            "conv-grant",
            LatticeSubjectSelector.User(writer),
            LatticeScope.Tree(tree),
            LatticeOperation.Write,
            LatticeEffect.Allow));
        await _fixture.RebuildAAsync();

        // Replicate the policy tree A -> B (drives site B's IReplicationApplier).
        await _fixture.ReplicatePolicyTreeAtoBAsync();

        // The replicated write must have landed on site B's default-deny policy
        // tree despite the applier having no user identity (system-origin apply
        // bypass): the policy tree is non-empty when read under the admin.
        using (AuthReplicationClusterFixture.AsSubject(AuthReplicationClusterFixture.BootstrapAdmin))
        {
            var cursor = await _fixture.LatticeB(LatticeSystemTreeNames.AuthPolicy).OpenEntryCursorAsync();
            var page = await _fixture.LatticeB(LatticeSystemTreeNames.AuthPolicy).NextEntriesAsync(cursor, 8);
            Assert.That(page.Entries, Is.Not.Empty, "the replicated policy write must have landed on site B");
        }

        // Settle site B's snapshot off the now-replicated policy tree.
        await _fixture.RebuildBAsync();

        // The decision has flipped: the writer is now allowed on site B.
        using (AuthReplicationClusterFixture.AsSubject(writer))
        {
            await _fixture.LatticeB(tree).SetAsync("k", Bytes("v"));
        }

        using (AuthReplicationClusterFixture.AsSubject(AuthReplicationClusterFixture.BootstrapAdmin))
        {
            var stored = await _fixture.LatticeB(tree).GetAsync("k");
            Assert.That(stored, Is.Not.Null, "the now-authorized write must have persisted on site B");
        }
    }

    [Test]
    public async Task Revoke_on_site_a_converges_to_site_b_and_denies_a_previously_allowed_write()
    {
        const string tree = "conv-revoke";
        const string writer = "revoke-writer";

        // Author the grant on site A and replicate it so site B allows the write.
        await _fixture.StoreA.PutRuleAsync(new LatticeAuthorizationRule(
            "revoke-grant",
            LatticeSubjectSelector.User(writer),
            LatticeScope.Tree(tree),
            LatticeOperation.Write,
            LatticeEffect.Allow));
        await _fixture.RebuildAAsync();
        await _fixture.ReplicatePolicyTreeAtoBAsync();
        await _fixture.RebuildBAsync();

        using (AuthReplicationClusterFixture.AsSubject(writer))
        {
            await _fixture.LatticeB(tree).SetAsync("before", Bytes("v"));
        }

        // Revoke the grant on site A and replicate the deletion to site B.
        Assert.That(
            await _fixture.StoreA.RemoveRuleAsync(tree, "revoke-grant"),
            Is.True,
            "the grant must have existed to be revoked");
        await _fixture.RebuildAAsync();
        await _fixture.ReplicatePolicyRevokeAtoBAsync(tree, "revoke-grant");
        await _fixture.RebuildBAsync();

        // The decision has flipped back: the writer is denied on site B once the
        // revoke has converged, proving a revoke propagates through the same
        // system-tree replication special case a grant does.
        using (AuthReplicationClusterFixture.AsSubject(writer))
        {
            Assert.That(
                async () => await _fixture.LatticeB(tree).SetAsync("after", Bytes("v")),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "site B must deny the write once the revoke has replicated");
        }

        using (AuthReplicationClusterFixture.AsSubject(AuthReplicationClusterFixture.BootstrapAdmin))
        {
            Assert.That(await _fixture.LatticeB(tree).GetAsync("after"), Is.Null,
                "the denied post-revoke write must not have persisted on site B");
        }
    }
}
