using System.Text;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Acceptance coverage for the optional strict-consistency policy-epoch fence
/// (issue #982). With a tree opted into
/// <see cref="LatticeAuthOptions.StrictConsistencyTrees"/>, a user write made
/// under an ambient required-epoch floor that the local compiled policy has not
/// reached is rejected; the same write with no floor (the eventual default), or a
/// floor the local epoch has met, or a write to a non-strict tree, or a read, is
/// not fenced. This closes the cross-cluster revoke window without changing the
/// zero-cost eventual path.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class StrictConsistencyFenceIntegrationTests
{
    private AuthReplicationClusterFixture _fixture = null!;
    private long _epoch;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthReplicationClusterFixture();
        await _fixture.InitializeAsync();

        // Grant a writer Write and a reader Read on the strict tree, plus a writer
        // on a non-strict "loose" tree, all on site B, then rebuild once and record
        // the resulting epoch as the fence reference point.
        await _fixture.StoreB.PutRuleAsync(new LatticeAuthorizationRule(
            "sf-write",
            LatticeSubjectSelector.User("strict-writer"),
            LatticeScope.Tree(AuthReplicationClusterFixture.StrictTree),
            LatticeOperation.Write,
            LatticeEffect.Allow));
        await _fixture.StoreB.PutRuleAsync(new LatticeAuthorizationRule(
            "sf-read",
            LatticeSubjectSelector.User("strict-reader"),
            LatticeScope.Tree(AuthReplicationClusterFixture.StrictTree),
            LatticeOperation.Read,
            LatticeEffect.Allow));
        await _fixture.StoreB.PutRuleAsync(new LatticeAuthorizationRule(
            "sf-loose",
            LatticeSubjectSelector.User("loose-writer"),
            LatticeScope.Tree("loose-app"),
            LatticeOperation.Write,
            LatticeEffect.Allow));
        _epoch = await _fixture.RebuildBAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    [Test]
    public async Task Write_to_a_strict_tree_with_no_floor_is_not_fenced()
    {
        using (AuthReplicationClusterFixture.AsSubject("strict-writer"))
        {
            await _fixture.LatticeB(AuthReplicationClusterFixture.StrictTree).SetAsync("k-eventual", Bytes("v"));
        }

        using (AuthReplicationClusterFixture.AsSubject(AuthReplicationClusterFixture.BootstrapAdmin))
        {
            Assert.That(
                await _fixture.LatticeB(AuthReplicationClusterFixture.StrictTree).GetAsync("k-eventual"),
                Is.EqualTo(Bytes("v")),
                "the eventual path accepts and persists the write");
        }
    }

    [Test]
    public void Write_to_a_strict_tree_under_a_stale_floor_is_fenced()
    {
        using (AuthReplicationClusterFixture.AsSubject("strict-writer"))
        using (LatticePolicyEpochFenceContext.RequireAtLeast(_epoch + 1000))
        {
            Assert.That(
                async () => await _fixture.LatticeB(AuthReplicationClusterFixture.StrictTree).SetAsync("k-fenced", Bytes("v")),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "a write under a floor the local epoch has not reached must be fenced");
        }
    }

    [Test]
    public async Task Write_to_a_strict_tree_under_a_met_floor_is_not_fenced()
    {
        using (AuthReplicationClusterFixture.AsSubject("strict-writer"))
        using (LatticePolicyEpochFenceContext.RequireAtLeast(_epoch))
        {
            await _fixture.LatticeB(AuthReplicationClusterFixture.StrictTree).SetAsync("k-met", Bytes("v"));
        }

        using (AuthReplicationClusterFixture.AsSubject(AuthReplicationClusterFixture.BootstrapAdmin))
        {
            Assert.That(
                await _fixture.LatticeB(AuthReplicationClusterFixture.StrictTree).GetAsync("k-met"),
                Is.EqualTo(Bytes("v")),
                "a floor the local epoch has already met does not fence the write");
        }
    }

    [Test]
    public async Task Write_to_a_non_strict_tree_under_a_stale_floor_is_not_fenced()
    {
        using (AuthReplicationClusterFixture.AsSubject("loose-writer"))
        using (LatticePolicyEpochFenceContext.RequireAtLeast(_epoch + 1000))
        {
            await _fixture.LatticeB("loose-app").SetAsync("k-loose", Bytes("v"));
        }

        using (AuthReplicationClusterFixture.AsSubject(AuthReplicationClusterFixture.BootstrapAdmin))
        {
            Assert.That(
                await _fixture.LatticeB("loose-app").GetAsync("k-loose"),
                Is.EqualTo(Bytes("v")),
                "the fence is per-tree: a non-strict tree is never fenced");
        }
    }

    [Test]
    public async Task Read_from_a_strict_tree_under_a_stale_floor_is_not_fenced()
    {
        // Seed a value the reader can observe (writer, unfenced).
        using (AuthReplicationClusterFixture.AsSubject("strict-writer"))
        {
            await _fixture.LatticeB(AuthReplicationClusterFixture.StrictTree).SetAsync("k-read", Bytes("payload"));
        }

        using (AuthReplicationClusterFixture.AsSubject("strict-reader"))
        using (LatticePolicyEpochFenceContext.RequireAtLeast(_epoch + 1000))
        {
            var value = await _fixture.LatticeB(AuthReplicationClusterFixture.StrictTree).GetAsync("k-read");
            Assert.That(value, Is.Not.Null, "a read is never fenced, even under a stale floor");
        }
    }
}
