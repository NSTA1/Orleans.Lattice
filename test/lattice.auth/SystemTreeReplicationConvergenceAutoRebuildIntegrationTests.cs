using System.Text;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Acceptance coverage that a replicated reserved-policy-tree write drives site B's
/// authorization snapshot rebuild <b>automatically</b> - relying solely on the
/// receiver-side mutation-observer publication a real replication apply must fire,
/// with no test-only forced <c>RebuildNowAsync</c> seam. This is the enforcement
/// half of replicating the reserved system trees: a grant or revoke that converges
/// in state on the peer must also be enforced there without any unrelated event
/// happening to rebuild the peer snapshot first.
/// </summary>
/// <remarks>
/// The companion <see cref="SystemTreeReplicationConvergenceIntegrationTests"/> proves
/// state convergence and the system-origin apply bypass, but it settles site B's
/// snapshot with an explicit forced rebuild after every step, which masks whether
/// the replicated apply itself triggers the rebuild. These tests deliberately never
/// call the forced-rebuild seam after replicating; they poll the live decision with
/// a bounded timeout because the rebuild runs on a background continuation.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class SystemTreeReplicationConvergenceAutoRebuildIntegrationTests
{
    private static readonly TimeSpan SettleTimeout = TimeSpan.FromSeconds(10);

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

    /// <summary>
    /// Attempts a governed write on site B as <paramref name="subject"/> and reports
    /// whether the access gate allowed it. A denied write throws and yields
    /// <c>false</c>; an allowed write persists and yields <c>true</c>. Reading the
    /// decision this way never forces a snapshot rebuild - the gate only warms an
    /// empty snapshot once and thereafter reads whatever the observer-driven rebuild
    /// has swapped in.
    /// </summary>
    private async Task<bool> WriteAllowedOnBAsync(string tree, string subject, string key)
    {
        using (AuthReplicationClusterFixture.AsSubject(subject))
        {
            try
            {
                await _fixture.LatticeB(tree).SetAsync(key, Bytes("v"));
                return true;
            }
            catch (LatticeAuthorizationDeniedException)
            {
                return false;
            }
        }
    }

    private async Task<bool> PollAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + SettleTimeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return true;
            }

            await Task.Delay(100);
        }

        return await condition();
    }

    [Test]
    public async Task Replicated_grant_is_enforced_on_site_b_without_a_forced_rebuild()
    {
        const string tree = "auto-grant-app";
        const string writer = "auto-grant-writer";

        // Site B denies the writer before any policy has replicated. This first
        // gate evaluation auto-warms B's empty snapshot; it is not a forced rebuild.
        Assert.That(
            await WriteAllowedOnBAsync(tree, writer, "warm"),
            Is.False,
            "site B must deny the write before the grant has replicated");

        // Author the grant on site A's policy tree (no snapshot rebuild needed -
        // replication scans the raw policy-tree entries, not the compiled snapshot).
        await _fixture.StoreA.PutRuleAsync(new LatticeAuthorizationRule(
            "auto-grant",
            LatticeSubjectSelector.User(writer),
            LatticeScope.Tree(tree),
            LatticeOperation.Write,
            LatticeEffect.Allow));

        // Replicate the policy tree A -> B. The receiver apply must fire the
        // reserved-tree mutation observer so B rebuilds its snapshot automatically.
        await _fixture.ReplicatePolicyTreeAtoBAsync();

        // No forced RebuildBAsync here. Poll the live decision until the automatic
        // rebuild has flipped it to allow.
        var allowed = await PollAsync(() => WriteAllowedOnBAsync(tree, writer, "after-grant"));
        Assert.That(
            allowed,
            Is.True,
            "the replicated grant must be enforced on site B via the automatic "
            + "observer-driven snapshot rebuild, with no forced rebuild");
    }

    [Test]
    public async Task Replicated_revoke_is_enforced_on_site_b_without_a_forced_rebuild()
    {
        const string tree = "auto-revoke-app";
        const string writer = "auto-revoke-writer";

        // Grant then replicate so site B allows the writer - proven without a forced
        // rebuild so this setup itself exercises the auto-rebuild path.
        await _fixture.StoreA.PutRuleAsync(new LatticeAuthorizationRule(
            "auto-revoke-grant",
            LatticeSubjectSelector.User(writer),
            LatticeScope.Tree(tree),
            LatticeOperation.Write,
            LatticeEffect.Allow));
        await _fixture.ReplicatePolicyTreeAtoBAsync();

        var allowed = await PollAsync(() => WriteAllowedOnBAsync(tree, writer, "before-revoke"));
        Assert.That(
            allowed,
            Is.True,
            "the replicated grant must be enforced on site B before the revoke");

        // Revoke on site A and replicate the deletion to site B.
        Assert.That(
            await _fixture.StoreA.RemoveRuleAsync(tree, "auto-revoke-grant"),
            Is.True,
            "the grant must have existed to be revoked");
        await _fixture.ReplicatePolicyRevokeAtoBAsync(tree, "auto-revoke-grant");

        // No forced RebuildBAsync here. Poll until the automatic rebuild has denied
        // the writer again. A revoke that converges in state but is never enforced
        // is the exact defect this test guards against.
        var denied = await PollAsync(async () => !await WriteAllowedOnBAsync(tree, writer, "after-revoke"));
        Assert.That(
            denied,
            Is.True,
            "the replicated revoke must be enforced on site B via the automatic "
            + "observer-driven snapshot rebuild, with no forced rebuild");
    }
}
