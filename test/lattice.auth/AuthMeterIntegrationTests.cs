using System.Text;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// End-to-end observability coverage: with the enforcement gate live, a gated
/// decision increments the <c>orleans.lattice.auth</c> decisions counter with the
/// operation / tree / effect tags a metrics listener observes.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AuthMeterIntegrationTests
{
    private AuthClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    [Test]
    public async Task A_denied_write_increments_the_decisions_counter_with_deny_tags()
    {
        var tree = $"metric-deny-{Guid.NewGuid():N}";
        await _fixture.RebuildPolicyAsync();

        using var collector = new MeterCollector<long>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.DecisionsName);

        using (AuthClusterFixture.AsSubject("nobody"))
        {
            Assert.That(
                async () => await _fixture.Lattice(tree).SetAsync("k", Bytes("v")),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
        }

        var mine = collector.Measurements
            .Where(m => m.Tags.Any(t => t.Key == LatticeAuthMetrics.TagTree && Equals(t.Value, tree)))
            .ToList();

        Assert.That(mine, Is.Not.Empty, "the gated decision must be counted on the auth meter");
        Assert.That(
            mine.Any(m => m.Tags.Any(t => t.Key == LatticeAuthMetrics.TagEffect && Equals(t.Value, LatticeAuthMetrics.EffectDeny))),
            Is.True,
            "the denied decision must carry the deny effect tag");
        Assert.That(
            mine.Any(m => m.Tags.Any(t => t.Key == LatticeAuthMetrics.TagOperation && Equals(t.Value, "Write"))),
            Is.True,
            "the decision must carry the attempted operation tag");
    }

    [Test]
    public async Task A_bootstrap_admin_allow_increments_the_decisions_counter_with_allow_tags()
    {
        var tree = $"metric-allow-{Guid.NewGuid():N}";
        await _fixture.RebuildPolicyAsync();

        using var collector = new MeterCollector<long>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.DecisionsName);

        using (AuthClusterFixture.AsSubject(AuthClusterFixture.BootstrapAdmin))
        {
            await _fixture.Lattice(tree).SetAsync("k", Bytes("v"));
        }

        var mine = collector.Measurements
            .Where(m => m.Tags.Any(t => t.Key == LatticeAuthMetrics.TagTree && Equals(t.Value, tree)))
            .ToList();

        Assert.That(
            mine.Any(m => m.Tags.Any(t => t.Key == LatticeAuthMetrics.TagEffect && Equals(t.Value, LatticeAuthMetrics.EffectAllow))),
            Is.True,
            "the bootstrap-admin allow must be counted with the allow effect tag");
    }
}
