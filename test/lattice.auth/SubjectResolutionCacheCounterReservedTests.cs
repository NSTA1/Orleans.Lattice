using System.Text;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Regression guard locking the documented reality that the subject-resolution
/// cache hit / miss counters on the <c>orleans.lattice.auth</c> meter are a
/// <b>reserved seam that is not emitted in this version</b>. The
/// <see cref="LatticeAuthMetrics.RecordSubjectResolutionCacheHit"/> /
/// <see cref="LatticeAuthMetrics.RecordSubjectResolutionCacheMiss"/> entry points
/// exist and work (covered by <c>LatticeAuthMetricsTests</c>), but the shipped
/// caller-identity resolution pipeline never calls them, so driving real
/// authorized traffic - which warms and then hits the membership resolution cache
/// on every gated call - must leave both counters at zero. If a future change
/// begins emitting them, this test fails and the observability docs must be
/// updated in the same change so the docs never claim a metric that does not
/// emit (and vice versa).
/// </summary>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class SubjectResolutionCacheCounterReservedTests
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

    [Test]
    public async Task Repeated_same_subject_resolution_never_emits_the_reserved_subject_cache_counters()
    {
        const string tree = "reserved-subject-cache";
        await _fixture.Store.PutRuleAsync(new LatticeAuthorizationRule(
            "rw",
            LatticeSubjectSelector.User("cache-user"),
            LatticeScope.Tree(tree),
            LatticeOperation.Read | LatticeOperation.Write,
            LatticeEffect.Allow));
        await _fixture.RebuildPolicyAsync();

        using var hits = new MeterCollector<long>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.SubjectResolutionCacheHitsName);
        using var misses = new MeterCollector<long>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.SubjectResolutionCacheMissesName);

        // Drive many gated operations as the same subject: the first resolves the
        // subject (a would-be cache miss), the rest reuse it (would-be cache hits).
        // With the seam unwired, neither counter advances.
        for (var i = 0; i < 16; i++)
        {
            using (AuthClusterFixture.AsSubject("cache-user"))
            {
                await _fixture.Lattice(tree).SetAsync($"k{i}", Encoding.UTF8.GetBytes("v"));
                _ = await _fixture.Lattice(tree).GetAsync($"k{i}");
            }
        }

        Assert.That(hits.Measurements, Is.Empty,
            "the subject-cache hits counter is a reserved seam and must not be emitted by the resolution pipeline");
        Assert.That(misses.Measurements, Is.Empty,
            "the subject-cache misses counter is a reserved seam and must not be emitted by the resolution pipeline");
    }
}
