using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Allocation tests for subject-relative (templated) scopes (issue #6061). A warm
/// point decision that a templated rule grants, through the decision engine, stays
/// allocation-free for the group closure shapes the membership package produces; and
/// a range request inside the caller's resolved prefix resolves uniformly without
/// allocating.
/// </summary>
[TestFixture]
public sealed class TemplatedScopeAllocationTests
{
    private const int Small = 100;
    private const int Large = 10_000;
    private const string Images = "images";

    private static readonly LatticeSubjectSelector Members = LatticeSubjectSelector.Group("members");

    private static readonly LatticeAuthorizationRule[] Rules =
    [
        new("owner", Members, LatticeScope.Prefix(Images, "{subject}/"), LatticeOperation.Read, LatticeEffect.Allow),
        new("group", Members, LatticeScope.Prefix(Images, "g/{group:grp:}/"), LatticeOperation.Read, LatticeEffect.Allow),
        new("profile", Members, LatticeScope.Key(Images, "p/{subject}/me"), LatticeOperation.Read, LatticeEffect.Allow),
        new("literal", Members, LatticeScope.Prefix(Images, "pub/"), LatticeOperation.Read, LatticeEffect.Allow),
    ];

    private static readonly LatticeSubject SetSubject = new(
        "u1", new HashSet<string>(["members", "grp:hikers", "other"], StringComparer.Ordinal));

    private static async Task<LatticeDecisionEngine> CreateEngineAsync()
    {
        var store = new CovPolicyStore();
        store.Rules.AddRange(Rules);
        var maintainer = new CompiledPolicySnapshotMaintainer(
            store, NullLogger<CompiledPolicySnapshotMaintainer>.Instance, timeProvider: null, tenantLayer: null);
        await maintainer.RebuildNowAsync();
        return new LatticeDecisionEngine(maintainer, new CovOptionsMonitor<LatticeAuthOptions>(new LatticeAuthOptions()), tenantLayer: null);
    }

    private static long PointGrowth(LatticeDecisionEngine engine, LatticeSubject subject, string key) =>
        AllocationProbe.Growth(
            prepare: _ => engine,
            measure: (state, size) =>
            {
                long allowed = 0;
                for (var i = 0; i < size; i++)
                {
                    if (state.Evaluate(subject, Images, LatticeOperation.Read, key).Allowed)
                    {
                        allowed++;
                    }
                }

                AllocationProbe.ScalarSink += allowed;
            },
            smallSize: Small,
            largeSize: Large);

    [TestCase("u1/photo.jpg")]
    [TestCase("g/hikers/photo.jpg")]
    [TestCase("p/u1/me")]
    [TestCase("pub/x")]
    public async Task Warm_templated_allow_decision_allocates_nothing_for_a_hash_set_closure(string key)
    {
        var engine = await CreateEngineAsync();
        Assert.That(engine.Evaluate(SetSubject, Images, LatticeOperation.Read, key).Allowed, Is.True, "precondition");

        Assert.That(PointGrowth(engine, SetSubject, key), Is.Zero);
    }

    [Test]
    public async Task Warm_group_templated_allow_allocates_nothing_for_an_array_closure()
    {
        var engine = await CreateEngineAsync();
        var subject = new LatticeSubject("u1", new[] { "members", "grp:hikers" });
        Assert.That(engine.Evaluate(subject, Images, LatticeOperation.Read, "g/hikers/x").Allowed, Is.True, "precondition");

        Assert.That(PointGrowth(engine, subject, "g/hikers/x"), Is.Zero);
    }

    [Test]
    public void Uniform_range_inside_the_resolved_prefix_allocates_nothing()
    {
        var policy = CompiledPolicy.Compile(Rules);
        var options = new LatticeAuthOptions();
        Assert.That(
            PolicyEvaluator.Evaluate(policy, options, SetSubject, Images, LatticeOperation.Read, null, "u1/", "u10").KeyFilter,
            Is.Null,
            "precondition: the range is uniformly allowed");

        var growth = AllocationProbe.Growth(
            prepare: _ => policy,
            measure: (state, size) =>
            {
                long allowed = 0;
                for (var i = 0; i < size; i++)
                {
                    if (PolicyEvaluator.Evaluate(state, options, SetSubject, Images, LatticeOperation.Read, null, "u1/", "u10").Allowed)
                    {
                        allowed++;
                    }
                }

                AllocationProbe.ScalarSink += allowed;
            },
            smallSize: Small,
            largeSize: Large);

        Assert.That(growth, Is.Zero);
    }
}
