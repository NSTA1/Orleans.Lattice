using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.TestingHost;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// End-to-end integration tests proving that schema enforcement covers the
/// <b>cross-tree</b> atomic write path (<see cref="LatticeCrossTreeAtomicWriteExtensions.SetManyAtomicAsync"/>),
/// not just single-tree writes. A non-compliant leg is rejected with
/// <see cref="LatticeSchemaViolationException"/> before any tree is mutated
/// (all-or-nothing is preserved), and a fully compliant cross-tree write commits
/// on every participating tree. Runs on a real single-silo
/// <see cref="Orleans.TestingHost.TestCluster"/> with enforcement registered; the
/// cross-tree saga is reminder-driven and has no timing dependence.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class CrossTreeSchemaEnforcementIntegrationTests
{
    private SchemaRemediationClusterFixture _fixture = null!;

    private IGrainFactory Grains => _fixture.Cluster.GrainFactory;

    private IServiceProvider SiloServices =>
        _fixture.Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SchemaRemediationClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static List<KeyValuePair<string, byte[]>> Entries(params (string Key, string Value)[] items) =>
        items.Select(i => new KeyValuePair<string, byte[]>(i.Key, Utf8(i.Value))).ToList();

    private async Task RequireJsonAsync(params string[] treeIds)
    {
        var admin = SiloServices.GetRequiredService<ILatticeSchemaAdmin>();
        var policy = new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() });
        foreach (var treeId in treeIds)
        {
            await admin.SetPolicyAsync(treeId, policy);
        }
    }

    [Test]
    public async Task Cross_tree_atomic_write_rejects_a_non_compliant_leg_and_mutates_no_tree()
    {
        const string compliantTree = "xt-enforce-compliant";
        const string offendingTree = "xt-enforce-offending";
        await RequireJsonAsync(compliantTree, offendingTree);

        var batches = new List<LatticeTreeBatch>
        {
            new(compliantTree, Entries(("ok", "{\"v\":1}"))),
            new(offendingTree, Entries(("bad", "not-json"))),
        };

        // The interceptor runs at the coordinator, before any leg is staged, so the
        // caller sees the same violation a single-tree write would raise.
        Assert.That(
            async () => await Grains.SetManyAtomicAsync(batches, Guid.NewGuid().ToString("N")),
            Throws.TypeOf<LatticeSchemaViolationException>());

        // All-or-nothing: the compliant leg's value was never committed either.
        Assert.That(await Grains.GetGrain<ILattice>(compliantTree).GetAsync("ok"), Is.Null);
        Assert.That(await Grains.GetGrain<ILattice>(offendingTree).GetAsync("bad"), Is.Null);
    }

    [Test]
    public async Task Cross_tree_atomic_write_commits_when_every_leg_is_compliant()
    {
        const string treeA = "xt-enforce-a";
        const string treeB = "xt-enforce-b";
        await RequireJsonAsync(treeA, treeB);

        var batches = new List<LatticeTreeBatch>
        {
            new(treeA, Entries(("a1", "{\"v\":1}"), ("a2", "{\"v\":2}"))),
            new(treeB, Entries(("b1", "{\"v\":3}"))),
        };

        var outcome = await Grains.SetManyAtomicAsync(batches, Guid.NewGuid().ToString("N"));

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        Assert.That(
            Encoding.UTF8.GetString((await Grains.GetGrain<ILattice>(treeA).GetAsync("a1"))!),
            Does.Contain("\"v\":1"));
        Assert.That(
            Encoding.UTF8.GetString((await Grains.GetGrain<ILattice>(treeB).GetAsync("b1"))!),
            Does.Contain("\"v\":3"));
    }
}
