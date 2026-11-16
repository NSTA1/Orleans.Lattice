using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.TestingHost;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Regression tests for issue #6063: under strict ingest, an atomic write that
/// deletes a key on a tree with a schema policy always aborted at prepare. The
/// saga's internal batch leg re-consulted the write interceptor for every entry,
/// including the delete entries, whose empty value buffer failed the policy and
/// was dead-lettered - silently dropped from the batch, so the tombstone was
/// never prepared and the original-prepare-stamp read-back found no bucket for
/// it. Runs on a real single-silo <see cref="TestCluster"/> with strict ingest
/// enabled; nothing is timing-dependent.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class StrictIngestAtomicDeleteIntegrationTests
{
    private TestCluster _cluster = null!;

    private IGrainFactory Grains => _cluster.GrainFactory;

    private IServiceProvider SiloServices =>
        _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private async Task RequireJsonAsync(string treeId)
    {
        var admin = SiloServices.GetRequiredService<ILatticeSchemaAdmin>();
        await admin.SetPolicyAsync(treeId, new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() }, strictIngest: true));
    }

    private Task<int> DeadLettersAsync(string treeId) =>
        SiloServices.GetRequiredService<ILatticeSchemaAdmin>().CountDeadLettersAsync(treeId);

    [Test]
    public async Task Cross_tree_atomic_delete_on_a_policy_tree_commits_under_strict_ingest()
    {
        const string groups = "i6063-groups";
        const string members = "i6063-members";
        await RequireJsonAsync(groups);

        var create = Grains.BeginAtomicWrite(Guid.NewGuid().ToString("N"));
        create.ForTree(groups).Set("g1", Utf8("{\"name\":\"g\"}"));
        create.ForTree(members).Set("g1/u1", Utf8("{}"));
        Assert.That(await create.CommitAsync(), Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));

        var delete = Grains.BeginAtomicWrite(Guid.NewGuid().ToString("N"));
        delete.ForTree(members).Delete("g1/u1");
        delete.ForTree(groups).Delete("g1");

        Assert.That(await delete.CommitAsync(), Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        Assert.Multiple(async () =>
        {
            Assert.That(await Grains.GetGrain<ILattice>(groups).GetAsync("g1"), Is.Null);
            Assert.That(await Grains.GetGrain<ILattice>(members).GetAsync("g1/u1"), Is.Null);
            Assert.That(await DeadLettersAsync(groups), Is.Zero);
        });
    }

    [Test]
    public async Task Single_tree_atomic_upsert_and_delete_on_a_policy_tree_commits_under_strict_ingest()
    {
        const string tree = "i6063-single";
        await RequireJsonAsync(tree);
        var lattice = Grains.GetGrain<ILattice>(tree);
        await lattice.SetAsync("old", Utf8("{\"v\":1}"));

        await lattice.SetManyAtomicAsync(
            new List<KeyValuePair<string, byte[]>> { new("new", Utf8("{\"v\":2}")) },
            new List<string> { "old" },
            Guid.NewGuid().ToString("N"));

        Assert.Multiple(async () =>
        {
            Assert.That(await lattice.GetAsync("old"), Is.Null);
            Assert.That(await lattice.GetAsync("new"), Is.Not.Null);
            Assert.That(await DeadLettersAsync(tree), Is.Zero);
        });
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.AddLatticeSchemaEnforcement(o => o.StrictIngest = true);
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
