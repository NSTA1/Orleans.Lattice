using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Membership;
using Orleans.TestingHost;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Audit for #4128 on the access-gate arm of <c>SetAliasAsync</c>: a user-origin alias change
/// consults the enforcing gate inside the registry's non-interleaved turn. On a freshly started
/// silo the gate is cold - the compiled policy snapshot has never been built - so the gate scans
/// the reserved <c>sys-auth-policy</c> tree from inside that turn, before any rule was written and
/// so before that tree was ever registered. The alias change must reach its decision (a denial,
/// for an anonymous caller under a default-deny policy) rather than deadlock on the registry. The
/// same holds for the membership directory's edge walk, which a credentialed caller's cold subject
/// resolution performs inside that turn.
/// </summary>
/// <remarks>
/// Lives beside the apps ownership tests because this project already hosts the auth and
/// membership add-ons and can address the internal registry; the defect is core's.
/// </remarks>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class AliasAccessGateColdSiloTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    private TestCluster _cluster = null!;

    [SetUp]
    public async Task SetUpAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [TearDown]
    public async Task TearDownAsync()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public void SetAlias_by_an_unauthorized_caller_on_a_cold_silo_is_denied_without_deadlocking()
    {
        var registry = _cluster.Client.GetLatticeRegistry();

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            () => registry.SetAliasAsync($"logical-{Guid.NewGuid():N}", $"physical-{Guid.NewGuid():N}").WaitAsync(Bound));
    }

    [Test]
    public async Task Resolving_groups_against_a_never_written_directory_does_not_register_its_tree()
    {
        // Subject resolution on a cache miss walks the membership edges from inside the
        // gate, and so from inside SetAliasAsync's turn. Asserting the registry state rather
        // than the absence of a hang, so a future change cannot reintroduce the registration
        // behind some other timing that happens not to deadlock this test.
        const string EdgesTree = "sys-membership-edges";
        var directory = Silo.GetRequiredService<ILatticeMembershipDirectory>();
        var registry = _cluster.Client.GetLatticeRegistry();

        Assert.That(await directory.GroupsOfAsync("alice").WaitAsync(Bound), Is.Empty);
        Assert.That(await directory.ExpandGroupsAsync(["readers"]).WaitAsync(Bound), Is.EquivalentTo(new[] { "readers" }));
        Assert.That(await registry.ExistsAsync(EdgesTree), Is.False);
    }

    [Test]
    public async Task Building_the_policy_snapshot_from_a_never_written_policy_tree_does_not_register_it()
    {
        const string PolicyTree = "sys-auth-policy";
        var store = Silo.GetRequiredService<ILatticeAuthorizationPolicyStore>();
        var registry = _cluster.Client.GetLatticeRegistry();

        var rules = new List<LatticeAuthorizationRule>();
        await foreach (var rule in store.ListRulesAsync())
        {
            rules.Add(rule);
        }

        Assert.That(rules, Is.Empty);
        Assert.That(await registry.ExistsAsync(PolicyTree), Is.False);
    }

    private IServiceProvider Silo => _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeMembership();
            siloBuilder.AddLatticeAuth(options => options.DefaultEffect = LatticeEffect.Deny);
        }
    }
}
