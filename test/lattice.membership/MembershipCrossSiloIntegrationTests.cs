using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.TestingHost;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Cluster-level regression test for issue #5518 on a real two-silo
/// <see cref="TestCluster"/>. A <c>sys-membership-*</c> write is observed by the
/// core change feed only on the silo hosting the committing leaf, so before the
/// cross-silo membership epoch every other silo kept serving its cached
/// resolution of a removed member - with the group they were removed from - for
/// up to <see cref="LatticeMembershipOptions.ResolutionCacheTtl"/> (five minutes
/// by default). The test warms the resolution cache on every silo, removes the
/// member through one silo, and then resolves on every silo, so whichever silo
/// hosts the committing leaf, the other one is exercised as the peer.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class MembershipCrossSiloIntegrationTests
{
    /// <summary>
    /// How long a removal may take to reach every silo. Propagation is one epoch
    /// advance plus at most one poll (about a second); the bound leaves headroom
    /// for a loaded CI host while staying far below the five-minute cache lifetime
    /// the defect served stale for.
    /// </summary>
    private static readonly TimeSpan PropagationBound = TimeSpan.FromSeconds(10);

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(2);
        builder.UseSharedInMemoryWal();
        builder.AddSiloBuilderConfigurator<MembershipClusterFixture.SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        await SharedInMemoryWal.AssertAllSilosShareOneWalAsync(_cluster);
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task Removing_a_member_through_one_silo_is_enforced_on_every_silo_within_the_propagation_bound()
    {
        const string Group = "xs-stewards";
        const string Member = "xs-priya";

        var silos = _cluster.Silos.OfType<InProcessSiloHandle>().Select(s => s.SiloHost.Services).ToArray();
        Assert.That(silos, Has.Length.EqualTo(2), "precondition: a two-silo cluster");
        var directory = silos[0].GetRequiredService<ILatticeMembershipDirectory>();

        await directory.UpsertGroupAsync(new MembershipGroup(Group));
        await directory.AddMemberAsync(Group, Member);
        var token = MembershipClusterFixture.MintToken(Member);

        // Warm every silo's resolution cache with the subject still in the group.
        for (var i = 0; i < silos.Length; i++)
        {
            var silo = silos[i];
            await TestPoll.UntilAsync(
                async () => (await ResolveAsync(silo, token)).GroupIds.Contains(Group),
                $"precondition: silo {i} resolves {Member} into {Group}",
                PropagationBound);
        }

        await directory.RemoveMemberAsync(Group, Member);
        var removed = Stopwatch.StartNew();

        for (var i = 0; i < silos.Length; i++)
        {
            var silo = silos[i];
            var remaining = PropagationBound - removed.Elapsed;
            var settled = remaining > TimeSpan.Zero
                && await TestPoll.TryUntilAsync(
                    async () => !(await ResolveAsync(silo, token)).GroupIds.Contains(Group),
                    remaining,
                    TimeSpan.FromMilliseconds(50));

            Assert.That(
                settled,
                Is.True,
                $"silo {i} must stop resolving {Member} into {Group} within {PropagationBound.TotalSeconds:0} s of a removal "
                + "committed through silo 0, not serve the cached pre-removal groups until the cache entry expires");
        }
    }

    private static async Task<LatticeSubject> ResolveAsync(IServiceProvider silo, string token)
    {
        var context = silo.GetRequiredService<ILatticeMembershipContext>();
        using (LatticeCredentialContext.Use(token, scheme: "Bearer"))
        {
            return await context.ResolveCurrentAsync();
        }
    }
}
