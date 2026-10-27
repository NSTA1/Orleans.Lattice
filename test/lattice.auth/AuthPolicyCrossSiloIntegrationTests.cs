using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tests.Fakes;
using Orleans.TestingHost;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Cluster-level regression test on a real two-silo <see cref="TestCluster"/>. A
/// policy write is observed by the core change feed only on the silo hosting the
/// committing policy leaf, so before the cross-silo policy epoch the other silo
/// kept its compiled snapshot - and kept admitting a revoked grant - until some
/// later policy write happened to commit on a leaf it hosted. The test revokes a
/// rule through one silo and then asks every silo's own
/// <see cref="ILatticeAccessGate"/>, so whichever silo hosts the leaf, the other
/// one is exercised as the peer.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AuthPolicyCrossSiloIntegrationTests
{
    private const string Tree = "payroll";
    private const string RuleId = "alice-payroll";

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(2);
        builder.UseSharedInMemoryWal();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
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
    public async Task Revoking_a_rule_through_one_silo_is_enforced_on_every_silo()
    {
        var silos = _cluster.Silos.OfType<InProcessSiloHandle>().Select(s => s.SiloHost.Services).ToArray();
        Assert.That(silos, Has.Length.EqualTo(2), "precondition: a two-silo cluster");
        var store = silos[0].GetRequiredService<ILatticeAuthorizationPolicyStore>();

        await store.PutRuleAsync(new LatticeAuthorizationRule(
            RuleId,
            LatticeSubjectSelector.User("alice"),
            LatticeScope.Tree(Tree),
            LatticeOperation.Read,
            LatticeEffect.Allow));

        for (var i = 0; i < silos.Length; i++)
        {
            Assert.That(await SettlesAsync(silos[i], allowed: true), Is.True, $"precondition: silo {i} admits the grant");
        }

        await store.RemoveRuleAsync(Tree, RuleId);

        for (var i = 0; i < silos.Length; i++)
        {
            Assert.That(
                await SettlesAsync(silos[i], allowed: false),
                Is.True,
                $"silo {i} must enforce a revocation committed through silo 0");
        }
    }

    private static async Task<bool> SettlesAsync(IServiceProvider services, bool allowed)
    {
        var gate = services.GetRequiredService<ILatticeAccessGate>();
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (true)
        {
            var decision = await gate.AuthorizeAsync(
                new LatticeAccessRequest(Tree, LatticeOperation.Read, new LatticeSubject("alice"), "k"));
            if (decision.Allowed == allowed)
            {
                return true;
            }

            if (DateTime.UtcNow > deadline)
            {
                return false;
            }

            await Task.Delay(100);
        }
    }

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
