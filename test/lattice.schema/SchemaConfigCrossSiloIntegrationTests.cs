using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Tests.Fakes;
using Orleans.TestingHost;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Regression tests for #5238 on a real two-silo <see cref="TestCluster"/>: a schema
/// policy or version-config change committed through one silo must reach every
/// other silo's cached provider within the documented propagation interval. Before
/// the fix a silo learned of a change only when it served the admin call or hosted
/// the leaf that committed the <c>sys-schema-*</c> write, and every other silo kept
/// its cached answer until restart.
/// </summary>
/// <remarks>
/// The store-level tests write through silo 0's store, which is the admin's durable
/// step without its local eviction: whichever silo does not host the committing leaf
/// can then only learn of the change through cross-silo propagation, so the test
/// exercises it deterministically wherever the leaf lands.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class SchemaConfigCrossSiloIntegrationTests
{
    // The documented bound (the default propagation interval, which this fixture
    // keeps) plus slack for publishing the change and for a loaded CI host.
    private static readonly TimeSpan Budget =
        new LatticeSchemaEnforcementOptions().PropagationInterval + TimeSpan.FromSeconds(5);

    private TestCluster _cluster = null!;
    private IServiceProvider[] _silos = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(2);
        builder.UseSharedInMemoryWal();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        await SharedInMemoryWal.AssertAllSilosShareOneWalAsync(_cluster);
        _silos = _cluster.Silos.OfType<InProcessSiloHandle>().Select(s => s.SiloHost.Services).ToArray();
        Assert.That(_silos, Has.Length.EqualTo(2), "precondition: a two-silo cluster");
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
    public async Task Policy_written_through_one_silo_reaches_every_silo_within_the_propagation_interval()
    {
        const string tree = "xsilo-policy-store";
        foreach (var silo in _silos)
        {
            Assert.That(
                await silo.GetRequiredService<ILatticeSchemaPolicyProvider>().GetCompiledPolicyAsync(tree),
                Is.Null,
                "precondition: every silo caches the tree as ungoverned");
        }

        await _silos[0].GetRequiredService<ILatticeSchemaPolicyStore>()
            .SetPolicyAsync(tree, new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() }));

        for (var i = 0; i < _silos.Length; i++)
        {
            var provider = _silos[i].GetRequiredService<ILatticeSchemaPolicyProvider>();
            var elapsed = await ConvergeAsync(async () => await provider.GetCompiledPolicyAsync(tree) is not null);
            Assert.That(elapsed, Is.Not.Null, $"silo {i} must observe a policy committed through silo 0 within {Budget}");
        }
    }

    [Test]
    public async Task Version_config_written_through_one_silo_reaches_every_silo_within_the_propagation_interval()
    {
        const string tree = "xsilo-version-store";
        foreach (var silo in _silos)
        {
            Assert.That(
                await silo.GetRequiredService<ILatticeSchemaVersionProvider>().GetConfigAsync(tree),
                Is.Null,
                "precondition: every silo caches the tree as unversioned");
        }

        await _silos[0].GetRequiredService<ILatticeSchemaVersionStore>()
            .SetConfigAsync(tree, new LatticeSchemaVersionConfig(schemaId: 1, targetVersion: 1));

        for (var i = 0; i < _silos.Length; i++)
        {
            var provider = _silos[i].GetRequiredService<ILatticeSchemaVersionProvider>();
            var elapsed = await ConvergeAsync(async () => await provider.GetConfigAsync(tree) is not null);
            Assert.That(elapsed, Is.Not.Null, $"silo {i} must observe a version config committed through silo 0 within {Budget}");
        }
    }

    [Test]
    public async Task Admin_policy_change_is_immediate_on_the_admin_silo_and_enforced_through_every_silo_within_the_propagation_interval()
    {
        const string tree = "xsilo-policy-admin";
        foreach (var silo in _silos)
        {
            await Lattice(silo, tree).SetAsync("warm", Encoding.UTF8.GetBytes("not json"));
        }

        await _silos[0].GetRequiredService<ILatticeSchemaAdmin>()
            .SetPolicyAsync(tree, new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() }));

        Assert.That(
            await _silos[0].GetRequiredService<ILatticeSchemaPolicyProvider>().GetCompiledPolicyAsync(tree),
            Is.Not.Null,
            "the admin's own silo must enforce the new policy as soon as SetPolicyAsync returns");

        for (var i = 0; i < _silos.Length; i++)
        {
            var lattice = Lattice(_silos[i], tree);
            var elapsed = await ConvergeAsync(async () =>
            {
                try
                {
                    await lattice.SetAsync("k" + i, Encoding.UTF8.GetBytes("not json"));
                    return false;
                }
                catch (LatticeSchemaViolationException)
                {
                    return true;
                }
            });
            Assert.That(elapsed, Is.Not.Null, $"a write through silo {i} must be validated under the new policy within {Budget}");
        }
    }

    [Test]
    public async Task Epoch_grain_advances_monotonically_within_one_incarnation()
    {
        var grain = _cluster.GrainFactory.GetGrain<ISchemaConfigEpochGrain>("xsilo-epoch-probe");

        var before = await grain.GetAsync();
        var advanced = await grain.AdvanceAsync();
        var after = await grain.GetAsync();

        Assert.That(advanced.Incarnation, Is.EqualTo(before.Incarnation));
        Assert.That(advanced.Version, Is.EqualTo(before.Version + 1));
        Assert.That(after, Is.EqualTo(advanced));
    }

    private static ILattice Lattice(IServiceProvider silo, string tree) =>
        silo.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(tree);

    private static async Task<TimeSpan?> ConvergeAsync(Func<Task<bool>> converged)
    {
        var clock = Stopwatch.StartNew();
        while (true)
        {
            if (await converged())
            {
                return clock.Elapsed;
            }

            if (clock.Elapsed > Budget)
            {
                return null;
            }

            await Task.Delay(50);
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeSchemaEnforcement();
            siloBuilder.AddLatticeSchemaVersioning();
        }
    }
}
