using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Storage;

/// <summary>
/// End-to-end tests for the grain-storage fencing check that <c>AddLattice</c>
/// registers (issue #4200): a real silo runs the probe as it becomes active, finds
/// Orleans' memory storage fenced, finds a non-fencing provider unfenced, and fails
/// to start on the latter only when the mode is
/// <see cref="LatticeGrainStorageFencingMode.Reject"/>.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class GrainStorageFencingCheckIntegrationTests
{
    [Test]
    public async Task Silo_with_orleans_memory_storage_passes_the_check()
    {
        await using var cluster = await DeployAsync<MemoryStorageConfigurator>();

        var check = ResolveCheck(cluster);

        Assert.That(check.LastResult, Is.Not.Null, "the probe ran during silo start");
        Assert.That(check.LastResult!.Verdict, Is.EqualTo(GrainStorageFencingVerdict.Fenced), check.LastResult.Reason);
    }

    [Test]
    public async Task Silo_with_non_fencing_storage_is_flagged_and_still_starts_by_default()
    {
        await using var cluster = await DeployAsync<NonFencingWarnConfigurator>();

        var check = ResolveCheck(cluster);

        Assert.That(check.LastResult!.Verdict, Is.EqualTo(GrainStorageFencingVerdict.Unfenced));
    }

    [Test]
    public async Task Silo_with_non_fencing_storage_fails_to_start_in_reject_mode()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<NonFencingRejectConfigurator>();
        var cluster = builder.Build();
        try
        {
            var ex = Assert.CatchAsync(() => cluster.DeployAsync());

            Assert.That(Chain(ex).OfType<OrleansConfigurationException>().Any(), Is.True, ex!.ToString());
        }
        finally
        {
            try
            {
                await cluster.DisposeAsync();
            }
            catch
            {
                // The silo never started; disposal of a half-started cluster may throw.
            }
        }
    }

    [Test]
    public async Task Silo_with_check_disabled_does_not_probe()
    {
        await using var cluster = await DeployAsync<NonFencingDisabledConfigurator>();

        var check = ResolveCheck(cluster);

        Assert.That(check.LastResult, Is.Null);
    }

    [Test]
    public void ConfigureLatticeGrainStorageFencing_null_arguments_throw()
    {
        var silo = NSubstitute.Substitute.For<ISiloBuilder>();

        Assert.Throws<ArgumentNullException>(() => LatticeServiceCollectionExtensions.ConfigureLatticeGrainStorageFencing(null!, _ => { }));
        Assert.Throws<ArgumentNullException>(() => silo.ConfigureLatticeGrainStorageFencing(null!));
    }

    private static async Task<TestCluster> DeployAsync<TConfigurator>()
        where TConfigurator : ISiloConfigurator, new()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<TConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    private static GrainStorageFencingCheck ResolveCheck(TestCluster cluster)
    {
        var services = ((InProcessSiloHandle)cluster.Primary).SiloHost.Services;
        return services.GetServices<ILifecycleParticipant<ISiloLifecycle>>()
            .OfType<GrainStorageFencingCheck>()
            .Single();
    }

    private static IEnumerable<Exception> Chain(Exception? ex)
    {
        while (ex is not null)
        {
            yield return ex;
            if (ex is AggregateException aggregate)
            {
                foreach (var inner in aggregate.InnerExceptions.SelectMany(Chain))
                {
                    yield return inner;
                }

                yield break;
            }

            ex = ex.InnerException;
        }
    }

    private static void AddNonFencingLattice(ISiloBuilder siloBuilder)
    {
        siloBuilder.AddLattice((silo, name) =>
            silo.Services.AddKeyedSingleton<IGrainStorage>(name, (_, _) => new NonFencingGrainStorage()));
        siloBuilder.UseInMemoryReminderService();
    }

    private sealed class MemoryStorageConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.ConfigureLatticeGrainStorageFencing(o => o.Mode = LatticeGrainStorageFencingMode.Reject);
        }
    }

    private sealed class NonFencingWarnConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder) => AddNonFencingLattice(siloBuilder);
    }

    private sealed class NonFencingRejectConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            AddNonFencingLattice(siloBuilder);
            siloBuilder.ConfigureLatticeGrainStorageFencing(o => o.Mode = LatticeGrainStorageFencingMode.Reject);
        }
    }

    private sealed class NonFencingDisabledConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            AddNonFencingLattice(siloBuilder);
            siloBuilder.ConfigureLatticeGrainStorageFencing(o => o.Mode = LatticeGrainStorageFencingMode.Disabled);
        }
    }
}
