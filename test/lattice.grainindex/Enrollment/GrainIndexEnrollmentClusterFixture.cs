using Orleans.Hosting;
using Orleans.TestingHost;

namespace Orleans.Lattice.GrainIndex.Tests.Enrollment;

/// <summary>
/// A single-silo <see cref="TestCluster"/> hosting the core lattice, memory
/// grain storage, and three declared grain indexes, used by the enrolment
/// integration tests.
/// </summary>
/// <remarks>
/// <para>
/// The silo composes as every other lattice cluster fixture in the repository
/// does: <c>AddLattice</c> over in-memory grain storage, then
/// <c>UseInMemoryReminderService</c> - core lattice grains such as the
/// tombstone-compaction grain take an <c>IReminderRegistry</c> dependency, so a
/// silo that hosts the lattice without it cannot activate them - then the
/// package's own registration.
/// </para>
/// <para>
/// The background outbox drain is switched off. Nothing about the outbox itself
/// changes: entries are still recorded, and the tests apply them by calling the
/// drain directly. That is what makes the retry tests deterministic - the pass
/// happens at an exact, chosen moment rather than whenever a timer fires
/// alongside the assertions.
/// </para>
/// <para>
/// One silo rather than the <see cref="TestClusterBuilder"/> default, so a
/// second silo's start-up reconciliation cannot race the first's over the same
/// registry keys.
/// </para>
/// </remarks>
public sealed class GrainIndexEnrollmentClusterFixture
{
    /// <summary>The index tracking <see cref="IIndexedUserGrain"/>, in synchronous mode.</summary>
    public const string UsersIndex = "enrolment-users";

    /// <summary>The index tracking <see cref="IEventualUserGrain"/>, in eventual mode.</summary>
    public const string EventualIndex = "enrolment-eventual";

    /// <summary>The index tracking <see cref="IBaseClassUserGrain"/>, in synchronous mode.</summary>
    public const string BaseClassIndex = "enrolment-baseclass";

    /// <summary>The deployed cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>Deploys the cluster.</summary>
    /// <returns>A task that completes when the silo is ready.</returns>
    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    /// <summary>Stops and disposes the cluster.</summary>
    /// <returns>A task that completes when the silo has stopped.</returns>
    public async Task DisposeAsync()
    {
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddMemoryGrainStorage("Default");

            siloBuilder.AddGrainIndex<IIndexedUserGrain, IndexedUserState>(
                static cfg => cfg
                    .WithName(UsersIndex)
                    .Include(x => x.Age)
                    .Include(x => x.Country));

            siloBuilder.AddGrainIndex<IEventualUserGrain, IndexedUserState>(
                static cfg => cfg
                    .WithName(EventualIndex)
                    .Include(x => x.Age));

            siloBuilder.AddGrainIndex<IBaseClassUserGrain, IndexedUserState>(
                static cfg => cfg
                    .WithName(BaseClassIndex)
                    .Include(x => x.Age));

            siloBuilder.ConfigureGrainIndex(
                EventualIndex,
                static options => options.ProjectionMode = GrainIndexProjectionMode.Eventual);

            siloBuilder.ConfigureGrainIndexOutbox(static options => options.Enabled = false);
        }
    }
}
