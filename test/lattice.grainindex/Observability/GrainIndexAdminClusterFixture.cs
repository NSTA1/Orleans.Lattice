using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.GrainIndex.Registry;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.GrainIndex.Tests.Observability;

/// <summary>
/// A single-silo <see cref="TestCluster"/> hosting the core lattice, memory
/// grain storage, one declared grain index, and a key source over a dormant
/// population, used by the observability and administrative integration tests.
/// </summary>
/// <remarks>
/// <para>
/// It mirrors the backfill fixture, which is the model for a cluster fixture in
/// this package: <c>AddLattice</c> over in-memory grain storage, then
/// <c>UseInMemoryReminderService</c> - mandatory, because the backfill is
/// reminder-driven and the core lattice grains take an <c>IReminderRegistry</c>
/// dependency of their own - then the package's own registration. It exposes the
/// silo's services because <see cref="TestCluster.ServiceProvider"/> is the
/// client's container, not the silo's, and the administrative surface is a
/// silo-side service.
/// </para>
/// <para>
/// The index's background driver is off and the outbox drain is off. Nothing
/// about the crawl changes: its checkpoint is still durable and its controls
/// still work. It means every pass happens exactly when a test asks for one, so
/// no test in this area waits on wall-clock time, a timer, or a scheduler.
/// </para>
/// <para>
/// Each test seeds a fresh population under fresh keys, written straight into
/// the storage provider the grains read from so that none of them is ever
/// activated - a grain seeded through its own interface would index itself on
/// the spot and leave the crawl nothing to do, and a population reused across
/// tests would still be active from the previous test's crawl.
/// </para>
/// </remarks>
public sealed class GrainIndexAdminClusterFixture
{
    /// <summary>The index the silo declares at start-up.</summary>
    public const string IndexName = "admin-subjects";

    /// <summary>The number of grains a single pass visits in these tests.</summary>
    public const int BatchSize = 2;

    /// <summary>The number of grains in each test's population.</summary>
    public const int PopulationSize = 6;

    /// <summary>The number of index entries one seeded grain contributes.</summary>
    public const int EntriesPerGrain = 2;

    /// <summary>The country every seeded grain stores.</summary>
    public const string Country = "GB";

    /// <summary>The state name the indexed grain's <c>[Indexed]</c> parameter declares.</summary>
    internal const string StateName = "admin-user";

    /// <summary>The storage provider the indexed grain's state is persisted through.</summary>
    internal const string StorageName = "Default";

    private static string[] _population = [];

    /// <summary>
    /// The population the key source currently describes, which the fixture
    /// replaces for each test.
    /// </summary>
    /// <remarks>
    /// It is static because the key source is constructed by the silo's
    /// container and outlives any one test. NUnit runs a fixture's tests one at
    /// a time, so there is no concurrent reader.
    /// </remarks>
    public static IReadOnlyList<string> Population => _population;

    /// <summary>The deployed cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary silo's service provider, where the index declarations live.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The silo-side administrative surface under test.</summary>
    public IGrainIndexAdmin Admin => SiloServices.GetRequiredService<IGrainIndexAdmin>();

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

    /// <summary>
    /// Seeds a fresh, never-activated population under keys unique to
    /// <paramref name="runId"/>, and points the key source at it.
    /// </summary>
    /// <param name="runId">A token unique to the calling test. Must not be <c>null</c>.</param>
    /// <returns>The population's keys, in ascending ordinal order.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="runId"/> is <c>null</c>.</exception>
    public async Task<string[]> SeedDormantPopulationAsync(string runId)
    {
        ArgumentNullException.ThrowIfNull(runId);

        var keys = new string[PopulationSize];
        for (var i = 0; i < PopulationSize; i++)
            keys[i] = $"{runId}-subject-{i + 1}";

        var storage = SiloServices.GetRequiredKeyedService<IGrainStorage>(StorageName);
        for (var i = 0; i < keys.Length; i++)
        {
            // GetGrain resolves a reference without addressing the grain, so
            // nothing here activates anything.
            var grainId = Cluster.GrainFactory.GetGrain<IAdminUserGrain>(keys[i]).GetGrainId();
            var state = new GrainState<AdminUserState>(
                new AdminUserState { Age = 20 + i, Country = Country });

            await storage.WriteStateAsync(StateName, grainId, state);
        }

        _population = keys;
        return keys;
    }

    /// <summary>
    /// Returns the index to the state it would be in if it had just been
    /// declared: every entry gone, every enrolment marker gone, no checkpoint,
    /// and the needs-backfill flag raised as the reconciler leaves it.
    /// </summary>
    /// <returns>A task that completes when the index has been emptied.</returns>
    public async Task ResetIndexAsync()
    {
        using (LatticeSystemOrigin.Enter())
        {
            var registry = Cluster.GrainFactory.GetGrain<ILattice>(GrainIndexRegistryTrees.RegistryTree);
            await registry.DeleteRangeAsync(
                GrainIndexRegistryKeys.SeenPrefix(IndexName),
                GrainIndexRegistryKeys.SeenPrefixEnd(IndexName));

            await registry.DeleteRangeAsync(
                GrainIndexRegistryKeys.Checkpoint(IndexName),
                GrainIndexRegistryKeys.Checkpoint(IndexName) + "\u0000");

            var tree = Cluster.GrainFactory.GetGrain<ILattice>(GrainIndexTreeNames.ForIndex(IndexName));
            await tree.DeleteRangeAsync("\u0000", "\uFFFF");
        }

        var store = SiloServices.GetRequiredService<IGrainIndexRegistryStore>();
        var record = await store.ReadAsync(IndexName, CancellationToken.None);
        if (record is { NeedsBackfill: false })
        {
            await store.WriteAsync(
                IndexName,
                new GrainIndexRegistryRecord(
                    record.Descriptor,
                    record.KeyCodecId,
                    record.Fingerprint,
                    needsBackfill: true),
                CancellationToken.None);
        }
    }

    /// <summary>
    /// Drives the crawl to completion, one explicit pass at a time, with the
    /// number of passes bounded by the population rather than by a timeout.
    /// </summary>
    /// <returns>The number of passes run.</returns>
    public async Task<int> DrainBackfillAsync()
    {
        var admin = Admin;
        var maxPasses = (PopulationSize / BatchSize) + 3;
        for (var pass = 1; pass <= maxPasses; pass++)
        {
            var result = await admin.RunBackfillPassAsync(IndexName);
            if (result.State != GrainIndexBackfillState.Running)
                return pass;
        }

        return maxPasses;
    }

    /// <summary>The number of entries the index tree currently holds.</summary>
    /// <returns>The entry count.</returns>
    public async Task<int> IndexEntryCountAsync()
    {
        using (LatticeSystemOrigin.Enter())
        {
            var tree = Cluster.GrainFactory.GetGrain<ILattice>(GrainIndexTreeNames.ForIndex(IndexName));
            return await tree.CountAsync();
        }
    }

    /// <summary>
    /// A key source over the current population that also reports its size, so
    /// the percent-complete progress has a denominator.
    /// </summary>
    public sealed class PopulationKeySource : IGrainKeySource
    {
        /// <inheritdoc />
        public async IAsyncEnumerable<string> EnumerateKeysAsync(
            string? resumeAfterExclusive,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var population = _population;
            for (var i = 0; i < population.Length; i++)
            {
                var key = population[i];
                if (resumeAfterExclusive is not null
                    && string.CompareOrdinal(key, resumeAfterExclusive) <= 0)
                {
                    continue;
                }

                cancellationToken.ThrowIfCancellationRequested();
                yield return key;
                await Task.Yield();
            }
        }

        /// <inheritdoc />
        public ValueTask<long?> TryGetApproximateCountAsync(CancellationToken cancellationToken) =>
            ValueTask.FromResult<long?>(_population.Length);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddMemoryGrainStorage(StorageName);

            siloBuilder.AddGrainIndex<IAdminUserGrain, AdminUserState>(
                static cfg => cfg
                    .WithName(IndexName)
                    .Include(x => x.Age)
                    .Include(x => x.Country));

            siloBuilder.AddGrainIndexKeySource<PopulationKeySource>(IndexName);

            siloBuilder.ConfigureGrainIndex(IndexName, static options =>
            {
                options.BackfillBatchSize = BatchSize;
                options.BackfillEnabled = false;
            });

            siloBuilder.ConfigureGrainIndexOutbox(static options => options.Enabled = false);
        }
    }
}
