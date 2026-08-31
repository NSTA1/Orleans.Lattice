using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.GrainIndex.Registry;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.GrainIndex.Tests.Backfill;

/// <summary>
/// A single-silo <see cref="TestCluster"/> hosting the core lattice, memory
/// grain storage, one declared grain index, and a key source over a dormant
/// population, used by the backfill integration tests.
/// </summary>
/// <remarks>
/// <para>
/// The silo composes as every other lattice cluster fixture in the repository
/// does: <c>AddLattice</c> over in-memory grain storage, then
/// <c>UseInMemoryReminderService</c> - which this fixture needs more than most,
/// since the backfill is reminder-driven and the core lattice grains take an
/// <c>IReminderRegistry</c> dependency too - then the package's own registration.
/// </para>
/// <para>
/// The index's background driver is switched off. Nothing about the crawl itself
/// changes: its checkpoint is still durable, its control primitives still work,
/// and the tests run its passes by calling them. That is what makes the pacing
/// and resume tests deterministic - a pass happens at an exact, chosen moment
/// rather than whenever a timer fires alongside the assertions. No test in this
/// area waits on wall-clock time. The outbox drain is off for the same reason.
/// </para>
/// <para>
/// Each test gets a <b>fresh population under fresh keys</b>, seeded straight
/// into the storage provider the grains read from so that none of them is ever
/// activated. That matters twice over: a grain seeded through its own interface
/// would index itself on the spot and leave the crawl nothing to do, and a
/// population reused across tests would still be activated from the previous
/// test's crawl. Fresh keys make every test's population genuinely dormant
/// without depending on a deactivation having settled.
/// </para>
/// </remarks>
public sealed class GrainIndexBackfillClusterFixture
{
    /// <summary>The index the silo declares at start-up.</summary>
    public const string IndexName = "backfill-subjects";

    /// <summary>The number of grains a single pass visits in these tests.</summary>
    public const int BatchSize = 2;

    /// <summary>The number of grains in each test's population.</summary>
    public const int PopulationSize = 6;

    /// <summary>The country every seeded grain stores, so one predicate selects a whole population.</summary>
    public const string Country = "GB";

    /// <summary>The state name the indexed grain's <c>[Indexed]</c> parameter declares.</summary>
    internal const string StateName = "backfill-user";

    /// <summary>The storage provider the indexed grain's state is persisted through.</summary>
    internal const string StorageName = "Default";

    private static string[] _population = [];

    /// <summary>
    /// The population the key source currently describes, which the fixture
    /// replaces for each test.
    /// </summary>
    /// <remarks>
    /// It is static because the key source is constructed by the silo's
    /// container and outlives any one test, and mutable because each test needs
    /// its own never-activated key set. NUnit runs a fixture's tests one at a
    /// time, so there is no concurrent reader.
    /// </remarks>
    public static IReadOnlyList<string> Population => _population;

    /// <summary>The deployed cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary silo's service provider, where the index declarations live.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

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
            var grainId = Cluster.GrainFactory.GetGrain<IBackfillUserGrain>(keys[i]).GetGrainId();
            var state = new GrainState<BackfillUserState>(
                new BackfillUserState { Age = 20 + i, Country = Country });

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

    /// <summary>The encoded grain keys the index currently records as enrolled.</summary>
    /// <returns>The enrolled keys, in ascending order.</returns>
    public async Task<List<string>> EnrolledKeysAsync()
    {
        var enrolled = new List<string>();
        var prefix = GrainIndexRegistryKeys.SeenPrefix(IndexName);

        using (LatticeSystemOrigin.Enter())
        {
            var registry = Cluster.GrainFactory.GetGrain<ILattice>(GrainIndexRegistryTrees.RegistryTree);
            var keys = registry.KeysAsync(prefix, GrainIndexRegistryKeys.SeenPrefixEnd(IndexName));
            await foreach (var key in keys)
                enrolled.Add(key[prefix.Length..]);
        }

        return enrolled;
    }

    /// <summary>The number of entries the index tree currently holds.</summary>
    /// <returns>The entry count.</returns>
    public async Task<int> IndexEntryCountAsync()
    {
        var count = 0;
        using (LatticeSystemOrigin.Enter())
        {
            var tree = Cluster.GrainFactory.GetGrain<ILattice>(GrainIndexTreeNames.ForIndex(IndexName));
            await foreach (var _ in tree.KeysAsync("\u0000", "\uFFFF"))
                count++;
        }

        return count;
    }

    /// <summary>
    /// A key source over the current population. Ordinal ordering and exclusive
    /// resumption are its whole contract.
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
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddMemoryGrainStorage(StorageName);

            siloBuilder.AddGrainIndex<IBackfillUserGrain, BackfillUserState>(
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
