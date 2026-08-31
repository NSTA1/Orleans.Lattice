using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.GrainIndex.Registry;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.GrainIndex.Tests.EndToEnd;

/// <summary>
/// A single-silo <see cref="TestCluster"/> hosting the core lattice, memory
/// grain storage, and the three grain indexes the end-to-end tests need: one
/// onboarded purely by the activation path, one onboarded purely by the
/// background crawl, and one whose declaration the drift tests change underneath
/// it.
/// </summary>
/// <remarks>
/// <para>
/// The silo composes as every other lattice cluster fixture in this package
/// does: <c>AddLattice</c> over in-memory grain storage, then
/// <c>UseInMemoryReminderService</c> - which is mandatory rather than
/// decorative, because the core lattice grains take an <c>IReminderRegistry</c>
/// dependency and a silo without one fails while activating them - then the
/// package's own registration. The silo's own services are exposed because
/// <see cref="TestCluster.ServiceProvider"/> is the <i>client's</i> container
/// and every seam these tests drive is silo-side.
/// </para>
/// <para>
/// Every background driver is off: no backfill reminder, no backfill timer, and
/// no outbox drain. Nothing about the machinery changes - checkpoints are still
/// durable and the controls still work - but a pass happens exactly when a test
/// asks for one. That is what lets the whole area assert convergence without a
/// single <c>Task.Delay</c>, timeout, or clock reading: the drain loop below is
/// bounded by a pass count derived from the population size.
/// </para>
/// <para>
/// The two convergence paths are deliberately given <b>separate indexes over
/// separate grain types but the same key strings and the same states</b>. An
/// entry key carries the property name, the encoded value, and the grain key,
/// and the encoded key of a string-keyed grain is its key verbatim, so the two
/// paths must produce byte-identical entry sets. Comparing them is therefore a
/// direct equality rather than a translation.
/// </para>
/// </remarks>
public sealed class GrainIndexEndToEndClusterFixture
{
    /// <summary>The index onboarded only by the activation path.</summary>
    public const string ActiveIndex = "e2e-active";

    /// <summary>The index onboarded only by the background backfill crawl.</summary>
    public const string BackfillIndex = "e2e-backfill";

    /// <summary>The index whose declaration the drift tests change.</summary>
    public const string DriftIndex = "e2e-drift";

    /// <summary>The number of grains a single crawl pass visits.</summary>
    public const int BatchSize = 2;

    /// <summary>The number of grains in each test's population.</summary>
    public const int PopulationSize = 6;

    /// <summary>The number of index entries one grain contributes under the live declarations.</summary>
    public const int EntriesPerGrain = 2;

    /// <summary>The storage provider every indexed grain's state is persisted through.</summary>
    internal const string StorageName = "Default";

    private static string[] _backfillPopulation = [];
    private static string[] _driftPopulation = [];

    /// <summary>The deployed cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>
    /// The primary silo's service provider. The cluster's own
    /// <see cref="TestCluster.ServiceProvider"/> is the <i>client's</i>, which
    /// carries none of the silo-side registrations these tests drive.
    /// </summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The silo-side administrative surface that steps the crawl.</summary>
    public IGrainIndexAdmin Admin => SiloServices.GetRequiredService<IGrainIndexAdmin>();

    /// <summary>
    /// The declaration the live indexes project, mirrored so a test can compute
    /// the exact entry set a population ought to produce.
    /// </summary>
    /// <remarks>
    /// An entry key is independent of the index and the grain type, so one
    /// projector describes the expected shape of every live index here.
    /// </remarks>
    /// <returns>The live two-property declaration.</returns>
    public static GrainIndexDefinition<IEndToEndDormantUserGrain, EndToEndUserState> LiveDefinition() =>
        new(
            BackfillIndex,
            StringGrainKeyCodec<IEndToEndDormantUserGrain>.Instance,
            [
                new TypedGrainIndexProperty<EndToEndUserState, int>("Age", static s => s.Age),
                new TypedGrainIndexProperty<EndToEndUserState, string>("Country", static s => s.Country),
            ]);

    /// <summary>
    /// The superseded declaration the rebuild test builds an index under before
    /// the current one replaces it: it projects a property the live declaration
    /// does not, so its entries are visibly orphaned by the change.
    /// </summary>
    /// <returns>The superseded two-property declaration.</returns>
    public static GrainIndexDefinition<IEndToEndDriftUserGrain, EndToEndUserState> SupersededDefinition() =>
        new(
            DriftIndex,
            StringGrainKeyCodec<IEndToEndDriftUserGrain>.Instance,
            [
                new TypedGrainIndexProperty<EndToEndUserState, int>("Age", static s => s.Age),
                new TypedGrainIndexProperty<EndToEndUserState, string>("Nickname", static s => s.Nickname),
            ]);

    /// <summary>
    /// Builds a population whose keys are unique to <paramref name="runId"/> and
    /// ascend ordinally, which is the ordering a key source must honour.
    /// </summary>
    /// <param name="runId">A token unique to the calling test. Must not be <c>null</c>.</param>
    /// <returns>The population, in ascending key order.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="runId"/> is <c>null</c>.</exception>
    public static EndToEndPerson[] Population(string runId)
    {
        ArgumentNullException.ThrowIfNull(runId);

        var people = new EndToEndPerson[PopulationSize];
        for (var i = 0; i < PopulationSize; i++)
        {
            people[i] = new EndToEndPerson(
                $"{runId}-user-{i + 1}",
                new EndToEndUserState
                {
                    Age = 20 + (i * 5),
                    Country = i % 2 == 0 ? "GB" : "FR",
                    Nickname = $"nick-{i + 1}",
                });
        }

        return people;
    }

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
    /// Writes a population through the grains' own interface, so each one
    /// activates and enrols itself exactly as ordinary traffic would.
    /// </summary>
    /// <param name="people">The population to write. Must not be <c>null</c>.</param>
    /// <returns>A task that completes when every grain's entries are durable.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="people"/> is <c>null</c>.</exception>
    public async Task WriteThroughGrainsAsync(EndToEndPerson[] people)
    {
        ArgumentNullException.ThrowIfNull(people);

        for (var i = 0; i < people.Length; i++)
        {
            var person = people[i];
            await Cluster.GrainFactory
                .GetGrain<IEndToEndActiveUserGrain>(person.Key)
                .SetAsync(person.State.Age, person.State.Country);
        }
    }

    /// <summary>
    /// Seeds a never-activated population for the backfill index and points its
    /// key source at it.
    /// </summary>
    /// <param name="people">The population to seed. Must not be <c>null</c>.</param>
    /// <returns>A task that completes when every grain's state is durable.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="people"/> is <c>null</c>.</exception>
    public async Task SeedDormantBackfillAsync(EndToEndPerson[] people)
    {
        ArgumentNullException.ThrowIfNull(people);

        await SeedDormantAsync<IEndToEndDormantUserGrain>(EndToEndStateNames.Dormant, people);
        _backfillPopulation = KeysOf(people);
    }

    /// <summary>
    /// Seeds a never-activated population for the drift index and points its key
    /// source at it.
    /// </summary>
    /// <param name="people">The population to seed. Must not be <c>null</c>.</param>
    /// <returns>A task that completes when every grain's state is durable.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="people"/> is <c>null</c>.</exception>
    public async Task SeedDormantDriftAsync(EndToEndPerson[] people)
    {
        ArgumentNullException.ThrowIfNull(people);

        await SeedDormantAsync<IEndToEndDriftUserGrain>(EndToEndStateNames.Drift, people);
        _driftPopulation = KeysOf(people);
    }

    /// <summary>
    /// Returns an index to the state it would be in if it had just been
    /// declared: every entry gone, every enrolment marker gone, no checkpoint,
    /// and the needs-backfill flag raised as the reconciler leaves it.
    /// </summary>
    /// <param name="indexName">The index to empty. Must not be <c>null</c>.</param>
    /// <returns>A task that completes when the index has been emptied.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    public async Task ResetIndexAsync(string indexName)
    {
        ArgumentNullException.ThrowIfNull(indexName);

        using (LatticeSystemOrigin.Enter())
        {
            var registry = Cluster.GrainFactory.GetGrain<ILattice>(GrainIndexRegistryTrees.RegistryTree);
            await registry.DeleteRangeAsync(
                GrainIndexRegistryKeys.SeenPrefix(indexName),
                GrainIndexRegistryKeys.SeenPrefixEnd(indexName));

            await registry.DeleteRangeAsync(
                GrainIndexRegistryKeys.PendingPrefix(indexName),
                GrainIndexRegistryKeys.PendingPrefixEnd(indexName));

            await registry.DeleteRangeAsync(
                GrainIndexRegistryKeys.Checkpoint(indexName),
                GrainIndexRegistryKeys.Checkpoint(indexName) + "\u0000");

            var tree = Cluster.GrainFactory.GetGrain<ILattice>(GrainIndexTreeNames.ForIndex(indexName));
            await tree.DeleteRangeAsync("\u0000", "\uFFFF");
        }

        var store = SiloServices.GetRequiredService<IGrainIndexRegistryStore>();
        var record = await store.ReadAsync(indexName, CancellationToken.None);
        if (record is { NeedsBackfill: false })
        {
            await store.WriteAsync(
                indexName,
                new GrainIndexRegistryRecord(
                    record.Descriptor,
                    record.KeyCodecId,
                    record.Fingerprint,
                    needsBackfill: true),
                CancellationToken.None);
        }
    }

    /// <summary>
    /// Drives a crawl to completion, one explicit pass at a time, with the pass
    /// count bounded by the population rather than by a timeout.
    /// </summary>
    /// <param name="indexName">The index whose crawl to drive. Must not be <c>null</c>.</param>
    /// <returns>The number of passes run.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    public async Task<int> DrainBackfillAsync(string indexName)
    {
        ArgumentNullException.ThrowIfNull(indexName);

        var admin = Admin;

        // One pass per batch, plus the pass that finds the source exhausted and
        // a small allowance for a batch whose grains were all skipped. Nothing
        // here observes a clock: the loop either sees the crawl leave the
        // running state or exhausts a bound the population size fixes.
        var maxPasses = (PopulationSize / BatchSize) + 3;
        for (var pass = 1; pass <= maxPasses; pass++)
        {
            var result = await admin.RunBackfillPassAsync(indexName);
            if (result.State != GrainIndexBackfillState.Running)
                return pass;
        }

        return maxPasses;
    }

    /// <summary>Reads every entry an index's tree currently holds.</summary>
    /// <param name="indexName">The index to read. Must not be <c>null</c>.</param>
    /// <returns>The entries, in ascending key order.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    public async Task<List<GrainIndexEntry>> IndexEntriesAsync(string indexName)
    {
        ArgumentNullException.ThrowIfNull(indexName);

        var entries = new List<GrainIndexEntry>();
        using (LatticeSystemOrigin.Enter())
        {
            var tree = Cluster.GrainFactory.GetGrain<ILattice>(GrainIndexTreeNames.ForIndex(indexName));
            await foreach (var entry in tree.EntriesAsync("\u0000", "\uFFFF"))
                entries.Add(new GrainIndexEntry(entry.Key, entry.Value));
        }

        return entries;
    }

    /// <summary>The encoded grain keys an index currently records as enrolled.</summary>
    /// <param name="indexName">The index to read. Must not be <c>null</c>.</param>
    /// <returns>The enrolled keys, in ascending order.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    public async Task<List<string>> EnrolledKeysAsync(string indexName)
    {
        ArgumentNullException.ThrowIfNull(indexName);

        var enrolled = new List<string>();
        var prefix = GrainIndexRegistryKeys.SeenPrefix(indexName);

        using (LatticeSystemOrigin.Enter())
        {
            var registry = Cluster.GrainFactory.GetGrain<ILattice>(GrainIndexRegistryTrees.RegistryTree);
            var keys = registry.KeysAsync(prefix, GrainIndexRegistryKeys.SeenPrefixEnd(indexName));
            await foreach (var key in keys)
                enrolled.Add(key[prefix.Length..]);
        }

        return enrolled;
    }

    /// <summary>Resolves a declared index's query surface from the silo's container.</summary>
    /// <typeparam name="TGrain">The indexed grain interface type.</typeparam>
    /// <param name="indexName">The declared index name. Must not be <c>null</c>.</param>
    /// <returns>The query surface.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    public IGrainIndex<TGrain, EndToEndUserState> Index<TGrain>(string indexName)
        where TGrain : IGrain
    {
        ArgumentNullException.ThrowIfNull(indexName);

        return SiloServices
            .GetRequiredService<IGrainIndexProvider>()
            .GetIndex<TGrain, EndToEndUserState>(indexName);
    }

    private static string[] KeysOf(EndToEndPerson[] people)
    {
        var keys = new string[people.Length];
        for (var i = 0; i < people.Length; i++)
            keys[i] = people[i].Key;

        return keys;
    }

    private async Task SeedDormantAsync<TGrain>(string stateName, EndToEndPerson[] people)
        where TGrain : IGrainWithStringKey
    {
        var storage = SiloServices.GetRequiredKeyedService<IGrainStorage>(StorageName);
        for (var i = 0; i < people.Length; i++)
        {
            // GetGrain resolves a reference without addressing the grain, so
            // nothing here activates anything - which is the whole point: an
            // activation is precisely what would index the grain and leave the
            // crawl with nothing to do.
            var grainId = Cluster.GrainFactory.GetGrain<TGrain>(people[i].Key).GetGrainId();
            await storage.WriteStateAsync(
                stateName, grainId, new GrainState<EndToEndUserState>(people[i].State));
        }
    }

    /// <summary>
    /// A key source over a population the fixture replaces for each test.
    /// Ascending ordinal order and exclusive resumption are its whole contract.
    /// </summary>
    public abstract class EndToEndKeySource : IGrainKeySource
    {
        /// <summary>The population this source enumerates.</summary>
        protected abstract string[] Keys { get; }

        /// <inheritdoc />
        public async IAsyncEnumerable<string> EnumerateKeysAsync(
            string? resumeAfterExclusive,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var keys = Keys;
            for (var i = 0; i < keys.Length; i++)
            {
                var key = keys[i];
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
            ValueTask.FromResult<long?>(Keys.Length);
    }

    /// <summary>The key source the backfill index crawls.</summary>
    public sealed class BackfillKeySource : EndToEndKeySource
    {
        /// <inheritdoc />
        protected override string[] Keys => _backfillPopulation;
    }

    /// <summary>The key source the drift index crawls.</summary>
    public sealed class DriftKeySource : EndToEndKeySource
    {
        /// <inheritdoc />
        protected override string[] Keys => _driftPopulation;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddMemoryGrainStorage(StorageName);

            // No key source: this index has no crawl at all, so anything it
            // holds can only have arrived through an activation.
            siloBuilder.AddGrainIndex<IEndToEndActiveUserGrain, EndToEndUserState>(
                static cfg => cfg
                    .WithName(ActiveIndex)
                    .Include(x => x.Age)
                    .Include(x => x.Country));

            siloBuilder.AddGrainIndex<IEndToEndDormantUserGrain, EndToEndUserState>(
                static cfg => cfg
                    .WithName(BackfillIndex)
                    .Include(x => x.Age)
                    .Include(x => x.Country));

            siloBuilder.AddGrainIndex<IEndToEndDriftUserGrain, EndToEndUserState>(
                static cfg => cfg
                    .WithName(DriftIndex)
                    .Include(x => x.Age)
                    .Include(x => x.Country));

            siloBuilder.AddGrainIndexKeySource<BackfillKeySource>(BackfillIndex);
            siloBuilder.AddGrainIndexKeySource<DriftKeySource>(DriftIndex);

            siloBuilder.ConfigureGrainIndex(ActiveIndex, static options => options.BackfillEnabled = false);

            siloBuilder.ConfigureGrainIndex(BackfillIndex, static options =>
            {
                options.BackfillBatchSize = BatchSize;
                options.BackfillEnabled = false;
            });

            siloBuilder.ConfigureGrainIndex(DriftIndex, static options =>
            {
                options.BackfillBatchSize = BatchSize;
                options.BackfillEnabled = false;
            });

            siloBuilder.ConfigureGrainIndexOutbox(static options => options.Enabled = false);
        }
    }
}
