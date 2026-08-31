using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.GrainIndex.Registry;
using Orleans.Serialization;

namespace Orleans.Lattice.GrainIndex.Tests.Registry;

/// <summary>
/// End-to-end coverage of the grain-index registry against a real silo, a real
/// registry <see cref="ILattice"/> tree, and the real Orleans wire format.
/// <para>
/// A "restart" is modelled by reconciling a fresh declaration set against the
/// same durable registry tree, which is exactly what a silo coming back up with
/// an edited declaration does - and unlike tearing the cluster down it does not
/// depend on in-memory grain storage outliving the silo that owns it.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class GrainIndexRegistryIntegrationTests
{
    private readonly GrainIndexRegistryClusterFixture _fixture = new();
    private ServiceProvider _serializerProvider = null!;
    private OrleansGrainIndexSerializer<GrainIndexRegistryRecord> _serializer = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        await _fixture.InitializeAsync();

        var services = new ServiceCollection();
        services.AddSerializer();
        _serializerProvider = services.BuildServiceProvider();
        _serializer = new OrleansGrainIndexSerializer<GrainIndexRegistryRecord>(
            _serializerProvider.GetRequiredService<Serializer<GrainIndexRegistryRecord>>());
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        _serializerProvider.Dispose();
        await _fixture.DisposeAsync();
    }

    private IGrainIndexRegistryStore Store() =>
        new GrainIndexRegistryStore(_fixture.Cluster.GrainFactory, _serializer);

    /// <summary>
    /// Reconciles <paramref name="declare"/> against the live registry tree,
    /// which is what a silo start does for the declaration set it is holding.
    /// </summary>
    private async Task ReconcileAsync(
        Action<StubSiloBuilder> declare,
        ILatticeMergeModeResolver? mergeModeResolver = null)
    {
        var builder = new StubSiloBuilder();
        declare(builder);
        builder.Services.AddOptions();
        await using var provider = builder.BuildServiceProvider();

        await new GrainIndexRegistryReconciler(
                provider.GetRequiredService<IOptions<GrainIndexDeclarationOptions>>(),
                provider.GetRequiredService<IOptionsMonitor<GrainIndexOptions>>(),
                Store(),
                new CapturingLogger<GrainIndexRegistryReconciler>(),
                mergeModeResolver)
            .ReconcileAsync(CancellationToken.None);
    }

    /// <summary>
    /// Reads a persisted record straight off the registry tree, bypassing the
    /// store, so the test proves the key layout and the wire format rather than
    /// trusting the abstraction that wrote them.
    /// </summary>
    private async Task<GrainIndexRegistryRecord?> ReadRawAsync(string indexName)
    {
        var tree = _fixture.Cluster.GrainFactory
            .GetGrain<ILattice>(GrainIndexRegistryTrees.RegistryTree);

        using (LatticeSystemOrigin.Enter())
        {
            var bytes = await tree.GetAsync(
                GrainIndexRegistryKeys.Definition(indexName), CancellationToken.None);
            return bytes is null ? null : _serializer.Deserialize(bytes);
        }
    }

    [Test]
    public async Task Declaring_an_index_persists_its_definition_and_fingerprint_at_silo_start()
    {
        var record = await ReadRawAsync(GrainIndexRegistryClusterFixture.DeclaredIndexName);

        Assert.Multiple(() =>
        {
            Assert.That(record, Is.Not.Null,
                "The silo declares an index, so its start-up must have registered it.");
            Assert.That(
                record!.Descriptor.Name,
                Is.EqualTo(GrainIndexRegistryClusterFixture.DeclaredIndexName));
            Assert.That(
                record.Descriptor.TreeName,
                Is.EqualTo(GrainIndexTreeNames.ForIndex(GrainIndexRegistryClusterFixture.DeclaredIndexName)));
            Assert.That(
                record.Descriptor.Properties.Select(p => p.Name),
                Is.EqualTo(new[] { "Age", "Country" }));
            Assert.That(record.Fingerprint.Value, Has.Length.EqualTo(32));
            Assert.That(record.NeedsBackfill, Is.True,
                "A brand-new index has no entries yet, so it owes a backfill.");
        });
    }

    [Test]
    public async Task The_persisted_fingerprint_survives_the_round_trip_through_the_registry_tree()
    {
        var record = await ReadRawAsync(GrainIndexRegistryClusterFixture.DeclaredIndexName);

        Assert.That(
            record!.Fingerprint,
            Is.EqualTo(GrainIndexFingerprint.Compute(record.Descriptor, record.KeyCodecId)),
            "The stored fingerprint must still describe the stored declaration after a real "
            + "serialize / persist / deserialize cycle, or every restart would see phantom drift.");
    }

    [Test]
    public async Task Restarting_with_an_unchanged_declaration_leaves_the_stored_record_intact()
    {
        const string Index = "integration-unchanged";
        await ReconcileAsync(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName(Index).Include(x => x.Age).Include(x => x.Country)));
        var before = await ReadRawAsync(Index);

        await ReconcileAsync(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName(Index).Include(x => x.Age).Include(x => x.Country)));
        var after = await ReadRawAsync(Index);

        Assert.Multiple(() =>
        {
            Assert.That(after!.Fingerprint, Is.EqualTo(before!.Fingerprint));
            Assert.That(after.NeedsBackfill, Is.EqualTo(before.NeedsBackfill));
        });
    }

    [Test]
    public async Task Restarting_with_a_breaking_change_rejects_and_names_the_offending_fields()
    {
        const string Index = "integration-breaking";
        await ReconcileAsync(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName(Index).Include(x => x.Age).Include(x => x.Country)));

        Assert.That(
            async () => await ReconcileAsync(static builder => builder
                .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                    static cfg => cfg.WithName(Index).Include(x => x.Age))),
            Throws.TypeOf<GrainIndexConfigurationDriftException>()
                .With.Property(nameof(GrainIndexConfigurationDriftException.ChangedFields))
                .EqualTo(new[] { GrainIndexDefinitionField.Properties })
                .And.Message.Contains(Index));

        Assert.That(
            (await ReadRawAsync(Index))!.Descriptor.Properties, Has.Count.EqualTo(2),
            "A rejected start must leave the stored record exactly as it was.");
    }

    [Test]
    public async Task Restarting_with_a_changed_key_codec_rejects()
    {
        const string Index = "integration-codec";
        await ReconcileAsync(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName(Index).Include(x => x.Age)));

        Assert.That(
            async () => await ReconcileAsync(static builder => builder
                .AddGrainIndex<ITestGuidKeyedGrain, TestGrainState>(
                    static cfg => cfg.WithName(Index).Include(x => x.Age))),
            Throws.TypeOf<GrainIndexConfigurationDriftException>()
                .With.Property(nameof(GrainIndexConfigurationDriftException.ChangedFields))
                .Contains(GrainIndexDefinitionField.KeyCodec));
    }

    [Test]
    public async Task Restarting_with_a_drift_safe_change_updates_the_stored_record_without_throwing()
    {
        const string Index = "integration-safe";
        await ReconcileAsync(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName(Index).Include(x => x.Age)));

        await ReconcileAsync(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName(Index).Include(x => x.Age))
            .ConfigureGrainIndex(Index, static options => options.AllowReplication = true));

        Assert.That((await ReadRawAsync(Index))!.Descriptor.AllowReplication, Is.True,
            "A drift-safe change is accepted, so the durable record must reflect it.");
    }

    [Test]
    public async Task The_rebuild_policy_accepts_a_breaking_change_and_marks_the_index_for_backfill()
    {
        const string Index = "integration-rebuild";
        await ReconcileAsync(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName(Index).Include(x => x.Age).Include(x => x.Country)));

        await ReconcileAsync(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName(Index).Include(x => x.Age))
            .ConfigureGrainIndex(
                Index, static options => options.DriftPolicy = GrainIndexDriftPolicy.Rebuild));

        var record = await ReadRawAsync(Index);

        Assert.Multiple(() =>
        {
            Assert.That(record!.Descriptor.Properties, Has.Count.EqualTo(1));
            Assert.That(record.NeedsBackfill, Is.True);
        });
    }

    [Test]
    public async Task A_replicated_index_tree_without_the_opt_in_rejects_start_up()
    {
        const string Index = "integration-replicated-denied";
        var resolver = new FakeMergeModeResolver()
            .Replicating(GrainIndexTreeNames.ForIndex(Index), LatticeMergeMode.OrSet);

        Assert.That(
            async () => await ReconcileAsync(
                static builder => builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                    static cfg => cfg.WithName(Index).Include(x => x.Age)),
                resolver),
            Throws.TypeOf<GrainIndexReplicationNotAllowedException>()
                .With.Property(nameof(GrainIndexReplicationNotAllowedException.TreeName))
                .EqualTo(GrainIndexTreeNames.ForIndex(Index)));

        Assert.That(await ReadRawAsync(Index), Is.Null,
            "The guard runs before anything is persisted, so a rejected index leaves no record.");
    }

    [Test]
    public async Task A_replicated_index_tree_with_the_opt_in_proceeds()
    {
        const string Index = "integration-replicated-allowed";
        var resolver = new FakeMergeModeResolver()
            .Replicating(GrainIndexTreeNames.ForIndex(Index), LatticeMergeMode.OrSet);

        await ReconcileAsync(
            static builder => builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName(Index).Include(x => x.Age).AllowReplication()),
            resolver);

        var record = await ReadRawAsync(Index);

        Assert.Multiple(() =>
        {
            Assert.That(record, Is.Not.Null,
                "An opted-in index must register normally rather than being blocked.");
            Assert.That(record!.Descriptor.AllowReplication, Is.True);
        });
    }

    [Test]
    public async Task The_cores_default_resolver_makes_the_replication_guard_a_no_op()
    {
        const string Index = "integration-default-resolver";

        // The resolver the core registers when no replication package is
        // present. Reconciling against it must behave exactly as reconciling
        // against no resolver at all.
        var coreDefault = _fixture.Cluster.ServiceProvider.GetService<ILatticeMergeModeResolver>();

        await ReconcileAsync(
            static builder => builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName(Index).Include(x => x.Age)),
            coreDefault);

        Assert.Multiple(() =>
        {
            Assert.That(coreDefault, Is.Not.Null,
                "The core registers a default resolver, so the guard always has something to ask.");
            Assert.That(
                coreDefault!.Resolve(GrainIndexTreeNames.ForIndex(Index)), Is.Null,
                "The default reports every tree as not replicated.");
        });

        Assert.That(await ReadRawAsync(Index), Is.Not.Null,
            "A host with no replication package registered must reconcile normally.");
    }

    [Test]
    public async Task A_persisted_record_is_stored_under_the_definition_segment_of_the_registry_tree()
    {
        const string Index = "integration-key-layout";
        await ReconcileAsync(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName(Index).Include(x => x.Age)));

        var tree = _fixture.Cluster.GrainFactory
            .GetGrain<ILattice>(GrainIndexRegistryTrees.RegistryTree);

        List<string> keys;
        using (LatticeSystemOrigin.Enter())
        {
            keys = await tree
                .KeysAsync(
                    GrainIndexRegistryKeys.DefinitionPrefix(),
                    GrainIndexRegistryKeys.DefinitionPrefixEnd(),
                    cancellationToken: CancellationToken.None)
                .ToListAsync();
        }

        Assert.That(keys, Does.Contain(GrainIndexRegistryKeys.Definition(Index)),
            "Listing every registered index must be one contiguous range scan, which is the "
            + "reason the registry is a tree rather than a grain.");
    }
}
