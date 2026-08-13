namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// End-to-end coverage of the typed <see cref="LatticeTreeAdminApiGrpcClient"/> over
/// a live, co-hosted gRPC server bound to a real Orleans cluster's
/// <see cref="ILatticeTreeAdmin"/> facade. At this scaffolding stage the facade
/// exposes the capability probe (composing the wrapped schema facade) and the
/// unauthenticated auth-scheme discovery RPC; this fixture drives both over the wire
/// with a permissive authorizer so the transport succeeds and the facade's own gate
/// is the only guard.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeTreeAdminGrpcClientE2ETests
{
    private const string Tree = "customers";

    private GrpcTreeAdminClusterFixture _fixture = null!;
    private GrpcTreeAdminHost _host = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new GrpcTreeAdminClusterFixture();
        await _fixture.InitializeAsync();

        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("k", "{}"u8.ToArray());

        _host = await _fixture.CreateGrpcHostAsync(new AllowAllTreeAdminApiAuthorizer());
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_host is not null)
        {
            await _host.DisposeAsync();
        }

        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    [Test]
    public async Task probe_capabilities_reports_the_target_tree_and_composed_schema()
    {
        var capabilities = await _host.Client.ProbeCapabilitiesAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(capabilities.TreeId, Is.EqualTo(Tree));
            // Composed schema capabilities ride along, keyed to the same tree.
            Assert.That(capabilities.Schema, Is.Not.Null);
            Assert.That(capabilities.Schema.TreeId, Is.EqualTo(Tree));
            // The test cluster registers no auth add-on, so the no-op gate allows the
            // whole-tree admin probe.
            Assert.That(capabilities.CanAdministerTree, Is.True);
        });
    }

    [Test]
    public async Task get_auth_scheme_is_reachable_over_the_client()
    {
        var schemes = await _host.Client.GetAuthSchemeAsync();

        Assert.That(schemes, Is.Not.Null);
    }

    [Test]
    public async Task get_shard_hotness_round_trips_over_the_client()
    {
        var report = await _host.Client.GetShardHotnessAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeId, Is.EqualTo(Tree));
            Assert.That(report.ShardCount, Is.GreaterThan(0));
            Assert.That(report.Shards, Is.Not.Null);
        });
    }

    [Test]
    public async Task get_diagnostics_round_trips_over_the_client()
    {
        var report = await _host.Client.GetDiagnosticsAsync(Tree, deep: false);

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeId, Is.EqualTo(Tree));
            Assert.That(report.Deep, Is.False);
            Assert.That(report.Shards, Is.Not.Null);
        });
    }

    [Test]
    public async Task inspect_shard_map_round_trips_over_the_client()
    {
        var inspection = await _host.Client.InspectShardMapAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(inspection.TreeId, Is.EqualTo(Tree));
            Assert.That(inspection.PhysicalTreeId, Is.Not.Empty);
            Assert.That(inspection.VirtualShardCount, Is.GreaterThan(0));
        });
    }

    [Test]
    public async Task get_projection_digest_round_trips_over_the_client()
    {
        var digest = await _host.Client.GetProjectionDigestAsync(Tree, 0);

        Assert.Multiple(() =>
        {
            Assert.That(digest.TreeId, Is.EqualTo(Tree));
            Assert.That(digest.ShardIndex, Is.EqualTo(0));
            Assert.That(digest.HashHex, Is.Not.Null);
        });
    }

    [Test]
    public async Task get_tree_stats_round_trips_over_the_client()
    {
        var stats = await _host.Client.GetTreeStatsAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(stats.TreeId, Is.EqualTo(Tree));
            Assert.That(stats.ShardCount, Is.GreaterThan(0));
        });
    }

    [Test]
    public async Task get_storage_usage_round_trips_over_the_client()
    {
        var summary = await _host.Client.GetStorageUsageAsync(deep: false);

        Assert.Multiple(() =>
        {
            Assert.That(summary, Is.Not.Null);
            Assert.That(summary.Deep, Is.False);
            Assert.That(summary.Trees, Is.Not.Null);
        });
    }

    [Test]
    public async Task tree_lifecycle_round_trips_over_the_client()
    {
        const string lifecycleTree = "lifecycle-e2e";

        var created = await _host.Client.CreateTreeAsync(lifecycleTree, shardCount: 4, maxLeafKeys: 32);
        Assert.Multiple(() =>
        {
            Assert.That(created.TreeId, Is.EqualTo(lifecycleTree));
            Assert.That(created.Created, Is.True);
            Assert.That(created.ShardCount, Is.EqualTo(4));
        });

        // Idempotent re-create reports Created=false.
        var recreated = await _host.Client.CreateTreeAsync(lifecycleTree);
        Assert.That(recreated.Created, Is.False);

        var exists = await _host.Client.CheckTreeExistsAsync(lifecycleTree);
        Assert.That(exists.Exists, Is.True);

        var config = await _host.Client.SetTreeConfigAsync(lifecycleTree, new TreeConfigurationUpdate
        {
            ApplyPublishEvents = true,
            PublishEvents = false,
        });
        Assert.That(config.PublishEvents, Is.False);

        var readBack = await _host.Client.GetTreeConfigAsync(lifecycleTree);
        Assert.Multiple(() =>
        {
            Assert.That(readBack.Exists, Is.True);
            Assert.That(readBack.PublishEvents, Is.False);
        });

        var alias = await _host.Client.SetTreeAliasAsync(lifecycleTree, "phys-" + lifecycleTree);
        Assert.That(alias.IsAliased, Is.True);

        var resolved = await _host.Client.ResolveTreeAliasAsync(lifecycleTree);
        Assert.That(resolved.PhysicalTreeId, Is.EqualTo("phys-" + lifecycleTree));

        var shardMap = await _host.Client.GetShardMapAsync(lifecycleTree);
        Assert.That(shardMap.TreeId, Is.EqualTo(lifecycleTree));
    }

    [Test]
    public async Task tree_deletion_lifecycle_round_trips_over_the_client()
    {
        const string deletionTree = "deletion-e2e";

        await _host.Client.CreateTreeAsync(deletionTree, shardCount: 2);
        var live = await _host.Client.GetTreeDeletionStatusAsync(deletionTree);
        Assert.Multiple(() =>
        {
            Assert.That(live.TreeId, Is.EqualTo(deletionTree));
            Assert.That(live.IsDeleted, Is.False);
            Assert.That(live.CanRecover, Is.False);
        });

        var deleted = await _host.Client.DeleteTreeAsync(deletionTree);
        Assert.Multiple(() =>
        {
            Assert.That(deleted.IsDeleted, Is.True);
            Assert.That(deleted.DeletedAtUtc, Is.Not.Null);
            Assert.That(deleted.RecoveryDeadlineUtc, Is.Not.Null);
            Assert.That(deleted.CanRecover, Is.True);
        });

        var status = await _host.Client.GetTreeDeletionStatusAsync(deletionTree);
        Assert.That(status.IsDeleted, Is.True);

        var recovered = await _host.Client.RecoverTreeAsync(deletionTree);
        Assert.Multiple(() =>
        {
            Assert.That(recovered.IsDeleted, Is.False);
            Assert.That(recovered.CanRecover, Is.False);
        });
    }

    [Test]
    public async Task purge_without_confirmation_is_rejected_over_the_client()
    {
        const string purgeTree = "purge-e2e";

        await _host.Client.CreateTreeAsync(purgeTree, shardCount: 2);
        await _host.Client.DeleteTreeAsync(purgeTree);

        Assert.That(
            async () => await _host.Client.PurgeTreeAsync(purgeTree, confirm: false),
            Throws.Exception);
    }

    [Test]
    public async Task bulk_load_streams_begin_append_commit_over_the_client()
    {
        const string bulkTree = "bulk-load-e2e";
        const string op = "load-e2e-1";

        await _host.Client.CreateTreeAsync(bulkTree, shardCount: 2);

        var session = await _host.Client.BeginBulkLoadAsync(bulkTree, op);
        Assert.Multiple(() =>
        {
            Assert.That(session.TreeId, Is.EqualTo(bulkTree));
            Assert.That(session.OperationId, Is.EqualTo(op));
        });

        var ack0 = await _host.Client.AppendBulkLoadAsync(bulkTree, op, 0, Chunk("a", "b", "c"));
        Assert.Multiple(() =>
        {
            Assert.That(ack0.AcceptedEntryCount, Is.EqualTo(3));
            Assert.That(ack0.NextChunkIndex, Is.EqualTo(1));
        });

        var ack1 = await _host.Client.AppendBulkLoadAsync(bulkTree, op, 1, Chunk("d", "e"));
        Assert.That(ack1.AcceptedEntryCount, Is.EqualTo(2));

        // Idempotent re-drive of chunk 0 does not double-count on commit.
        await _host.Client.AppendBulkLoadAsync(bulkTree, op, 0, Chunk("a", "b", "c"));

        var result = await _host.Client.CommitBulkLoadAsync(bulkTree, op);
        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo(bulkTree));
            Assert.That(result.TotalLiveKeys, Is.EqualTo(5));
        });

        // The grafted keys are readable through the core grain.
        var value = await _fixture.GrainFactory.GetGrain<ILattice>(bulkTree).GetAsync("c");
        Assert.That(value, Is.Not.Null);
    }

    [Test]
    public async Task bulk_load_begin_on_non_empty_tree_is_rejected_over_the_client()
    {
        const string nonEmptyTree = "bulk-load-nonempty-e2e";

        await _fixture.GrainFactory.GetGrain<ILattice>(nonEmptyTree).SetAsync("seed", "{}"u8.ToArray());

        Assert.That(
            async () => await _host.Client.BeginBulkLoadAsync(nonEmptyTree, "op"),
            Throws.Exception);
    }

    [Test]
    public async Task bulk_load_append_out_of_order_chunk_is_rejected_over_the_client()
    {
        const string orderTree = "bulk-load-order-e2e";
        const string op = "load-order-1";

        await _host.Client.CreateTreeAsync(orderTree, shardCount: 2);
        await _host.Client.BeginBulkLoadAsync(orderTree, op);

        Assert.That(
            async () => await _host.Client.AppendBulkLoadAsync(orderTree, op, 0, Chunk("a", "c", "b")),
            Throws.Exception);
    }

    private static IReadOnlyList<Orleans.Lattice.Api.Data.DataEntry> Chunk(params string[] keys)
        => keys.Select(k => new Orleans.Lattice.Api.Data.DataEntry { Key = k, Value = "{}"u8.ToArray() }).ToArray();
}
