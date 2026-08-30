using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Schema.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Fault, rollback, and abort-path tests for <see cref="LatticeSchemaRemediationGrain"/>:
/// a transform or validation failure discards the destination tree and aborts, a
/// state-write failure rolls the phase back rather than half-advancing, and a
/// failure to discard the destination is logged without masking the original abort.
/// </summary>
public partial class LatticeSchemaRemediationGrainTests
{
    private static SchemaRemediationState InFlightTransform(
        LatticeSchemaRemediationPhase phase,
        LatticeValueTransform transform,
        LatticeSchemaPolicy? policy = null) =>
        new()
        {
            InProgress = true,
            Phase = phase,
            OperationId = "op-extra",
            DestinationTreeId = TreeId + "/remediated/op-extra",
            SourcePhysicalTreeId = TreeId,
            Transform = transform,
            TargetPolicy = policy ?? JsonPolicy(),
        };

    [Test]
    public void GrainContext_returns_constructor_context()
    {
        var h = CreateGrain(Array.Empty<(string, string)>());

        var grainBase = (IGrainBase)h.Grain;

        Assert.That(grainBase.GrainContext.GrainId.Key.ToString(), Is.EqualTo(TreeId));
    }

    [Test]
    public async Task GetStatusAsync_reports_in_flight_state()
    {
        var seed = InFlightTransform(
            LatticeSchemaRemediationPhase.Build,
            LatticeValueTransform.Passthrough(),
            JsonPolicy());
        seed.ScannedCount = 4;
        var h = CreateGrain(Array.Empty<(string, string)>(), seed);

        var report = await h.Grain.GetStatusAsync();

        Assert.That(report.InProgress, Is.True);
        Assert.That(report.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Build));
        Assert.That(report.ScannedCount, Is.EqualTo(4));
        Assert.That(report.DestinationTreeId, Is.EqualTo(seed.DestinationTreeId));
        Assert.That(report.OperationId, Is.EqualTo(seed.OperationId));
    }

    [Test]
    public async Task RunRemediationPassAsync_build_transform_failure_discards_destination_and_aborts()
    {
        var seed = InFlightTransform(
            LatticeSchemaRemediationPhase.Build,
            LatticeValueTransform.Passthrough(),
            JsonPolicy());
        var h = CreateGrain(new[] { ("k1", "not-json") }, seed);

        await h.Grain.RunRemediationPassAsync();

        await h.Destination.Received(1).DeleteTreeAsync(Arg.Any<CancellationToken>());
        Assert.That(h.State.State.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Aborted));
        Assert.That(h.State.State.LastReport!.Value.OffendingKey, Is.EqualTo("k1"));
    }

    [Test]
    public async Task RunRemediationPassAsync_logs_and_continues_when_discarding_destination_fails()
    {
        var seed = InFlightTransform(
            LatticeSchemaRemediationPhase.Build,
            LatticeValueTransform.Passthrough(),
            MaxLenPolicy(3));
        var h = CreateGrain(new[] { ("k1", "{\"too\":\"big\"}") }, seed);
        h.Destination.DeleteTreeAsync(Arg.Any<CancellationToken>())
            .Returns<Task>(_ => throw new InvalidOperationException("delete failed"));

        await h.Grain.RunRemediationPassAsync();

        Assert.That(h.State.State.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Aborted));
        Assert.That(h.State.State.LastReport!.Value.OffendingKey, Is.EqualTo("k1"));
    }

    [Test]
    public void RunRemediationPassAsync_advance_phase_write_failure_rolls_back()
    {
        var seed = InFlightTransform(
            LatticeSchemaRemediationPhase.DryRun,
            LatticeValueTransform.Passthrough(),
            JsonPolicy());
        var h = CreateGrain(new[] { ("k1", "{\"a\":1}") }, seed);
        h.State.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.That(
            async () => await h.Grain.RunRemediationPassAsync(),
            Throws.InvalidOperationException);
        Assert.That(h.State.State.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.DryRun));
        Assert.That(h.State.State.ScannedCount, Is.Zero);
    }

    [Test]
    public void RunRemediationPassAsync_complete_write_failure_rolls_back()
    {
        var seed = InFlightTransform(
            LatticeSchemaRemediationPhase.Cutover,
            LatticeValueTransform.Passthrough(),
            JsonPolicy());
        seed.ScannedCount = 5;
        var h = CreateGrain(Array.Empty<(string, string)>(), seed);
        h.State.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.That(
            async () => await h.Grain.RunRemediationPassAsync(),
            Throws.InvalidOperationException);
        Assert.That(h.State.State.InProgress, Is.True);
        Assert.That(h.State.State.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Cutover));
        Assert.That(h.State.State.LastReport, Is.Null);
    }

    [Test]
    public void RunRemediationPassAsync_abort_write_failure_rolls_back()
    {
        var seed = InFlightTransform(
            LatticeSchemaRemediationPhase.DryRun,
            LatticeValueTransform.Passthrough(),
            MaxLenPolicy(3));
        var h = CreateGrain(new[] { ("k1", "{\"too\":\"big\"}") }, seed);
        h.State.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.That(
            async () => await h.Grain.RunRemediationPassAsync(),
            Throws.InvalidOperationException);
        Assert.That(h.State.State.InProgress, Is.True);
        Assert.That(h.State.State.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.DryRun));
        Assert.That(h.State.State.LastReport, Is.Null);
        Assert.That(h.State.State.ScannedCount, Is.Zero);
    }

    [Test]
    public void RunRemediationPassAsync_in_flight_migration_without_registry_throws()
    {
        var seed = new SchemaRemediationState
        {
            InProgress = true,
            Phase = LatticeSchemaRemediationPhase.Build,
            OperationId = "opm",
            DestinationTreeId = TreeId + "/remediated/opm",
            SourcePhysicalTreeId = TreeId,
            Mode = SchemaRemediationMode.SchemaVersionMigration,
            MigrationSchemaId = MigSchemaId,
            MigrationTargetVersion = 2,
        };
        var h = CreateGrainBytes(new[] { ("k1", Env(1, "{\"a\":1}")) }, seedState: seed, schemaRegistry: null);

        Assert.That(
            async () => await h.Grain.RunRemediationPassAsync(),
            Throws.InvalidOperationException.With.Message.Contains("Schema versioning is not registered"));
    }

    [Test]
    public async Task StartVersionMigrationAsync_with_policy_validates_legacy_plain_value_without_stripping()
    {
        var registry = MigratingRegistry();
        var h = CreateGrainBytes(
            new[] { ("k1", Utf8("{\"a\":1}")) },
            schemaRegistry: registry,
            existingPolicy: JsonPolicy());

        var report = await h.Grain.StartVersionMigrationAsync(MigSchemaId, 2);

        Assert.That(report.Succeeded, Is.True);
        await h.Destination.Received(1).SetAsync("k1", Arg.Is<byte[]>(v => VersionOf(v) == 2));
    }

    [Test]
    public async Task StartVersionMigrationAsync_without_policy_store_runs_as_unenforced_migration()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("remediation", TreeId));
        var source = Substitute.For<ILattice>();
        source.EntriesAsync().Returns(EntriesBytes(("k1", Env(1, "{\"a\":1}"))));
        source.GetRoutingAsync().Returns(new ValueTask<RoutingInfo>(
            new RoutingInfo(TreeId, new ShardMap { Slots = new[] { 0 } })));
        var destination = Substitute.For<ILattice>();
        var registryGrain = Substitute.For<ILatticeRegistry>();
        registryGrain.ResolveAsync(TreeId).Returns(TreeId);
        var shard = Substitute.For<IShardRootGrain>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(TreeId).Returns(source);
        grainFactory.GetGrain<ILattice>(
            Arg.Is<string>(s => s != null && s.StartsWith(TreeId + "/remediated/", StringComparison.Ordinal)))
            .Returns(destination);
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registryGrain);
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shard);
        var state = new FakePersistentState<SchemaRemediationState>();
        var grain = new LatticeSchemaRemediationGrain(
            context,
            grainFactory,
            Options.Create(new LatticeSchemaEnforcementOptions()),
            NullLogger<LatticeSchemaRemediationGrain>.Instance,
            state,
            policyStore: null,
            policyProvider: null,
            schemaRegistry: MigratingRegistry());

        var report = await grain.StartVersionMigrationAsync(MigSchemaId, 2);

        Assert.That(report.Succeeded, Is.True);
        await destination.Received(1).SetAsync("k1", Arg.Is<byte[]>(v => VersionOf(v) == 2));
    }

    [Test]
    public void StartAsync_null_seed_policy_is_not_the_same_parameters()
    {
        var seed = InFlightTransform(LatticeSchemaRemediationPhase.Build, LatticeValueTransform.Passthrough(), JsonPolicy());
        seed.TargetPolicy = null;
        var h = CreateGrain(Array.Empty<(string, string)>(), seed);

        Assert.That(
            async () => await h.Grain.StartAsync(LatticeValueTransform.Passthrough(), JsonPolicy()),
            Throws.InvalidOperationException);
    }

    [Test]
    public void StartAsync_different_transform_kind_is_not_the_same_parameters()
    {
        var seed = InFlightTransform(LatticeSchemaRemediationPhase.Build, LatticeValueTransform.DropMember("old"), JsonPolicy());
        var h = CreateGrain(Array.Empty<(string, string)>(), seed);

        Assert.That(
            async () => await h.Grain.StartAsync(LatticeValueTransform.Passthrough(), JsonPolicy()),
            Throws.InvalidOperationException);
    }

    [Test]
    public void StartAsync_different_transform_child_count_is_not_the_same_parameters()
    {
        var seed = InFlightTransform(
            LatticeSchemaRemediationPhase.Build,
            LatticeValueTransform.Passthrough(LatticeValueTransform.DropMember("old")),
            JsonPolicy());
        var h = CreateGrain(Array.Empty<(string, string)>(), seed);

        Assert.That(
            async () => await h.Grain.StartAsync(
                LatticeValueTransform.Passthrough(
                    LatticeValueTransform.DropMember("old"),
                    LatticeValueTransform.DropMember("older")),
                JsonPolicy()),
            Throws.InvalidOperationException);
    }

    [Test]
    public void StartAsync_different_nested_transform_child_is_not_the_same_parameters()
    {
        var seed = InFlightTransform(
            LatticeSchemaRemediationPhase.Build,
            LatticeValueTransform.Passthrough(
                LatticeValueTransform.SetMember("x", LatticeValueTransform.Const(LatticeConstant.Integer(1)))),
            JsonPolicy());
        var h = CreateGrain(Array.Empty<(string, string)>(), seed);

        Assert.That(
            async () => await h.Grain.StartAsync(
                LatticeValueTransform.Passthrough(
                    LatticeValueTransform.SetMember("x", LatticeValueTransform.Const(LatticeConstant.Integer(2)))),
                JsonPolicy()),
            Throws.InvalidOperationException);
    }

    [Test]
    public void StartAsync_different_predicate_operator_is_not_the_same_parameters()
    {
        var thenBranch = LatticeValueTransform.Const(LatticeConstant.Text("yes"));
        var elseBranch = LatticeValueTransform.Const(LatticeConstant.Text("no"));
        var seed = InFlightTransform(
            LatticeSchemaRemediationPhase.Build,
            LatticeValueTransform.Passthrough(
                LatticeValueTransform.SetMember(
                    "x",
                    LatticeValueTransform.Conditional(
                        LatticePredicateNode.Compare(
                            LatticeComparisonOperator.Equal,
                            LatticePredicateNode.Member("a"),
                            LatticePredicateNode.Const(LatticeConstant.Integer(1))),
                        thenBranch,
                        elseBranch))),
            JsonPolicy());
        var h = CreateGrain(Array.Empty<(string, string)>(), seed);

        Assert.That(
            async () => await h.Grain.StartAsync(
                LatticeValueTransform.Passthrough(
                    LatticeValueTransform.SetMember(
                        "x",
                        LatticeValueTransform.Conditional(
                            LatticePredicateNode.Compare(
                                LatticeComparisonOperator.NotEqual,
                                LatticePredicateNode.Member("a"),
                                LatticePredicateNode.Const(LatticeConstant.Integer(1))),
                            thenBranch,
                            elseBranch))),
                JsonPolicy()),
            Throws.InvalidOperationException);
    }

    [Test]
    public void StartAsync_different_predicate_child_count_is_not_the_same_parameters()
    {
        var seededCondition = new LatticePredicateNode
        {
            Kind = LatticePredicateNodeKind.Boolean,
            BooleanOperator = LatticeBooleanOperator.And,
            Children = [LatticePredicateNode.Member("a")],
        };
        var retryCondition = new LatticePredicateNode
        {
            Kind = LatticePredicateNodeKind.Boolean,
            BooleanOperator = LatticeBooleanOperator.And,
            Children = [LatticePredicateNode.Member("a"), LatticePredicateNode.Member("b")],
        };
        var seed = InFlightTransform(
            LatticeSchemaRemediationPhase.Build,
            LatticeValueTransform.Passthrough(
                LatticeValueTransform.SetMember(
                    "x",
                    LatticeValueTransform.Conditional(
                        seededCondition,
                        LatticeValueTransform.Const(LatticeConstant.Text("yes")),
                        LatticeValueTransform.Const(LatticeConstant.Text("no"))))),
            JsonPolicy());
        var h = CreateGrain(Array.Empty<(string, string)>(), seed);

        Assert.That(
            async () => await h.Grain.StartAsync(
                LatticeValueTransform.Passthrough(
                    LatticeValueTransform.SetMember(
                        "x",
                        LatticeValueTransform.Conditional(
                            retryCondition,
                            LatticeValueTransform.Const(LatticeConstant.Text("yes")),
                            LatticeValueTransform.Const(LatticeConstant.Text("no"))))),
                JsonPolicy()),
            Throws.InvalidOperationException);
    }

    [Test]
    public void StartAsync_different_nested_predicate_child_is_not_the_same_parameters()
    {
        static LatticePredicateNode CompareTo(long value) =>
            LatticePredicateNode.Compare(
                LatticeComparisonOperator.Equal,
                LatticePredicateNode.Member("a"),
                LatticePredicateNode.Const(LatticeConstant.Integer(value)));

        var seed = InFlightTransform(
            LatticeSchemaRemediationPhase.Build,
            LatticeValueTransform.Passthrough(
                LatticeValueTransform.SetMember(
                    "x",
                    LatticeValueTransform.Conditional(
                        LatticePredicateNode.Bool(LatticeBooleanOperator.And, CompareTo(1)),
                        LatticeValueTransform.Const(LatticeConstant.Text("yes")),
                        LatticeValueTransform.Const(LatticeConstant.Text("no"))))),
            JsonPolicy());
        var h = CreateGrain(Array.Empty<(string, string)>(), seed);

        Assert.That(
            async () => await h.Grain.StartAsync(
                LatticeValueTransform.Passthrough(
                    LatticeValueTransform.SetMember(
                        "x",
                        LatticeValueTransform.Conditional(
                            LatticePredicateNode.Bool(LatticeBooleanOperator.And, CompareTo(2)),
                            LatticeValueTransform.Const(LatticeConstant.Text("yes")),
                            LatticeValueTransform.Const(LatticeConstant.Text("no"))))),
                JsonPolicy()),
            Throws.InvalidOperationException);
    }

    [Test]
    public async Task RunRemediationPassAsync_build_failure_with_empty_value_records_empty_preview()
    {
        var seed = InFlightTransform(
            LatticeSchemaRemediationPhase.Build,
            LatticeValueTransform.Passthrough(),
            JsonPolicy());
        var h = CreateGrainBytes(new[] { ("k1", Array.Empty<byte>()) }, seed);

        await h.Grain.RunRemediationPassAsync();

        Assert.That(h.State.State.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Aborted));
        Assert.That(h.State.State.LastReport!.Value.OffendingValuePreview, Is.Empty);
    }
}
