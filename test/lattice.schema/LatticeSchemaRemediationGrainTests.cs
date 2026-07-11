using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Schema.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaRemediationGrain"/>: the durable
/// shadow-build-and-cutover coordinator. Cover the dry-run abort gate (no cutover,
/// original untouched), the successful build -> cutover -> alias repoint -> policy
/// install path, the build-time abort discarding the partial destination,
/// idempotent same-parameter resume, different-parameter rejection while in flight,
/// durable-state resumption after a simulated reactivation, and rollback on a
/// <c>WriteStateAsync</c> failure. All phases are driven synchronously - no timing
/// or ordering dependence.
/// </summary>
public class LatticeSchemaRemediationGrainTests
{
    private const string TreeId = "orders";

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Entries(
        params (string Key, string Value)[] items)
    {
        foreach (var (key, value) in items)
        {
            yield return new KeyValuePair<string, byte[]>(key, Utf8(value));
        }

        await Task.CompletedTask;
    }

    private sealed class Harness
    {
        public required LatticeSchemaRemediationGrain Grain { get; init; }
        public required ILattice Source { get; init; }
        public required ILattice Destination { get; init; }
        public required ILatticeRegistry Registry { get; init; }
        public required ILatticeSchemaPolicyStore PolicyStore { get; init; }
        public required FakePersistentState<SchemaRemediationState> State { get; init; }
    }

    private static Harness CreateGrain(
        (string Key, string Value)[] sourceEntries,
        SchemaRemediationState? seedState = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("remediation", TreeId));

        var source = Substitute.For<ILattice>();
        source.EntriesAsync().Returns(_ => Entries(sourceEntries));
        // Source routing for cutover: a single-shard identity map on the physical
        // tree that equals the (never-aliased) logical tree id.
        source.GetRoutingAsync().Returns(new ValueTask<RoutingInfo>(
            new RoutingInfo(TreeId, new ShardMap { Slots = new[] { 0 } })));

        var destination = Substitute.For<ILattice>();

        var registry = Substitute.For<ILatticeRegistry>();
        // A never-aliased source tree resolves its physical id to its own name.
        registry.ResolveAsync(TreeId).Returns(TreeId);

        var shard = Substitute.For<IShardRootGrain>();

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(TreeId).Returns(source);
        grainFactory.GetGrain<ILattice>(
            Arg.Is<string>(s => s != null && s.StartsWith(TreeId + "/remediated/", StringComparison.Ordinal)))
            .Returns(destination);
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shard);

        var policyStore = Substitute.For<ILatticeSchemaPolicyStore>();
        var policyProvider = Substitute.For<ILatticeSchemaPolicyProvider>();

        var state = new FakePersistentState<SchemaRemediationState>();
        if (seedState is not null)
        {
            state.State = seedState;
        }

        var options = Options.Create(new LatticeSchemaEnforcementOptions());

        var grain = new LatticeSchemaRemediationGrain(
            context,
            grainFactory,
            policyStore,
            policyProvider,
            options,
            NullLogger<LatticeSchemaRemediationGrain>.Instance,
            state);

        return new Harness
        {
            Grain = grain,
            Source = source,
            Destination = destination,
            Registry = registry,
            PolicyStore = policyStore,
            State = state,
        };
    }

    private static LatticeSchemaPolicy JsonPolicy() => new(new[] { LatticeSchemaRule.Json() });

    private static LatticeSchemaPolicy MaxLenPolicy(int max) => new(new[] { LatticeSchemaRule.MaxLength(max) });

    [Test]
    public async Task StartAsync_all_values_remediate_cuts_over_and_installs_policy()
    {
        var policy = JsonPolicy();
        var h = CreateGrain(new[] { ("k1", "{\"a\":1}"), ("k2", "{\"b\":2}") });

        var report = await h.Grain.StartAsync(LatticeValueTransform.Passthrough(), policy);

        Assert.That(report.Succeeded, Is.True);
        Assert.That(report.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Completed));
        Assert.That(report.ScannedCount, Is.EqualTo(2));
        Assert.That(report.DestinationTreeId, Does.StartWith(TreeId + "/remediated/"));

        // Destination populated with both entries.
        await h.Destination.Received(1).SetAsync("k1", Arg.Any<byte[]>());
        await h.Destination.Received(1).SetAsync("k2", Arg.Any<byte[]>());

        // Cutover repointed the logical tree to the destination and installed the policy.
        await h.Registry.Received(1).SetAliasAsync(TreeId, report.DestinationTreeId!);
        await h.PolicyStore.Received(1).SetPolicyAsync(TreeId, policy, Arg.Any<CancellationToken>());

        Assert.That(h.State.State.InProgress, Is.False);
        Assert.That(h.State.State.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Completed));
    }

    [Test]
    public async Task StartAsync_dry_run_violation_aborts_with_no_cutover_and_leaves_original_untouched()
    {
        var policy = MaxLenPolicy(3);
        var h = CreateGrain(new[] { ("k1", "{}"), ("k2", "{\"too\":\"big\"}") });

        var report = await h.Grain.StartAsync(LatticeValueTransform.Passthrough(), policy);

        Assert.That(report.DidAbort, Is.True);
        Assert.That(report.Succeeded, Is.False);
        Assert.That(report.OffendingKey, Is.EqualTo("k2"));
        Assert.That(report.Reason, Is.Not.Null.And.Not.Empty);

        // No destination was built and nothing was cut over: the original is untouched.
        await h.Destination.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
        await h.Registry.DidNotReceive().SetAliasAsync(Arg.Any<string>(), Arg.Any<string>());
        await h.PolicyStore.DidNotReceive().SetPolicyAsync(Arg.Any<string>(), Arg.Any<LatticeSchemaPolicy>(), Arg.Any<CancellationToken>());

        Assert.That(h.State.State.InProgress, Is.False);
        Assert.That(h.State.State.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Aborted));
    }

    [Test]
    public async Task StartAsync_transform_failure_on_malformed_value_aborts_before_cutover()
    {
        // A non-JSON value makes the identity JSON transform throw; the dry-run gate
        // catches it and aborts before any destination is built.
        var h = CreateGrain(new[] { ("k1", "not-json") });

        var report = await h.Grain.StartAsync(LatticeValueTransform.Passthrough(), JsonPolicy());

        Assert.That(report.DidAbort, Is.True);
        Assert.That(report.OffendingKey, Is.EqualTo("k1"));
        await h.Registry.DidNotReceive().SetAliasAsync(Arg.Any<string>(), Arg.Any<string>());
    }

    [Test]
    public async Task StartAsync_empty_tree_cuts_over_with_zero_scanned()
    {
        var policy = JsonPolicy();
        var h = CreateGrain(Array.Empty<(string, string)>());

        var report = await h.Grain.StartAsync(LatticeValueTransform.Passthrough(), policy);

        Assert.That(report.Succeeded, Is.True);
        Assert.That(report.ScannedCount, Is.Zero);
        await h.Registry.Received(1).SetAliasAsync(TreeId, report.DestinationTreeId!);
        await h.PolicyStore.Received(1).SetPolicyAsync(TreeId, policy, Arg.Any<CancellationToken>());
    }

    [Test]
    public void StartAsync_uncompilable_regex_policy_throws_argument_exception()
    {
        var badPolicy = new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Regex("(") });
        var h = CreateGrain(Array.Empty<(string, string)>());

        Assert.That(
            async () => await h.Grain.StartAsync(LatticeValueTransform.Passthrough(), badPolicy),
            Throws.ArgumentException);
    }

    [Test]
    public void StartAsync_null_policy_throws()
    {
        var h = CreateGrain(Array.Empty<(string, string)>());

        Assert.That(
            async () => await h.Grain.StartAsync(LatticeValueTransform.Passthrough(), null!),
            Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public void StartAsync_different_parameters_while_in_flight_throws_invalid_operation()
    {
        var seed = new SchemaRemediationState
        {
            InProgress = true,
            Phase = LatticeSchemaRemediationPhase.Build,
            OperationId = "op1",
            DestinationTreeId = TreeId + "/remediated/op1",
            Transform = LatticeValueTransform.Passthrough(),
            TargetPolicy = JsonPolicy(),
        };
        var h = CreateGrain(Array.Empty<(string, string)>(), seed);

        // Different policy while a remediation is in flight.
        Assert.That(
            async () => await h.Grain.StartAsync(LatticeValueTransform.Passthrough(), MaxLenPolicy(10)),
            Throws.TypeOf<InvalidOperationException>());
    }

    [Test]
    public async Task StartAsync_same_parameters_while_in_flight_resumes_idempotently()
    {
        var policy = JsonPolicy();
        var transform = LatticeValueTransform.Passthrough();
        var seed = new SchemaRemediationState
        {
            InProgress = true,
            Phase = LatticeSchemaRemediationPhase.Cutover,
            OperationId = "op1",
            DestinationTreeId = TreeId + "/remediated/op1",
            SourcePhysicalTreeId = TreeId,
            Transform = transform,
            TargetPolicy = policy,
            ScannedCount = 3,
        };
        var h = CreateGrain(new[] { ("k1", "{\"a\":1}") }, seed);

        var report = await h.Grain.StartAsync(transform, policy);

        Assert.That(report.Succeeded, Is.True);
        // Resumed straight at cutover: alias repointed to the already-built destination.
        await h.Registry.Received(1).SetAliasAsync(TreeId, "orders/remediated/op1");
        await h.PolicyStore.Received(1).SetPolicyAsync(TreeId, policy, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RunRemediationPassAsync_resumes_a_durable_build_after_reactivation()
    {
        // Simulate a reactivation: durable state says a build is in flight at the
        // Build phase. A fresh grain over that state must resume and cut over.
        var policy = JsonPolicy();
        var seed = new SchemaRemediationState
        {
            InProgress = true,
            Phase = LatticeSchemaRemediationPhase.Build,
            OperationId = "op7",
            DestinationTreeId = TreeId + "/remediated/op7",
            SourcePhysicalTreeId = TreeId,
            Transform = LatticeValueTransform.Passthrough(),
            TargetPolicy = policy,
        };
        var h = CreateGrain(new[] { ("k1", "{\"a\":1}") }, seed);

        await h.Grain.RunRemediationPassAsync();

        await h.Destination.Received(1).SetAsync("k1", Arg.Any<byte[]>());
        await h.Registry.Received(1).SetAliasAsync(TreeId, "orders/remediated/op7");
        Assert.That(h.State.State.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Completed));
        Assert.That(h.State.State.InProgress, Is.False);
    }

    [Test]
    public async Task RunRemediationPassAsync_is_noop_when_idle()
    {
        var h = CreateGrain(new[] { ("k1", "{\"a\":1}") });

        await h.Grain.RunRemediationPassAsync();

        await h.Registry.DidNotReceive().SetAliasAsync(Arg.Any<string>(), Arg.Any<string>());
        Assert.That(h.State.WriteCount, Is.Zero);
    }

    [Test]
    public void StartAsync_write_state_failure_on_initiate_rolls_back_and_does_not_touch_original()
    {
        var h = CreateGrain(new[] { ("k1", "{\"a\":1}") });
        h.State.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.That(
            async () => await h.Grain.StartAsync(LatticeValueTransform.Passthrough(), JsonPolicy()),
            Throws.InvalidOperationException);

        // Rolled back: not in progress, nothing cut over.
        Assert.That(h.State.State.InProgress, Is.False);
        Assert.That(h.State.State.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Idle));
    }

    [Test]
    public async Task GetStatusAsync_reports_idle_before_any_remediation()
    {
        var h = CreateGrain(Array.Empty<(string, string)>());

        var report = await h.Grain.GetStatusAsync();

        Assert.That(report.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Idle));
        Assert.That(report.InProgress, Is.False);
    }

    [Test]
    public async Task RunRemediationPassAsync_build_time_violation_discards_partial_destination_and_does_not_cut_over()
    {
        // Seed directly at the Build phase (dry-run already passed) with a source
        // whose second entry violates the policy - the build-window quiescence
        // caveat. The build must discard the partial destination and abort without
        // cutting over.
        var policy = MaxLenPolicy(3);
        var seed = new SchemaRemediationState
        {
            InProgress = true,
            Phase = LatticeSchemaRemediationPhase.Build,
            OperationId = "opX",
            DestinationTreeId = TreeId + "/remediated/opX",
            Transform = LatticeValueTransform.Passthrough(),
            TargetPolicy = policy,
        };
        var h = CreateGrain(new[] { ("k1", "{}"), ("k2", "{\"too\":\"big\"}") }, seed);

        await h.Grain.RunRemediationPassAsync();

        // k1 was written before k2 failed; the partial destination is discarded.
        await h.Destination.Received(1).SetAsync("k1", Arg.Any<byte[]>());
        await h.Destination.Received(1).DeleteTreeAsync(Arg.Any<CancellationToken>());
        await h.Registry.DidNotReceive().SetAliasAsync(Arg.Any<string>(), Arg.Any<string>());
        await h.PolicyStore.DidNotReceive().SetPolicyAsync(Arg.Any<string>(), Arg.Any<LatticeSchemaPolicy>(), Arg.Any<CancellationToken>());
        Assert.That(h.State.State.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Aborted));
        Assert.That(h.State.State.LastReport!.Value.OffendingKey, Is.EqualTo("k2"));
    }

    [Test]
    public async Task GetStatusAsync_reports_last_outcome_after_completion()
    {
        var h = CreateGrain(new[] { ("k1", "{\"a\":1}") });
        await h.Grain.StartAsync(LatticeValueTransform.Passthrough(), JsonPolicy());

        var report = await h.Grain.GetStatusAsync();

        Assert.That(report.Succeeded, Is.True);
        Assert.That(report.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Completed));
    }
}
