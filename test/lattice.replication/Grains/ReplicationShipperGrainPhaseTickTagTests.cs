using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Lattice.Testing;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Coverage for the <c>tree</c> tag the shipper contributes to
/// <see cref="LatticeMetrics.CoordinatorPhaseTickFailures"/>.
/// <para>
/// The shipper's grain key is <c>{treeName}/{peerClusterId}</c>. The base
/// coordinator's default tags that raw key, so before the override this
/// counter reported a composite string in a field operators filter by tree
/// name - a value that is false rather than missing, with nothing in the
/// series to reveal it.
/// </para>
/// <para>
/// Every assertion here arms the real phase timer on a real
/// <see cref="ReplicationShipperGrain"/> and drives a tick that genuinely
/// throws, then reads the emitted tag off a <see cref="MeterListener"/>.
/// Asserting that a <c>tree</c> label merely exists would pass with the
/// override deleted, because the composite carries that label too.
/// </para>
/// </summary>
[TestFixture]
public class ReplicationShipperGrainPhaseTickTagTests
{
    private const string Tree = "shipper-tag-tree";
    private const string Peer = "site-b";
    private const string CompositeKey = $"{Tree}/{Peer}";

    /// <summary>
    /// Fails the named-options lookup <c>PumpOnceAsync</c> performs on its
    /// first line, which sits outside every one of the shipper's internal
    /// drain/transport catch blocks and so reaches the base class's
    /// phase-tick handler. This is the real uncaught shape: the shipper
    /// already converts expected transient faults into its own backoff, so
    /// what surfaces on the coordinator counter is the unexpected residue.
    /// <see cref="CurrentValue"/> still answers because the base class reads
    /// it to size the timer period before any tick runs.
    /// </summary>
    private sealed class FailingNamedOptionsMonitor(LatticeReplicationOptions current)
        : IOptionsMonitor<LatticeReplicationOptions>
    {
        public LatticeReplicationOptions CurrentValue => current;

        public LatticeReplicationOptions Get(string? name) =>
            throw new OptionsValidationException(
                name ?? "", typeof(LatticeReplicationOptions), ["injected phase-tick fault"]);

        public IDisposable? OnChange(Action<LatticeReplicationOptions, string?> listener) => null;
    }

    private sealed record Harness(ReplicationShipperGrain Grain, ITimerRegistry Timers);

    private static Harness Create()
    {
        var timers = Substitute.For<ITimerRegistry>();
        timers.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(Substitute.For<IGrainTimer>());

        var services = new ServiceCollection();
        services.AddSingleton(timers);

        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("shipper", CompositeKey));
        ctx.ActivationServices.Returns(services.BuildServiceProvider());

        var grain = new ReplicationShipperGrain(
            ctx,
            Substitute.For<IReminderRegistry>(),
            NullLogger<ReplicationShipperGrain>.Instance,
            new FailingNamedOptionsMonitor(new LatticeReplicationOptions()),
            Substitute.For<IReplicationTransport>(),
            Substitute.For<IReplicationBatchEncoder>(),
            Substitute.For<IWalRecordEncoder>(),
            Substitute.For<IWalCursorRegistry>(),
            Substitute.For<IGrainFactory>(),
            new FakePersistentState<ReplicationShipperState>(),
            new ReplicationPeerStats(),
            Substitute.For<ILatticeMergeModeResolver>(),
            new WireVersionNegotiationState(),
            Substitute.For<IReplicationDigestProbeTransport>());

        grain.InitializeForTesting(Tree, Peer);
        return new Harness(grain, timers);
    }

    /// <summary>
    /// The callback the grain handed the timer registry. Invoking it directly
    /// is what makes the failure a real exception travelling the real
    /// phase-tick handler rather than a simulation of one.
    /// </summary>
    private static Func<CancellationToken, Task> CapturedTick(ITimerRegistry registry)
    {
        var call = registry.ReceivedCalls()
            .Last(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));
        return (Func<CancellationToken, Task>)call.GetArguments()[2]!;
    }

    private sealed record Measurement(long Value, string? Tree, string? Tenant);

    private static async Task<List<Measurement>> RecordAsync(Func<Task> body)
    {
        var measurements = new List<Measurement>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.CoordinatorPhaseTickFailures,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                string? tree = null;
                string? tenant = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree) tree = tag.Value?.ToString();
                    if (tag.Key == LatticeTenantLabel.TagTenant) tenant = tag.Value?.ToString();
                }

                lock (measurements)
                {
                    measurements.Add(new Measurement(value, tree, tenant));
                }
            }));

        await body();

        lock (measurements)
        {
            return [.. measurements];
        }
    }

    private static async Task<List<Measurement>> ArmAndFailOnceAsync()
    {
        var h = Create();
        return await RecordAsync(async () =>
        {
            await h.Grain.EnsureActiveAsync(CancellationToken.None);
            await CapturedTick(h.Timers)(CancellationToken.None);
        });
    }

    [Test]
    public async Task A_shipper_phase_tick_failure_tags_the_tree_name_alone()
    {
        // The load-bearing assertion: the emitted value under a real fault.
        var measurements = await ArmAndFailOnceAsync();

        Assert.That(
            measurements.Where(m => m.Value == 1).Select(m => m.Tree).ToArray(),
            Is.EqualTo(new[] { Tree }).AsCollection,
            "A shipper failure must be attributable to the tree whose work was discarded.");
    }

    [Test]
    public async Task The_composite_grain_key_never_reaches_the_tree_tag()
    {
        // The negative half. Without it, an override returning the whole key
        // would still satisfy 'a tree tag was emitted'.
        var measurements = await ArmAndFailOnceAsync();

        Assert.Multiple(() =>
        {
            Assert.That(measurements, Is.Not.Empty,
                "The counter must emit at all, or this assertion is vacuous.");
            Assert.That(measurements.Select(m => m.Tree), Has.None.EqualTo(CompositeKey),
                "'tree/peer' is not a tree name; an operator filtering by tree would never match it.");
            Assert.That(measurements.Select(m => m.Tree), Has.None.Contains("/"),
                "No peer cluster id may ride the tree dimension.");
        });
    }

    [Test]
    public async Task The_zero_prime_and_the_failure_agree_on_the_tree()
    {
        // A prime tagged differently from the failure exports a permanent zero
        // beside a series that appears from nowhere on the first failure -
        // which is the reading defect this counter exists to remove.
        var measurements = await ArmAndFailOnceAsync();

        var prime = measurements.First(m => m.Value == 0);
        var failure = measurements.First(m => m.Value == 1);

        Assert.Multiple(() =>
        {
            Assert.That(prime.Tree, Is.EqualTo(Tree));
            Assert.That(failure.Tree, Is.EqualTo(prime.Tree));
            Assert.That(failure.Tenant, Is.EqualTo(prime.Tenant));
        });
    }

    [Test]
    public async Task The_tenant_dimension_is_derived_from_the_same_subject_as_the_tree_tag()
    {
        // NOT a mutation-bound assertion, and it must not be read as one: this
        // test passes with the override deleted. LatticeTenantLabel.ForTree
        // reads the segment after a 't/' prefix, which on 't/{tenant}/{name}'
        // sits before the first separator, so appending '/{peer}' cannot move
        // it. The composite therefore produced the right tenant by accident
        // while the tree tag beside it was false - a second trusted field
        // riding on the same subject and surviving by luck rather than design.
        // Kept as a coupling guard: the tenant must keep being derived from
        // MetricsTreeId, so that a future subject change moves both fields
        // together instead of splitting them.
        var measurements = await ArmAndFailOnceAsync();

        var failure = measurements.First(m => m.Value == 1);

        Assert.That(failure.Tenant, Is.EqualTo(LatticeTenantLabel.Resolve(Tree)));
    }
}
