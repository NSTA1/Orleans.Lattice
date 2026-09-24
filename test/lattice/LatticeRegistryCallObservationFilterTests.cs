using System.Diagnostics.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeRegistryCallObservationFilter"/>, the
/// caller-side, always-on registry call histogram added for issue #3088.
/// <para>
/// The load-bearing case is the timeout arm. Registry contention was diagnosed
/// from an Orleans log field that vanishes silo-wide under saturation, and its
/// absence read as "the registry was idle". The grain-body census cannot close
/// that gap because a call that is never admitted produces no body sample. The
/// timeout test proves a never-served call is still recorded, by method, under
/// its own outcome - which is what makes "unreachable" a distinct reading from
/// "fine" and "slow".
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeRegistryCallObservationFilterTests
{
    [Test]
    public void Invoke_when_a_registry_call_times_out_records_a_timeout_sample_tagged_by_method()
    {
        var samples = Capture(() =>
        {
            var context = RegistryContext(nameof(ILatticeRegistry.ResolveAsync));
            context.Invoke().Returns(Task.FromException(new TimeoutException("Response did not arrive on time")));

            Assert.ThrowsAsync<TimeoutException>(() => new LatticeRegistryCallObservationFilter().Invoke(context));
        });

        Assert.That(samples, Has.Count.EqualTo(1), "a never-served registry call must still produce exactly one sample");
        Assert.That(samples[0].Method, Is.EqualTo(nameof(ILatticeRegistry.ResolveAsync)));
        Assert.That(samples[0].Outcome, Is.EqualTo(LatticeRegistryCallObservationFilter.TimeoutOutcome));
        Assert.That(samples[0].Tenant, Is.EqualTo(LatticeTenantLabel.PlatformTenant));
    }

    [Test]
    public async Task Invoke_when_a_registry_call_completes_records_a_completed_sample_tagged_by_method()
    {
        List<Sample> samples = [];
        using (Listen(samples))
        {
            var context = RegistryContext(nameof(ILatticeRegistry.UpdateAsync));
            context.Invoke().Returns(Task.CompletedTask);

            await new LatticeRegistryCallObservationFilter().Invoke(context);
        }

        Assert.That(samples, Has.Count.EqualTo(1));
        Assert.That(samples[0].Method, Is.EqualTo(nameof(ILatticeRegistry.UpdateAsync)));
        Assert.That(samples[0].Outcome, Is.EqualTo(LatticeRegistryCallObservationFilter.CompletedOutcome));
        Assert.That(samples[0].Value, Is.GreaterThanOrEqualTo(0d));
    }

    [Test]
    public void Invoke_when_a_registry_call_faults_records_a_faulted_sample_and_rethrows()
    {
        var samples = Capture(() =>
        {
            var context = RegistryContext(nameof(ILatticeRegistry.GetEntryAsync));
            context.Invoke().Returns(Task.FromException(new InvalidOperationException("boom")));

            Assert.ThrowsAsync<InvalidOperationException>(() => new LatticeRegistryCallObservationFilter().Invoke(context));
        });

        Assert.That(samples, Has.Count.EqualTo(1));
        Assert.That(samples[0].Method, Is.EqualTo(nameof(ILatticeRegistry.GetEntryAsync)));
        Assert.That(samples[0].Outcome, Is.EqualTo(LatticeRegistryCallObservationFilter.FaultedOutcome));
    }

    [Test]
    public async Task Invoke_when_the_call_is_not_a_registry_call_forwards_without_recording()
    {
        List<Sample> samples = [];
        var context = Substitute.For<IOutgoingGrainCallContext>();
        context.InterfaceMethod.Returns(typeof(ILattice).GetMethods()[0]);
        context.MethodName.Returns(nameof(ILatticeRegistry.ResolveAsync));
        context.Invoke().Returns(Task.CompletedTask);

        using (Listen(samples))
        {
            await new LatticeRegistryCallObservationFilter().Invoke(context);
        }

        await context.Received(1).Invoke();
        Assert.That(samples, Is.Empty);
    }

    [Test]
    public void Invoke_when_context_is_null_throws()
    {
        Assert.Throws<ArgumentNullException>(() => new LatticeRegistryCallObservationFilter().Invoke(null!));
    }

    [Test]
    public void MethodNames_covers_every_registry_member_including_non_interleaved_mutators()
    {
        var names = LatticeRegistryCallObservationFilter.MethodNames();

        Assert.That(
            names,
            Is.SupersetOf(new[]
            {
                nameof(ILatticeRegistry.ResolveAsync),
                nameof(ILatticeRegistry.UpdateAsync),
                nameof(ILatticeRegistry.SetShardMapAsync),
                nameof(ILatticeRegistry.ReassignSlotsAsync),
            }));
        Assert.That(names, Is.Unique);
        Assert.That(names, Is.Ordered.Using((IComparer<string>)StringComparer.Ordinal));
    }

    [TestCase(null)]
    [TestCase("NotARegistryMember")]
    public void MethodTag_when_name_is_not_on_the_interface_returns_the_other_bucket(string? name)
    {
        var tag = LatticeRegistryCallObservationFilter.MethodTag(name);

        Assert.That(tag.Key, Is.EqualTo(LatticeMetrics.TagMethod));
        Assert.That(tag.Value, Is.EqualTo(LatticeRegistryCallObservationFilter.UnknownMethod));
    }

    [Test]
    public void MethodTag_when_name_is_on_the_interface_returns_the_same_frozen_pair_each_time()
    {
        var first = LatticeRegistryCallObservationFilter.MethodTag(nameof(ILatticeRegistry.ResolveAsync));
        var second = LatticeRegistryCallObservationFilter.MethodTag(nameof(ILatticeRegistry.ResolveAsync));

        Assert.That(first.Value, Is.EqualTo(nameof(ILatticeRegistry.ResolveAsync)));
        Assert.That(second.Value, Is.SameAs(first.Value));
    }

    [Test]
    public void RegistryCallerOutcomeTag_maps_each_ending_to_its_arm_under_the_outcome_key()
    {
        var completed = LatticeRegistryCallObservationFilter.RegistryCallerOutcomeTag(null);
        var timeout = LatticeRegistryCallObservationFilter.RegistryCallerOutcomeTag(new TimeoutException());
        var faulted = LatticeRegistryCallObservationFilter.RegistryCallerOutcomeTag(new InvalidOperationException());

        Assert.Multiple(() =>
        {
            Assert.That(completed.Key, Is.EqualTo(LatticeMetrics.TagOutcome));
            Assert.That(completed.Value, Is.EqualTo(LatticeRegistryCallObservationFilter.CompletedOutcome));
            Assert.That(timeout.Key, Is.EqualTo(LatticeMetrics.TagOutcome));
            Assert.That(timeout.Value, Is.EqualTo(LatticeRegistryCallObservationFilter.TimeoutOutcome));
            Assert.That(faulted.Key, Is.EqualTo(LatticeMetrics.TagOutcome));
            Assert.That(faulted.Value, Is.EqualTo(LatticeRegistryCallObservationFilter.FaultedOutcome));
        });
    }

    [Test]
    public void AddLattice_installs_the_registry_filter_exactly_once_when_called_twice()
    {
        var services = new ServiceCollection();
        var builder = new SiloBuilderStub(services);

        builder.AddLattice((_, _) => { });
        builder.AddLattice((_, _) => { });

        Assert.That(
            services.Count(d => d.ServiceType == typeof(IOutgoingGrainCallFilter)
                && d.ImplementationType == typeof(LatticeRegistryCallObservationFilter)),
            Is.EqualTo(1));
    }

    private static IOutgoingGrainCallContext RegistryContext(string methodName)
    {
        var context = Substitute.For<IOutgoingGrainCallContext>();
        context.InterfaceMethod.Returns(typeof(ILatticeRegistry).GetMethods().First(m => m.Name == methodName));
        context.MethodName.Returns(methodName);
        return context;
    }

    private static List<Sample> Capture(Action act)
    {
        List<Sample> samples = [];
        using (Listen(samples))
        {
            act();
        }

        return samples;
    }

    /// <summary>
    /// Listens on the caller-side histogram and keeps only samples recorded on the
    /// test's own thread. The substituted calls complete synchronously, so the
    /// filter records on the calling thread; concurrent fixtures driving real
    /// registry traffic record on pool threads and are excluded.
    /// </summary>
    private static MeterListener Listen(List<Sample> samples)
    {
        var threadId = Environment.CurrentManagedThreadId;
        return MeterListening.StartForInstrument(
            LatticeMetrics.RegistryCallerDuration,
            listener => listener.SetMeasurementEventCallback<double>(
                (_, value, tags, _) =>
                {
                    if (Environment.CurrentManagedThreadId != threadId)
                    {
                        return;
                    }

                    string? method = null, outcome = null, tenant = null;
                    foreach (var tag in tags)
                    {
                        switch (tag.Key)
                        {
                            case LatticeMetrics.TagMethod: method = tag.Value as string; break;
                            case LatticeMetrics.TagOutcome: outcome = tag.Value as string; break;
                            case LatticeTenantLabel.TagTenant: tenant = tag.Value as string; break;
                        }
                    }

                    lock (samples)
                    {
                        samples.Add(new Sample(value, method, outcome, tenant));
                    }
                }));
    }

    private sealed record Sample(double Value, string? Method, string? Outcome, string? Tenant);

    private sealed class SiloBuilderStub(IServiceCollection services) : ISiloBuilder
    {
        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();

        public IServiceCollection Services { get; } = services;
    }
}
