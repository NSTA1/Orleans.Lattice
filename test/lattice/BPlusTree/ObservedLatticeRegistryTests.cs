using System.Diagnostics.Metrics;
using System.Reflection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for <see cref="ObservedLatticeRegistry"/> and
/// <see cref="LatticeRegistryGrainFactoryExtensions.GetLatticeRegistry"/>, the
/// caller-side registry call histogram added for issue #3088.
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
public sealed class ObservedLatticeRegistryTests
{
    [Test]
    public void Call_when_the_registry_times_out_records_a_timeout_sample_tagged_by_method_and_rethrows()
    {
        var inner = Substitute.For<ILatticeRegistry>();
        inner.ResolveAsync("t").Returns(Task.FromException<string>(new TimeoutException("Response did not arrive on time")));

        var samples = Capture(() =>
            Assert.ThrowsAsync<TimeoutException>(() => ObservedLatticeRegistry.Wrap(inner).ResolveAsync("t")));

        Assert.That(samples, Has.Count.EqualTo(1), "a never-served registry call must still produce exactly one sample");
        Assert.That(samples[0].Method, Is.EqualTo(nameof(ILatticeRegistry.ResolveAsync)));
        Assert.That(samples[0].Outcome, Is.EqualTo(ObservedLatticeRegistry.TimeoutOutcome));
        Assert.That(samples[0].Tenant, Is.EqualTo(LatticeTenantLabel.PlatformTenant));
    }

    [Test]
    public async Task Call_when_the_registry_completes_records_a_completed_sample_tagged_by_method()
    {
        var inner = Substitute.For<ILatticeRegistry>();
        var entry = new TreeRegistryEntry();
        inner.UpdateAsync("t", entry).Returns(Task.CompletedTask);

        List<Sample> samples = [];
        using (Listen(samples))
        {
            await ObservedLatticeRegistry.Wrap(inner).UpdateAsync("t", entry);
        }

        Assert.That(samples, Has.Count.EqualTo(1));
        Assert.That(samples[0].Method, Is.EqualTo(nameof(ILatticeRegistry.UpdateAsync)));
        Assert.That(samples[0].Outcome, Is.EqualTo(ObservedLatticeRegistry.CompletedOutcome));
        Assert.That(samples[0].Value, Is.GreaterThanOrEqualTo(0d));
    }

    [Test]
    public void Call_when_the_registry_faults_records_a_faulted_sample_and_rethrows()
    {
        var inner = Substitute.For<ILatticeRegistry>();
        inner.GetEntryAsync("t").Returns(Task.FromException<TreeRegistryEntry?>(new InvalidOperationException("boom")));

        var samples = Capture(() =>
            Assert.ThrowsAsync<InvalidOperationException>(() => ObservedLatticeRegistry.Wrap(inner).GetEntryAsync("t")));

        Assert.That(samples, Has.Count.EqualTo(1));
        Assert.That(samples[0].Method, Is.EqualTo(nameof(ILatticeRegistry.GetEntryAsync)));
        Assert.That(samples[0].Outcome, Is.EqualTo(ObservedLatticeRegistry.FaultedOutcome));
    }

    [Test]
    public async Task Call_returns_the_inner_result_unchanged()
    {
        var inner = Substitute.For<ILatticeRegistry>();
        inner.ResolveAsync("t").Returns("physical");

        Assert.That(await ObservedLatticeRegistry.Wrap(inner).ResolveAsync("t"), Is.EqualTo("physical"));
    }

    /// <summary>
    /// Drives every interface member through the decorator by reflection, so a
    /// member added to <see cref="ILatticeRegistry"/> is covered with no edit here,
    /// and a forwarding member that calls the wrong overload, drops or reorders an
    /// argument, or records under a copy-pasted <c>nameof</c> fails by name.
    /// </summary>
    [Test]
    public async Task Every_member_forwards_its_exact_arguments_to_the_same_member_and_records_under_its_own_name()
    {
        var inner = Substitute.For<ILatticeRegistry>();
        var observed = ObservedLatticeRegistry.Wrap(inner);
        var members = typeof(ILatticeRegistry).GetMethods();
        var recordedNames = new HashSet<string>(StringComparer.Ordinal);

        Assert.That(members, Is.Not.Empty, "reflection found no registry members, so this test would pass vacuously");

        foreach (var member in members)
        {
            inner.ClearReceivedCalls();
            var args = member.GetParameters().Select((p, i) => SampleArgument(p, i)).ToArray();

            List<Sample> samples = [];
            using (Listen(samples))
            {
                await (Task)member.Invoke(observed, args)!;
            }

            var call = inner.ReceivedCalls().ToList();
            Assert.That(call, Has.Count.EqualTo(1), $"{Describe(member)} must forward exactly one call");
            Assert.That(call[0].GetMethodInfo(), Is.EqualTo(member), $"{Describe(member)} forwarded to a different member");
            Assert.That(call[0].GetArguments(), Is.EqualTo(args), $"{Describe(member)} did not forward its arguments unchanged");
            Assert.That(samples, Has.Count.EqualTo(1), $"{Describe(member)} must record exactly one sample");
            Assert.That(samples[0].Method, Is.EqualTo(member.Name), $"{Describe(member)} recorded under the wrong method tag");
            Assert.That(samples[0].Outcome, Is.EqualTo(ObservedLatticeRegistry.CompletedOutcome));
            recordedNames.Add(samples[0].Method!);
        }

        Assert.That(recordedNames, Is.EquivalentTo(ObservedLatticeRegistry.MethodNames()));
    }

    [Test]
    public void GetLatticeRegistry_returns_the_decorator_over_the_registry_singleton()
    {
        var grain = Substitute.For<ILatticeRegistry>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(grain);

        var registry = factory.GetLatticeRegistry();

        Assert.That(ObservedLatticeRegistry.TryGetInner(registry, out var inner), Is.True,
            "GetLatticeRegistry must return the timing decorator, or registry calls go unrecorded.");
        Assert.That(inner, Is.SameAs(grain));
    }

    [Test]
    public void TryGetInner_when_the_registry_is_not_a_decorator_returns_false()
    {
        var grain = Substitute.For<ILatticeRegistry>();

        Assert.That(ObservedLatticeRegistry.TryGetInner(grain, out var inner), Is.False);
        Assert.That(inner, Is.Null);
    }

    [Test]
    public void Wrap_when_the_inner_registry_is_null_throws()
    {
        Assert.Throws<ArgumentNullException>(() => ObservedLatticeRegistry.Wrap(null!));
    }

    [Test]
    public void Decorator_type_is_private_so_orleans_does_not_register_it_as_a_grain_class()
    {
        // The Orleans code generator registers every public or internal concrete
        // class implementing a grain interface as a grain class. A non-private
        // decorator becomes a second ILatticeRegistry implementation and every
        // GetGrain<ILatticeRegistry> fails as ambiguous.
        var type = ObservedLatticeRegistry.Wrap(Substitute.For<ILatticeRegistry>()).GetType();

        Assert.That(type.IsNestedPrivate, Is.True,
            $"{type.FullName} must be a private nested type; an internal or public grain-interface "
            + "implementation is registered by Orleans as a competing ILatticeRegistry grain class.");
    }

    [Test]
    public void GetLatticeRegistry_when_the_factory_is_null_throws()
    {
        Assert.Throws<ArgumentNullException>(() => ((IGrainFactory)null!).GetLatticeRegistry());
    }

    [Test]
    public void MethodNames_covers_every_registry_member_including_non_interleaved_mutators()
    {
        var names = ObservedLatticeRegistry.MethodNames();

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
        var tag = ObservedLatticeRegistry.MethodTag(name);

        Assert.That(tag.Key, Is.EqualTo(LatticeMetrics.TagMethod));
        Assert.That(tag.Value, Is.EqualTo(ObservedLatticeRegistry.UnknownMethod));
    }

    [Test]
    public void MethodTag_when_name_is_on_the_interface_returns_the_same_frozen_pair_each_time()
    {
        var first = ObservedLatticeRegistry.MethodTag(nameof(ILatticeRegistry.ResolveAsync));
        var second = ObservedLatticeRegistry.MethodTag(nameof(ILatticeRegistry.ResolveAsync));

        Assert.That(first.Value, Is.EqualTo(nameof(ILatticeRegistry.ResolveAsync)));
        Assert.That(second.Value, Is.SameAs(first.Value));
    }

    [Test]
    public void RegistryCallerOutcomeTag_maps_each_ending_to_its_arm_under_the_outcome_key()
    {
        var completed = ObservedLatticeRegistry.RegistryCallerOutcomeTag(null);
        var timeout = ObservedLatticeRegistry.RegistryCallerOutcomeTag(new TimeoutException());
        var faulted = ObservedLatticeRegistry.RegistryCallerOutcomeTag(new InvalidOperationException());

        Assert.Multiple(() =>
        {
            Assert.That(completed.Key, Is.EqualTo(LatticeMetrics.TagOutcome));
            Assert.That(completed.Value, Is.EqualTo(ObservedLatticeRegistry.CompletedOutcome));
            Assert.That(timeout.Key, Is.EqualTo(LatticeMetrics.TagOutcome));
            Assert.That(timeout.Value, Is.EqualTo(ObservedLatticeRegistry.TimeoutOutcome));
            Assert.That(faulted.Key, Is.EqualTo(LatticeMetrics.TagOutcome));
            Assert.That(faulted.Value, Is.EqualTo(ObservedLatticeRegistry.FaultedOutcome));
        });
    }

    private static string Describe(MethodInfo member) =>
        $"{member.Name}({string.Join(", ", member.GetParameters().Select(p => p.ParameterType.Name))})";

    /// <summary>
    /// A distinct, non-default value per parameter so a dropped or swapped
    /// argument is visible. Throws for an unrecognised parameter type, so a new
    /// registry member with a new parameter shape fails here loudly rather than
    /// being forwarded with an uninformative default.
    /// </summary>
    private static object? SampleArgument(ParameterInfo parameter, int index)
    {
        var type = Nullable.GetUnderlyingType(parameter.ParameterType) ?? parameter.ParameterType;
        if (type == typeof(string)) return $"arg-{index}";
        if (type == typeof(int)) return 40 + index;
        if (type == typeof(long)) return 400L + index;
        if (type == typeof(bool)) return true;
        if (type == typeof(TimeSpan)) return TimeSpan.FromSeconds(index + 1);
        if (type == typeof(int[])) return new[] { index, index + 1 };
        if (type == typeof(HistoryRetentionMode)) return Enum.GetValues<HistoryRetentionMode>()[^1];
        if (type == typeof(TreeRegistryEntry)) return new TreeRegistryEntry();
        if (type == typeof(ShardMap)) return ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, 2);
        if (type == typeof(IReadOnlyList<string>)) return new[] { $"arg-{index}" };
        if (type == typeof(IReadOnlyCollection<(int Partition, string ProviderKey)>)) return new[] { (index, $"arg-{index}") };
        throw new InvalidOperationException(
            $"No sample value for registry parameter '{parameter.Name}' of type {parameter.ParameterType}; add one here.");
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
    /// decorator records on the calling thread; concurrent fixtures driving real
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
}
