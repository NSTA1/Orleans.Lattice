using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.GrainIndex.Enrollment;
using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex.Tests.Enrollment;

/// <summary>
/// Covers <see cref="IndexedAttributeMapper"/>: the binding that turns an
/// <see cref="IndexedAttribute"/> parameter into an index-aware state object,
/// and the cases where it deliberately does nothing.
/// </summary>
[TestFixture]
public sealed class IndexedAttributeMapperTests
{
    /// <summary>A constructor surface carrying the annotated parameters under test.</summary>
    private sealed class FacetProbe
    {
        public FacetProbe([Indexed("user", "Store")] IPersistentState<IndexedTestState> state) => _ = state;

        public FacetProbe([Indexed] IPersistentState<IndexedTestState> unnamed, int ignored)
        {
            _ = unnamed;
            _ = ignored;
        }

        public FacetProbe([Indexed] string wrong) => _ = wrong;
    }

    private static ParameterInfo ParameterOf(params Type[] signature) =>
        typeof(FacetProbe).GetConstructor(signature)!.GetParameters()[0];

    private static IndexedAttribute AttributeOn(ParameterInfo parameter) =>
        parameter.GetCustomAttribute<IndexedAttribute>()!;

    private static IGrainContext ContextWith(
        IPersistentState<IndexedTestState> inner,
        GrainIndexEnrollmentSet<IndexedTestState> set,
        out IGrainLifecycle lifecycle,
        out List<IPersistentStateConfiguration> configurations)
    {
        var captured = new List<IPersistentStateConfiguration>();
        configurations = captured;

        var stateFactory = Substitute.For<IPersistentStateFactory>();
        stateFactory
            .Create<IndexedTestState>(Arg.Any<IGrainContext>(), Arg.Any<IPersistentStateConfiguration>())
            .Returns(call =>
            {
                captured.Add(call.ArgAt<IPersistentStateConfiguration>(1));
                return inner;
            });

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(stateFactory);
        services.AddSingleton(set);
        var provider = services.BuildServiceProvider();

        lifecycle = Substitute.For<IGrainLifecycle>();
        var context = Substitute.For<IGrainContext>();
        context.ActivationServices.Returns(provider);
        context.ObservableLifecycle.Returns(lifecycle);
        context.GrainId.Returns(EnrollmentTestIndex.Identity("alice"));
        context.GrainInstance.Returns(EnrollmentTestIndex.GrainInstance());
        return context;
    }

    [Test]
    public void A_parameter_that_is_not_a_persistent_state_is_rejected_with_an_explanation()
    {
        Assert.That(
            () => new IndexedAttributeMapper().GetFactory(
                ParameterOf(typeof(string)),
                AttributeOn(ParameterOf(typeof(string)))),
            Throws.ArgumentException.With.Message.Contains("IPersistentState"),
            "The failure has to name the fix, or an author sees only a reflection error from deep "
            + "inside Orleans' activator.");
    }

    [Test]
    public void A_null_argument_is_rejected()
    {
        var parameter = ParameterOf(typeof(IPersistentState<IndexedTestState>));

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new IndexedAttributeMapper().GetFactory(null!, AttributeOn(parameter)),
                Throws.ArgumentNullException);
            Assert.That(
                () => new IndexedAttributeMapper().GetFactory(parameter, null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void An_annotated_parameter_receives_an_index_aware_state_object()
    {
        var parameter = ParameterOf(typeof(IPersistentState<IndexedTestState>));
        var inner = new RecordingPersistentState<IndexedTestState>(new IndexedTestState());
        var set = new GrainIndexEnrollmentSet<IndexedTestState>(
            [EnrollmentTestIndex.Enroller(new RecordingEnrollmentStore())]);
        var context = ContextWith(inner, set, out _, out _);

        var created = new IndexedAttributeMapper().GetFactory(parameter, AttributeOn(parameter))(context);

        Assert.That(created, Is.InstanceOf<IndexedPersistentState<IndexedTestState>>());
    }

    [Test]
    public void The_state_object_is_attached_to_the_grains_lifecycle()
    {
        var parameter = ParameterOf(typeof(IPersistentState<IndexedTestState>));
        var inner = new RecordingPersistentState<IndexedTestState>(new IndexedTestState());
        var set = new GrainIndexEnrollmentSet<IndexedTestState>(
            [EnrollmentTestIndex.Enroller(new RecordingEnrollmentStore())]);
        var context = ContextWith(inner, set, out var lifecycle, out _);

        new IndexedAttributeMapper().GetFactory(parameter, AttributeOn(parameter))(context);

        lifecycle.Received(1).Subscribe(
            Arg.Any<string>(),
            GrainLifecycleStage.Activate,
            Arg.Any<ILifecycleObserver>());
    }

    [Test]
    public void The_attributes_state_and_storage_names_reach_the_persistence_factory()
    {
        var parameter = ParameterOf(typeof(IPersistentState<IndexedTestState>));
        var inner = new RecordingPersistentState<IndexedTestState>(new IndexedTestState());
        var set = new GrainIndexEnrollmentSet<IndexedTestState>([]);
        var context = ContextWith(inner, set, out _, out var configurations);

        new IndexedAttributeMapper().GetFactory(parameter, AttributeOn(parameter))(context);

        Assert.Multiple(() =>
        {
            Assert.That(configurations, Has.Count.EqualTo(1));
            Assert.That(configurations[0].StateName, Is.EqualTo("user"));
            Assert.That(configurations[0].StorageName, Is.EqualTo("Store"),
                "The grain's persistence must be configured exactly as [PersistentState] would have "
                + "configured it, or annotating an existing grain would move its stored state.");
        });
    }

    [Test]
    public void An_unnamed_state_takes_the_parameters_own_name()
    {
        var parameter = ParameterOf(typeof(IPersistentState<IndexedTestState>), typeof(int));
        var inner = new RecordingPersistentState<IndexedTestState>(new IndexedTestState());
        var set = new GrainIndexEnrollmentSet<IndexedTestState>([]);
        var context = ContextWith(inner, set, out _, out var configurations);

        new IndexedAttributeMapper().GetFactory(parameter, AttributeOn(parameter))(context);

        Assert.That(configurations[0].StateName, Is.EqualTo("unnamed"),
            "This mirrors [PersistentState], so swapping one attribute for the other never changes "
            + "the storage key.");
    }

    [Test]
    public void A_state_type_no_index_projects_is_left_unwrapped()
    {
        var parameter = ParameterOf(typeof(IPersistentState<IndexedTestState>));
        var inner = new RecordingPersistentState<IndexedTestState>(new IndexedTestState());
        var set = new GrainIndexEnrollmentSet<IndexedTestState>([]);
        var context = ContextWith(inner, set, out var lifecycle, out _);

        var created = new IndexedAttributeMapper().GetFactory(parameter, AttributeOn(parameter))(context);

        Assert.Multiple(() =>
        {
            Assert.That(created, Is.SameAs(inner),
                "An attribute that currently matches nothing must cost the grain nothing, which is "
                + "what makes it safe to annotate ahead of declaring the index.");
            Assert.That(lifecycle.ReceivedCalls(), Is.Empty);
        });
    }
}
