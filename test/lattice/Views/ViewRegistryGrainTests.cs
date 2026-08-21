using NSubstitute;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

[TestFixture]
public sealed class ViewRegistryGrainTests
{
    [Test]
    public async Task RegisterAsync_retry_after_failed_initial_persist_writes_again()
    {
        var state = new FakePersistentState<ViewRegistryState>
        {
            ThrowOnWrite = new InvalidOperationException("storage unavailable"),
        };
        var grain = CreateGrain(state);
        var registration = Registration("runtime");

        Assert.That(
            async () => await grain.RegisterAsync(registration),
            Throws.TypeOf<InvalidOperationException>());
        Assert.That(await grain.ListAsync(), Is.Empty);

        await grain.RegisterAsync(registration);
        var registrations = await grain.ListAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(1));
            Assert.That(registrations.Single(), Is.SameAs(registration));
        });
    }

    [Test]
    public async Task RegisterAsync_failed_replacement_restores_previous_registration()
    {
        var previous = Registration("runtime", "v1");
        var state = new FakePersistentState<ViewRegistryState>();
        state.State.Registrations[previous.ViewName] = previous;
        state.ThrowOnWrite = new InvalidOperationException("storage unavailable");
        var grain = CreateGrain(state);

        Assert.That(
            async () => await grain.RegisterAsync(Registration("runtime", "v2")),
            Throws.TypeOf<InvalidOperationException>());

        Assert.That((await grain.ListAsync()).Single(), Is.SameAs(previous));
    }

    [Test]
    public async Task RegisterAsync_identical_registration_does_not_write()
    {
        var registration = Registration("runtime");
        var state = new FakePersistentState<ViewRegistryState>();
        state.State.Registrations[registration.ViewName] = registration;
        var grain = CreateGrain(state);

        await grain.RegisterAsync(registration);

        Assert.That(state.WriteCount, Is.Zero);
    }

    private static ViewRegistryGrain CreateGrain(FakePersistentState<ViewRegistryState> state) =>
        new(Substitute.For<IGrainContext>(), state);

    private static RuntimeViewRegistration Registration(
        string viewName,
        string projectionVersion = "v1") =>
        new()
        {
            ViewName = viewName,
            SourceTreeId = $"source-{viewName}",
            ProjectionTypeName = typeof(PredicateLatticeViewProjection).FullName!,
            ProjectionVersion = projectionVersion,
        };
}
