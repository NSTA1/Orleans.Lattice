using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>Availability before connection, after a cached load, and without an optional connection.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataAreaAvailabilityTests : DataTestContext
{
    [TestCase(LatticeConnectionState.Disconnected, false, "Connect to a cluster to browse its data.")]
    [TestCase(LatticeConnectionState.Faulted, false, "Connect to a cluster to browse its data.")]
    [TestCase(LatticeConnectionState.Faulted, true, "Sign in to browse this cluster's data.")]
    public async Task GetAvailability_disconnected_before_load_explains_the_required_action(
        LatticeConnectionState state, bool requiresAuthentication, string reason)
    {
        RegisterConnection(state, requiresAuthentication);
        Client.WithTree("orders");

        var result = await Area.GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Kind, Is.EqualTo(AreaAvailabilityKind.Unavailable));
            Assert.That(result.Reason, Is.EqualTo(reason));
            Assert.That(Client.Calls, Is.Empty, "an unconfigured or refused connection must not trigger a probe");
            Assert.That(Services.GetRequiredService<DataDirectory>().Loaded, Is.Null);
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task GetAvailability_disconnected_after_load_keeps_the_cached_directory_visible(bool requiresAuthentication)
    {
        RegisterConnection(LatticeConnectionState.Faulted, requiresAuthentication);
        Client.WithTree("orders");
        var directory = Services.GetRequiredService<DataDirectory>();
        var loaded = await directory.LoadAsync();
        var calls = Client.Calls.Count;

        var result = await Area.GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(loaded.Select(entry => entry.LogicalId), Is.EqualTo(new[] { "orders" }));
            Assert.That(result, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(directory.Loaded, Is.SameAs(loaded));
            Assert.That(Client.Calls, Has.Count.EqualTo(calls));
        });
    }

    [TestCase(LatticeConnectionState.Connected)]
    [TestCase(LatticeConnectionState.Reconnecting)]
    public async Task GetAvailability_usable_connection_probes_the_catalogue(LatticeConnectionState state)
    {
        RegisterConnection(state);
        Client.WithTree("orders");

        Assert.That(await Area.GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Visible));
        Assert.That(Client.Calls, Does.Contain(nameof(ILatticeStateClient.ListTreesAsync)));
    }

    [Test]
    public async Task GetAvailability_unresolvable_optional_connection_still_uses_the_registered_reader()
    {
        Services.RemoveAll<ILatticeStateConnection>();
        Services.AddScoped<ILatticeStateConnection>(provider =>
            provider.GetRequiredKeyedService<ILatticeStateConnection>("not-registered"));
        Client.WithTree("orders");

        var result = await Area.GetAvailabilityAsync(CancellationToken.None);

        Assert.That(result, Is.EqualTo(AreaAvailability.Visible));
        Assert.That(Client.Calls, Does.Contain(nameof(ILatticeStateClient.ListTreesAsync)));
    }

    private void RegisterConnection(LatticeConnectionState state, bool requiresAuthentication = false)
    {
        var connection = Substitute.For<ILatticeStateConnection>();
        connection.Status.Returns(new LatticeConnectionStatus(state, null, "Not shown to the caller.", requiresAuthentication));
        Services.AddSingleton(connection);
    }
}
