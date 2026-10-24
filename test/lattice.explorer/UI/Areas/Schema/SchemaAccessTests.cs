using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>Capability replies cannot outlive the caller or invalidation that admitted them.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaAccessTests : SchemaTestContext
{
    [Test]
    public async Task Availability_reply_from_a_previous_identity_is_not_returned_or_cached()
    {
        var access = Services.GetRequiredService<SchemaAccess>();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Schema.CapabilityGate = gate;
        var pending = access.GetAvailabilityAsync(CancellationToken.None);
        Schema.DefaultCapabilities = FakeSchemaControl.None;
        Auth.SignIn("reader");
        gate.SetResult();

        Assert.That(await pending, Is.EqualTo(AreaAvailability.Hidden));
        Assert.That(await access.GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden));
        Assert.That(Schema.CountOf("ProbeCapabilities"), Is.EqualTo(2));
    }

    [Test]
    public async Task Grants_reply_from_a_previous_identity_is_not_returned_or_cached()
    {
        var access = Services.GetRequiredService<SchemaAccess>();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Schema.CapabilityGate = gate;
        var pending = access.GetGrantsAsync("orders", false, CancellationToken.None);
        Schema.DefaultCapabilities = FakeSchemaControl.None;
        Auth.SignIn("reader");
        gate.SetResult();

        Assert.That((await pending).HasAny, Is.False);
        Assert.That((await access.GetGrantsAsync("orders", false, CancellationToken.None)).HasAny, Is.False);
        Assert.That(Schema.CountOf("ProbeCapabilities"), Is.EqualTo(2));
    }

    [Test]
    public async Task An_explicit_invalidation_discards_an_in_flight_probe_for_the_same_caller()
    {
        var access = Services.GetRequiredService<SchemaAccess>();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Schema.CapabilityGate = gate;
        var pending = access.GetGrantsAsync("orders", false, CancellationToken.None);
        access.Forget();
        gate.SetResult();

        Assert.That((await pending).HasAny, Is.False);
        Assert.That((await access.GetGrantsAsync("orders", false, CancellationToken.None)).HasAny, Is.True);
        Assert.That(Schema.CountOf("ProbeCapabilities"), Is.EqualTo(2));
    }
}
