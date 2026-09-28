using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

public partial class LatticeSchemaRemediationGrainTests
{
    [Test]
    public void External_idle_pass_cannot_release_a_control_plane_reservation()
    {
        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(LatticeInternalOriginEnforcementMarker))
            .Returns(new LatticeInternalOriginEnforcementMarker());
        var harness = CreateGrainCore(
            () => Entries(), new SchemaRemediationState { AliasReservationId = "remediation:orphan" },
            null, null, services);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => harness.Grain.RunRemediationPassAsync());
        Assert.That(harness.State.State.AliasReservationId, Is.EqualTo("remediation:orphan"));
    }

    [Test]
    public async Task Idle_pass_releases_an_orphan_preparation_idempotently()
    {
        var harness = CreateGrain([], new SchemaRemediationState { AliasReservationId = "remediation:orphan" });
        await harness.Grain.RunRemediationPassAsync();
        await harness.Grain.RunRemediationPassAsync();
        Assert.That(harness.State.State.AliasReservationId, Is.Null);
    }
}
