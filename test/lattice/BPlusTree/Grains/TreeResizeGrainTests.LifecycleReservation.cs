using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TreeResizeGrainTests
{
    [Test]
    public void External_idle_pass_cannot_release_a_control_plane_reservation()
    {
        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(LatticeInternalOriginEnforcementMarker))
            .Returns(new LatticeInternalOriginEnforcementMarker());
        var (grain, state, _, _, _) = CreateGrain(activationServices: services);
        state.State.AliasReservationId = "resize:orphan";
        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => grain.RunResizePassAsync());
        Assert.That(state.State.AliasReservationId, Is.EqualTo("resize:orphan"));
        Assert.That(state.WriteCount, Is.Zero);
    }

    [Test]
    public async Task Idle_pass_releases_a_persisted_orphan_reservation_after_reactivation()
    {
        var (grain, state, _, factory, _) = CreateGrain();
        state.State.AliasReservationId = "resize:orphan";
        await grain.RunResizePassAsync();
        await factory.GetGrain<ITreeDeletionGrain>(TreeId).Received(1)
            .EndAliasChangeAsync("resize:orphan");
        Assert.That(state.State.AliasReservationId, Is.Null);
    }

    [Test]
    public async Task Failed_release_retains_the_id_for_the_next_idle_pass()
    {
        var (grain, state, _, factory, _) = CreateGrain();
        state.State.AliasReservationId = "resize:orphan";
        var deletion = factory.GetGrain<ITreeDeletionGrain>(TreeId);
        deletion.EndAliasChangeAsync("resize:orphan").ThrowsAsync(new IOException("unavailable"));
        Assert.ThrowsAsync<IOException>(() => grain.RunResizePassAsync());
        Assert.That(state.State.AliasReservationId, Is.EqualTo("resize:orphan"));
        deletion.EndAliasChangeAsync("resize:orphan").Returns(Task.CompletedTask);
        await grain.RunResizePassAsync();
        Assert.That(state.State.AliasReservationId, Is.Null);
    }

    [Test]
    public void Resize_and_undo_refuse_when_lifecycle_cannot_be_reserved()
    {
        var (grain, _, _, factory, _) = CreateGrain();
        factory.GetGrain<ITreeDeletionGrain>(TreeId).BeginAliasChangeAsync(Arg.Any<string>())
            .ThrowsAsync(new InvalidOperationException("logically deleted"));
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.ResizeAsync(64, 64));
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.UndoResizeAsync());
    }
}
