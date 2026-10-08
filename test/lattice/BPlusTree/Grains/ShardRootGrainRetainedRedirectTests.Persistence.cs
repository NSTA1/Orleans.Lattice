using System.Text.Json;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class ShardRootGrainRetainedRedirectTests
{
    [Test]
    public async Task MarkRetainedRedirectAsync_failure_restores_state_so_retry_persists()
    {
        var state = StateWithRedirect();
        var previous = state.State.RetainedRedirect;
        var grain = CreateGrain(state);
        state.ThrowOnWrite = new IOException("storage unavailable");
        Assert.ThrowsAsync<IOException>(() => grain.MarkRetainedRedirectAsync("other-destination", "new-op", "other-logical"));
        Assert.That(state.State.RetainedRedirect, Is.SameAs(previous));
        Assert.That(state.State.AdditionalRetainedRedirects, Is.Null);
        ((IGrainBase)grain).GrainContext.Received(1).Deactivate(Arg.Any<DeactivationReason>());

        await grain.MarkRetainedRedirectAsync("other-destination", "new-op", "other-logical");
        Assert.That(state.State.AdditionalRetainedRedirects![LogicalTreeId], Is.SameAs(previous));
        Assert.That(state.WriteCount, Is.EqualTo(1));
    }

    [Test]
    public async Task MarkRetainedRedirectAsync_ambiguous_storage_ack_reactivates_and_restores_prior_fence_on_rollback()
    {
        var state = StateWithRedirect();
        var grain = CreateGrain(state);
        string? durable = null;
        state.OnWriteState = value =>
        {
            durable = JsonSerializer.Serialize(value);
            throw new IOException("fence persisted but acknowledgement lost");
        };
        Assert.ThrowsAsync<IOException>(() => grain.MarkRetainedRedirectAsync("new-copy", "new-op", LogicalTreeId));
        ((IGrainBase)grain).GrainContext.Received(1).Deactivate(Arg.Any<DeactivationReason>());
        var loaded = new FakePersistentState<ShardRootState>
        {
            State = JsonSerializer.Deserialize<ShardRootState>(durable!)!,
        };
        await CreateGrain(loaded).ClearRetainedRedirectIfOwnedAsync("new-op");
        Assert.That(loaded.State.RetainedRedirect!.OperationId, Is.EqualTo(OperationId));
        Assert.That(loaded.State.PreviousRetainedRedirects, Is.Null);
    }

    [Test]
    public async Task ClearRetainedRedirectIfOwnedAsync_preserves_unrelated_aliases_and_operations()
    {
        var state = StateWithRedirect();
        var grain = CreateGrain(state);
        await grain.MarkRetainedRedirectAsync("other-destination", "other-op", "other-logical");
        await grain.ClearRetainedRedirectIfOwnedAsync("never-installed");
        Assert.That(state.WriteCount, Is.EqualTo(1), "a conditional rollback does not touch another operation");
        await grain.ClearRetainedRedirectIfOwnedAsync(OperationId);
        Assert.That(state.State.AdditionalRetainedRedirects, Is.Null);
        Assert.That(state.State.RetainedRedirect!.LogicalTreeId, Is.EqualTo("other-logical"));
        RequestContext.Set(MarkerKey, "other-logical");
        Assert.ThrowsAsync<StaleTreeRoutingException>(() => grain.GetAsync("k"));
    }

    [Test]
    public async Task ClearRetainedRedirectIfOwnedAsync_restores_a_pre_existing_redirect_for_the_same_alias()
    {
        var state = StateWithRedirect();
        var previous = state.State.RetainedRedirect;
        var grain = CreateGrain(state);
        await grain.MarkRetainedRedirectAsync("new-copy", "new-op", LogicalTreeId);
        await grain.ClearRetainedRedirectIfOwnedAsync("new-op");
        Assert.That(state.State.RetainedRedirect, Is.SameAs(previous));
        Assert.That(state.State.PreviousRetainedRedirects, Is.Null);
        RequestContext.Set(MarkerKey, LogicalTreeId);
        Assert.ThrowsAsync<StaleTreeRoutingException>(() => grain.GetAsync("k"));
    }

    [Test]
    public async Task ReleaseRetainedRedirectAsync_only_releases_the_requested_alias()
    {
        var state = StateWithRedirect();
        var grain = CreateGrain(state);
        await grain.MarkRetainedRedirectAsync("other-destination", "other-op", "other-logical");
        await grain.ReleaseRetainedRedirectAsync(LogicalTreeId);
        Assert.That(state.State.AdditionalRetainedRedirects, Is.Null);
        RequestContext.Set(MarkerKey, "other-logical");
        Assert.ThrowsAsync<StaleTreeRoutingException>(() => grain.GetAsync("k"));
    }
}
