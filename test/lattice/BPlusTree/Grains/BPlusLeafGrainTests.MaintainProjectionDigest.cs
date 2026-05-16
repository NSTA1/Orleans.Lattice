using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the <see cref="LatticeOptions.MaintainProjectionDigest"/>
/// opt-out. Verifies that flipping the option to <c>false</c> takes the
/// trimmed mutation path (no XOR fold, no persisted-hash mutation), throws
/// from <c>GetProjectionDigestAsync</c>, and that the default of <c>true</c>
/// preserves the original incremental-maintenance behaviour.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public void MaintainProjectionDigest_default_is_true()
    {
        // The compile-time default must remain true so existing
        // deployments and tests observe incremental digest maintenance
        // without opting in.
        Assert.That(new LatticeOptions().MaintainProjectionDigest, Is.True);
        Assert.That(LatticeOptions.DefaultMaintainProjectionDigest, Is.True);
    }

    [Test]
    public async Task GetProjectionDigestAsync_throws_when_maintenance_disabled()
    {
        var grain = CreateGrain(options: new LatticeOptions { MaintainProjectionDigest = false });
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            async () => await grain.GetProjectionDigestAsync());

        Assert.That(ex!.Message, Does.Contain(nameof(LatticeOptions.MaintainProjectionDigest)));
        Assert.That(ex.Message, Does.Contain("disabled"));
    }

    [Test]
    public async Task GetProjectionDigestAsync_throws_on_empty_leaf_when_disabled()
    {
        // Read-path check must hold even when no mutation has landed yet,
        // because the cached opt-out flag is hydrated from the resolver
        // before the branch.
        var grain = CreateGrain(options: new LatticeOptions { MaintainProjectionDigest = false });

        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await grain.GetProjectionDigestAsync());
    }

    [Test]
    public async Task Mutations_skip_projection_hash_update_when_disabled()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, options: new LatticeOptions { MaintainProjectionDigest = false });

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));
        await grain.DeleteAsync("k1");

        Assert.That(state.State.ProjectionHash, Is.Null,
            "Disabled maintenance must not initialise the persisted hash slot.");
    }

    [Test]
    public async Task Mutations_still_update_entries_when_disabled()
    {
        // The opt-out only suppresses digest maintenance and upward
        // publication - the user-visible Entries map and the resulting
        // Get/Delete semantics are unchanged.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, options: new LatticeOptions { MaintainProjectionDigest = false });

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));

        Assert.That(await grain.GetAsync("k1"), Is.Not.Null);
        Assert.That(await grain.GetAsync("k2"), Is.Not.Null);
        Assert.That(state.State.Entries.Count, Is.EqualTo(2));

        await grain.DeleteAsync("k1");
        Assert.That(state.State.Entries["k1"].IsTombstone, Is.True);
    }

    [Test]
    public async Task Mutations_update_projection_hash_when_enabled()
    {
        // The explicit-enabled case must still drive the hash forward,
        // pinning the default-behaviour regression so a future refactor
        // cannot silently flip the option's effect.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, options: new LatticeOptions { MaintainProjectionDigest = true });

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.That(state.State.ProjectionHash, Is.Not.Null);
        Assert.That(state.State.ProjectionHash!.Length, Is.EqualTo(16));
        Assert.That(state.State.ProjectionHash.Any(b => b != 0), Is.True,
            "An enabled-maintenance write must produce a non-zero XOR fold.");
    }

    [Test]
    public async Task GetProjectionDigestAsync_succeeds_when_enabled()
    {
        // Symmetric to the disabled-throws case: with the option explicitly
        // true the digest API returns a well-formed result.
        var grain = CreateGrain(options: new LatticeOptions { MaintainProjectionDigest = true });
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var digest = await grain.GetProjectionDigestAsync();

        Assert.That(digest.Hash, Is.Not.Null);
        Assert.That(digest.Hash.Length, Is.EqualTo(16));
        Assert.That(digest.EntryCount, Is.EqualTo(1));
    }
}
