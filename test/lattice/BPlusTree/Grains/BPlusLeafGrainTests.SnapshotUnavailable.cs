using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3641, the leaf half. Under the "snapshot unavailable" ambient a
/// multi-key fan-out has no single registry decision view, so a leaf that
/// reaches a prepared key throws
/// <see cref="LatticeTransactionOutcomeUnavailableException"/> instead of
/// resolving the saga against the registry at its own moment - which is what
/// let a failed lattice-level snapshot tear a read across leaves. A read that
/// reaches no prepared key completes, and the registry is never consulted
/// either way.
/// <para>
/// Each "throws" test goes red when the marker is not set (verified by
/// perturbing <c>BeginUnavailableScope</c>): the leaf then resolves the prepare
/// through the reachable registry substitute and returns a value.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task GetManyAsync_under_an_unavailable_scope_reaching_a_prepared_key_throws_without_consulting_the_registry()
    {
        LatticeRegistrySnapshotContext.Current = null;
        var (grain, registry) = BuildLeafWithRegistry(TxStatus.Committed);
        var txid = Guid.NewGuid();
        await SeedPreparedOverrideAsync(grain, txid);

        using (LatticeRegistrySnapshotContext.BeginUnavailableScope())
        {
            var ex = Assert.ThrowsAsync(Is.TypeOf<LatticeTransactionOutcomeUnavailableException>(),
                async () => await grain.GetManyAsync(["k"])) as LatticeTransactionOutcomeUnavailableException;
            Assert.That(ex!.TransactionIds, Is.EqualTo(new[] { txid }));
            Assert.That(ex.TreeId, Is.EqualTo(SnapshotPrecedenceTreeId));
        }

        await registry.DidNotReceive().GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>());
        await registry.DidNotReceive().GetStatusAsync(Arg.Any<Guid>());
    }

    [Test]
    public async Task GetManyAsync_under_an_unavailable_scope_reaching_no_prepared_key_succeeds()
    {
        LatticeRegistrySnapshotContext.Current = null;
        var (grain, registry) = BuildLeafWithRegistry(TxStatus.Committed);
        await SeedPreparedOverrideAsync(grain, Guid.NewGuid());
        await grain.SetAsync("plain", [9]);

        Dictionary<string, byte[]> result;
        using (LatticeRegistrySnapshotContext.BeginUnavailableScope())
        {
            result = await grain.GetManyAsync(["plain"]);
        }

        Assert.That(result["plain"], Is.EqualTo(new byte[] { 9 }));
        await registry.DidNotReceive().GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>());
    }

    [Test]
    public async Task GetKeysAsync_under_an_unavailable_scope_reaching_a_prepared_key_throws()
    {
        LatticeRegistrySnapshotContext.Current = null;
        var (grain, _) = BuildLeafWithRegistry(TxStatus.Committed);
        await SeedPreparedOverrideAsync(grain, Guid.NewGuid());

        using (LatticeRegistrySnapshotContext.BeginUnavailableScope())
        {
            Assert.That(async () => await grain.GetKeysAsync(),
                Throws.TypeOf<LatticeTransactionOutcomeUnavailableException>());
        }
    }

    [Test]
    public async Task GetKeysAsync_under_an_unavailable_scope_over_a_range_without_a_prepared_key_succeeds()
    {
        LatticeRegistrySnapshotContext.Current = null;
        var (grain, _) = BuildLeafWithRegistry(TxStatus.Committed);
        await SeedPreparedOverrideAsync(grain, Guid.NewGuid());
        await grain.SetAsync("x", [9]);

        List<string> keys;
        using (LatticeRegistrySnapshotContext.BeginUnavailableScope())
        {
            keys = await grain.GetKeysAsync(startInclusive: "x");
        }

        Assert.That(keys, Is.EqualTo(new[] { "x" }));
    }

    [Test]
    public async Task CountAsync_under_an_unavailable_scope_reaching_a_prepared_key_throws()
    {
        LatticeRegistrySnapshotContext.Current = null;
        var (grain, _) = BuildLeafWithRegistry(TxStatus.Committed);
        await SeedPreparedOverrideAsync(grain, Guid.NewGuid());

        using (LatticeRegistrySnapshotContext.BeginUnavailableScope())
        {
            Assert.That(async () => await grain.CountAsync(),
                Throws.TypeOf<LatticeTransactionOutcomeUnavailableException>());
        }
    }

    [Test]
    public async Task CountAsync_under_an_unavailable_scope_on_a_leaf_without_prepares_succeeds()
    {
        LatticeRegistrySnapshotContext.Current = null;
        var (grain, registry) = BuildLeafWithRegistry(TxStatus.Committed);
        await grain.SetAsync("a", [1]);
        await grain.SetAsync("b", [2]);

        int count;
        using (LatticeRegistrySnapshotContext.BeginUnavailableScope())
        {
            count = await grain.CountAsync();
        }

        Assert.That(count, Is.EqualTo(2));
        await registry.DidNotReceive().GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>());
    }

    [Test]
    public async Task GetAsync_under_an_unavailable_scope_reaching_a_prepared_key_throws_without_consulting_the_registry()
    {
        LatticeRegistrySnapshotContext.Current = null;
        var (grain, registry) = BuildLeafWithRegistry(TxStatus.Committed);
        var txid = Guid.NewGuid();
        await SeedPreparedOverrideAsync(grain, txid);

        using (LatticeRegistrySnapshotContext.BeginUnavailableScope())
        {
            var ex = Assert.ThrowsAsync(Is.TypeOf<LatticeTransactionOutcomeUnavailableException>(),
                async () => await grain.GetAsync("k")) as LatticeTransactionOutcomeUnavailableException;
            Assert.That(ex!.Key, Is.EqualTo("k"));
        }

        await registry.DidNotReceive().GetStatusAsync(Arg.Any<Guid>());
    }
}
