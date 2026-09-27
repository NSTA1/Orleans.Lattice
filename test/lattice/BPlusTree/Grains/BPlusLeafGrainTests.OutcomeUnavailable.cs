using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #2215. A read of a key under a prepared saga mutation must ask the
/// per-tree registry for the saga's outcome, and when that call fails in
/// transport the read throws the typed, retryable
/// <see cref="LatticeTransactionOutcomeUnavailableException"/> - never the raw
/// transport exception and never a guessed value. Each discriminator below
/// fails on the unfixed code (the raw <see cref="TimeoutException"/> escapes,
/// which the <c>Throws.TypeOf</c> constraint rejects) and passes with the fix.
/// <para>
/// The guards pin the unchanged half of the rule: a registry that answers
/// <see cref="TxStatus.Indeterminate"/> still hides the key, and a fault that is
/// not a transport failure propagates as itself.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static (BPlusLeafGrain Grain, ITxRegistryGrain Registry) BuildLeafWithFailingRegistry(Exception fault)
    {
        var (grain, registry) = BuildLeafWithRegistry(TxStatus.Committed);
        registry.GetStatusAsync(Arg.Any<Guid>()).Returns(Task.FromException<TxStatus>(fault));
        registry.GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>())
            .Returns(Task.FromException<Dictionary<Guid, TxStatus>>(fault));
        return (grain, registry);
    }

    private static void AssertOutcomeUnavailable(
        LatticeTransactionOutcomeUnavailableException? ex, Guid txid, string? key, Exception fault)
    {
        Assert.That(ex, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(ex!.TreeId, Is.EqualTo(SnapshotPrecedenceTreeId));
            Assert.That(ex.Key, Is.EqualTo(key));
            Assert.That(ex.KeyCount, Is.EqualTo(1));
            Assert.That(ex.TransactionIds, Is.EqualTo(new[] { txid }));
            Assert.That(ex.InnerException, Is.SameAs(fault));
        });
    }

    // ---- discriminators: single-key paths (ResolvePendingStatusAsync) ----

    [Test]
    public async Task GetAsync_with_an_unreachable_registry_throws_outcome_unavailable_not_a_raw_timeout()
    {
        LatticeRegistrySnapshotContext.Current = null;
        var fault = new TimeoutException("registry response timeout");
        var (grain, _) = BuildLeafWithFailingRegistry(fault);
        var txid = Guid.NewGuid();
        await SeedPreparedOverrideAsync(grain, txid);

        var ex = Assert.ThrowsAsync(Is.TypeOf<LatticeTransactionOutcomeUnavailableException>(),
            async () => await grain.GetAsync("k")) as LatticeTransactionOutcomeUnavailableException;

        AssertOutcomeUnavailable(ex, txid, "k", fault);
    }

    [Test]
    public async Task GetWithVersionAsync_with_an_unreachable_registry_throws_outcome_unavailable_not_a_raw_timeout()
    {
        LatticeRegistrySnapshotContext.Current = null;
        var fault = new TimeoutException("registry response timeout");
        var (grain, _) = BuildLeafWithFailingRegistry(fault);
        var txid = Guid.NewGuid();
        await SeedPreparedOverrideAsync(grain, txid);

        var ex = Assert.ThrowsAsync(Is.TypeOf<LatticeTransactionOutcomeUnavailableException>(),
            async () => await grain.GetWithVersionAsync("k")) as LatticeTransactionOutcomeUnavailableException;

        AssertOutcomeUnavailable(ex, txid, "k", fault);
    }

    [Test]
    public async Task ExistsAsync_with_an_unreachable_registry_throws_outcome_unavailable_not_a_raw_timeout()
    {
        LatticeRegistrySnapshotContext.Current = null;
        var fault = new TimeoutException("registry response timeout");
        var (grain, _) = BuildLeafWithFailingRegistry(fault);
        var txid = Guid.NewGuid();
        await SeedPreparedOverrideAsync(grain, txid);

        var ex = Assert.ThrowsAsync(Is.TypeOf<LatticeTransactionOutcomeUnavailableException>(),
            async () => await grain.ExistsAsync("k")) as LatticeTransactionOutcomeUnavailableException;

        AssertOutcomeUnavailable(ex, txid, "k", fault);
    }

    [Test]
    public async Task GetAsync_with_an_unavailable_registry_silo_throws_outcome_unavailable()
    {
        // An OrleansException-family transport failure is translated exactly as
        // a response timeout is.
        LatticeRegistrySnapshotContext.Current = null;
        var fault = new SiloUnavailableException("registry silo gone");
        var (grain, _) = BuildLeafWithFailingRegistry(fault);
        var txid = Guid.NewGuid();
        await SeedPreparedOverrideAsync(grain, txid);

        var ex = Assert.ThrowsAsync(Is.TypeOf<LatticeTransactionOutcomeUnavailableException>(),
            async () => await grain.GetAsync("k")) as LatticeTransactionOutcomeUnavailableException;

        AssertOutcomeUnavailable(ex, txid, "k", fault);
    }

    // ---- discriminators: scan path (SnapshotPendingForReadAsync) ----

    [Test]
    public async Task GetManyAsync_with_an_unreachable_registry_throws_outcome_unavailable_not_a_raw_timeout()
    {
        LatticeRegistrySnapshotContext.Current = null;
        var fault = new TimeoutException("registry response timeout");
        var (grain, _) = BuildLeafWithFailingRegistry(fault);
        var txid = Guid.NewGuid();
        await SeedPreparedOverrideAsync(grain, txid);

        var ex = Assert.ThrowsAsync(Is.TypeOf<LatticeTransactionOutcomeUnavailableException>(),
            async () => await grain.GetManyAsync(["k"])) as LatticeTransactionOutcomeUnavailableException;

        AssertOutcomeUnavailable(ex, txid, key: null, fault);
    }

    [Test]
    public async Task GetKeysAsync_with_an_unreachable_registry_throws_outcome_unavailable_not_a_raw_timeout()
    {
        LatticeRegistrySnapshotContext.Current = null;
        var fault = new TimeoutException("registry response timeout");
        var (grain, _) = BuildLeafWithFailingRegistry(fault);
        var txid = Guid.NewGuid();
        await SeedPreparedOverrideAsync(grain, txid);

        var ex = Assert.ThrowsAsync(Is.TypeOf<LatticeTransactionOutcomeUnavailableException>(),
            async () => await grain.GetKeysAsync()) as LatticeTransactionOutcomeUnavailableException;

        AssertOutcomeUnavailable(ex, txid, key: null, fault);
    }

    // ---- guards: pass on both arms ----

    [Test]
    public async Task GetAsync_with_an_indeterminate_registry_answer_still_hides_the_key()
    {
        LatticeRegistrySnapshotContext.Current = null;
        var (grain, _) = BuildLeafWithRegistry(TxStatus.Indeterminate);
        await SeedPreparedOverrideAsync(grain, Guid.NewGuid());

        Assert.That(await grain.GetAsync("k"), Is.Null,
            "registry says unknown -> hidden, unchanged by the unreachable-registry rule");
        Assert.That(await grain.ExistsAsync("k"), Is.False);
    }

    // The multi-key leaf paths do not honour Indeterminate -> hidden today; that
    // pre-existing defect is tracked separately as #3665, so there is no
    // multi-key guard here.

    [Test]
    public async Task GetAsync_with_a_non_transport_registry_fault_propagates_the_fault_unchanged()
    {
        // Only a transport failure is translated: a genuine registry fault is not
        // a statement that the outcome is merely unavailable.
        LatticeRegistrySnapshotContext.Current = null;
        var fault = new InvalidOperationException("registry bug");
        var (grain, _) = BuildLeafWithFailingRegistry(fault);
        await SeedPreparedOverrideAsync(grain, Guid.NewGuid());

        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.GetAsync("k"));
        Assert.That(ex, Is.SameAs(fault));
    }

    [Test]
    public async Task GetManyAsync_with_a_cancelled_registry_call_propagates_the_cancellation()
    {
        LatticeRegistrySnapshotContext.Current = null;
        var fault = new OperationCanceledException("cancelled");
        var (grain, _) = BuildLeafWithFailingRegistry(fault);
        await SeedPreparedOverrideAsync(grain, Guid.NewGuid());

        Assert.That(async () => await grain.GetManyAsync(["k"]), Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task GetAsync_with_an_unreachable_registry_on_a_key_without_a_prepare_does_not_consult_it()
    {
        // The registry is only asked when the result depends on it.
        LatticeRegistrySnapshotContext.Current = null;
        var (grain, registry) = BuildLeafWithFailingRegistry(new TimeoutException("down"));
        await grain.SetAsync("plain", [9]);

        Assert.That(await grain.GetAsync("plain"), Is.EqualTo(new byte[] { 9 }));
        await registry.DidNotReceive().GetStatusAsync(Arg.Any<Guid>());
    }
}
