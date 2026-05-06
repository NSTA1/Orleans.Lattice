using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

public partial class ReplicationTxBufferGrainTests
{
    // -------- RegisterBlacklistedTransactionsAsync — argument validation --------

    [Test]
    public async Task RegisterBlacklistedTransactionsAsync_throws_on_null_list()
    {
        var (grain, _, _, _) = await CreateGrainAsync();

        Assert.That(
            async () => await grain.RegisterBlacklistedTransactionsAsync(null!, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task RegisterBlacklistedTransactionsAsync_rejects_empty_guid_with_index_in_message()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var ids = new[] { Guid.NewGuid(), Guid.Empty, Guid.NewGuid() };

        var ex = Assert.ThrowsAsync<ArgumentException>(
            async () => await grain.RegisterBlacklistedTransactionsAsync(ids, CancellationToken.None));
        Assert.That(ex!.Message, Does.Contain("transactionIds[1]"));
    }

    [Test]
    public async Task RegisterBlacklistedTransactionsAsync_empty_list_is_noop()
    {
        var (grain, _, _, _) = await CreateGrainAsync();

        Assert.That(
            async () => await grain.RegisterBlacklistedTransactionsAsync(Array.Empty<Guid>(), CancellationToken.None),
            Throws.Nothing);
    }

    [Test]
    public async Task RegisterBlacklistedTransactionsAsync_observes_cancellation()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.RegisterBlacklistedTransactionsAsync(new[] { Guid.NewGuid() }, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task RegisterBlacklistedTransactionsAsync_idempotent_on_re_add()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var tx = Guid.NewGuid();

        await grain.RegisterBlacklistedTransactionsAsync(new[] { tx }, CancellationToken.None);
        await grain.RegisterBlacklistedTransactionsAsync(new[] { tx }, CancellationToken.None);

        // Bypass still fires after redundant re-add.
        var result = await grain.AdmitAsync(MakeEntry(tx, 3, 0), CancellationToken.None);
        Assert.That(result.BlacklistedBypass, Is.True);
    }

    [Test]
    public async Task RegisterBlacklistedTransactionsAsync_unions_across_multiple_calls()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var tx1 = Guid.NewGuid();
        var tx2 = Guid.NewGuid();

        await grain.RegisterBlacklistedTransactionsAsync(new[] { tx1 }, CancellationToken.None);
        await grain.RegisterBlacklistedTransactionsAsync(new[] { tx2 }, CancellationToken.None);

        var r1 = await grain.AdmitAsync(MakeEntry(tx1, 2, 0), CancellationToken.None);
        var r2 = await grain.AdmitAsync(MakeEntry(tx2, 2, 0), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(r1.BlacklistedBypass, Is.True);
            Assert.That(r2.BlacklistedBypass, Is.True);
        });
    }

    // -------- AdmitAsync — bypass routing --------

    [Test]
    public async Task AdmitAsync_returns_BlacklistedBypass_for_blacklisted_txid()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var tx = Guid.NewGuid();
        await grain.RegisterBlacklistedTransactionsAsync(new[] { tx }, CancellationToken.None);

        var result = await grain.AdmitAsync(MakeEntry(tx, 5, 2), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.BlacklistedBypass, Is.True);
            Assert.That(result.BatchComplete, Is.False);
            Assert.That(result.Deduped, Is.False);
            Assert.That(result.CompletedBatch, Is.Empty);
        });
    }

    [Test]
    public async Task AdmitAsync_blacklisted_entry_does_not_persist_to_system_tree()
    {
        var (grain, data, _, _) = await CreateGrainAsync();
        var tx = Guid.NewGuid();
        await grain.RegisterBlacklistedTransactionsAsync(new[] { tx }, CancellationToken.None);

        await grain.AdmitAsync(MakeEntry(tx, 3, 0), CancellationToken.None);

        // No staged atomic-batch entries (the 'b/' prefix). The
        // blacklist token under 'x/' is expected and persists for
        // reactivation rehydration.
        Assert.That(data.Keys.Where(k => k.StartsWith("b/", StringComparison.Ordinal)), Is.Empty);
    }

    [Test]
    public async Task AdmitAsync_blacklisted_entry_does_not_change_count_or_bytes()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var tx = Guid.NewGuid();
        await grain.RegisterBlacklistedTransactionsAsync(new[] { tx }, CancellationToken.None);

        await grain.AdmitAsync(MakeEntry(tx, 3, 0), CancellationToken.None);
        await grain.AdmitAsync(MakeEntry(tx, 3, 1), CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await grain.CountTransactionsAsync(CancellationToken.None), Is.EqualTo(0));
            Assert.That(await grain.CountBytesAsync(CancellationToken.None), Is.EqualTo(0L));
        });
    }

    [Test]
    public async Task AdmitAsync_non_blacklisted_admits_normally_alongside_blacklisted()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var blocked = Guid.NewGuid();
        var live = Guid.NewGuid();
        await grain.RegisterBlacklistedTransactionsAsync(new[] { blocked }, CancellationToken.None);

        var bypass = await grain.AdmitAsync(MakeEntry(blocked, 2, 0), CancellationToken.None);
        var partial = await grain.AdmitAsync(MakeEntry(live, 2, 0), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(bypass.BlacklistedBypass, Is.True);
            Assert.That(partial.BlacklistedBypass, Is.False);
            Assert.That(partial.BatchComplete, Is.False);
        });
        Assert.That(await grain.CountTransactionsAsync(CancellationToken.None), Is.EqualTo(1));
    }

    [Test]
    public async Task AdmitAsync_blacklisted_bypass_is_idempotent_across_re_delivery()
    {
        var (grain, data, _, _) = await CreateGrainAsync();
        var tx = Guid.NewGuid();
        await grain.RegisterBlacklistedTransactionsAsync(new[] { tx }, CancellationToken.None);

        var r1 = await grain.AdmitAsync(MakeEntry(tx, 3, 0), CancellationToken.None);
        var r2 = await grain.AdmitAsync(MakeEntry(tx, 3, 0), CancellationToken.None);
        var r3 = await grain.AdmitAsync(MakeEntry(tx, 3, 1), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(r1.BlacklistedBypass, Is.True);
            Assert.That(r2.BlacklistedBypass, Is.True);
            Assert.That(r3.BlacklistedBypass, Is.True);
            // No staged atomic-batch entries persisted (those use the
            // 'b/' prefix). The blacklist token under 'x/' is expected
            // and is the seam that survives reactivation.
            Assert.That(data.Keys.Where(k => k.StartsWith("b/", StringComparison.Ordinal)), Is.Empty);
        });
    }

    [Test]
    public async Task AdmitAsync_throws_on_zero_hlc_even_for_blacklisted_txid()
    {
        // The Timestamp <= Zero guard precedes the blacklist short-circuit; a
        // malformed entry must surface a typed exception rather than silently
        // bypass through the blacklist path.
        var (grain, _, _, _) = await CreateGrainAsync();
        var tx = Guid.NewGuid();
        await grain.RegisterBlacklistedTransactionsAsync(new[] { tx }, CancellationToken.None);

        var entry = MakeEntry(tx, 3, 0) with { Timestamp = HybridLogicalClock.Zero };

        Assert.That(
            async () => await grain.AdmitAsync(entry, CancellationToken.None),
            Throws.InstanceOf<ArgumentException>());
    }

    // -------- Persistence --------

    [Test]
    public async Task RegisterBlacklistedTransactionsAsync_persists_under_x_prefix()
    {
        // The blacklist token must land in the per-tree system tree
        // under the 'x/' key prefix so reactivation rehydrates it.
        // The prefix is disjoint from the staged-entry 'b/' prefix
        // ('b' < 'x' in ASCII), so range scans over either prefix do
        // not collide.
        var (store, data) = FakeSystemLattice.Create();
        var grain = await NewGrainAsync(store);
        var tx = Guid.NewGuid();

        await grain.RegisterBlacklistedTransactionsAsync(new[] { tx }, CancellationToken.None);

        var blacklistRows = data.Keys
            .Where(k => k.StartsWith("x/", StringComparison.Ordinal))
            .ToList();
        Assert.Multiple(() =>
        {
            Assert.That(blacklistRows, Has.Count.EqualTo(1));
            // Key shape: "x/{txid in 'N' format}".
            Assert.That(blacklistRows[0], Is.EqualTo($"x/{tx:N}"));
        });
    }

    [Test]
    public async Task Blacklist_persists_across_grain_reactivation()
    {
        // A blacklisted transaction registered on one activation must
        // continue to bypass admission after a fresh activation reads
        // the same backing system tree.
        var (store, _) = FakeSystemLattice.Create();
        var first = await NewGrainAsync(store);
        var tx = Guid.NewGuid();
        await first.RegisterBlacklistedTransactionsAsync(new[] { tx }, CancellationToken.None);

        var second = await NewGrainAsync(store);

        var result = await second.AdmitAsync(MakeEntry(tx, 3, 0), CancellationToken.None);
        Assert.That(result.BlacklistedBypass, Is.True);
    }

    [Test]
    public async Task Blacklist_register_observes_cancellation_between_persists()
    {
        // A cancellation triggered partway through a multi-id register
        // surfaces a typed cancel and the prior id(s) that completed
        // their persist remain durable across reactivation. The
        // contract is "at-least-the-already-persisted-prefix": ids
        // ahead of the cancel may or may not be present after
        // reactivation.
        var (store, _) = FakeSystemLattice.Create();
        var first = await NewGrainAsync(store);
        var preCommitted = Guid.NewGuid();
        await first.RegisterBlacklistedTransactionsAsync(
            new[] { preCommitted }, CancellationToken.None);

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid() };

        Assert.That(
            async () => await first.RegisterBlacklistedTransactionsAsync(ids, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());

        // Reactivation: the pre-cancel id is still durable.
        var second = await NewGrainAsync(store);
        var result = await second.AdmitAsync(MakeEntry(preCommitted, 3, 0), CancellationToken.None);
        Assert.That(result.BlacklistedBypass, Is.True);
    }
}
