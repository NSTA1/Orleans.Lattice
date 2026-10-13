using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task GetVisibleRawEntries_committed_pending_value_survives_terminal_materialisation(bool equalBytes)
    {
        var leaf = CreateGrain();
        await leaf.SetAsync("k", [1]);
        var before = (await leaf.GetRawEntriesAsync(["k"]))[0]!.Value;
        var tx = Guid.NewGuid();
        byte[] prepared = equalBytes ? [1] : [2];
        await PreparePendingSetAsync(leaf, tx, "k", prepared);

        using (Committed(tx))
        {
            var pending = (await leaf.GetVisibleRawEntriesAsync(["k"]))[0];
            Assert.That(pending, Is.Not.Null);
            Assert.That(pending!.Value.Timestamp.CompareTo(before.Timestamp), Is.GreaterThan(0));
            Assert.That((await leaf.GetRawEntriesAsync(["k"]))[0], Is.EqualTo(before),
                "the logical winner is visible while the materialised row still lags");

            await leaf.ApplyTxTerminalAsync(tx, committed: true);

            var materialised = (await leaf.GetVisibleRawEntriesAsync(["k"]))[0];
            AssertLogicalRowPreserved(pending.Value, materialised);
            Assert.That(await leaf.GetAsync("k"), Is.EqualTo(prepared),
                "retiring the pending metadata must not expose the older cached value");
        }
    }

    [Test]
    public async Task GetVisibleRawEntries_committed_pending_tombstone_survives_terminal_materialisation()
    {
        var leaf = CreateGrain();
        await leaf.SetAsync("k", [1]);
        var before = (await leaf.GetRawEntriesAsync(["k"]))[0]!.Value;
        var tx = Guid.NewGuid();
        await PreparePendingDeleteAsync(leaf, tx, "k");

        using (Committed(tx))
        {
            var pending = (await leaf.GetVisibleRawEntriesAsync(["k"]))[0];
            Assert.That(pending, Is.Not.Null);
            Assert.That(pending!.Value.IsTombstone, Is.True);
            Assert.That(pending.Value.Timestamp.CompareTo(before.Timestamp), Is.GreaterThan(0));
            Assert.That((await leaf.GetRawEntriesAsync(["k"]))[0], Is.EqualTo(before),
                "the committed delete is logically visible while the materialised row still lags");

            await leaf.ApplyTxTerminalAsync(tx, committed: true);

            var materialised = (await leaf.GetVisibleRawEntriesAsync(["k"]))[0];
            AssertLogicalRowPreserved(pending.Value, materialised);
            Assert.That(await leaf.GetAsync("k"), Is.Null,
                "retiring the pending metadata must not resurrect the older cached value");
        }
    }

    [Test]
    public async Task GetVisibleRawEntries_detector_rejects_terminal_retirement_without_installation()
    {
        var leaf = CreateGrain();
        await leaf.SetAsync("k", [1]);
        var before = (await leaf.GetRawEntriesAsync(["k"]))[0]!.Value;
        var tx = Guid.NewGuid();
        await PreparePendingSetAsync(leaf, tx, "k", [2]);

        using (Committed(tx))
        {
            var committed = (await leaf.GetVisibleRawEntriesAsync(["k"]))[0]!.Value;
            Assert.That(committed.Timestamp.CompareTo(before.Timestamp), Is.GreaterThan(0));

            // Simulate the regression: retire the bucket as an abort even though
            // the decision says committed, without installing its prepared row.
            await leaf.ApplyTxTerminalAsync(tx, committed: false);
            var afterRetirement = (await leaf.GetVisibleRawEntriesAsync(["k"]))[0];

            var failure = Assert.Throws<MultipleAssertException>(
                () => AssertLogicalRowPreserved(committed, afterRetirement));
            Assert.That(failure!.Message, Does.Contain("payload"),
                "the negative control must fail because retirement revealed the older payload");
            Assert.That(failure.Message, Does.Contain("HLC"),
                "the negative control must fail because retirement regressed the row version");
            Assert.That(await leaf.GetAsync("k"), Is.EqualTo(new byte[] { 1 }));
        }
    }

    private static void AssertLogicalRowPreserved(LwwEntry expected, LwwEntry? actual)
    {
        Assert.Multiple(() =>
        {
            Assert.That(actual?.Value, Is.EqualTo(expected.Value),
                "logical payload must survive materialisation and metadata retirement");
            Assert.That(actual is { } row ? row.Timestamp.CompareTo(expected.Timestamp) : -1,
                Is.GreaterThanOrEqualTo(0),
                "logical HLC must not regress when pending metadata is retired");
            Assert.That(actual?.IsTombstone, Is.EqualTo(expected.IsTombstone),
                "logical tombstone kind must survive materialisation and metadata retirement");
        });
    }
}
