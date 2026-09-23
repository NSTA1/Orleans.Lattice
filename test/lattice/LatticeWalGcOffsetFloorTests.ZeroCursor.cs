using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Issue #3416: a pinned consumer that is present in the in-memory cursor
/// registry only with a <see cref="HybridLogicalClock.Zero"/> cursor (a
/// block-pin-only registration) must be read exactly as though it were absent
/// from the registry. Its Zero cursor contributes nothing to the registry
/// minimum, so its durable pin is the only retention evidence it has; skipping
/// that pin because the consumer is "present" left it guarded by neither the
/// block-pin clause nor the uncovered-cursor fold, and the offset axis trimmed
/// WAL it still owed.
/// </summary>
/// <remarks>
/// Each hole test runs the SAME population twice: once with the consumer
/// registered at Zero and once with it missing from the registry. The
/// registry-absent arm is the pre-existing, correct behaviour, so asserting both
/// arms against one expectation pins the fix to parity rather than to a number
/// derived from the fix itself.
/// </remarks>
public sealed partial class LatticeWalGcOffsetFloorTests
{
    [TestCase(true)]
    [TestCase(false)]
    public async Task RunOnceAsync_zero_cursor_consumer_with_zero_pin_and_no_offset_cover_blocks_the_trim(
        bool registeredAtZero)
    {
        // leaf-2 is a newborn leaf: it has registered a Zero cursor but never
        // checkpointed, so it holds a Zero durable pin and reports -1 (no offset
        // cover). That is exactly the shape the block-pin clause was written for,
        // and a registry-ABSENT leaf in this state blocks the pass. Pre-fix, the
        // registry-PRESENT arm skipped the pin before the block-pin clause AND
        // skipped the Zero cursor out of the uncovered fold, so the offset axis
        // admitted and trimmed all four entries.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));
        if (registeredAtZero)
        {
            // blockedAtHlc: null keeps the blocked-floor clause out of the
            // picture, so the only guard left to observe is the one under test.
            await registry.ReportCursorAsync(Tree, SilentConsumer, HybridLogicalClock.Zero, blockedAtHlc: null);
        }

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(20),
            [SilentConsumer] = HybridLogicalClock.Zero,
        };
        var durableOffsets = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            [LeafConsumer] = 3,
            [SilentConsumer] = -1,
        };
        var sut = new LatticeWalGc(Services(provider, durablePins, durableOffsets), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(0),
            "A never-checkpointed leaf with a Zero pin holds the pass whether or not it is registered at Zero.");
        Assert.That(await SurvivingOffsetsAsync(provider), Is.EqualTo(new[] { 0L, 1L, 2L, 3L }));
        Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.BlockedByUnusablePin));
        Assert.That(report.BlockingConsumerId, Is.EqualTo(SilentConsumer),
            "The blocker must be nameable, so the scheduler's blocked-leaf remedy can act on it.");
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task RunOnceAsync_zero_cursor_consumer_with_real_pin_lowers_the_floor_to_that_pin(
        bool registeredAtZero)
    {
        // leaf-2's durable pin (HLC 8) is its only frontier: its registry cursor
        // is Zero, so nothing was folded into the registry minimum for it. The
        // pin must therefore lower both the HLC floor and the uncovered cursor,
        // as it does for a registry-absent consumer. Entry 0 (HLC 10) is above
        // the pin, so the prefix scan stops there and nothing is trimmed.
        // Pre-fix, the registry-present arm skipped the pin, the floor stayed at
        // 20, the uncovered cursor at 30, and all four entries were trimmed.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));
        if (registeredAtZero)
        {
            await registry.ReportCursorAsync(Tree, SilentConsumer, HybridLogicalClock.Zero, blockedAtHlc: null);
        }

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(20),
            [SilentConsumer] = Hlc(8),
        };
        var durableOffsets = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            [LeafConsumer] = 3,
            [SilentConsumer] = -1,
        };
        var sut = new LatticeWalGc(Services(provider, durablePins, durableOffsets), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.MinCursor, Is.EqualTo(Hlc(8)),
            "The durable pin is the Zero-cursor consumer's only frontier and must set the floor.");
        Assert.That(report.EntriesTrimmed, Is.EqualTo(0));
        Assert.That(await SurvivingOffsetsAsync(provider), Is.EqualTo(new[] { 0L, 1L, 2L, 3L }));
    }

    [Test]
    public async Task RunOnceAsync_zero_cursor_consumer_covered_by_the_offset_floor_does_not_block_the_trim()
    {
        // THE DISCRIMINATING CONTROL. This is the shape issue #3416's own
        // reproduction describes: the consumer is registered at Zero with a Zero
        // pin, but it REPORTS a real checkpoint offset (3), so it is in the
        // coverage set and the durable offset floor already speaks for it
        // (issue #3094). It has durably scanned through offset 3, so every entry
        // here is safe to trim. A fix that simply blocked every registry-present
        // Zero-pin consumer would wedge this tree - the regression #3094 removed -
        // and would fail this test.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        await registry.ReportCursorAsync(Tree, LeafConsumer, HybridLogicalClock.Zero, blockedAtHlc: null);

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = HybridLogicalClock.Zero,
        };
        var durableOffsets = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            [LeafConsumer] = 3,
        };
        var sut = new LatticeWalGc(Services(provider, durablePins, durableOffsets), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.Available));
        Assert.That(report.BlockingConsumerId, Is.Null);
        Assert.That(report.EntriesTrimmed, Is.EqualTo(4),
            "A covered consumer's checkpoint offset admits the whole durably-scanned prefix.");
        Assert.That(await SurvivingOffsetsAsync(provider), Is.Empty);
    }
}
