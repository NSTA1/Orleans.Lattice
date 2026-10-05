using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="WalPurgeHoldGrain"/>, the per-log record of the
/// replication consumers a retention trim was forced past (issue #4534).
/// </summary>
[TestFixture]
public class WalPurgeHoldGrainTests
{
    private const string Shipper = "shipper/tree/site-b";

    private static (WalPurgeHoldGrain Grain, FakePersistentState<WalPurgeHoldState> State) CreateGrain()
    {
        var state = new FakePersistentState<WalPurgeHoldState>();
        return (new WalPurgeHoldGrain(state), state);
    }

    [Test]
    public async Task ReleaseIfCoveredAsync_releases_a_hold_every_named_partition_of_which_is_covered()
    {
        var (grain, state) = CreateGrain();
        await grain.AddAsync(Shipper, [4, -1]);

        var released = await grain.ReleaseIfCoveredAsync(Shipper, [5, 0]);

        Assert.Multiple(() =>
        {
            Assert.That(released, Is.True);
            Assert.That(state.State.Holds, Is.Empty, "the release is durable");
        });
    }

    [Test]
    public async Task ReleaseIfCoveredAsync_keeps_a_hold_any_partition_of_which_is_at_or_past_the_position()
    {
        var (grain, state) = CreateGrain();
        await grain.AddAsync(Shipper, [4, 7]);

        var atTrim = await grain.ReleaseIfCoveredAsync(Shipper, [5, 7]);
        var missingPartition = await grain.ReleaseIfCoveredAsync(Shipper, [5]);

        Assert.Multiple(() =>
        {
            Assert.That(atTrim, Is.False, "a position at the trimmed offset never read it");
            Assert.That(missingPartition, Is.False, "a partition the consumer does not report is not covered");
            Assert.That(state.State.Holds.ContainsKey(Shipper), Is.True);
        });
    }

    [Test]
    public async Task ReleaseIfCoveredAsync_with_no_positions_releases_unconditionally_and_is_false_without_a_hold()
    {
        var (grain, state) = CreateGrain();
        await grain.AddAsync(Shipper, [1_000]);

        var released = await grain.ReleaseIfCoveredAsync(Shipper, null);
        var again = await grain.ReleaseIfCoveredAsync(Shipper, null);

        Assert.Multiple(() =>
        {
            Assert.That(released, Is.True);
            Assert.That(again, Is.False, "nothing is left to release");
            Assert.That(state.State.Holds, Is.Empty);
        });
    }
}
