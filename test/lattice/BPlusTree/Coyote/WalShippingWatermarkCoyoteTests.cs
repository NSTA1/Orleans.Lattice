using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for the WAL shard shipping-read
/// durable-contiguous watermark (the sender-side residual of #1076 / the
/// cold-partition first-batch replication gap). They are tagged
/// <c>[Category("Coyote")]</c> so the fast dev loop and the per-package
/// deterministic CI step skip them; a dedicated CI step runs this category. See the
/// "Coyote concurrency tier" section of
/// <c>.github/instructions/testing.instructions.md</c>.
/// <para>
/// The safety test drives the real production rule
/// (<see cref="Orleans.Lattice.BPlusTree.WalShippingWatermark"/>) that
/// <c>WalShardGrain.ReadShippingAsync</c> / <c>ReadAsync</c> clamp every page at,
/// and the guard test removes the watermark so Coyote re-finds the prefix-hole data
/// loss - a vacuous model that still passed with the guard removed would be
/// rejected.
/// </para>
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class WalShippingWatermarkCoyoteTests
{
    /// <summary>
    /// The fix: clamping every shipping read at the durable-contiguous watermark
    /// admits no schedule of out-of-order flush completions where a cursor-advancing
    /// reader ships an offset above an unfilled prefix hole - for a two-offset burst
    /// and wider - and the reader always catches up once the hole fills.
    /// </summary>
    [Test]
    public void Watermark_never_ships_an_offset_above_a_prefix_hole([Values(2, 3, 4)] int offsetCount)
    {
        CoyoteModelHarness.AssertNoInterleavingViolation(
            new WalShippingWatermarkModel(offsetCount, WalShippingWatermarkMode.DurableContiguousWatermark));
    }

    /// <summary>
    /// The guard: clamping at the raw next-offset tail instead of the
    /// durable-contiguous watermark lets a higher window that persists before a
    /// lower in-flight one be shipped, advancing the reader's cursor past the hole,
    /// and Coyote must find the stranded offset. This proves the watermark clamp in
    /// the safety test above is load-bearing rather than decorative.
    /// </summary>
    [Test]
    public void Raw_tail_without_watermark_strands_an_in_flight_offset([Values(2, 3)] int offsetCount)
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new WalShippingWatermarkModel(offsetCount, WalShippingWatermarkMode.RawNextOffsetTail));
    }
}
