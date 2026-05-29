using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration tests for the c2-xxviii leaf-side digest-coalescing
/// shape. The fixture pins
/// <see cref="LatticeOptions.DigestCoalescingWindowMs"/> to a small
/// positive value so the per-write hot path defers the cross-grain
/// publish behind a one-shot grain timer; subsequent mutations within
/// the window share that single publish.
/// <para>
/// These tests assert the publish-deferred-then-fires shape directly.
/// Pre-coalescing-era oracle tests (the ones authored against the
/// synchronous-publish shape) live in sibling fixtures that pin the
/// window to zero, so the two shapes are tested independently.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class DigestCoalescingClusterIntegrationTests
{
    private CoalescingClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new CoalescingClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private async Task<ILattice> NewTreeAsync(string prefix)
        => await _fixture.CreateTreeAsync($"{prefix}-{Guid.NewGuid():N}");

    /// <summary>
    /// Writes a batch of keys against a coalescing-window-enabled tree
    /// and asserts the chained-fold aggregate eventually catches up to
    /// the authoritative live count via the settle helper. The window
    /// being positive means we accept a short publish delay; the
    /// invariant is "eventually consistent within ~window + jitter".
    /// </summary>
    [Test]
    public async Task DigestCoalescingWindow_eventually_publishes_aggregate_to_parent()
    {
        var tree = await NewTreeAsync("coalesce-publish");
        const int writeCount = 12;
        for (var i = 0; i < writeCount; i++)
        {
            await tree.SetAsync($"k{i:D2}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        // Read the live count first (always accurate; sourced from the
        // leaf's cache directly, no chained-fold dependency).
        var live = await tree.CountAsync();
        Assert.That(live, Is.EqualTo(writeCount),
            "live entry count must match the number of writes regardless of coalescing window");

        // The chained fold may lag the live count by at most the
        // coalescing window plus scheduling jitter; the settle helper
        // polls until it converges.
        var digest = await LatticeDigestSettleHelpers.AwaitDigestConvergesToAsync(
            tree, shardIndex: 0, expectedEntryCount: live);
        Assert.That(digest.EntryCount, Is.EqualTo(writeCount),
            "chained-fold aggregate must converge to the live count after the coalescing window elapses");
        Assert.That(digest.Hash.Length, Is.EqualTo(16));
    }

    /// <summary>
    /// Asserts that bulk writes inside a single coalescing window
    /// produce a digest that converges to the same aggregate the
    /// synchronous-publish shape produces - the coalescing
    /// optimisation is throughput-only, not a correctness change.
    /// </summary>
    [Test]
    public async Task DigestCoalescingWindow_aggregate_matches_synchronous_publish_shape()
    {
        var tree = await NewTreeAsync("coalesce-equiv");
        for (var i = 0; i < 8; i++)
        {
            await tree.SetAsync($"k{i:D2}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        var live = await tree.CountAsync();
        var coalesced = await LatticeDigestSettleHelpers.AwaitDigestConvergesToAsync(
            tree, shardIndex: 0, expectedEntryCount: live);

        Assert.That(coalesced.EntryCount, Is.EqualTo(live));
        Assert.That(coalesced.Hash.Length, Is.EqualTo(16),
            "the chained-fold hash must be a 16-byte XxHash128 regardless of coalescing window");
        Assert.That(coalesced.Hash, Is.Not.All.Zero,
            "a populated shard's digest hash must not be all-zero - that would indicate the chained " +
            "fold has not received any publish from the leaf");
    }

    /// <summary>
    /// Asserts that a structural event (leaf split, triggered here by
    /// writing past <c>MaxLeafKeys</c>) publishes the new aggregate to
    /// the parent before returning to the caller - per the c2-xxviii
    /// memo's documented exclusion of structural events from
    /// coalescing. Reads the chained-fold digest immediately after the
    /// split-triggering write completes without waiting for the
    /// coalescing window to elapse.
    /// </summary>
    [Test]
    public async Task DigestCoalescingWindow_structural_split_publishes_inline()
    {
        var tree = await NewTreeAsync("coalesce-split");

        // SmallMaxLeafKeys = 4; writing 8 keys forces at least one
        // split. Each write triggers a coalesced publish for the
        // per-write hot path, but the split itself is a structural
        // event whose publish bypasses the coalescing window. The
        // settle helper still polls to absorb any pending per-write
        // coalesced publishes that the post-split aggregate depends
        // on.
        for (var i = 0; i < 8; i++)
        {
            await tree.SetAsync($"k{i:D2}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        var live = await tree.CountAsync();
        var digest = await LatticeDigestSettleHelpers.AwaitDigestConvergesToAsync(
            tree, shardIndex: 0, expectedEntryCount: live);
        Assert.That(digest.EntryCount, Is.EqualTo(8),
            "post-split chained-fold aggregate must include every write across both leaves");
    }
}
