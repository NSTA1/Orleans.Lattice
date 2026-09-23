using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Reusable conformance suite for the whole <c>IWalStorageProvider</c> seam.
/// <para>
/// <see cref="WalOffsetAllocationContractTestsBase"/> pins the two properties
/// that sit on the WAL grain's offset-allocation path: a monotonic high-water
/// mark, and a reconcile that never loses an acknowledged append. This suite
/// pins the rest of what the interface documents, which until now each
/// provider's own tests covered unevenly or not at all:
/// </para>
/// <list type="bullet">
/// <item>append atomicity, overlap rejection, out-of-order arrival, and gaps;</item>
/// <item>verbatim payloads across both the entry-shaped and pre-encoded paths;</item>
/// <item>read bounds, and entry/encoded read equivalence;</item>
/// <item>lowest offset, trim inclusivity and idempotence;</item>
/// <item>compaction leaving logical contents unchanged;</item>
/// <item>retained and physical byte accounting;</item>
/// <item>tree and shard isolation, durability across a restart;</item>
/// <item>null tree id rejection and pre-cancellation.</item>
/// </list>
/// <para>
/// The base is <see langword="abstract"/> so it is never discovered on its own;
/// the inherited <c>[Test]</c>s run through the concrete subclass in each
/// provider's test assembly.
/// </para>
/// </summary>
public abstract partial class WalStorageProviderContractTestsBase
{
    /// <summary>Tree id most tests write to.</summary>
    protected const string TreeId = "tree-provider-contract";

    /// <summary>A second tree id, for isolation tests.</summary>
    protected const string OtherTreeId = "tree-provider-contract-other";

    /// <summary>Shard index most tests write to.</summary>
    protected const int Shard = 0;

    private const int ReadAll = 1024;

    /// <summary>
    /// Creates a probe over fresh, empty storage. Each test disposes its own
    /// probe, so implementations may allocate per-test scratch storage.
    /// </summary>
    protected abstract Task<IWalStorageProviderContractProbe> CreateProbeAsync();

    [Test]
    public async Task Append_round_trips_offsets_keys_and_values_verbatim()
    {
        await using var probe = await CreateProbeAsync();
        var entries = Entries(0, 4);

        await probe.AppendAsync(TreeId, Shard, entries, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(Describe(await ReadAllAsync(probe)), Is.EqualTo(Describe(entries)));
            Assert.That(await probe.GetHighestOffsetAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(3L));
        });
    }

    [Test]
    public async Task Encoded_append_is_equivalent_to_entry_append()
    {
        await using var probe = await CreateProbeAsync();
        var entries = Entries(0, 4);

        await probe.AppendEncodedAsync(TreeId, Shard, entries, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(Describe(await ReadAllAsync(probe)), Is.EqualTo(Describe(entries)));
            Assert.That(await probe.GetHighestOffsetAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(3L));
        });
    }

    /// <summary>
    /// The interface requires a provider to reject overlap with any persisted
    /// offset and makes an append all-or-nothing, so a batch that straddles the
    /// tail must fail as a whole: neither its fresh offset nor its overlapping
    /// one may replace or join what is already stored.
    /// </summary>
    [Test]
    public async Task Append_overlapping_a_persisted_offset_is_rejected_and_keeps_none_of_the_batch()
    {
        await using var probe = await CreateProbeAsync();
        var original = Entries(0, 3);
        await probe.AppendAsync(TreeId, Shard, original, CancellationToken.None);

        var overlapping = Entries(2, 2, salt: 0xEE);
        Assert.That(
            async () => await probe.AppendAsync(TreeId, Shard, overlapping, CancellationToken.None),
            Throws.Exception,
            "An append overlapping a persisted offset must be rejected.");

        await probe.ReopenAsync(CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(
                Describe(await ReadAllAsync(probe)),
                Is.EqualTo(Describe(original)),
                "The rejected batch must leave no entry behind and must not overwrite the persisted one.");
            Assert.That(await probe.GetHighestOffsetAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(2L));
        });
    }

    /// <summary>
    /// The narrow half of overlap rejection: re-appending a batch that is
    /// already persisted, which is what an ambiguous-failure retry looks like,
    /// must fail and must not replace the stored values.
    /// </summary>
    [Test]
    public async Task Replaying_a_persisted_batch_is_rejected_and_keeps_the_original_values()
    {
        await using var probe = await CreateProbeAsync();
        var original = Entries(0, 3);
        await probe.AppendAsync(TreeId, Shard, original, CancellationToken.None);

        Assert.That(
            async () => await probe.AppendAsync(TreeId, Shard, Entries(0, 3, salt: 0xEE), CancellationToken.None),
            Throws.Exception,
            "Re-appending persisted offsets must be rejected.");

        await probe.ReopenAsync(CancellationToken.None);

        Assert.That(Describe(await ReadAllAsync(probe)), Is.EqualTo(Describe(original)));
    }

    /// <summary>
    /// With more than one pending batch per shard, batches can reach the
    /// provider out of order. The interface requires offsets to be kept
    /// verbatim without assuming contiguity with the persisted tail.
    /// </summary>
    [Test]
    public async Task Batches_arriving_out_of_order_read_back_in_offset_order()
    {
        await using var probe = await CreateProbeAsync();
        var later = Entries(4, 4);
        var earlier = Entries(0, 4);

        await probe.AppendAsync(TreeId, Shard, later, CancellationToken.None);
        await probe.AppendAsync(TreeId, Shard, earlier, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(Describe(await ReadAllAsync(probe)), Is.EqualTo(Describe(earlier.Concat(later))));
            Assert.That(await probe.GetHighestOffsetAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(7L));
            Assert.That(await probe.GetLowestOffsetAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(0L));
        });
    }

    /// <summary>
    /// A failed flush may leave a permanent gap, which consumers must observe
    /// honestly rather than having it papered over or treated as the tail.
    /// </summary>
    [Test]
    public async Task A_permanent_gap_is_read_honestly_and_does_not_cap_the_tail()
    {
        await using var probe = await CreateProbeAsync();
        var head = Entries(0, 2);
        var afterGap = Entries(4, 2);

        await probe.AppendAsync(TreeId, Shard, head, CancellationToken.None);
        await probe.AppendAsync(TreeId, Shard, afterGap, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(Offsets(await ReadAllAsync(probe)), Is.EqualTo(new[] { 0L, 1L, 4L, 5L }));
            Assert.That(await probe.GetHighestOffsetAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(5L));
        });
    }

    [Test]
    public async Task Appended_entries_survive_a_reopen()
    {
        await using var probe = await CreateProbeAsync();
        var first = Entries(0, 3);
        var second = Entries(3, 3);
        await probe.AppendAsync(TreeId, Shard, first, CancellationToken.None);
        await probe.AppendEncodedAsync(TreeId, Shard, second, CancellationToken.None);

        await probe.ReopenAsync(CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(Describe(await ReadAllAsync(probe)), Is.EqualTo(Describe(first.Concat(second))));
            Assert.That(await probe.GetHighestOffsetAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(5L));
            Assert.That(await probe.GetLowestOffsetAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(0L));
        });
    }

    [Test]
    public async Task Shards_and_trees_are_isolated()
    {
        await using var probe = await CreateProbeAsync();
        await probe.AppendAsync(TreeId, Shard, Entries(0, 3), CancellationToken.None);

        await probe.TrimAsync(TreeId, Shard + 1, 10L, CancellationToken.None);
        await probe.TrimAsync(OtherTreeId, Shard, 10L, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(Offsets(await ReadAllAsync(probe)), Is.EqualTo(new[] { 0L, 1L, 2L }), "A trim of another shard or tree must not touch this one.");
            Assert.That(await ReadAllAsync(probe, TreeId, Shard + 1), Is.Empty);
            Assert.That(await ReadAllAsync(probe, OtherTreeId, Shard), Is.Empty);
            Assert.That(await probe.GetLowestOffsetAsync(TreeId, Shard + 1, CancellationToken.None), Is.EqualTo(-1L));
            Assert.That(await probe.GetLowestOffsetAsync(OtherTreeId, Shard, CancellationToken.None), Is.EqualTo(-1L));
        });
    }

    /// <summary>
    /// Builds <paramref name="count"/> dense entries from
    /// <paramref name="first"/>. <paramref name="salt"/> varies the value so a
    /// test can tell a replacement from the original.
    /// </summary>
    protected static WalContractEntry[] Entries(long first, int count, byte salt = 0) =>
        Enumerable.Range(0, count)
            .Select(i => first + i)
            .Select(offset => new WalContractEntry(offset, $"k{offset}", [(byte)(offset & 0xFF), salt, 0x5A]))
            .ToArray();

    /// <summary>Value-shaped renderings for comparison.</summary>
    protected static string[] Describe(IEnumerable<WalContractEntry> entries) =>
        entries.Select(static e => e.ToString()).ToArray();

    /// <summary>The offsets of <paramref name="entries"/>, in order.</summary>
    protected static long[] Offsets(IEnumerable<WalContractEntry> entries) =>
        entries.Select(static e => e.Offset).ToArray();

    /// <summary>Reads the whole log for one shard from its head.</summary>
    protected static Task<IReadOnlyList<WalContractEntry>> ReadAllAsync(
        IWalStorageProviderContractProbe probe,
        string treeId = TreeId,
        int shardIndex = Shard) =>
        probe.ReadAsync(treeId, shardIndex, -1L, ReadAll, CancellationToken.None);
}
