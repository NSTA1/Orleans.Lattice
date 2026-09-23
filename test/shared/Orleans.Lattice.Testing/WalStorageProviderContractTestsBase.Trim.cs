using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Trim and compaction half of <see cref="WalStorageProviderContractTestsBase"/>.
/// The high-water-mark side of trim lives in
/// <see cref="WalOffsetAllocationContractTestsBase"/>; this half pins the live
/// range trim leaves behind.
/// </summary>
public abstract partial class WalStorageProviderContractTestsBase
{
    [Test]
    public async Task Lowest_offset_is_minus_one_before_any_append()
    {
        await using var probe = await CreateProbeAsync();

        Assert.That(await probe.GetLowestOffsetAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(-1L));
    }

    [Test]
    public async Task Lowest_offset_of_an_untrimmed_shard_is_its_first_entry()
    {
        await using var probe = await CreateProbeAsync();
        await probe.AppendAsync(TreeId, Shard, Entries(0, 3), CancellationToken.None);

        Assert.That(await probe.GetLowestOffsetAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(0L));
    }

    [Test]
    public async Task Trim_is_inclusive_and_the_lowest_offset_moves_to_the_first_retained_entry()
    {
        await using var probe = await CreateProbeAsync();
        var entries = Entries(0, 6);
        await probe.AppendAsync(TreeId, Shard, entries[..3], CancellationToken.None);
        await probe.AppendAsync(TreeId, Shard, entries[3..], CancellationToken.None);

        await probe.TrimAsync(TreeId, Shard, 2L, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(Describe(await ReadAllAsync(probe)), Is.EqualTo(Describe(entries[3..])));
            Assert.That(await probe.GetLowestOffsetAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(3L));
            Assert.That(await probe.GetHighestOffsetAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(5L));
        });
    }

    [Test]
    public async Task A_trimmed_prefix_stays_trimmed_across_a_reopen()
    {
        await using var probe = await CreateProbeAsync();
        var entries = Entries(0, 6);
        await probe.AppendAsync(TreeId, Shard, entries[..3], CancellationToken.None);
        await probe.AppendAsync(TreeId, Shard, entries[3..], CancellationToken.None);
        await probe.TrimAsync(TreeId, Shard, 2L, CancellationToken.None);

        await probe.ReopenAsync(CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(Describe(await ReadAllAsync(probe)), Is.EqualTo(Describe(entries[3..])));
            Assert.That(await probe.GetLowestOffsetAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(3L));
        });
    }

    [Test]
    public async Task Trim_below_the_current_floor_is_a_no_op()
    {
        await using var probe = await CreateProbeAsync();
        var entries = Entries(0, 6);
        await probe.AppendAsync(TreeId, Shard, entries[..3], CancellationToken.None);
        await probe.AppendAsync(TreeId, Shard, entries[3..], CancellationToken.None);
        await probe.TrimAsync(TreeId, Shard, 2L, CancellationToken.None);

        await probe.TrimAsync(TreeId, Shard, 2L, CancellationToken.None);
        await probe.TrimAsync(TreeId, Shard, 0L, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(Describe(await ReadAllAsync(probe)), Is.EqualTo(Describe(entries[3..])));
            Assert.That(await probe.GetLowestOffsetAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(3L));
        });
    }

    // Deliberately NOT asserted: that a trim beyond the tail "reserves the trim
    // point for a future append" by suppressing entries later appended at or
    // below it. The interface permits such a trim, and FileWalShard persists the
    // floor unconditionally, but AzureTableWalStorageProvider (the reference)
    // only deletes rows that exist and records no floor. The permitted half is
    // pinned below; the reservation half would be the suite inventing a rule.

    [Test]
    public async Task Trim_beyond_the_tail_is_permitted_and_empties_the_shard()
    {
        await using var probe = await CreateProbeAsync();
        await probe.AppendAsync(TreeId, Shard, Entries(0, 3), CancellationToken.None);

        await probe.TrimAsync(TreeId, Shard, 50L, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await ReadAllAsync(probe), Is.Empty);
            Assert.That(await probe.GetLowestOffsetAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(-1L));
            Assert.That(await probe.GetHighestOffsetAsync(TreeId, Shard, CancellationToken.None), Is.GreaterThanOrEqualTo(2L));
        });
    }

    /// <summary>
    /// <c>EvaluateCompactionAsync</c> carries no watermark and must leave the
    /// shard's logical contents identical, whether or not the provider chooses
    /// to reclaim anything.
    /// </summary>
    [Test]
    public async Task Evaluate_compaction_leaves_logical_contents_unchanged()
    {
        await using var probe = await CreateProbeAsync();
        for (var batch = 0; batch < 8; batch++)
        {
            await probe.AppendAsync(TreeId, Shard, Entries(batch * 4L, 4), CancellationToken.None);
        }

        await probe.TrimAsync(TreeId, Shard, 23L, CancellationToken.None);
        var before = Describe(await ReadAllAsync(probe));
        var lowest = await probe.GetLowestOffsetAsync(TreeId, Shard, CancellationToken.None);
        var highest = await probe.GetHighestOffsetAsync(TreeId, Shard, CancellationToken.None);

        await probe.EvaluateCompactionAsync(TreeId, Shard, CancellationToken.None);
        await probe.EvaluateCompactionAsync(TreeId, Shard, CancellationToken.None);

        await AssertLogicalContentsAsync(probe, before, lowest, highest, "after compaction");

        await probe.ReopenAsync(CancellationToken.None);

        await AssertLogicalContentsAsync(probe, before, lowest, highest, "after compaction and a reopen");
    }

    [Test]
    public async Task Evaluate_compaction_on_a_never_written_shard_is_harmless()
    {
        await using var probe = await CreateProbeAsync();

        await probe.EvaluateCompactionAsync(TreeId, Shard, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await ReadAllAsync(probe), Is.Empty);
            Assert.That(await probe.GetHighestOffsetAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(-1L));
        });
    }

    private static async Task AssertLogicalContentsAsync(
        IWalStorageProviderContractProbe probe,
        string[] entries,
        long lowest,
        long highest,
        string when)
    {
        Assert.Multiple(async () =>
        {
            Assert.That(Describe(await ReadAllAsync(probe)), Is.EqualTo(entries), $"Entries must be unchanged {when}.");
            Assert.That(await probe.GetLowestOffsetAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(lowest), $"Lowest offset must be unchanged {when}.");
            Assert.That(await probe.GetHighestOffsetAsync(TreeId, Shard, CancellationToken.None), Is.EqualTo(highest), $"Highest offset must be unchanged {when}.");
        });
    }
}
