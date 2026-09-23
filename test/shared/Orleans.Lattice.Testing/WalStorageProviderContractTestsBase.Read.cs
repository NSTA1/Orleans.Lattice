using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>Read-path half of <see cref="WalStorageProviderContractTestsBase"/>.</summary>
public abstract partial class WalStorageProviderContractTestsBase
{
    [Test]
    public async Task Read_is_strictly_exclusive_of_its_lower_bound()
    {
        await using var probe = await CreateProbeAsync();
        await probe.AppendAsync(TreeId, Shard, Entries(0, 6), CancellationToken.None);

        var read = await probe.ReadAsync(TreeId, Shard, 2L, 1024, CancellationToken.None);

        Assert.That(Offsets(read), Is.EqualTo(new[] { 3L, 4L, 5L }));
    }

    [Test]
    public async Task Read_yields_at_most_max_entries_from_the_lower_bound()
    {
        await using var probe = await CreateProbeAsync();
        await probe.AppendAsync(TreeId, Shard, Entries(0, 6), CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(Offsets(await probe.ReadAsync(TreeId, Shard, -1L, 2, CancellationToken.None)), Is.EqualTo(new[] { 0L, 1L }));
            Assert.That(Offsets(await probe.ReadAsync(TreeId, Shard, 1L, 3, CancellationToken.None)), Is.EqualTo(new[] { 2L, 3L, 4L }));
            Assert.That(Offsets(await probe.ReadAsync(TreeId, Shard, -1L, 1, CancellationToken.None)), Is.EqualTo(new[] { 0L }));
        });
    }

    /// <summary>
    /// The paging idiom every consumer uses: read a page, then read again from
    /// the last offset seen. It must visit each entry exactly once.
    /// </summary>
    [Test]
    public async Task Paging_from_the_last_offset_seen_visits_every_entry_once()
    {
        await using var probe = await CreateProbeAsync();
        var entries = Entries(0, 3).Concat(Entries(3, 4)).Concat(Entries(9, 3)).ToArray();
        await probe.AppendAsync(TreeId, Shard, entries[..3], CancellationToken.None);
        await probe.AppendAsync(TreeId, Shard, entries[3..7], CancellationToken.None);
        await probe.AppendAsync(TreeId, Shard, entries[7..], CancellationToken.None);

        var seen = new List<WalContractEntry>();
        var from = -1L;
        for (var guard = 0; guard < 100; guard++)
        {
            var page = await probe.ReadAsync(TreeId, Shard, from, 2, CancellationToken.None);
            if (page.Count == 0)
            {
                break;
            }

            seen.AddRange(page);
            from = page[^1].Offset;
        }

        Assert.That(Describe(seen), Is.EqualTo(Describe(entries)));
    }

    [Test]
    public async Task Read_past_the_tail_is_empty()
    {
        await using var probe = await CreateProbeAsync();
        await probe.AppendAsync(TreeId, Shard, Entries(0, 3), CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await probe.ReadAsync(TreeId, Shard, 2L, 1024, CancellationToken.None), Is.Empty);
            Assert.That(await probe.ReadAsync(TreeId, Shard, 50L, 1024, CancellationToken.None), Is.Empty);
        });
    }

    [Test]
    public async Task Read_of_a_never_written_shard_is_empty()
    {
        await using var probe = await CreateProbeAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(await ReadAllAsync(probe), Is.Empty);
            var page = await probe.ReadEncodedAsync(TreeId, Shard, -1L, 1024, CancellationToken.None);
            Assert.That(page.Entries, Is.Empty);
            Assert.That(page.HighestOffsetInclusive, Is.EqualTo(-1L), "An empty encoded page reports -1.");
        });
    }

    /// <summary>
    /// The bytes-shaped read must return exactly the entries the entry-shaped
    /// read yields for the same bounds, whether the provider overrides it or
    /// uses the interface default.
    /// </summary>
    [TestCase(-1L, 1024)]
    [TestCase(1L, 1024)]
    [TestCase(-1L, 2)]
    [TestCase(3L, 3)]
    public async Task Encoded_read_matches_entry_read(long fromOffsetExclusive, int maxEntries)
    {
        await using var probe = await CreateProbeAsync();
        await probe.AppendAsync(TreeId, Shard, Entries(0, 3), CancellationToken.None);
        await probe.AppendEncodedAsync(TreeId, Shard, Entries(3, 4), CancellationToken.None);

        var entries = await probe.ReadAsync(TreeId, Shard, fromOffsetExclusive, maxEntries, CancellationToken.None);
        var page = await probe.ReadEncodedAsync(TreeId, Shard, fromOffsetExclusive, maxEntries, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(Describe(page.Entries), Is.EqualTo(Describe(entries)));
            Assert.That(page.HighestOffsetInclusive, Is.EqualTo(entries.Count == 0 ? -1L : entries[^1].Offset));
        });
    }
}
