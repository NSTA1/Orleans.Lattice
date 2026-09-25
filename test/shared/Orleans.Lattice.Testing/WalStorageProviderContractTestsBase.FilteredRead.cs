using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Filtered-read half of <see cref="WalStorageProviderContractTestsBase"/>
/// (issue #3565): every provider, whether it overrides the filtered read or
/// inherits the interface default, must return the same entries for the same
/// window and filter.
/// </summary>
/// <remarks>
/// Keys are <c>k{offset}</c>, so a window of single-digit offsets orders
/// ordinally the same way it orders numerically and a range such as
/// <c>[k2, k4)</c> owns exactly offsets 2 and 3.
/// </remarks>
public abstract partial class WalStorageProviderContractTestsBase
{
    [Test]
    public async Task Filtered_read_delivers_owned_entries_and_the_last_examined_entry_routing_only()
    {
        await using var probe = await CreateProbeAsync();
        var entries = Entries(0, 6);
        await probe.AppendAsync(TreeId, Shard, entries, CancellationToken.None);

        var read = await probe.ReadFilteredAsync(TreeId, Shard, -1L, 5L, 1024, "k2", "k4", CancellationToken.None);

        Assert.That(
            Describe(read),
            Is.EqualTo(Describe([entries[2], entries[3], new WalContractEntry(5, "k5", [])])),
            "Offsets 2 and 3 arrive in full; offset 5 is the last examined entry and arrives routing-only.");
    }

    [Test]
    public async Task Filtered_read_counts_excluded_entries_against_max_entries()
    {
        await using var probe = await CreateProbeAsync();
        await probe.AppendAsync(TreeId, Shard, Entries(0, 6), CancellationToken.None);

        var read = await probe.ReadFilteredAsync(TreeId, Shard, -1L, 5L, 2, "k4", "k6", CancellationToken.None);

        Assert.That(
            Describe(read),
            Is.EqualTo(Describe([new WalContractEntry(1, "k1", [])])),
            "Two entries are examined, both excluded, so the read reports how far it got and stops.");
    }

    [Test]
    public async Task Filtered_read_examines_nothing_past_its_inclusive_upper_bound()
    {
        await using var probe = await CreateProbeAsync();
        var entries = Entries(0, 6);
        await probe.AppendAsync(TreeId, Shard, entries, CancellationToken.None);

        var read = await probe.ReadFilteredAsync(TreeId, Shard, 0L, 3L, 1024, "k2", "k6", CancellationToken.None);

        Assert.That(Describe(read), Is.EqualTo(Describe([entries[2], entries[3]])));
    }

    [Test]
    public async Task Filtered_read_with_an_unbounded_range_matches_the_entry_read_over_the_window()
    {
        await using var probe = await CreateProbeAsync();
        await probe.AppendAsync(TreeId, Shard, Entries(0, 3), CancellationToken.None);
        await probe.AppendEncodedAsync(TreeId, Shard, Entries(3, 3), CancellationToken.None);

        var filtered = await probe.ReadFilteredAsync(TreeId, Shard, 0L, 4L, 1024, null, null, CancellationToken.None);
        var entries = await probe.ReadAsync(TreeId, Shard, 0L, 4, CancellationToken.None);

        Assert.That(Describe(filtered), Is.EqualTo(Describe(entries)));
    }

    [Test]
    public async Task Filtered_read_classifies_encoded_and_entry_appends_alike()
    {
        await using var probe = await CreateProbeAsync();
        var head = Entries(0, 3);
        var tail = Entries(3, 3);
        await probe.AppendAsync(TreeId, Shard, head, CancellationToken.None);
        await probe.AppendEncodedAsync(TreeId, Shard, tail, CancellationToken.None);

        var read = await probe.ReadFilteredAsync(TreeId, Shard, -1L, 5L, 1024, "k1", "k5", CancellationToken.None);

        Assert.That(
            Describe(read),
            Is.EqualTo(Describe([head[1], head[2], tail[0], tail[1], new WalContractEntry(5, "k5", [])])),
            "The pre-encoded path strips the tree id from each record; classifying it must not depend on that.");
    }

    [Test]
    public async Task Filtered_read_of_an_empty_window_is_empty()
    {
        await using var probe = await CreateProbeAsync();
        await probe.AppendAsync(TreeId, Shard, Entries(0, 3), CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await probe.ReadFilteredAsync(TreeId, Shard, 2L, 10L, 1024, "k0", "k9", CancellationToken.None), Is.Empty,
                "A window past the tail holds nothing.");
            Assert.That(await probe.ReadFilteredAsync(TreeId, Shard, 1L, 1L, 1024, "k0", "k9", CancellationToken.None), Is.Empty,
                "An inverted window holds nothing.");
            Assert.That(await probe.ReadFilteredAsync(TreeId, Shard + 1, -1L, 10L, 1024, "k0", "k9", CancellationToken.None), Is.Empty,
                "A never-written shard holds nothing.");
        });
    }

    /// <summary>
    /// The replay idiom: read a filtered page, then read again from the last
    /// offset it returned. It must reach every owned entry exactly once and walk
    /// the whole window, however little of it is owned.
    /// </summary>
    [Test]
    public async Task Filtered_paging_from_the_last_offset_seen_visits_every_owned_entry_once()
    {
        await using var probe = await CreateProbeAsync();
        var entries = Entries(0, 10);
        await probe.AppendAsync(TreeId, Shard, entries, CancellationToken.None);

        var owned = new List<WalContractEntry>();
        var from = -1L;
        var pages = 0;
        for (; pages < 100; pages++)
        {
            var page = await probe.ReadFilteredAsync(TreeId, Shard, from, 9L, 3, "k3", "k6", CancellationToken.None);
            if (page.Count == 0)
            {
                break;
            }

            owned.AddRange(page.Where(e => e.Value.Length > 0));
            from = page[^1].Offset;
        }

        Assert.Multiple(() =>
        {
            Assert.That(Describe(owned), Is.EqualTo(Describe(entries[3..6])));
            Assert.That(from, Is.EqualTo(9L), "The walk must reach the end of the window.");
            Assert.That(pages, Is.EqualTo(4), "Ten entries at three examined per page is four pages.");
        });
    }
}
