using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Guards the two leaf range-read trims that removed work the scan had already
/// done.
/// <para>
/// The first replaced up to five per-row ordinal comparisons with at most one.
/// <c>CountAsync</c>, <c>GetKeysAsync</c> and <c>GetEntriesAsync</c> fold every
/// bound into a half-open window before enumerating, so re-testing
/// <c>startInclusive</c>, <c>endExclusive</c> and <c>beforeExclusive</c> per row
/// could not change an answer. Only
/// <c>afterExclusive</c> survives, because a lower bound is inclusive and the
/// one row equal to it is admitted by the window. These tests pin every bound
/// combination against an independent oracle so a future edit cannot quietly
/// drop the surviving guard or re-introduce a redundant one that disagrees.
/// (The in-flight split key used to be folded in here too; issue #3918 removed
/// that bound outright, because a row the donor still holds is one the sibling
/// has not taken.)
/// </para>
/// <para>
/// The second made the terminal re-sort conditional. The windowed scan emits in
/// ascending ordinal order, so the only way a result can be unsorted is the tail
/// that appends fresh committed pending keys - which these tests force, on keys
/// chosen to sort before the scan's output, so a skipped sort would be visible
/// as an out-of-order result rather than as an invisible fast path.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly string[] BoundProbeKeys =
        [.. Enumerable.Range(0, 12).Select(i => $"k{i:D2}")];

    [TearDown]
    public void ClearRangeBoundHoistAmbientContext()
    {
        LatticeTransactionContext.Set(Guid.Empty);
        LatticeRegistrySnapshotContext.Current = null;
    }

    private static async Task<BPlusLeafGrain> CreateBoundProbeLeafAsync()
    {
        var grain = CreateGrain(maxLeafKeys: 1024);
        foreach (var key in BoundProbeKeys) await grain.SetAsync(key, [1]);
        return grain;
    }

    private static IEnumerable<string> ExpectedKeys(
        string? startInclusive, string? endExclusive, string? afterExclusive, string? beforeExclusive)
        => BoundProbeKeys
            .Where(key =>
                (startInclusive is null || string.CompareOrdinal(key, startInclusive) >= 0)
                && (endExclusive is null || string.CompareOrdinal(key, endExclusive) < 0)
                && (afterExclusive is null || string.CompareOrdinal(key, afterExclusive) > 0)
                && (beforeExclusive is null || string.CompareOrdinal(key, beforeExclusive) < 0))
            .OrderBy(key => key, StringComparer.Ordinal);

    // Every bound null, each bound alone, both ends together, the exclusive
    // pair together, a bound landing exactly on a present key, one landing
    // between keys, and an empty window.
    private static readonly (string? Start, string? End, string? After, string? Before)[] BoundCases =
    [
        (null, null, null, null),
        ("k03", null, null, null),
        (null, "k07", null, null),
        (null, null, "k03", null),
        (null, null, null, "k07"),
        ("k03", "k07", null, null),
        (null, null, "k03", "k07"),
        ("k03", "k07", "k04", "k06"),
        // afterExclusive exactly on the lower bound: the one case the window
        // cannot express, because the range admits the key equal to its start.
        ("k03", "k07", "k03", null),
        // afterExclusive above startInclusive, so it is the effective lower bound.
        ("k01", null, "k05", null),
        // Bounds between keys, so no bound lands on a present key.
        ("k03x", "k07x", null, null),
        // Empty and inverted windows.
        ("k07", "k03", null, null),
        ("zzz", null, null, null),
        (null, "aaa", null, null),
    ];

    [Test]
    public async Task GetKeysAsync_matches_the_bound_oracle_for_every_bound_combination()
    {
        var grain = await CreateBoundProbeLeafAsync();

        foreach (var (start, end, after, before) in BoundCases)
        {
            var actual = await grain.GetKeysAsync(start, end, after, before);
            Assert.That(
                actual,
                Is.EqualTo(ExpectedKeys(start, end, after, before).ToList()),
                $"start='{start}' end='{end}' after='{after}' before='{before}'");
        }
    }

    [Test]
    public async Task GetEntriesAsync_matches_the_bound_oracle_for_every_bound_combination()
    {
        var grain = await CreateBoundProbeLeafAsync();

        foreach (var (start, end, after, before) in BoundCases)
        {
            var actual = await grain.GetEntriesAsync(start, end, after, before);
            Assert.That(
                actual.Select(entry => entry.Key),
                Is.EqualTo(ExpectedKeys(start, end, after, before).ToList()),
                $"start='{start}' end='{end}' after='{after}' before='{before}'");
        }
    }

    [Test]
    public async Task CountAsync_matches_the_bound_oracle_for_every_range()
    {
        var grain = await CreateBoundProbeLeafAsync();

        foreach (var (start, end, _, _) in BoundCases)
        {
            var actual = await grain.CountAsync(start, end);
            Assert.That(
                actual,
                Is.EqualTo(ExpectedKeys(start, end, null, null).Count()),
                $"start='{start}' end='{end}'");
        }
    }

    [Test]
    public async Task Range_reads_exclude_the_key_equal_to_afterExclusive()
    {
        var grain = await CreateBoundProbeLeafAsync();

        // The window's lower bound is inclusive, so "k05" is admitted by the
        // range itself and only the surviving per-row guard can reject it.
        Assert.Multiple(async () =>
        {
            Assert.That(await grain.GetKeysAsync(afterExclusive: "k05"), Does.Not.Contain("k05"));
            Assert.That(
                (await grain.GetEntriesAsync(afterExclusive: "k05")).Select(entry => entry.Key),
                Does.Not.Contain("k05"));
            Assert.That(await grain.GetKeysAsync(startInclusive: "k05"), Does.Contain("k05"));
        });
    }

    [Test]
    public async Task Range_reads_stay_ordinally_sorted_when_a_pending_key_appends_out_of_order()
    {
        var grain = CreateGrain(maxLeafKeys: 1024);
        foreach (var key in new[] { "k20", "k30", "k40" }) await grain.SetAsync(key, [1]);

        // A prepared write on a key absent from the cache: it can only reach the
        // result through the tail loop, which appends in dictionary order. "k10"
        // sorts before every committed key, so a skipped sort is observable.
        var txid = Guid.NewGuid();
        LatticeTransactionContext.Set(txid);
        try
        {
            using (LatticePreparedContext.BeginScope()) await grain.SetAsync("k10", [2]);
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }

        var snapshot = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };
        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var keys = await grain.GetKeysAsync();
            Assert.That(keys, Is.EqualTo(new[] { "k10", "k20", "k30", "k40" }));

            var entries = await grain.GetEntriesAsync();
            Assert.That(
                entries.Select(entry => entry.Key),
                Is.EqualTo(new[] { "k10", "k20", "k30", "k40" }));
        }
    }

    [Test]
    public async Task Range_reads_stay_sorted_when_no_pending_key_appends()
    {
        var grain = await CreateBoundProbeLeafAsync();

        var keys = await grain.GetKeysAsync();
        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EqualTo(BoundProbeKeys));
            Assert.That(keys, Is.Ordered.Using<string>(StringComparer.Ordinal));
        });
    }
}
