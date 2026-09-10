using NSubstitute;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Coverage for the progress-replenished stall budget (issue 2539).
/// <para>
/// The stall resume budget was monotonic: it was declared once outside the
/// reopen loop and only ever incremented, never reset when the walk delivered
/// records. It therefore capped the <em>total</em> faults a walk could absorb
/// over its whole life rather than the <em>futile</em> ones, so a walk whose
/// range grows with the corpus died at its third stall no matter how much
/// progress it had banked in between. These tests pin that progress now
/// replenishes the budget, and - just as importantly - that a walk which makes
/// no progress is still governed exactly as it was.
/// </para>
/// </summary>
public partial class ResilientScanExtensionsTests
{
    [Test]
    public async Task ScanKeysAsync_replenishes_the_stall_budget_on_every_yielded_key()
    {
        // Ten stalls against a budget of two. Each page delivers one key before
        // stalling, so every stall is preceded by real progress and the walk is
        // strictly converging. Under a monotonic budget this dies on the third
        // stall having banked two keys.
        const int pages = 10;
        var lattice = Substitute.For<ILattice>();
        var callIndex = 0;
        lattice.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var index = callIndex++;
                return index < pages
                    ? StalledKeys(new[] { $"k{index:D2}" }, stallAfter: 1)
                    : ScriptedKeys(Array.Empty<string>(), abortAfter: int.MaxValue);
            });

        var keys = await CollectAsync(lattice.ScanKeysAsync());

        Assert.That(keys, Has.Count.EqualTo(pages), "a walk that progresses between stalls must converge");
        Assert.That(keys[0], Is.EqualTo("k00"));
        Assert.That(keys[^1], Is.EqualTo($"k{pages - 1:D2}"));
    }

    [Test]
    public async Task ScanEntriesAsync_replenishes_the_stall_budget_on_every_yielded_entry()
    {
        const int pages = 10;
        var lattice = Substitute.For<ILattice>();
        var callIndex = 0;
        lattice.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var index = callIndex++;
                return index < pages
                    ? StalledEntries(new[] { ($"k{index:D2}", index) }, stallAfter: 1)
                    : StalledEntries(Array.Empty<(string, int)>(), stallAfter: int.MaxValue);
            });

        var entries = await CollectAsync(lattice.ScanEntriesAsync());

        Assert.That(entries, Has.Count.EqualTo(pages), "a walk that progresses between stalls must converge");
        Assert.That(entries[^1].Key, Is.EqualTo($"k{pages - 1:D2}"));
    }

    [Test]
    public void ScanKeysAsync_still_exhausts_the_budget_when_progress_stops()
    {
        // The replenishment is the inverse of the progress GATE that issue 2456
        // measured and removed: progress restores budget, it never refuses a
        // resume. A walk that stops advancing must therefore still die.
        //
        // The scripting is deliberate. The first page stalls at the origin so a
        // resume is genuinely SPENT before any progress happens; only then does
        // a page deliver a key. Without the reset that second stall is the
        // budget's last and the walk dies one call earlier, so the call count
        // discriminates the fix rather than merely tolerating it.
        var lattice = Substitute.For<ILattice>();
        var calls = 0;
        lattice.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var index = calls++;
                return index == 1
                    ? StalledKeys(new[] { "a" }, stallAfter: 1)
                    : StalledKeys(Array.Empty<string>(), stallAfter: 0);
            });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await CollectAsync(lattice.ScanKeysAsync()));

        // One spent resume, then a productive page that buys it back, then the
        // full budget in consecutive futile stalls. The single yielded key
        // replenishes the budget once; it does not make the walk immortal.
        Assert.That(
            calls,
            Is.EqualTo(2 + LatticeExtensions.DefaultScanStallResumeAttempts),
            "progress replenishes the budget once; it does not make the walk immortal");
    }

    [Test]
    public void ScanKeysAsync_stops_at_the_total_ceiling_even_while_it_is_progressing()
    {
        // Replenishment removes the length cap, so something must still bound a
        // walk that dribbles one record per stall forever - otherwise the scan
        // never returns, the caller's next reminder tick never fires, and the
        // failure is never surfaced at all. That is strictly worse than failing,
        // which is why the ceiling exists.
        var lattice = Substitute.For<ILattice>();
        var calls = 0;
        var next = 0;
        lattice.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                calls++;
                return StalledKeys(new[] { $"k{next++:D4}" }, stallAfter: 1);
            });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await CollectAsync(lattice.ScanKeysAsync()));

        Assert.That(
            calls,
            Is.EqualTo(1 + LatticeExtensions.DefaultScanStallResumeCeiling),
            "an endlessly dribbling source must terminate at the total ceiling");
    }

    [Test]
    public void ScanKeysAsync_origin_parked_walk_is_governed_exactly_as_before()
    {
        // The cold-replay population issue 2278 measured stalls at or near the
        // origin, banking nothing. Replenishment must be inert for it.
        var lattice = Substitute.For<ILattice>();
        var calls = 0;
        lattice.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                calls++;
                return StalledKeys(Array.Empty<string>(), stallAfter: 0);
            });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await CollectAsync(lattice.ScanKeysAsync()));

        Assert.That(calls, Is.EqualTo(1 + LatticeExtensions.DefaultScanStallResumeAttempts));
    }
}
