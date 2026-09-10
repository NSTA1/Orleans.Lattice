using NSubstitute;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// The view mirror of <c>ResilientScanExtensionsTests.StallReplenish</c> for
/// issue 2539: progress replenishes the stall resume budget.
/// <para>
/// The view maintainers carry their own copy of the reopen loop, so they
/// carried their own copy of the monotonic budget. This fixture exists because
/// a defect present at four sites is most likely to be half-fixed at the two
/// nobody was looking at.
/// </para>
/// </summary>
public partial class ResilientViewScanExtensionsTests
{
    [Test]
    public async Task ScanKeysAsync_replenishes_the_stall_budget_on_every_yielded_key()
    {
        const int pages = 10;
        var view = Substitute.For<ILatticeView>();
        var callIndex = 0;
        view.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var index = callIndex++;
                return index < pages
                    ? StalledKeys(new[] { $"k{index:D2}" }, stallAfter: 1)
                    : ScriptedKeys(Array.Empty<string>(), abortAfter: int.MaxValue);
            });

        var keys = await CollectAsync(view.ScanKeysAsync());

        Assert.That(keys, Has.Count.EqualTo(pages), "a view walk that progresses between stalls must converge");
        Assert.That(keys[^1], Is.EqualTo($"k{pages - 1:D2}"));
    }

    [Test]
    public async Task ScanEntriesAsync_replenishes_the_stall_budget_on_every_yielded_entry()
    {
        const int pages = 10;
        var view = Substitute.For<ILatticeView>();
        var callIndex = 0;
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var index = callIndex++;
                return index < pages
                    ? StalledEntries(new[] { ($"k{index:D2}", index) }, stallAfter: 1)
                    : ScriptedEntries(Array.Empty<(string, int)>(), abortAfter: int.MaxValue);
            });

        var entries = await CollectAsync(view.ScanEntriesAsync());

        Assert.That(entries, Has.Count.EqualTo(pages), "a view walk that progresses between stalls must converge");
        Assert.That(entries[^1].Key, Is.EqualTo($"k{pages - 1:D2}"));
    }

    [Test]
    public void ScanKeysAsync_still_exhausts_the_budget_when_progress_stops()
    {
        // See the core fixture: the first page stalls at the origin so a resume
        // is genuinely spent before any progress happens, which is what makes
        // the call count discriminate the reset rather than merely tolerate it.
        var view = Substitute.For<ILatticeView>();
        var calls = 0;
        view.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var index = calls++;
                return index == 1
                    ? StalledKeys(new[] { "a" }, stallAfter: 1)
                    : StalledKeys(Array.Empty<string>(), stallAfter: 0);
            });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await CollectAsync(view.ScanKeysAsync()));

        Assert.That(
            calls,
            Is.EqualTo(2 + LatticeExtensions.DefaultScanStallResumeAttempts),
            "progress replenishes the budget once; it does not make the walk immortal");
    }

    [Test]
    public void ScanKeysAsync_stops_at_the_total_ceiling_even_while_it_is_progressing()
    {
        var view = Substitute.For<ILatticeView>();
        var calls = 0;
        var next = 0;
        view.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                calls++;
                return StalledKeys(new[] { $"k{next++:D4}" }, stallAfter: 1);
            });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await CollectAsync(view.ScanKeysAsync()));

        Assert.That(
            calls,
            Is.EqualTo(1 + LatticeExtensions.DefaultScanStallResumeCeiling),
            "an endlessly dribbling view walk must terminate at the total ceiling");
    }
}
