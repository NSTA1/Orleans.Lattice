using NSubstitute;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Coverage for the progress-replenished abort reconnect budget (issue 6080).
/// <para>
/// The <see cref="EnumerationAbortedException"/> reopen budget was a per-scan
/// lifetime count, so a long walk that kept making progress between aborts -
/// a scope status walk during a rolling deploy, say - died on its
/// <c>maxAttempts + 1</c>th abort however much it had delivered. These tests
/// pin that every yielded record replenishes the consecutive budget, that a
/// walk which makes no progress still exhausts it exactly as before, and that
/// <see cref="LatticeExtensions.DefaultScanReconnectCeiling"/> still bounds the
/// lifetime total.
/// </para>
/// </summary>
public partial class ResilientScanExtensionsTests
{
    [Test]
    public async Task ScanKeysAsync_survives_more_aborts_than_the_budget_when_each_segment_progresses()
    {
        // Twelve aborts against the default budget of eight; each segment yields
        // one key before aborting. Under the lifetime budget this died on the
        // ninth abort.
        const int segments = 12;
        var lattice = Substitute.For<ILattice>();
        var callIndex = 0;
        StubKeys(lattice, _ =>
        {
            var index = callIndex++;
            return index < segments
                ? ScriptedKeys(new[] { $"k{index:D2}" }, abortAfter: 1)
                : ScriptedKeys(Array.Empty<string>(), abortAfter: int.MaxValue);
        });

        var keys = await CollectAsync(lattice.ScanKeysAsync());

        Assert.That(keys, Has.Count.EqualTo(segments));
        Assert.That(keys, Is.Unique);
        Assert.That(callIndex, Is.EqualTo(segments + 1));
    }

    [Test]
    public async Task ScanEntriesAsync_survives_more_aborts_than_the_budget_when_each_segment_progresses()
    {
        const int segments = 6;
        var lattice = Substitute.For<ILattice>();
        var callIndex = 0;
        lattice.EntriesAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var index = callIndex++;
                return index < segments
                    ? ScriptedEntries(new[] { ($"k{index:D2}", index) }, abortAfter: 1)
                    : ScriptedEntries(Array.Empty<(string, int)>(), abortAfter: int.MaxValue);
            });

        var entries = await CollectAsync(lattice.ScanEntriesAsync(maxAttempts: 2));

        Assert.That(entries, Has.Count.EqualTo(segments));
        Assert.That(entries[^1].Key, Is.EqualTo($"k{segments - 1:D2}"));
    }

    [Test]
    public void ScanKeysAsync_still_exhausts_the_budget_on_consecutive_futile_aborts()
    {
        // Abort at the origin, then deliver one key and abort, then abort twice
        // more without progress. With maxAttempts 2 the reset after "a" lets the
        // walk spend two more reopens, so it fails on the fourth open. Under the
        // lifetime budget it failed one open earlier.
        var lattice = Substitute.For<ILattice>();
        var callIndex = 0;
        StubKeys(lattice, _ =>
        {
            var index = callIndex++;
            return index == 1
                ? ScriptedKeys(new[] { "a" }, abortAfter: 1)
                : ScriptedKeys(Array.Empty<string>(), abortAfter: 0);
        });

        Assert.ThrowsAsync<EnumerationAbortedException>(async () =>
        {
            await foreach (var _ in lattice.ScanKeysAsync(maxAttempts: 2))
            {
            }
        });
        Assert.That(callIndex, Is.EqualTo(4));
    }

    [Test]
    public void MayReopenAfterAbort_is_gated_by_both_the_consecutive_budget_and_the_lifetime_ceiling()
    {
        var ceiling = LatticeExtensions.DefaultScanReconnectCeiling;

        Assert.That(ResilientScanResume.MayReopenAfterAbort(0, ceiling - 1, budget: 8), Is.True);
        Assert.That(ResilientScanResume.MayReopenAfterAbort(0, ceiling, budget: 8), Is.False,
            "a walk that dribbles progress between aborts is bounded by the ceiling");
        Assert.That(ResilientScanResume.MayReopenAfterAbort(8, 8, budget: 8), Is.False,
            "consecutive futile aborts still exhaust the budget");
        Assert.That(ResilientScanResume.MayReopenAfterAbort(0, 0, budget: 0), Is.False,
            "a zero budget still fails fast");
        Assert.That(ResilientScanResume.AbortCeiling(ceiling * 2), Is.EqualTo(ceiling * 2),
            "a budget above the ceiling raises the ceiling rather than being silently cut");
    }
}
