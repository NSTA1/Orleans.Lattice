using NSubstitute;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// View parity for the progress-replenished abort reconnect budget (issue
/// 6080). See <c>ResilientScanExtensionsTests.AbortBudgetReplenish</c>.
/// </summary>
public partial class ResilientViewScanExtensionsTests
{
    [Test]
    public async Task ScanKeysAsync_survives_more_aborts_than_the_budget_when_each_segment_progresses()
    {
        const int segments = 6;
        var view = Substitute.For<ILatticeView>();
        var callIndex = 0;
        StubKeys(view, _ =>
        {
            var index = callIndex++;
            return index < segments
                ? ScriptedKeys(new[] { $"k{index:D2}" }, abortAfter: 1)
                : ScriptedKeys(Array.Empty<string>(), abortAfter: int.MaxValue);
        });

        var keys = await CollectAsync(view.ScanKeysAsync(maxAttempts: 2));

        Assert.That(keys, Has.Count.EqualTo(segments));
        Assert.That(keys, Is.Unique);
    }

    [Test]
    public async Task ScanEntriesAsync_survives_more_aborts_than_the_budget_when_each_segment_progresses()
    {
        const int segments = 6;
        var view = Substitute.For<ILatticeView>();
        var callIndex = 0;
        view.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var index = callIndex++;
                return index < segments
                    ? ScriptedEntries(new[] { ($"k{index:D2}", index) }, abortAfter: 1)
                    : ScriptedEntries(Array.Empty<(string, int)>(), abortAfter: int.MaxValue);
            });

        var entries = await CollectAsync(view.ScanEntriesAsync(maxAttempts: 2));

        Assert.That(entries, Has.Count.EqualTo(segments));
        Assert.That(entries[^1].Key, Is.EqualTo($"k{segments - 1:D2}"));
    }
}
