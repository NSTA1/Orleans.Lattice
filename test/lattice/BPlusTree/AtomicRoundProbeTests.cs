using NSubstitute;
using NUnit.Framework;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Pins how <see cref="AtomicRoundProbe"/> classifies a read: a timed-out read is a
/// liveness fault only when the probe opts in, and a torn or out-of-range view is an
/// atomicity failure whether or not it does (#4407).
/// </summary>
[TestFixture]
public class AtomicRoundProbeTests
{
    private const string Prefix = "probe";

    private static ILattice CreateTree(Func<List<string>, Task<Dictionary<string, byte[]>>> read)
    {
        var tree = Substitute.For<ILattice>();
        tree.SetManyAtomicAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
        tree.GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
            .Returns(call => read(call.Arg<List<string>>()));
        return tree;
    }

    private static Task<Dictionary<string, byte[]>> TimesOut(List<string> _) =>
        Task.FromException<Dictionary<string, byte[]>>(new TimeoutException("Response did not arrive on time in 00:00:30"));

    private static Func<List<string>, Task<Dictionary<string, byte[]>>> Uniform(int round) =>
        keys => Task.FromResult(keys.Select((k, i) => (k, i)).ToDictionary(p => p.k, p => AtomicRoundProbe.Value(round, p.i)));

    [Test]
    public async Task WriteNextRoundAsync_read_times_out_with_liveness_classification_records_liveness_fault_not_failure()
    {
        var probe = new AtomicRoundProbe(CreateTree(TimesOut), Prefix, keyCount: 4, classifyTimeoutsAsLiveness: true);

        var round = await probe.WriteNextRoundAsync();

        Assert.Multiple(() =>
        {
            Assert.That(round, Is.EqualTo(1));
            Assert.That(probe.RoundsCommitted, Is.EqualTo(1));
            Assert.That(probe.Failures, Is.Empty);
            Assert.That(probe.LivenessFaults, Has.Count.EqualTo(1));
            Assert.That(probe.LivenessFaults.Single(), Does.Contain("post-commit read of round 1"));
            Assert.That(probe.LivenessFaults.Single(), Does.Contain("GetManyAsync timed out after"));
            Assert.That(probe.LivenessFaults.Single(), Does.Contain("Response did not arrive on time"));
            Assert.That(probe.Polls, Is.Zero);
        });
    }

    [Test]
    public async Task WriteNextRoundAsync_read_times_out_by_default_records_failure()
    {
        var probe = new AtomicRoundProbe(CreateTree(TimesOut), Prefix, keyCount: 4);

        await probe.WriteNextRoundAsync();

        Assert.Multiple(() =>
        {
            Assert.That(probe.LivenessFaults, Is.Empty);
            Assert.That(probe.Failures, Has.Count.EqualTo(1));
            Assert.That(probe.Failures.Single(), Does.Contain(nameof(TimeoutException)));
        });
    }

    [Test]
    public async Task WriteNextRoundAsync_torn_view_with_liveness_classification_still_records_failure()
    {
        Task<Dictionary<string, byte[]>> Torn(List<string> keys) =>
            Task.FromResult(keys.Select((k, i) => (k, i))
                .ToDictionary(p => p.k, p => AtomicRoundProbe.Value(p.i == 0 ? 0 : 1, p.i)));
        var probe = new AtomicRoundProbe(CreateTree(Torn), Prefix, keyCount: 4, classifyTimeoutsAsLiveness: true);

        await probe.WriteNextRoundAsync();

        Assert.Multiple(() =>
        {
            Assert.That(probe.LivenessFaults, Is.Empty);
            Assert.That(probe.Failures, Has.Count.EqualTo(1));
            Assert.That(probe.Failures.Single(), Does.Contain("TORN"));
        });
    }

    [Test]
    public async Task WriteNextRoundAsync_partially_hidden_view_with_liveness_classification_still_records_failure()
    {
        Task<Dictionary<string, byte[]>> Partial(List<string> keys) =>
            Task.FromResult(keys.Select((k, i) => (k, i)).Where(p => p.i != 2)
                .ToDictionary(p => p.k, p => AtomicRoundProbe.Value(1, p.i)));
        var probe = new AtomicRoundProbe(CreateTree(Partial), Prefix, keyCount: 4, classifyTimeoutsAsLiveness: true);

        await probe.WriteNextRoundAsync();

        Assert.Multiple(() =>
        {
            Assert.That(probe.LivenessFaults, Is.Empty);
            Assert.That(probe.Failures, Has.Count.EqualTo(1));
            Assert.That(probe.Failures.Single(), Does.Contain("TORN"));
        });
    }

    [Test]
    public async Task WriteNextRoundAsync_stale_uniform_view_with_liveness_classification_still_records_failure()
    {
        var probe = new AtomicRoundProbe(CreateTree(Uniform(0)), Prefix, keyCount: 4, classifyTimeoutsAsLiveness: true);

        await probe.WriteNextRoundAsync();

        Assert.Multiple(() =>
        {
            Assert.That(probe.LivenessFaults, Is.Empty);
            Assert.That(probe.Failures, Has.Count.EqualTo(1));
            Assert.That(probe.Failures.Single(), Does.Contain("outside"));
        });
    }

    [Test]
    public async Task WriteNextRoundAsync_committed_uniform_view_with_liveness_classification_records_nothing()
    {
        var probe = new AtomicRoundProbe(CreateTree(Uniform(1)), Prefix, keyCount: 4, classifyTimeoutsAsLiveness: true);

        await probe.WriteNextRoundAsync();

        Assert.Multiple(() =>
        {
            Assert.That(probe.Failures, Is.Empty);
            Assert.That(probe.LivenessFaults, Is.Empty);
            Assert.That(probe.Polls, Is.EqualTo(1));
            Assert.That(probe.UniformPolls, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Summary_after_liveness_fault_reports_its_count()
    {
        var probe = new AtomicRoundProbe(CreateTree(TimesOut), Prefix, keyCount: 4, classifyTimeoutsAsLiveness: true);

        await probe.WriteNextRoundAsync();

        Assert.That(probe.Summary(), Does.Contain("livenessFaults=1"));
    }
}
