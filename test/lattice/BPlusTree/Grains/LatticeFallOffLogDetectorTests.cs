using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="LatticeFallOffLogDetector"/>. The detector
/// is the leaf activation gate that classifies the replay path before
/// the materialiser drives <c>ILeafProjection.Apply</c> over the WAL
/// slice. These tests pin the contract that the <c>-1</c> "nothing
/// applied" sentinel - emitted by the operator-driven projection
/// rebuild path and by a freshly persisted leaf with no observed
/// activations - is accepted as a valid checkpoint offset and does not
/// false-positive any of the three fall-off triggers (WAL trim,
/// replay budget, projection age).
/// </summary>
[TestFixture]
public sealed class LatticeFallOffLogDetectorTests
{
    private const string TreeId = "tree-detector";
    private const int ShardIndex = 0;

    private static (LatticeFallOffLogDetector Detector, ICommitLogReader Reader) CreateDetector(
        long head,
        long tail)
    {
        var reader = Substitute.For<ICommitLogReader>();
        reader.GetHeadOffsetAsync(TreeId, ShardIndex, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(head));
        reader.GetTailOffsetAsync(TreeId, ShardIndex, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(tail));

        var sc = new ServiceCollection();
        sc.AddSingleton(reader);
        var services = sc.BuildServiceProvider();

        return (new LatticeFallOffLogDetector(services), reader);
    }

    private static async Task<ResolvedLatticeOptions> BuildOptionsAsync(
        LatticeOptions? baseOptions = null)
    {
        var resolver = TestOptionsResolver.Create(
            baseOptions: baseOptions ?? new LatticeOptions(),
            maxLeafKeys: 128,
            shardCount: 1,
            factory: Substitute.For<IGrainFactory>());
        return await resolver.ResolveAsync(TreeId).ConfigureAwait(false);
    }

    [Test]
    public async Task ClassifyAsync_with_nothing_applied_sentinel_returns_TailReplay_when_WAL_empty()
    {
        // Empty WAL: head == tail == 0. The "nothing applied" sentinel
        // is -1; the gap (head - checkpoint) == 1 must not exceed the
        // default replay budget, the WAL-trim check must short-circuit
        // because checkpoint <= 0, and the age check is disabled.
        var (detector, _) = CreateDetector(head: 0, tail: 0);
        var options = await BuildOptionsAsync();

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: -1,
            checkpointAge: TimeSpan.Zero,
            options,
            CancellationToken.None);

        Assert.That(decision, Is.EqualTo(FallOffLogDecision.TailReplay));
    }

    [Test]
    public async Task ClassifyAsync_with_nothing_applied_sentinel_returns_TailReplay_when_WAL_populated_and_within_budget()
    {
        // After an operator-driven rebuild, the leaf persists
        // checkpoint = -1 and re-activates against a populated WAL.
        // The detector must classify this as a tail replay so the
        // materialiser reads (-1, head] == [0, head] inclusive,
        // covering offset 0 (which the rebuild bug previously
        // skipped under checkpoint = 0).
        var (detector, _) = CreateDetector(head: 50, tail: 0);
        var options = await BuildOptionsAsync();

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: -1,
            checkpointAge: TimeSpan.Zero,
            options,
            CancellationToken.None);

        Assert.That(decision, Is.EqualTo(FallOffLogDecision.TailReplay));
    }

    [Test]
    public async Task ClassifyAsync_with_nothing_applied_sentinel_does_not_trigger_WAL_trim_path()
    {
        // The WAL-trim trigger fires when tail > checkpoint AND
        // checkpoint > 0. A checkpoint of -1 must NOT be treated as
        // "trimmed past": the leaf has not applied anything yet, so
        // every offset >= 0 is legitimately in-scope for replay.
        var (detector, _) = CreateDetector(head: 100, tail: 50);
        var options = await BuildOptionsAsync();

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: -1,
            checkpointAge: TimeSpan.Zero,
            options,
            CancellationToken.None);

        // Within budget (gap = 101 << default 10 000), so TailReplay.
        Assert.That(decision, Is.EqualTo(FallOffLogDecision.TailReplay));
    }

    [Test]
    public async Task ClassifyAsync_with_nothing_applied_sentinel_respects_replay_budget()
    {
        // Sentinel must still honour the replay-budget trigger: the
        // detector classifies the gap (head - checkpoint) against
        // MaxLeafReplayEntries, so a large WAL beyond the budget
        // selects the configured recovery policy.
        var (detector, _) = CreateDetector(head: 100, tail: 0);
        var options = await BuildOptionsAsync(new LatticeOptions
        {
            MaxLeafReplayEntries = 10,
            ProjectionRebuildPolicy = ProjectionRebuildPolicy.Fail,
        });

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: -1,
            checkpointAge: TimeSpan.Zero,
            options,
            CancellationToken.None);

        Assert.That(decision, Is.EqualTo(FallOffLogDecision.Fail));
    }

    [Test]
    public void ClassifyAsync_rejects_checkpoint_below_minus_one()
    {
        // -1 is the lowest legal checkpoint (the "nothing applied"
        // sentinel). Anything lower is a programming error and must
        // surface as ArgumentOutOfRangeException so callers can't
        // silently downgrade to "always tail replay".
        var (detector, _) = CreateDetector(head: 0, tail: 0);

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
        {
            var options = await BuildOptionsAsync();
            await detector.ClassifyAsync(
                TreeId, ShardIndex,
                checkpointOffset: -2,
                checkpointAge: TimeSpan.Zero,
                options,
                CancellationToken.None);
        });
    }

    [Test]
    public async Task ClassifyAsync_accepts_zero_checkpoint_without_triggering_WAL_trim()
    {
        // The pre-existing default-0 path must remain a tail replay:
        // a leaf whose in-memory state already covers offset 0 (i.e.
        // the legacy semantics for materialiser-not-yet-engaged
        // grains) is the gate that protects against the off-by-one
        // the rebuild path hit. checkpoint > 0 is the only state
        // where the WAL-trim trigger should fire.
        var (detector, _) = CreateDetector(head: 0, tail: 0);
        var options = await BuildOptionsAsync();

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: 0,
            checkpointAge: TimeSpan.Zero,
            options,
            CancellationToken.None);

        Assert.That(decision, Is.EqualTo(FallOffLogDecision.TailReplay));
    }

    [Test]
    public async Task ClassifyAsync_with_positive_checkpoint_below_tail_triggers_WAL_trim()
    {
        // checkpoint > 0 and tail > checkpoint: WAL has been trimmed
        // past the leaf's last applied offset. Recovery routes via
        // the configured ProjectionRebuildPolicy.
        var (detector, _) = CreateDetector(head: 100, tail: 50);
        var options = await BuildOptionsAsync(new LatticeOptions
        {
            ProjectionRebuildPolicy = ProjectionRebuildPolicy.SnapshotThenWal,
        });

        var decision = await detector.ClassifyAsync(
            TreeId, ShardIndex,
            checkpointOffset: 25,
            checkpointAge: TimeSpan.Zero,
            options,
            CancellationToken.None);

        Assert.That(decision, Is.EqualTo(FallOffLogDecision.SnapshotThenWal));
    }
}
