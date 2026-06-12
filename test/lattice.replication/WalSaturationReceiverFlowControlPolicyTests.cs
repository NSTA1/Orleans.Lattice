using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Tests for <see cref="WalSaturationReceiverFlowControlPolicy"/> - the
/// receiver-side <see cref="IReceiverFlowControlPolicy"/> that maps the core
/// library's <see cref="WalSaturationState"/> regime onto the
/// <see cref="ReceiverFlowControlHint"/> stamped on each
/// <see cref="ReplicationAck"/>.
/// </summary>
[TestFixture]
public class WalSaturationReceiverFlowControlPolicyTests
{
    private const string Tree = "tree-a";
    private const int ShipBatchSize = 256;

    private static WalSaturationReceiverFlowControlPolicy CreatePolicy(
        IWalSaturationSignal? signal,
        WalSaturationReceiverFlowControlOptions? tuning = null,
        int shipBatchSize = ShipBatchSize)
    {
        var repOptions = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        repOptions.Get(Arg.Any<string>())
            .Returns(new LatticeReplicationOptions { ShipBatchSize = shipBatchSize });

        var fcOptions = Substitute.For<IOptionsMonitor<WalSaturationReceiverFlowControlOptions>>();
        fcOptions.Get(Arg.Any<string>())
            .Returns(tuning ?? new WalSaturationReceiverFlowControlOptions());

        return new WalSaturationReceiverFlowControlPolicy(signal, repOptions, fcOptions);
    }

    private static IWalSaturationSignal SignalReturning(WalSaturationState state)
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(state);
        return signal;
    }

    private static ReceiverFlowControlContext Context(string treeName = Tree) => new()
    {
        TreeName = treeName,
        OriginClusterId = "site-b",
        EntryCount = 32,
        ApplyDurationMs = 5d,
    };

    [Test]
    public async Task EvaluateAsync_returns_none_when_tree_is_healthy()
    {
        var policy = CreatePolicy(SignalReturning(WalSaturationState.Healthy));

        var hint = await policy.EvaluateAsync(Context(), CancellationToken.None);

        Assert.That(hint, Is.EqualTo(ReceiverFlowControlHint.None));
    }

    [Test]
    public async Task EvaluateAsync_returns_none_when_no_signal_is_registered()
    {
        var policy = CreatePolicy(signal: null);

        var hint = await policy.EvaluateAsync(Context(), CancellationToken.None);

        Assert.That(hint, Is.EqualTo(ReceiverFlowControlHint.None));
    }

    [Test]
    public async Task EvaluateAsync_returns_none_for_empty_tree_name()
    {
        var policy = CreatePolicy(SignalReturning(WalSaturationState.Saturated));

        var hint = await policy.EvaluateAsync(Context(treeName: string.Empty), CancellationToken.None);

        Assert.That(hint, Is.EqualTo(ReceiverFlowControlHint.None));
    }

    [Test]
    public async Task EvaluateAsync_throttled_halves_batch_and_requests_short_pause()
    {
        var policy = CreatePolicy(SignalReturning(WalSaturationState.Throttled));

        var hint = await policy.EvaluateAsync(Context(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            // ceil(256 * 0.5) = 128
            Assert.That(hint.SuggestedBatchSize, Is.EqualTo(128));
            Assert.That(hint.PauseForMs,
                Is.EqualTo(WalSaturationReceiverFlowControlOptions.DefaultThrottledPauseMs));
        });
    }

    [Test]
    public async Task EvaluateAsync_saturated_drip_feeds_and_requests_long_pause()
    {
        var policy = CreatePolicy(SignalReturning(WalSaturationState.Saturated));

        var hint = await policy.EvaluateAsync(Context(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(hint.SuggestedBatchSize,
                Is.EqualTo(WalSaturationReceiverFlowControlOptions.DefaultSaturatedBatchSize));
            Assert.That(hint.PauseForMs,
                Is.EqualTo(WalSaturationReceiverFlowControlOptions.DefaultSaturatedPauseMs));
        });
    }

    [Test]
    public async Task EvaluateAsync_throttled_ratio_is_clamped_to_ship_batch_size()
    {
        var tuning = new WalSaturationReceiverFlowControlOptions { ThrottledBatchRatio = 5.0d };
        var policy = CreatePolicy(SignalReturning(WalSaturationState.Throttled), tuning);

        var hint = await policy.EvaluateAsync(Context(), CancellationToken.None);

        Assert.That(hint.SuggestedBatchSize, Is.EqualTo(ShipBatchSize));
    }

    [Test]
    public async Task EvaluateAsync_throttled_ratio_floor_never_drops_below_one()
    {
        var tuning = new WalSaturationReceiverFlowControlOptions { ThrottledBatchRatio = -1.0d };
        var policy = CreatePolicy(SignalReturning(WalSaturationState.Throttled), tuning);

        var hint = await policy.EvaluateAsync(Context(), CancellationToken.None);

        Assert.That(hint.SuggestedBatchSize, Is.EqualTo(1));
    }

    [Test]
    public async Task EvaluateAsync_saturated_batch_size_is_clamped_to_one_minimum()
    {
        var tuning = new WalSaturationReceiverFlowControlOptions { SaturatedBatchSize = 0 };
        var policy = CreatePolicy(SignalReturning(WalSaturationState.Saturated), tuning);

        var hint = await policy.EvaluateAsync(Context(), CancellationToken.None);

        Assert.That(hint.SuggestedBatchSize, Is.EqualTo(1));
    }

    [Test]
    public async Task EvaluateAsync_saturated_batch_size_is_clamped_to_ship_batch_size()
    {
        var tuning = new WalSaturationReceiverFlowControlOptions { SaturatedBatchSize = 9999 };
        var policy = CreatePolicy(SignalReturning(WalSaturationState.Saturated), tuning, shipBatchSize: 64);

        var hint = await policy.EvaluateAsync(Context(), CancellationToken.None);

        Assert.That(hint.SuggestedBatchSize, Is.EqualTo(64));
    }

    [Test]
    public async Task EvaluateAsync_non_positive_pause_is_surfaced_as_null()
    {
        var tuning = new WalSaturationReceiverFlowControlOptions
        {
            ThrottledPauseMs = 0,
            SaturatedPauseMs = -10,
        };

        var throttled = await CreatePolicy(SignalReturning(WalSaturationState.Throttled), tuning)
            .EvaluateAsync(Context(), CancellationToken.None);
        var saturated = await CreatePolicy(SignalReturning(WalSaturationState.Saturated), tuning)
            .EvaluateAsync(Context(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(throttled.PauseForMs, Is.Null);
            Assert.That(saturated.PauseForMs, Is.Null);
        });
    }

    [Test]
    public async Task EvaluateAsync_uses_the_per_tree_ship_batch_size()
    {
        var repOptions = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        repOptions.Get(Tree).Returns(new LatticeReplicationOptions { ShipBatchSize = 40 });
        var fcOptions = Substitute.For<IOptionsMonitor<WalSaturationReceiverFlowControlOptions>>();
        fcOptions.Get(Arg.Any<string>()).Returns(new WalSaturationReceiverFlowControlOptions());

        var policy = new WalSaturationReceiverFlowControlPolicy(
            SignalReturning(WalSaturationState.Throttled), repOptions, fcOptions);

        var hint = await policy.EvaluateAsync(Context(), CancellationToken.None);

        // ceil(40 * 0.5) = 20
        Assert.That(hint.SuggestedBatchSize, Is.EqualTo(20));
        repOptions.Received().Get(Tree);
    }

    [Test]
    public async Task EvaluateAsync_queries_the_signal_for_the_context_tree()
    {
        var signal = SignalReturning(WalSaturationState.Throttled);
        var policy = CreatePolicy(signal);

        await policy.EvaluateAsync(Context(treeName: "some-other-tree"), CancellationToken.None);

        signal.Received().GetCurrentState("some-other-tree");
    }

    [Test]
    public async Task EvaluateAsync_throttled_handles_non_positive_ship_batch_size()
    {
        var policy = CreatePolicy(SignalReturning(WalSaturationState.Throttled), shipBatchSize: 0);

        var hint = await policy.EvaluateAsync(Context(), CancellationToken.None);

        // ShipBatchSize is floored to 1 so the suggestion never collapses to zero.
        Assert.That(hint.SuggestedBatchSize, Is.EqualTo(1));
    }

    [Test]
    public void EvaluateAsync_throws_when_cancelled()
    {
        var policy = CreatePolicy(SignalReturning(WalSaturationState.Saturated));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await policy.EvaluateAsync(Context(), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void Constructor_throws_for_null_required_monitors()
    {
        var fcOptions = Substitute.For<IOptionsMonitor<WalSaturationReceiverFlowControlOptions>>();
        var repOptions = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new WalSaturationReceiverFlowControlPolicy(null, null!, fcOptions),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                () => new WalSaturationReceiverFlowControlPolicy(null, repOptions, null!),
                Throws.InstanceOf<ArgumentNullException>());
        });
    }
}
