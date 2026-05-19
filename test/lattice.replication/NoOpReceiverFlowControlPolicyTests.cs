namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Tests for <see cref="NoOpReceiverFlowControlPolicy"/> - the default
/// receiver-side flow-control policy registered by
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>.
/// The policy is intentionally stateless and always returns
/// <see cref="ReceiverFlowControlHint.None"/> so opt-out hosts preserve
/// the existing blind-push behaviour.
/// </summary>
[TestFixture]
public class NoOpReceiverFlowControlPolicyTests
{
    [Test]
    public void Instance_is_non_null_singleton()
    {
        Assert.That(NoOpReceiverFlowControlPolicy.Instance, Is.Not.Null);
        Assert.That(NoOpReceiverFlowControlPolicy.Instance,
            Is.SameAs(NoOpReceiverFlowControlPolicy.Instance));
    }

    [Test]
    public async Task EvaluateAsync_returns_none_for_default_context()
    {
        var hint = await NoOpReceiverFlowControlPolicy.Instance
            .EvaluateAsync(default, CancellationToken.None);

        Assert.That(hint, Is.EqualTo(ReceiverFlowControlHint.None));
    }

    [Test]
    public async Task EvaluateAsync_returns_none_regardless_of_context_shape()
    {
        var ctx = new ReceiverFlowControlContext
        {
            TreeName = "tree",
            OriginClusterId = "site-b",
            EntryCount = 9999,
            ApplyDurationMs = 12345.6d,
        };

        var hint = await NoOpReceiverFlowControlPolicy.Instance
            .EvaluateAsync(ctx, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(hint.SuggestedBatchSize, Is.Null);
            Assert.That(hint.PauseForMs, Is.Null);
            Assert.That(hint, Is.EqualTo(ReceiverFlowControlHint.None));
        });
    }

    [Test]
    public void EvaluateAsync_throws_when_cancelled()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await NoOpReceiverFlowControlPolicy.Instance
                .EvaluateAsync(default, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}