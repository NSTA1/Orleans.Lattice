namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IReceiverFlowControlPolicy"/> implementation.
/// Always returns <see cref="ReceiverFlowControlHint.None"/>, which
/// preserves today's blind-push behaviour: the receiver carries no
/// preferred batch size and requests no pause. Hosts that want
/// receiver-driven throttling replace the registration via DI before
/// or after <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>.
/// </summary>
public sealed class NoOpReceiverFlowControlPolicy : IReceiverFlowControlPolicy
{
    /// <summary>
    /// Singleton instance. Stateless and thread-safe.
    /// </summary>
    public static NoOpReceiverFlowControlPolicy Instance { get; } = new();

    /// <inheritdoc />
    public ValueTask<ReceiverFlowControlHint> EvaluateAsync(
        ReceiverFlowControlContext context,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(ReceiverFlowControlHint.None);
    }
}
