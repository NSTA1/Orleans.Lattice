using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

internal sealed partial class LatticeGrain
{
    // Never renew this hold: a stalled reader must not indefinitely refuse saga
    // decisions. Gate release certifies the lease through the entire fan-out.
    internal static readonly TimeSpan GetManyDecisionGateLease = TimeSpan.FromSeconds(30);

    private async Task<Dictionary<string, byte[]>> GetManyDecisionGatedAsync(
        List<string> keys, KeyValuePair<string, object?> stageTagTree, CancellationToken cancellationToken)
    {
        logger.LogInformation("GetManyAsync for tree {TreeId} entering bounded decision-gated fallback after {Attempts} optimistic attempts.",
            TreeId, Math.Max(1, Options.MaxScanRetries));
        var clock = services.GetService(typeof(TimeProvider)) is TimeProvider configuredClock
            ? configuredClock : TimeProvider.System;
        using var timeout = new CancellationTokenSource(GetManyDecisionGateLease, clock);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        try
        {
            return await GetManyDecisionGatedCoreAsync(keys, stageTagTree, deadline.Token).WaitAsync(deadline.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new LatticeTransactionOutcomeUnavailableException(
                $"GetManyAsync for tree '{TreeId}' exceeded its bounded saga decision-gate deadline.") { TreeId = TreeId };
        }
        catch (TxDecisionGateRefusedException ex) when (ex.Refusal == TxDecisionGateRefusal.GateLapsed)
        {
            throw new LatticeTransactionOutcomeUnavailableException(
                $"GetManyAsync for tree '{TreeId}' could not establish a live bounded saga decision gate.", ex) { TreeId = TreeId };
        }
    }

    private async Task<Dictionary<string, byte[]>> GetManyDecisionGatedCoreAsync(
        List<string> keys, KeyValuePair<string, object?> stageTagTree, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var token = Guid.NewGuid();
        var highWater = await TxRegistryFanOut.AcquireCaptureGateAsync(
            grainFactory, TreeId, token, TxRegistryCaptureGateMode.Gate, GetManyDecisionGateLease,
            readGate: true, cancellationToken: cancellationToken);
        Dictionary<string, byte[]> result;
        bool valid;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            // D0 is local and immutable. The ordinary reader snapshot can
            // contain an uncached delegated verdict outside this gate's cut.
            var snapshot = await TxRegistryFanOut.GetCaptureGateSnapshotAsync(grainFactory, TreeId, token);
            result = await GetManyAsyncCore(keys, stageTagTree, cancellationToken, snapshot);
            cancellationToken.ThrowIfCancellationRequested();
        }
        finally
        {
            try
            {
                valid = await TxRegistryFanOut.ReleaseCaptureGateAsync(grainFactory, TreeId, highWater, token);
            }
            catch (Exception ex)
            {
                // A deadline may already have returned to the caller. Keep a
                // late cleanup failure observable; the unrenewed lease lapses.
                logger.LogWarning(ex, "GetManyAsync for tree {TreeId} failed to release decision gate {Token}; its finite lease will lapse.",
                    TreeId, token);
                throw;
            }
        }

        if (!valid)
            throw new LatticeTransactionOutcomeUnavailableException(
                $"GetManyAsync for tree '{TreeId}' lost its bounded saga decision gate or registry coverage during the read.") { TreeId = TreeId };
        return result;
    }
}
