using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Auth;

/// <summary>
/// Decides admin audit outcomes from the evidence of an anchor record, for both the
/// caller's finalize and the reconciler. The anchor alone never says what THIS call
/// did - a concurrent or replicated write can move it either way - so the only fact it
/// can prove is that nothing at all wrote it since the intent. That one test,
/// <see cref="CheckMovementAsync"/>, drives every decision:
/// <list type="bullet">
/// <item><description><see cref="AnchorMovement.Unmoved"/>: no revision of any kind
/// since the intent (its version equals the base version observed before the commit,
/// or, absent before, no revision in the history since the intent's time less the
/// clock-skew allowance);</description></item>
/// <item><description><see cref="AnchorMovement.Moved"/>: the version changed, the
/// anchor appeared, or the history holds any revision since the intent (a delete
/// between an absent before and an absent after included);</description></item>
/// <item><description><see cref="AnchorMovement.Unproven"/>: the history cannot prove
/// completeness (a lagging history view, a truncated window, a tree with no
/// versions).</description></item>
/// </list>
/// The reconciler (<see cref="ResolveAsync"/>) never claims a commit, and never claims
/// the absence of one while the operation could still be running: an accepted app
/// activation, a long cascade or a cancelled caller's commit can land well after the
/// facade returned. It records <see cref="LatticeAdminAuditOutcome.Indeterminate"/> once
/// the anchor moved, and otherwise waits until
/// <see cref="LatticeAuthOptions.AdminAuditMaxPendingAge"/>, then records
/// <see cref="LatticeAdminAuditReasonCodes.FailedNotCommitted"/> for an unmoved anchor and
/// <see cref="LatticeAdminAuditOutcome.Indeterminate"/> for an unproven one.
/// </summary>
internal static class AdminAuditEvidence
{
    /// <summary>
    /// How far before the intent's wall-clock time the history scan starts when the
    /// anchor did not exist before the call (so there is no base version to start
    /// from). Bounds cross-silo clock skew; an earlier revision only ever turns a result
    /// to moved, never to a claim.
    /// </summary>
    internal static readonly TimeSpan ClockSkewAllowance = TimeSpan.FromMinutes(5);

    /// <summary>Whether anything wrote an anchor since an intent was written.</summary>
    internal enum AnchorMovement
    {
        /// <summary>The evidence cannot prove either way.</summary>
        Unproven,

        /// <summary>Nothing wrote the anchor since the intent.</summary>
        Unmoved,

        /// <summary>Something (this call or any other writer) wrote the anchor since the intent.</summary>
        Moved,
    }

    /// <summary>
    /// Resolves <paramref name="intent"/>, or returns <see langword="null"/> to wait for
    /// more evidence.
    /// </summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="intent">The pending intent.</param>
    /// <param name="age">How long ago the intent was written.</param>
    /// <param name="maxPendingAge">The age after which the reconciler decides.</param>
    /// <param name="cancellationToken">Cancels the reads.</param>
    internal static async Task<LatticeAdminAuditCompletion?> ResolveAsync(
        IGrainFactory grainFactory,
        LatticeAdminAuditRecord intent,
        TimeSpan age,
        TimeSpan maxPendingAge,
        CancellationToken cancellationToken)
    {
        var expired = age >= maxPendingAge;
        var movement = await CheckMovementAsync(grainFactory, intent, viewProvesAbsence: expired, cancellationToken)
            .ConfigureAwait(false);
        return movement switch
        {
            AnchorMovement.Moved => Indeterminate,
            AnchorMovement.Unmoved when expired => new LatticeAdminAuditCompletion(
                LatticeAdminAuditOutcome.Failed, LatticeAdminAuditReasonCodes.FailedNotCommitted),
            _ when expired => Indeterminate,
            _ => null,
        };
    }

    /// <summary>The indeterminate completion: the actor is attributed, no change is claimed.</summary>
    internal static LatticeAdminAuditCompletion Indeterminate { get; } = new(
        LatticeAdminAuditOutcome.Indeterminate, LatticeAdminAuditReasonCodes.IndeterminateNoEvidence);

    /// <summary>
    /// Whether anything wrote <paramref name="intent"/>'s anchor since the intent.
    /// </summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="intent">The intent, carrying the anchor and its base observation.</param>
    /// <param name="viewProvesAbsence">
    /// Whether an empty history view may prove that nothing wrote an anchor that was
    /// absent before. A view lags its source, so this is set only once the intent has
    /// outlived the maximum pending age; an untruncated write-ahead-log window always
    /// proves it.
    /// </param>
    /// <param name="cancellationToken">Cancels the reads.</param>
    internal static async Task<AnchorMovement> CheckMovementAsync(
        IGrainFactory grainFactory,
        LatticeAdminAuditRecord intent,
        bool viewProvesAbsence,
        CancellationToken cancellationToken)
    {
        if (intent.AnchorTreeId is not { } treeId || intent.AnchorKey is not { } key)
        {
            return AnchorMovement.Unproven;
        }

        var anchor = new LatticeAdminAuditAnchor(treeId, key);
        var observed = await AdminAuditAnchorReader.ReadAsync(grainFactory, anchor, cancellationToken).ConfigureAwait(false);

        if (!observed.Versioned)
        {
            // No versions and no history: an equal value can hide a write and its undo.
            return observed.Hash == intent.AnchorBaseHash ? AnchorMovement.Unproven : AnchorMovement.Moved;
        }

        if (intent.AnchorBaseVersion is { } baseVersion)
        {
            // Every write, a delete included, gives the key a new version.
            return observed.Version is { } current && current.Equals(baseVersion)
                ? AnchorMovement.Unmoved
                : AnchorMovement.Moved;
        }

        if (observed.Version is not null)
        {
            return AnchorMovement.Moved;
        }

        // Absent before and absent after: only the history can tell a write-then-delete
        // from no write at all.
        return await ReadHistoryAsync(grainFactory, intent, anchor, viewProvesAbsence, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<AnchorMovement> ReadHistoryAsync(
        IGrainFactory grainFactory,
        LatticeAdminAuditRecord intent,
        LatticeAdminAuditAnchor anchor,
        bool viewProvesAbsence,
        CancellationToken cancellationToken)
    {
        var from = new HybridLogicalClock { WallClockTicks = (intent.TimestampUtc - ClockSkewAllowance).UtcTicks };
        var lattice = grainFactory.GetGrain<ILattice>(anchor.TreeId);
        EntryHistoryPage result;

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            result = await lattice
                .ScanEntryHistoryAsync(anchor.Key, from, null, 1, null, cancellationToken)
                .ConfigureAwait(false);
        }

        if (result.Revisions.Count > 0)
        {
            return AnchorMovement.Moved;
        }

        var complete = result.Source switch
        {
            EntryHistorySource.WalWindow => !(result.Truncated && result.EarliestAvailable.CompareTo(from) > 0),
            EntryHistorySource.View => viewProvesAbsence,
            _ => false,
        };
        return complete ? AnchorMovement.Unmoved : AnchorMovement.Unproven;
    }
}
