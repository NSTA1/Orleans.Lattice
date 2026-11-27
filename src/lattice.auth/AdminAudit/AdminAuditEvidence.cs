using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Auth;

/// <summary>
/// Resolves an admin audit intent left pending (the facade stopped between its
/// commit and its finalize, or failed in a way that could not prove either outcome)
/// from the evidence of its anchor record. The rule never claims a commit, and never
/// claims the absence of one while the operation could still be running: an accepted
/// app activation, a long cascade, or a cancelled caller's commit can land well after
/// the facade returned, so an unmoved anchor only proves "not committed yet".
/// <list type="bullet">
/// <item><description>the anchor still holds the version (or, for a tree with no
/// versions, the value, or for an anchor absent before, still absent) observed before
/// the commit, once the intent is older than
/// <see cref="LatticeAuthOptions.AdminAuditMaxPendingAge"/>: the intent is
/// <see cref="LatticeAdminAuditOutcome.Failed"/> with
/// <see cref="LatticeAdminAuditReasonCodes.FailedNotCommitted"/>; younger, it waits;</description></item>
/// <item><description>the anchor's history since that version is complete and holds
/// only revisions authored by another region (replication applies): this region
/// wrote nothing, so likewise <see cref="LatticeAdminAuditOutcome.Failed"/> once
/// the intent is older than the maximum pending age;</description></item>
/// <item><description>a local revision exists: something in this region wrote the
/// anchor, which may or may not have been this call, so
/// <see cref="LatticeAdminAuditOutcome.Indeterminate"/>;</description></item>
/// <item><description>otherwise the history does not yet prove completeness (a lagging
/// history view, a truncated write-ahead-log window): wait, and after
/// <see cref="LatticeAuthOptions.AdminAuditMaxPendingAge"/>
/// <see cref="LatticeAdminAuditOutcome.Indeterminate"/>.</description></item>
/// </list>
/// A committed call is therefore attributed by exactly one record (its own
/// finalize, or an indeterminate one carrying the same actor), and no record ever
/// claims a change that did not commit.
/// </summary>
internal static class AdminAuditEvidence
{
    /// <summary>
    /// How far before the intent's wall-clock time the history scan starts when the
    /// anchor did not exist before the call (so there is no base version to start
    /// from). Bounds cross-silo clock skew; an earlier local revision only ever turns a
    /// result to <see cref="LatticeAdminAuditOutcome.Indeterminate"/>, never to a claim.
    /// </summary>
    internal static readonly TimeSpan ClockSkewAllowance = TimeSpan.FromMinutes(5);

    private const int PageSize = 100;
    private const int MaxPages = 20;

    /// <summary>
    /// Resolves <paramref name="intent"/>, or returns <see langword="null"/> to wait for
    /// more evidence.
    /// </summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="intent">The pending intent.</param>
    /// <param name="age">How long ago the intent was written.</param>
    /// <param name="maxPendingAge">The age after which an unprovable intent is indeterminate.</param>
    /// <param name="cancellationToken">Cancels the reads.</param>
    internal static async Task<LatticeAdminAuditCompletion?> ResolveAsync(
        IGrainFactory grainFactory,
        LatticeAdminAuditRecord intent,
        TimeSpan age,
        TimeSpan maxPendingAge,
        CancellationToken cancellationToken)
    {
        var indeterminate = new LatticeAdminAuditCompletion(
            LatticeAdminAuditOutcome.Indeterminate, LatticeAdminAuditReasonCodes.IndeterminateNoEvidence);
        var notCommitted = new LatticeAdminAuditCompletion(
            LatticeAdminAuditOutcome.Failed, LatticeAdminAuditReasonCodes.FailedNotCommitted);

        if (intent.AnchorTreeId is not { } treeId || intent.AnchorKey is not { } key)
        {
            return indeterminate;
        }

        var anchor = new LatticeAdminAuditAnchor(treeId, key);
        var observed = await AdminAuditAnchorReader.ReadAsync(grainFactory, anchor, cancellationToken).ConfigureAwait(false);

        // An unmoved anchor proves only that nothing has committed YET: the operation
        // may still be running (an accepted activation, a long cascade, a cancelled
        // caller's commit). "Not committed" is therefore issued only once the intent has
        // outlived the maximum pending age; until then the intent waits.
        var expired = age >= maxPendingAge;
        LatticeAdminAuditCompletion? notCommittedOnceExpired = expired ? notCommitted : null;

        if (!observed.Versioned)
        {
            // A tree with no versions or history: only "the value never moved" is provable.
            return observed.Hash == intent.AnchorBaseHash ? notCommittedOnceExpired : indeterminate;
        }

        var unmoved = intent.AnchorBaseVersion is { } baseVersion
            ? observed.Version is { } current && current.Equals(baseVersion)
            : observed.Version is null;
        if (unmoved)
        {
            return notCommittedOnceExpired;
        }

        var verdict = await ReadHistoryAsync(grainFactory, intent, anchor, observed, cancellationToken).ConfigureAwait(false);
        return verdict switch
        {
            HistoryVerdict.LocalWrite => indeterminate,
            HistoryVerdict.OnlyRemoteWrites => notCommittedOnceExpired,
            _ => expired ? indeterminate : null,
        };
    }

    private static async Task<HistoryVerdict> ReadHistoryAsync(
        IGrainFactory grainFactory,
        LatticeAdminAuditRecord intent,
        LatticeAdminAuditAnchor anchor,
        AdminAuditAnchorObservation observed,
        CancellationToken cancellationToken)
    {
        var from = intent.AnchorBaseVersion
            ?? new HybridLogicalClock { WallClockTicks = (intent.TimestampUtc - ClockSkewAllowance).UtcTicks };
        var lattice = grainFactory.GetGrain<ILattice>(anchor.TreeId);

        string? continuation = null;
        var sawCurrent = false;
        EntryRevision? newest = null;
        EntryHistorySource source = EntryHistorySource.None;
        var truncated = false;

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            for (var page = 0; page < MaxPages; page++)
            {
                var result = await lattice
                    .ScanEntryHistoryAsync(anchor.Key, from, null, PageSize, continuation, cancellationToken)
                    .ConfigureAwait(false);
                source = result.Source;
                truncated |= result.Truncated && result.EarliestAvailable.CompareTo(from) > 0;

                foreach (var revision in result.Revisions)
                {
                    if (intent.AnchorBaseVersion is { } baseVersion && revision.Hlc.CompareTo(baseVersion) <= 0)
                    {
                        continue;
                    }

                    if (revision.OriginClusterId is null)
                    {
                        return HistoryVerdict.LocalWrite;
                    }

                    if (observed.Version is { } current && revision.Hlc.Equals(current))
                    {
                        sawCurrent = true;
                    }

                    newest = revision;
                }

                continuation = result.Continuation;
                if (continuation is null)
                {
                    break;
                }
            }
        }

        if (truncated || continuation is not null || source == EntryHistorySource.None)
        {
            return HistoryVerdict.Unproven;
        }

        // Complete only when the history reaches the anchor's present state: its current
        // version, or (anchor gone) a delete as the newest revision. A write-ahead-log
        // window is read synchronously with the commit, so an empty, untruncated window
        // is complete too; a history view may lag, so it never proves an absence alone.
        var complete = observed.Version is not null
            ? sawCurrent
            : newest is { Kind: HistoryRowKind.Delete or HistoryRowKind.RangeTombstone }
              || (newest is null && source == EntryHistorySource.WalWindow);

        return complete ? HistoryVerdict.OnlyRemoteWrites : HistoryVerdict.Unproven;
    }

    private enum HistoryVerdict
    {
        Unproven,
        LocalWrite,
        OnlyRemoteWrites,
    }
}
