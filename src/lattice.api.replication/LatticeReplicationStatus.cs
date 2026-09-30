using Microsoft.Extensions.Options;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// Default <see cref="ILatticeReplicationStatus"/> implementation. Registered as
/// a silo singleton by <c>AddLatticeReplicationStatusApi</c>. It reads the
/// cluster-wide per-peer telemetry through the internal
/// <see cref="IReplicationPeerStatusReader"/> (which never touches the ship or
/// apply path), authorizes every reported tree through the shared
/// <see cref="ReplicationAccessAuthorizer"/> fail-closed, reports each tree by
/// its effective id, and derives each link's health.
/// </summary>
/// <remarks>
/// <para>
/// <b>Authorization.</b> The capability is the one
/// <see cref="ILatticeReplicationControl.GetReplicationConfigAsync"/> requires. A
/// denied tree's rows are silently omitted, so the report never reveals a tree
/// outside the caller's grant; a tree filter the caller may not manage yields an
/// empty page without any telemetry being read.
/// </para>
/// <para>
/// <b>Tree ids.</b> Every link names its tree by the effective id the telemetry
/// state records: a bare name for a default-tenant tree, and the tenant-qualified
/// <c>t/{tenant}/{name}</c> id for a tenant's own tree. That is the id
/// <see cref="ILatticeReplicationControl.GetReplicationConfigAsync"/> names the
/// same tree by, so the two reports join on tree id (issue #4000). A tenant still
/// sees only the trees the access gate admits to it; the report never widens that.
/// </para>
/// <para>
/// <b>Paging.</b> The read path returns rows in a single total order keyed on the
/// effective tree id, so the continuation token carries only a key the caller was
/// shown. Rows for trees the caller may not see are skipped by reading further
/// rather than by ending the page early, so a continuation is only ever issued
/// for an authorized row. The total work is bounded by the number of recorded
/// links, which is bounded by the replicated tree count times the peer count.
/// </para>
/// </remarks>
internal sealed class LatticeReplicationStatus : ILatticeReplicationStatus
{
    private readonly IReplicationPeerStatusReader _reader;
    private readonly ReplicationAccessAuthorizer _authorizer;
    private readonly ITenantContextResolver _tenantResolver;
    private readonly IOptionsMonitor<LatticeReplicationOptions> _replicationOptions;
    private readonly IOptionsMonitor<LatticeReplicationStatusOptions> _statusOptions;

    /// <summary>Initializes a new <see cref="LatticeReplicationStatus"/>.</summary>
    /// <param name="reader">The cluster-wide peer-status read path. Must not be <c>null</c>.</param>
    /// <param name="authorizer">The fail-closed replication authorization seam. Must not be <c>null</c>.</param>
    /// <param name="tenantResolver">The active-tenant resolver used to fail closed on an unresolvable caller and to scope a tree filter. Must not be <c>null</c>.</param>
    /// <param name="replicationOptions">The replication options, read for the local region id. Must not be <c>null</c>.</param>
    /// <param name="statusOptions">The health thresholds. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">A required dependency is <c>null</c>.</exception>
    public LatticeReplicationStatus(
        IReplicationPeerStatusReader reader,
        ReplicationAccessAuthorizer authorizer,
        ITenantContextResolver tenantResolver,
        IOptionsMonitor<LatticeReplicationOptions> replicationOptions,
        IOptionsMonitor<LatticeReplicationStatusOptions> statusOptions)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(tenantResolver);
        ArgumentNullException.ThrowIfNull(replicationOptions);
        ArgumentNullException.ThrowIfNull(statusOptions);
        _reader = reader;
        _authorizer = authorizer;
        _tenantResolver = tenantResolver;
        _replicationOptions = replicationOptions;
        _statusOptions = statusOptions;
    }

    /// <inheritdoc />
    public async Task<ReplicationPeerStatusPage> GetPeerStatusAsync(
        ReplicationPeerStatusQuery query,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        var pageSize = query.ResolvePageSize();
        var after = ReplicationPeerStatusContinuation.Decode(query.ContinuationToken);
        var localRegionId = _replicationOptions.CurrentValue.ClusterId ?? string.Empty;

        await EnsureTenantResolvesAsync(cancellationToken).ConfigureAwait(false);

        string? treeFilter = null;
        if (!string.IsNullOrEmpty(query.TreeId))
        {
            // Scope the caller-supplied, tenant-local name to the caller's tenant so
            // the authorization check and the read act on the SAME effective tree.
            treeFilter = await _tenantResolver
                .ResolveEffectiveTreeIdAsync(query.TreeId, cancellationToken)
                .ConfigureAwait(false);
            if (!await _authorizer.IsAuthorizedAsync(treeFilter, cancellationToken).ConfigureAwait(false))
            {
                return ReplicationPeerStatusPage.Empty(localRegionId);
            }
        }

        var peerFilter = string.IsNullOrEmpty(query.PeerRegionId) ? null : query.PeerRegionId;
        var options = _statusOptions.CurrentValue;
        var entries = new List<ReplicationPeerStatusEntry>(pageSize);
        var verdicts = treeFilter is null ? new Dictionary<string, bool>(StringComparer.Ordinal) : null;
        ReplicationPeerStatusRow? lastReturned = null;
        var more = false;

        // One extra row per read lets a full page learn whether anything follows it
        // without a second round trip in the common, fully-authorized case.
        var readLimit = Math.Min(pageSize + 1, ReplicationPeerStatusReadRequest.MaxLimit);

        while (!more)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var scanFrom = after;
            var request = new ReplicationPeerStatusReadRequest
            {
                TreeId = treeFilter,
                Peer = peerFilter,
                After = after,
                Limit = readLimit,
            };
            var rows = await _reader.ReadAsync(request, cancellationToken).ConfigureAwait(false);

            ReplicationPeerStatusRow? lastScanned = null;
            foreach (var row in rows)
            {
                lastScanned = row;
                if (verdicts is not null && !await IsVisibleAsync(verdicts, row.Tree, cancellationToken).ConfigureAwait(false))
                {
                    continue;
                }

                if (entries.Count == pageSize)
                {
                    more = true;
                    break;
                }

                entries.Add(ToEntry(row, options));
                lastReturned = row;
            }

            if (lastScanned is { } scanned)
            {
                after = ReplicationPeerStatusOrder.CursorAfter(scanned);
            }

            // A short read is the end of the report. A read that did not move the
            // scan forward would repeat forever, so it ends the report too.
            if (rows.Count < request.EffectiveLimit || after == scanFrom)
            {
                break;
            }
        }

        var token = more && lastReturned is { } last
            ? ReplicationPeerStatusContinuation.Encode(ReplicationPeerStatusOrder.CursorAfter(last))
            : null;
        return new ReplicationPeerStatusPage(localRegionId, entries, token);
    }

    private async ValueTask EnsureTenantResolvesAsync(CancellationToken cancellationToken)
    {
        var tenant = _tenantResolver.TryResolveCurrent(out var resolved)
            ? resolved
            : await _tenantResolver.ResolveCurrentAsync(cancellationToken).ConfigureAwait(false);

        // Fail closed: a resolver denies by resolving the uninitialised "no tenant" value.
        if (tenant.Value is null)
        {
            throw new LatticeTenantAccessDeniedException();
        }
    }

    private async ValueTask<bool> IsVisibleAsync(
        Dictionary<string, bool> verdicts,
        string tree,
        CancellationToken cancellationToken)
    {
        if (!verdicts.TryGetValue(tree, out var visible))
        {
            visible = await _authorizer.IsAuthorizedAsync(tree, cancellationToken).ConfigureAwait(false);
            verdicts[tree] = visible;
        }

        return visible;
    }

    private static ReplicationPeerStatusEntry ToEntry(
        in ReplicationPeerStatusRow row,
        LatticeReplicationStatusOptions options) =>
        new(
            row.Tree,
            row.Peer,
            row.Direction == ReplicationContactDirection.Inbound
                ? ReplicationLinkDirection.Inbound
                : ReplicationLinkDirection.Outbound,
            row.EntriesBehind,
            row.BytesBehind,
            row.ConsecutiveErrors,
            ToTimeSpan(row.LastContactSeconds),
            row.InFlight,
            ReplicationLinkHealthClassifier.Classify(row, options));

    private static TimeSpan? ToTimeSpan(double seconds)
    {
        if (double.IsNaN(seconds))
        {
            return null;
        }

        return seconds >= TimeSpan.MaxValue.TotalSeconds ? TimeSpan.MaxValue : TimeSpan.FromSeconds(Math.Max(0d, seconds));
    }
}
