using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Replication;

/// <summary>
/// The Replication area's one door to the cluster, scoped per circuit: it reads the
/// peer status report (paging <see cref="ILatticeReplicationStatus"/> to its end)
/// and the enrolment report (<see cref="ILatticeReplicationControl"/>), keeps the
/// last of each briefly for the directory badge, Home and completions, and forgets
/// both whenever the connection, the signed-in identity or the asserted tenant
/// changes.
/// </summary>
/// <remarks>
/// Both facades are resolved optionally, so a host that serves neither leaves the
/// area hidden rather than faulting. Every facade fault is classified into a fixed
/// sentence (<see cref="ReplicationFault"/>); a cancellation is the only exception
/// that escapes a read.
/// </remarks>
internal sealed class ReplicationDataSource : IDisposable
{
    /// <summary>What the status read is called in a fault sentence.</summary>
    public const string StatusSubject = "replication status";

    /// <summary>What the enrolment read is called in a fault sentence.</summary>
    public const string ConfigSubject = "replication enrolment";

    /// <summary>
    /// The reserved tree id the enrolment probe asks about, so the answer is whether the
    /// caller may enable a tree it has not named yet. The probe has no side effects.
    /// </summary>
    public const string ProbeTreeId = "__replication_capability_probe__";

    private readonly IServiceProvider _services;
    private readonly TimeProvider _time;
    private readonly ReplicationOptions _options;
    private readonly IExplorerAuthSession? _auth;
    private readonly IExplorerSession? _session;
    private readonly ShellAssertedTenant _tenant;
    private readonly ShellCaller _caller;
    private readonly object _gate = new();
    private ShellCallerKey _memoCaller;
    private (ReplicationRead<ReplicationEstate> Read, DateTimeOffset At)? _estate;
    private (ReplicationRead<ReplicationConfigReport> Read, DateTimeOffset At)? _config;
    private (bool CanEnable, DateTimeOffset At)? _canEnable;
    private long _generation;
    private long _estateRead;
    private long _configRead;
    private long _probeRead;

    /// <summary>Creates the data source over the circuit's services.</summary>
    /// <param name="services">The circuit's service provider, which the facades are resolved from.</param>
    /// <param name="time">The clock the cache is measured on.</param>
    /// <param name="options">The cadences and read bounds.</param>
    public ReplicationDataSource(IServiceProvider services, TimeProvider time, ReplicationOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(time);
        ArgumentNullException.ThrowIfNull(options);
        _services = services;
        _time = time;
        _options = options;

        _auth = services.GetService<IExplorerAuthSession>();
        _session = services.GetService<IExplorerSession>();
        _tenant = services.GetService<ShellAssertedTenant>() ?? ShellAssertedTenant.None;
        _caller = ShellCaller.Of(services);
        if (_auth is not null)
        {
            _auth.AuthenticationChanged += Invalidate;
        }

        if (_session is not null)
        {
            _session.ConfigurationChanged += Invalidate;
        }
    }

    /// <summary>Raised after the cached reads are forgotten, so a page can read again.</summary>
    public event Action? Invalidated;

    /// <summary>Whether a status facade is registered at all.</summary>
    public bool HasStatus => Status is not null;

    /// <summary>Whether an enrolment facade is registered at all.</summary>
    public bool HasControl => Control is not null;

    /// <summary>Whether this circuit can read backfill progress for its asserted tenant.</summary>
    public bool HasTenantBackfill => RegionAdmin is not null && !string.IsNullOrEmpty(_tenant.ListingTenant);

    private ILatticeReplicationStatus? Status => _services.GetShellFacade<ILatticeReplicationStatus>();

    private ILatticeReplicationControl? Control => _services.GetShellFacade<ILatticeReplicationControl>();

    private ILatticeTenantRegionAdmin? RegionAdmin => _services.GetShellFacade<ILatticeTenantRegionAdmin>();

    /// <summary>Reads tenant residency and its local-region backfill progress.</summary>
    public Task<TenantRegionStatusReport> GetTenantRegionStatusAsync(CancellationToken cancellationToken)
    {
        var regionAdmin = RegionAdmin ?? throw new NotSupportedException("Tenant residency is not registered.");
        var tenantId = _tenant.ListingTenant;
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return regionAdmin.GetTenantRegionStatusAsync(tenantId, cancellationToken);
    }

    /// <summary>
    /// Every link this region reports. A cached read younger than
    /// <see cref="ReplicationOptions.CacheLifetime"/> is reused unless
    /// <paramref name="refresh"/> is set.
    /// </summary>
    /// <param name="refresh">Whether to read afresh.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    public async Task<ReplicationRead<ReplicationEstate>> GetEstateAsync(bool refresh, CancellationToken cancellationToken)
    {
        long generation;
        long request;
        lock (_gate)
        {
            ForgetIfTheCallerChanged();
            if (!refresh && Fresh(_estate?.At))
            {
                return _estate!.Value.Read;
            }

            generation = _generation;
            request = ++_estateRead;
        }

        var read = await ReadLinksAsync(treeId: null, cancellationToken).ConfigureAwait(false);
        lock (_gate)
        {
            ForgetIfTheCallerChanged();
            if (generation == _generation && request == _estateRead && !cancellationToken.IsCancellationRequested && Remembers(read.Fault))
            {
                _estate = (read, _time.GetUtcNow());
            }
        }

        return read;
    }

    /// <summary>One tree's links, always read afresh.</summary>
    /// <param name="treeId">The logical tree id.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    public Task<ReplicationRead<ReplicationEstate>> GetTreeLinksAsync(string treeId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return ReadLinksAsync(treeId, cancellationToken);
    }

    /// <summary>
    /// The trees the caller may manage, with their enrolment. A cached read is
    /// reused as for <see cref="GetEstateAsync"/>.
    /// </summary>
    /// <param name="refresh">Whether to read afresh.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    public async Task<ReplicationRead<ReplicationConfigReport>> GetConfigAsync(bool refresh, CancellationToken cancellationToken)
    {
        long generation;
        long request;
        lock (_gate)
        {
            ForgetIfTheCallerChanged();
            if (!refresh && Fresh(_config?.At))
            {
                return _config!.Value.Read;
            }

            generation = _generation;
            request = ++_configRead;
        }

        ReplicationRead<ReplicationConfigReport> read;
        var tenant = _tenant.ListingTenant;
        if (Control is not { } control)
        {
            read = ReplicationRead<ReplicationConfigReport>.Failure(ReplicationFault.NotServed(ConfigSubject));
        }
        else
        {
            try
            {
                var report = await control.GetReplicationConfigAsync(cancellationToken).ConfigureAwait(false) ?? ReplicationConfigReport.Empty;
                read = ReplicationRead<ReplicationConfigReport>.Success(ScopeToTenant(report, tenant));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                read = ReplicationRead<ReplicationConfigReport>.Failure(ReplicationFault.From(ex, ConfigSubject));
            }
        }

        lock (_gate)
        {
            ForgetIfTheCallerChanged();
            if (generation == _generation && request == _configRead && !cancellationToken.IsCancellationRequested && Remembers(read.Fault))
            {
                _config = (read, _time.GetUtcNow());
            }
        }

        return read;
    }

    /// <summary>
    /// Whether the caller may enable replication for a tree it has not named yet: the
    /// enrolment facade's capability probe over <see cref="ProbeTreeId"/> (issue #5478).
    /// The enrolment report cannot answer this, because it is readable by every caller
    /// and silently omits the trees they may not manage. Fails closed: no facade, a
    /// refusal and any fault read as <see langword="false"/>, and only an answer the
    /// cluster gave is remembered, as for <see cref="GetConfigAsync"/>. The answer is
    /// presentation only; the facade authorizes every enable on attempt.
    /// </summary>
    /// <param name="refresh">Whether to probe afresh.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns>Whether the caller holds the Replication capability over the probe tree.</returns>
    public async Task<bool> CanEnableAsync(bool refresh, CancellationToken cancellationToken)
    {
        long generation;
        long request;
        lock (_gate)
        {
            ForgetIfTheCallerChanged();
            if (!refresh && Fresh(_canEnable?.At))
            {
                return _canEnable!.Value.CanEnable;
            }

            generation = _generation;
            request = ++_probeRead;
        }

        if (Control is not { } control)
        {
            return false;
        }

        bool canEnable;
        try
        {
            var capabilities = await control.ProbeCapabilitiesAsync(ProbeTreeId, cancellationToken).ConfigureAwait(false);
            canEnable = capabilities?.CanConfigure == true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return false;
        }

        lock (_gate)
        {
            ForgetIfTheCallerChanged();
            if (generation == _generation && request == _probeRead && !cancellationToken.IsCancellationRequested)
            {
                _canEnable = (canEnable, _time.GetUtcNow());
            }
        }

        return canEnable;
    }

    /// <summary>Enables replication for a tree through the enrolment facade, then forgets the cached reads.</summary>
    /// <param name="treeId">The logical tree id.</param>
    /// <param name="mode">The merge mode to fix.</param>
    /// <param name="bootstrapSourceClusterId">The cluster to bootstrap a snapshot from, or <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <exception cref="NotSupportedException">No enrolment facade is registered.</exception>
    public async Task<ReplicationEnableResult> EnableAsync(
        string treeId,
        LatticeMergeMode mode,
        string? bootstrapSourceClusterId,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var control = Control ?? throw new NotSupportedException("No replication enrolment facade is registered.");
        try
        {
            return await control.EnableReplicationAsync(
                treeId,
                mode,
                string.IsNullOrWhiteSpace(bootstrapSourceClusterId) ? null : bootstrapSourceClusterId.Trim(),
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Invalidate();
        }
    }

    /// <summary>Disables replication for a tree through the enrolment facade, then forgets the cached reads.</summary>
    /// <param name="treeId">The logical tree id.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <exception cref="NotSupportedException">No enrolment facade is registered.</exception>
    public async Task<ReplicationDisableResult> DisableAsync(string treeId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var control = Control ?? throw new NotSupportedException("No replication enrolment facade is registered.");
        try
        {
            return await control.DisableReplicationAsync(treeId, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Invalidate();
        }
    }

    /// <summary>Forgets both cached reads.</summary>
    public void Invalidate()
    {
        lock (_gate)
        {
            _estate = null;
            _config = null;
            _canEnable = null;
            _generation++;
        }

        Invalidated?.Invoke();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_auth is not null)
        {
            _auth.AuthenticationChanged -= Invalidate;
        }

        if (_session is not null)
        {
            _session.ConfigurationChanged -= Invalidate;
        }
    }

    /// <summary>
    /// Forgets both reads, and moves the generation on so a read in flight is not
    /// remembered, when the caller (sign-in, endpoint or asserted tenant) differs
    /// from the one they were read for. Call under the gate.
    /// </summary>
    private void ForgetIfTheCallerChanged()
    {
        var caller = _caller.Current;
        if (_memoCaller != caller)
        {
            _memoCaller = caller;
            _estate = null;
            _config = null;
            _canEnable = null;
            _generation++;
        }
    }

    private bool Fresh(DateTimeOffset? at) => at is { } read && _time.GetUtcNow() - read < _options.CacheLifetime;

    // A read the cluster did not answer proved nothing, so the next read asks
    // again rather than reusing it (issue #5420); every other outcome is reused.
    private static bool Remembers(ReplicationFault? fault) => fault is not { IsTransient: true };

    /// <summary>
    /// The enrolment report narrowed to the trees a listing under
    /// <paramref name="tenant"/> shows. The area is tenant-scoped, and the cluster
    /// hands the reserved default tenant every tenant's trees and its system trees.
    /// </summary>
    /// <param name="report">The report as the facade returned it.</param>
    /// <param name="tenant">The tenant it was read under, or <see langword="null"/>.</param>
    /// <returns>The narrowed report; the same instance when nothing is dropped.</returns>
    internal static ReplicationConfigReport ScopeToTenant(ReplicationConfigReport report, string? tenant)
    {
        ArgumentNullException.ThrowIfNull(report);
        if (report.Trees.All(tree => ShellAssertedTenant.Lists(tenant, tree.TreeId)))
        {
            return report;
        }

        return report with { Trees = [.. report.Trees.Where(tree => ShellAssertedTenant.Lists(tenant, tree.TreeId))] };
    }

    private async Task<ReplicationRead<ReplicationEstate>> ReadLinksAsync(string? treeId, CancellationToken cancellationToken)
    {
        if (Status is not { } status)
        {
            return ReplicationRead<ReplicationEstate>.Failure(ReplicationFault.NotServed(StatusSubject));
        }

        try
        {
            // The estate is this tenant's links; one tree's links, asked for by its
            // address, are read as addressed. Both reports name a tenant's own trees
            // by their qualified t/{tenant}/{name} id (#4000), so the estate is scoped
            // by ownership exactly as the enrolment report is.
            var tenant = _tenant.ListingTenant;
            var links = new List<ReplicationPeerStatusEntry>();
            var seen = new HashSet<(string, string, ReplicationLinkDirection)>();
            var tokens = new HashSet<string>(StringComparer.Ordinal);
            var localRegion = string.Empty;
            string? token = null;
            var truncated = true;

            for (var page = 0; page < _options.MaxPages; page++)
            {
                var result = await status.GetPeerStatusAsync(
                    new ReplicationPeerStatusQuery { TreeId = treeId, PageSize = _options.PageSize, ContinuationToken = token },
                    cancellationToken).ConfigureAwait(false);

                localRegion = result.LocalRegionId;

                // One row per (tree, peer, direction): a page served twice adds nothing.
                links.AddRange(result.Peers.Where(link => seen.Add((link.TreeId, link.PeerRegionId, link.Direction))
                    && (treeId is not null || ShellAssertedTenant.Lists(tenant, link.TreeId))));
                token = result.ContinuationToken;
                if (token is null)
                {
                    truncated = false;
                    break;
                }

                if (!tokens.Add(token))
                {
                    // A token that repeats would page forever; what was read stands.
                    break;
                }
            }

            return ReplicationRead<ReplicationEstate>.Success(new ReplicationEstate(localRegion, links, truncated, _time.GetUtcNow()));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return ReplicationRead<ReplicationEstate>.Failure(ReplicationFault.From(ex, StatusSubject));
        }
    }
}
