using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Tenancy;
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

    private readonly IServiceProvider _services;
    private readonly TimeProvider _time;
    private readonly ReplicationOptions _options;
    private readonly IExplorerAuthSession? _auth;
    private readonly IExplorerSession? _session;
    private readonly ShellAssertedTenant _tenant;
    private readonly object _gate = new();
    private string? _memoTenant;
    private (ReplicationRead<ReplicationEstate> Read, DateTimeOffset At)? _estate;
    private (ReplicationRead<ReplicationConfigReport> Read, DateTimeOffset At)? _config;
    private long _generation;

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

    private ILatticeReplicationStatus? Status => _services.GetShellFacade<ILatticeReplicationStatus>();

    private ILatticeReplicationControl? Control => _services.GetShellFacade<ILatticeReplicationControl>();

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
        lock (_gate)
        {
            ForgetIfTenantChanged();
            if (!refresh && Fresh(_estate?.At))
            {
                return _estate!.Value.Read;
            }

            generation = _generation;
        }

        var read = await ReadLinksAsync(treeId: null, cancellationToken).ConfigureAwait(false);
        lock (_gate)
        {
            ForgetIfTenantChanged();
            if (generation == _generation)
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
        lock (_gate)
        {
            ForgetIfTenantChanged();
            if (!refresh && Fresh(_config?.At))
            {
                return _config!.Value.Read;
            }

            generation = _generation;
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
            ForgetIfTenantChanged();
            if (generation == _generation)
            {
                _config = (read, _time.GetUtcNow());
            }
        }

        return read;
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
    /// remembered, when the circuit asserts a different tenant from the one they
    /// were read under. Call under the gate.
    /// </summary>
    private void ForgetIfTenantChanged()
    {
        var tenant = _tenant.AssertedTenant;
        if (!ShellAssertedTenant.Same(_memoTenant, tenant))
        {
            _memoTenant = tenant;
            _estate = null;
            _config = null;
            _generation++;
        }
    }

    private bool Fresh(DateTimeOffset? at) => at is { } read && _time.GetUtcNow() - read < _options.CacheLifetime;

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

    /// <summary>
    /// Whether the estate for <paramref name="tenant"/> shows a link of
    /// <paramref name="treeId"/>. At the default tenant a link's tree id is in the
    /// cluster's ownership grammar, so only the default tenant's own trees are kept.
    /// Under another tenant the status report names that tenant's own trees by
    /// their bare names, which cannot be told from the default tenant's, so a bare
    /// name is kept and only a system tree or another tenant's qualified tree is
    /// dropped: a tenant's own link is never hidden.
    /// </summary>
    /// <param name="tenant">The listing tenant, or <see langword="null"/> with tenancy off.</param>
    /// <param name="treeId">The link's tree id.</param>
    /// <returns><see langword="true"/> when the estate shows the link.</returns>
    internal static bool ListsLink(string? tenant, string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        if (string.IsNullOrEmpty(tenant) || string.Equals(tenant, ExplorerTenantTrees.DefaultTenantId, StringComparison.Ordinal))
        {
            return ShellAssertedTenant.Lists(tenant, treeId);
        }

        return ExplorerTenantTrees.TryGetOwner(treeId, out var owner)
            && (owner == ExplorerTenantId.Default || owner.Value == tenant);
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
            // address, are read as addressed.
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
                    && (treeId is not null || ListsLink(tenant, link.TreeId))));
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
