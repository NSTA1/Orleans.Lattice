using System.Globalization;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>
/// The one place the Explorer creates a tree (G4, issue #5368): whether the caller
/// may create trees at all, the name and sizing rules the form checks, the
/// pre-flight a review runs on one name, and the create itself. The Cluster tree
/// list's <b>New tree...</b> dialog drives it, and any other surface that needs a
/// tree to exist first - the Data area's <b>New tree from file...</b> - drives the
/// same service, so every path refuses the same names for the same reasons.
/// </summary>
/// <remarks>
/// <para>
/// Every answer is advisory and fails closed: a refused, unserved or failed probe
/// reads as "no", and the cluster still authorises the create itself
/// (<see cref="ILatticeTreeAdmin.CreateTreeAsync"/> needs whole-tree administration
/// over the exact id, and a tenant's tree-count quota is charged at that seam).
/// </para>
/// <para>
/// Whether the caller may create trees at all is asked of a sentinel tree id that
/// is never created: a caller administering every tree (or every tree of its
/// tenant) is admitted by it. A caller whose administration is scoped to some trees
/// is admitted when it administers one of the first few listed trees, and the
/// review then checks the exact name it typed. The answer is remembered for the
/// caller it was read for (<see cref="ClusterFacades.Caller"/>) for
/// <see cref="Freshness"/>, so a sign-in, an endpoint or a tenant switch asks again.
/// </para>
/// </remarks>
internal sealed class ClusterTreeCreation
{
    /// <summary>The tree id the area-wide probe asks about. The probe has no side effects, and no tree of this name is ever created.</summary>
    public const string ProbeTreeId = "__cluster_create_tree_probe__";

    /// <summary>The library's default physical shard count, which the form starts from.</summary>
    public const int DefaultShardCount = 64;

    /// <summary>The library's default maximum keys per leaf node, which the form starts from.</summary>
    public const int DefaultMaxLeafKeys = 128;

    /// <summary>The library's default maximum children per internal node, which the form starts from.</summary>
    public const int DefaultMaxInternalChildren = 128;

    /// <summary>The most physical shards a tree may start with: one per virtual routing slot.</summary>
    public const int MaximumShardCount = 4096;

    /// <summary>The fewest keys per leaf node, the bound resize and snapshot also hold.</summary>
    public const int MinimumMaxLeafKeys = 2;

    /// <summary>The fewest children per internal node, the bound resize and snapshot also hold.</summary>
    public const int MinimumMaxInternalChildren = 3;

    /// <summary>How many listed trees the area-wide answer probes after the sentinel refuses.</summary>
    public const int MaximumListedProbes = 8;

    /// <summary>The quota dimension a tenant's tree count is refused under.</summary>
    internal const string TreeCountDimension = "trees";

    /// <summary>How long the area-wide answer is reused.</summary>
    public static readonly TimeSpan Freshness = TimeSpan.FromSeconds(30);

    private const string SystemTreePrefix = "_lattice_";
    private const string TenantTreePrefix = "t/";

    private readonly ClusterFacades _facades;
    private readonly ClusterTreeCatalog _catalog;
    private readonly ClusterTreeChanges _changes;
    private readonly IServiceProvider _services;
    private readonly TimeProvider _time;
    private (bool Answer, ShellCallerKey Caller, DateTimeOffset ReadAt)? _canCreate;
    private TreeCreationResult? _created;

    /// <summary>Creates the service for one circuit.</summary>
    /// <param name="facades">The Cluster area's facades.</param>
    /// <param name="catalog">The circuit's tree catalogue.</param>
    /// <param name="changes">Forgets every remembered tree list once a tree is created.</param>
    /// <param name="services">The circuit's services, from which the tenant quota facade is resolved optionally.</param>
    /// <param name="time">The clock freshness is measured on.</param>
    public ClusterTreeCreation(ClusterFacades facades, ClusterTreeCatalog catalog, ClusterTreeChanges changes, IServiceProvider services, TimeProvider time)
    {
        ArgumentNullException.ThrowIfNull(facades);
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(changes);
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(time);
        _facades = facades;
        _catalog = catalog;
        _changes = changes;
        _services = services;
        _time = time;
    }

    /// <summary>
    /// The tenant a tree created now lands in, for the review to name: the tenant a
    /// tenant-rooted address names, else the tenant the circuit asserts. The
    /// reserved default tenant, and no tenancy at all, are <see langword="null"/>.
    /// </summary>
    /// <param name="scope">The tenant a tenant-rooted address names, or <see langword="null"/>.</param>
    /// <returns>The tenant, or <see langword="null"/>.</returns>
    public string? TenantFor(string? scope) => ClusterTreeCatalog.NarrowingTenant(scope ?? _facades.AssertedTenant);

    /// <summary>Whether the caller may create trees on this cluster, failing closed.</summary>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns><see langword="true"/> only when a probe admitted the caller.</returns>
    public async Task<bool> CanCreateAsync(CancellationToken cancellationToken = default)
    {
        var caller = _facades.Caller;
        if (_canCreate is { } remembered && remembered.Caller == caller && _time.GetUtcNow() - remembered.ReadAt < Freshness)
        {
            return remembered.Answer;
        }

        if (_facades.TreeAdmin is not { } admin || !_facades.IsConnected)
        {
            return false;
        }

        var answer = await AdministersAsync(admin, ProbeTreeId, cancellationToken).ConfigureAwait(false)
            || await AdministersAListedTreeAsync(admin, cancellationToken).ConfigureAwait(false);

        // Remembered only for the caller it was read for, and only while that is still the caller.
        if (_facades.Caller == caller)
        {
            _canCreate = (answer, caller, _time.GetUtcNow());
        }

        return answer;
    }

    /// <summary>
    /// Checks a typed tree name against the reserved namespaces the cluster refuses
    /// and the shape the Explorer can address, and says what is wrong with it.
    /// </summary>
    /// <param name="text">The name as typed.</param>
    /// <returns>The reason it cannot be created, or <see langword="null"/> when it is acceptable.</returns>
    public static string? ValidateTreeId(string? text)
    {
        var treeId = text?.Trim();
        if (string.IsNullOrEmpty(treeId))
        {
            return "Name the tree.";
        }

        // The cluster's own wording (LatticeTreeAdmin.ThrowIfReserved), so the form
        // and the facade refuse a reserved name for the same reason.
        if (treeId.StartsWith(SystemTreePrefix, StringComparison.Ordinal))
        {
            return $"Tree id '{treeId}' is reserved: the '{SystemTreePrefix}' namespace is managed by the library.";
        }

        if (treeId.StartsWith(TenantTreePrefix, StringComparison.Ordinal))
        {
            return $"Tree id '{treeId}' is reserved: the '{TenantTreePrefix}' namespace is the structural tenant namespace. Name the tree without it; the tenant you are working in is added for you.";
        }

        if (treeId.Split('/').Any(string.IsNullOrEmpty))
        {
            return "Each part of a tree name, between slashes, needs at least one character.";
        }

        return ClusterAddresses.TryTree(treeId, ClusterTreeView.Overview, out _)
            ? null
            : $"The Explorer cannot address a tree by that name. Use at most {ClusterAddresses.MaximumPathSegments - 2} parts separated by slashes.";
    }

    /// <summary>Reads a whole number typed into a sizing field.</summary>
    /// <param name="text">The text typed.</param>
    /// <param name="minimum">The least value accepted.</param>
    /// <param name="maximum">The greatest value accepted.</param>
    /// <param name="value">The number, when it is in range.</param>
    /// <returns><see langword="true"/> for a whole number from <paramref name="minimum"/> to <paramref name="maximum"/>.</returns>
    public static bool TryParseSize(string? text, int minimum, int maximum, out int value) =>
        int.TryParse(text?.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out value)
        && value >= minimum
        && value <= maximum;

    /// <summary>
    /// The review's pre-flight for one name: whether the caller administers a tree
    /// of that name, whether one already exists, and, under a tenant, whether its
    /// tree quota has room. Each check that cannot be answered is left to the
    /// cluster, which still refuses the create itself.
    /// </summary>
    /// <param name="request">The staged tree.</param>
    /// <param name="scope">The tenant a tenant-rooted address names, or <see langword="null"/>.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>Why the tree cannot be created, or <see langword="null"/> when nothing stands in the way.</returns>
    public async Task<string?> CheckAsync(ClusterTreeCreationRequest request, string? scope, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (_facades.TreeAdmin is not { } admin)
        {
            return "This Explorer does not serve tree administration.";
        }

        if (!await AdministersAsync(admin, request.TreeId, cancellationToken).ConfigureAwait(false))
        {
            return $"You do not hold the administration grant over a tree named {request.TreeId}, so the cluster would refuse to create it.";
        }

        if (await ExistsAsync(admin, request.TreeId, cancellationToken).ConfigureAwait(false))
        {
            return $"A tree named {request.TreeId} already exists. Creating it again would change nothing; choose another name.";
        }

        return TenantFor(scope) is { } tenant && await TreeQuotaAsync(tenant, cancellationToken).ConfigureAwait(false) is { } trees && AtCeiling(trees)
            ? QuotaRefusal(tenant, trees.Usage, trees.Limit)
            : null;
    }

    /// <summary>
    /// Creates the tree, and on success forgets every remembered tree list so the
    /// Cluster and Data areas list it at once. A tree that already exists, a quota
    /// refusal, a denial and any other fault each come back as one sentence.
    /// </summary>
    /// <param name="request">The staged tree.</param>
    /// <param name="scope">The tenant a tenant-rooted address names, or <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>What the create came to.</returns>
    public async Task<ClusterTreeCreationOutcome> CreateAsync(ClusterTreeCreationRequest request, string? scope, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        TreeCreationResult? result;
        try
        {
            result = await _facades.RequireTreeAdmin()
                .CreateTreeAsync(request.TreeId, request.ShardCount, request.MaxLeafKeys, request.MaxInternalChildren, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (LatticeQuotaExceededException quota)
        {
            return ClusterTreeCreationOutcome.Refused(
                string.Equals(quota.Dimension, TreeCountDimension, StringComparison.Ordinal)
                    ? QuotaRefusal(TenantFor(scope) ?? quota.TenantId, quota.Current, quota.Limit)
                    : quota.Message);
        }
        catch (Exception exception) when (ClusterFaults.IsDenied(exception))
        {
            return ClusterTreeCreationOutcome.Refused($"You do not have permission to create a tree named {request.TreeId}.");
        }
        catch (Exception exception)
        {
            return ClusterTreeCreationOutcome.Refused(ClusterFaults.Describe(exception));
        }

        if (result is null)
        {
            return ClusterTreeCreationOutcome.Refused("The cluster could not complete the request.");
        }

        if (!result.Created)
        {
            // The facade's create is idempotent: a name already registered is
            // reported, not refused, and its configuration is left as it was.
            return ClusterTreeCreationOutcome.Refused(
                $"The cluster already has a tree named {request.TreeId}, so nothing was created and its configuration is unchanged.");
        }

        _changes.Changed();
        _created = result;
        return ClusterTreeCreationOutcome.Created(result);
    }

    /// <summary>
    /// Takes the tree this circuit created last, once, when it is <paramref name="treeId"/>,
    /// so the tree's page can offer what to do next on arrival and never again.
    /// </summary>
    /// <param name="treeId">The tree the page shows.</param>
    /// <param name="result">The cluster's answer to the create.</param>
    /// <returns><see langword="true"/> when the page shows the tree just created.</returns>
    public bool TryTakeCreated(string treeId, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TreeCreationResult? result)
    {
        result = null;
        if (_created is not { } created || !string.Equals(created.TreeId, treeId, StringComparison.Ordinal))
        {
            return false;
        }

        _created = null;
        result = created;
        return true;
    }

    /// <summary>The sentence a tenant at its tree quota is told.</summary>
    /// <param name="tenant">The tenant, or <see langword="null"/> when it is not known.</param>
    /// <param name="used">Its trees, when known.</param>
    /// <param name="limit">Its tree ceiling, when known.</param>
    /// <returns>The sentence.</returns>
    internal static string QuotaRefusal(string? tenant, long? used, long? limit)
    {
        var who = string.IsNullOrEmpty(tenant) ? "This tenant" : "Tenant " + tenant;
        var figures = limit is { } ceiling && ceiling > 0
            ? string.Create(CultureInfo.InvariantCulture, $": {ClusterFormat.Count(Math.Min(used ?? ceiling, ceiling))} of {ClusterFormat.Plural(ceiling, "tree")}")
            : string.Empty;
        return $"{who} is at its tree quota{figures}. Delete a tree, or ask a platform operator to raise the quota.";
    }

    // A tenant's tree count is refused once it would pass the burst-adjusted
    // ceiling admission engages at, so that is the ceiling the review compares.
    private static bool AtCeiling(TenantQuotaDimensionUsage trees) =>
        trees.Usage is { } used && (trees.BurstLimit ?? trees.Limit) is { } ceiling && used >= ceiling;

    private static async Task<bool> AdministersAsync(ILatticeTreeAdmin admin, string treeId, CancellationToken cancellationToken)
    {
        try
        {
            var capabilities = await admin.ProbeCapabilitiesAsync(treeId, cancellationToken).ConfigureAwait(false);
            return capabilities?.CanAdministerTree == true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            // Fail closed: a probe that cannot answer grants nothing.
            return false;
        }
    }

    private async Task<bool> AdministersAListedTreeAsync(ILatticeTreeAdmin admin, CancellationToken cancellationToken)
    {
        IReadOnlyList<ClusterTreeEntry> trees;
        try
        {
            trees = await _catalog.GetAsync(refresh: false, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return false;
        }

        var probed = 0;
        foreach (var tree in trees)
        {
            if (tree.Lifecycle != TreeLifecycleState.Active)
            {
                continue;
            }

            if (await AdministersAsync(admin, tree.TreeId, cancellationToken).ConfigureAwait(false))
            {
                return true;
            }

            if (++probed == MaximumListedProbes)
            {
                break;
            }
        }

        return false;
    }

    // The cluster answers for any tree the caller may read; a caller that may not,
    // or a fault, falls back to the listed trees, and the create still reports a
    // name that already exists.
    private async Task<bool> ExistsAsync(ILatticeTreeAdmin admin, string treeId, CancellationToken cancellationToken)
    {
        TreeExistenceResult? answer = null;
        try
        {
            answer = await admin.CheckTreeExistsAsync(treeId, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            // Answered from the listed trees below.
        }

        if (answer is not null)
        {
            return answer.Exists;
        }

        try
        {
            var trees = await _catalog.GetAsync(refresh: false, cancellationToken).ConfigureAwait(false);
            return trees.Any(tree => string.Equals(tree.TreeId, treeId, StringComparison.Ordinal));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return false;
        }
    }

    private async Task<TenantQuotaDimensionUsage?> TreeQuotaAsync(string tenant, CancellationToken cancellationToken)
    {
        if (string.Equals(tenant, ExplorerTenantTrees.DefaultTenantId, StringComparison.Ordinal)
            || _services.GetShellFacade<ILatticeTenantQuotaUsage>() is not { } quota)
        {
            return null;
        }

        try
        {
            return (await quota.GetQuotaUsageAsync(tenant, cancellationToken).ConfigureAwait(false)).TreeCount;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            // The cluster still charges the quota at the create; an unreadable
            // reading only means the review cannot warn ahead of it.
            return null;
        }
    }
}
