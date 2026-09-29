using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// Turns the grants a tenant has received into the shared rows of its Data
/// directory. Only an <see cref="TenantGrantLifecycleState.Active"/> grant - one
/// the tenant approved - shares anything; an offer not yet approved, a rejected
/// offer and a revoked grant are left out, exactly as the cluster's tenant gate
/// ignores them.
/// </summary>
/// <remarks>
/// <para>
/// <b>Addressing.</b> A grant's scope is a full <c>t/{owner}/...</c> tree id, or a
/// <c>/</c>-ended prefix of one. The state API passes an already-qualified id
/// through without composing it into the caller's tenant, and the tenant gate
/// admits the crossing on the grant, so a shared tree is read by exactly that id.
/// The row keeps it as its logical id and roots its address at the caller's own
/// tenant: <c>/t/globex/data/t/acme/orders</c>.
/// </para>
/// <para>
/// <b>Fail closed.</b> A scope outside the granting tenant's own namespace - a
/// bare name, a malformed id, another tenant's id - authorizes nothing at the
/// gate, so it is never listed, and is counted instead so the directory can say
/// so. A shared id that collides with an owned tree's logical id is dropped:
/// the address would be ambiguous, and the tenant's own tree wins.
/// </para>
/// <para>
/// <b>Prefixes.</b> A prefix grant is shown as one prefix row. The catalogue only
/// lists the caller's own tenant's trees, so the trees under another tenant's
/// prefix cannot be enumerated; a tree under it is opened by its full id, and the
/// directory resolves such an address against the grant.
/// </para>
/// </remarks>
internal static class DataSharedTrees
{
    private const string TenantPrefix = ExplorerTenantTrees.SegmentPrefix;

    /// <summary>The shared rows and what could not be shown.</summary>
    /// <param name="Entries">The shared trees and prefixes, sorted by id.</param>
    /// <param name="Unreadable">How many approved grants name a scope outside their granting tenant's trees.</param>
    public readonly record struct Result(IReadOnlyList<DataTreeEntry> Entries, int Unreadable);

    /// <summary>Builds the shared rows <paramref name="report"/> gives <paramref name="tenant"/>.</summary>
    /// <param name="report">The tenant's grant report.</param>
    /// <param name="tenant">The tenant whose directory is being built, which the rows' addresses are rooted at.</param>
    /// <param name="ownedLogicalIds">The logical ids the tenant's own trees and views already take.</param>
    public static Result Build(TenantGrantReport report, string tenant, IReadOnlySet<string> ownedLogicalIds)
    {
        ArgumentNullException.ThrowIfNull(report);
        ArgumentException.ThrowIfNullOrEmpty(tenant);
        ArgumentNullException.ThrowIfNull(ownedLogicalIds);

        var byId = new Dictionary<string, DataTreeEntry>(StringComparer.Ordinal);
        var unreadable = 0;
        foreach (var grant in report.Received)
        {
            if (grant is null
                || grant.State != TenantGrantLifecycleState.Active
                || !string.Equals(grant.GranteeTenantId, tenant, StringComparison.Ordinal)
                || string.IsNullOrEmpty(grant.GranterTenantId)
                || string.Equals(grant.GranterTenantId, tenant, StringComparison.Ordinal)
                || (grant.Operations & TenantGrantAccess.ReadWrite) == TenantGrantAccess.None)
            {
                continue;
            }

            if (!TryDescribeScope(grant.GranterTenantId, grant.Scope, out var id, out var isPrefix))
            {
                unreadable++;
                continue;
            }

            if (ownedLogicalIds.Contains(id))
            {
                continue;
            }

            var access = grant.Operations & TenantGrantAccess.ReadWrite;
            if (byId.TryGetValue(id, out var existing))
            {
                // The same scope from the same granter is one grant; keep the wider reading.
                byId[id] = existing with { SharedAccess = existing.SharedAccess | access };
                continue;
            }

            byId.Add(id, new DataTreeEntry
            {
                LogicalId = id,
                StateId = id,
                Kind = isPrefix ? DataTreeKind.Prefix : DataTreeKind.Tree,
                Tenant = tenant,
                SharedBy = grant.GranterTenantId,
                SharedAccess = access,
            });
        }

        var entries = byId.Values.ToArray();
        Array.Sort(entries, static (left, right) => string.CompareOrdinal(left.LogicalId, right.LogicalId));
        return new Result(entries, unreadable);
    }

    /// <summary>
    /// Reads a grant's scope as a shared tree or prefix id. Returns
    /// <see langword="false"/> for a scope that is not inside the granting tenant's
    /// own <c>t/{granter}/</c> namespace, or that has an empty segment.
    /// </summary>
    /// <param name="granter">The granting tenant.</param>
    /// <param name="scope">The grant's scope.</param>
    /// <param name="id">The id to list: the tree id, or the prefix with its trailing <c>/</c>.</param>
    /// <param name="isPrefix">Whether the scope is a prefix rather than one tree.</param>
    public static bool TryDescribeScope(string granter, string? scope, out string id, out bool isPrefix)
    {
        ArgumentException.ThrowIfNullOrEmpty(granter);
        id = string.Empty;
        isPrefix = false;
        if (string.IsNullOrEmpty(scope))
        {
            return false;
        }

        var root = TenantPrefix + granter + "/";
        if (!scope.StartsWith(root, StringComparison.Ordinal))
        {
            return false;
        }

        isPrefix = scope[^1] == '/';
        var end = isPrefix ? scope.Length - 1 : scope.Length;
        var name = end <= root.Length ? [] : scope.AsSpan(root.Length, end - root.Length);

        // The whole tenant (t/{granter}/) is a prefix with no name under it.
        if (name.IsEmpty)
        {
            if (!isPrefix)
            {
                return false;
            }

            id = scope;
            return true;
        }

        if (name[0] == '_' || name.StartsWith("sys-", StringComparison.Ordinal))
        {
            return false;
        }

        foreach (var segment in name.Split('/'))
        {
            if (segment.End.Value == segment.Start.Value)
            {
                return false;
            }
        }

        id = scope;
        return true;
    }

    /// <summary>
    /// Finds the shared entry that makes <paramref name="logicalId"/> readable: the
    /// shared tree itself, or a shared tree or prefix that covers it at a segment
    /// boundary, as the cluster's grant resolution does. A covered id that is not
    /// itself listed gets an entry of its own, with the covering grant's owner and
    /// access.
    /// </summary>
    /// <param name="shared">The shared entries.</param>
    /// <param name="logicalId">The id an address names.</param>
    /// <returns>The entry to open, or <see langword="null"/> when no grant covers the id.</returns>
    public static DataTreeEntry? Cover(IEnumerable<DataTreeEntry> shared, string logicalId)
    {
        ArgumentNullException.ThrowIfNull(shared);
        ArgumentException.ThrowIfNullOrEmpty(logicalId);
        DataTreeEntry? covering = null;
        foreach (var entry in shared)
        {
            if (!entry.IsShared)
            {
                continue;
            }

            if (entry.Kind == DataTreeKind.Tree && string.Equals(entry.LogicalId, logicalId, StringComparison.Ordinal))
            {
                return entry;
            }

            if (covering is null && Covers(entry.LogicalId, logicalId))
            {
                covering = entry;
            }
        }

        if (covering is null || logicalId[^1] == '/'
            || !TryDescribeScope(covering.SharedBy!, logicalId, out _, out _))
        {
            return null;
        }

        return covering with { LogicalId = logicalId, StateId = logicalId, Kind = DataTreeKind.Tree };
    }

    /// <summary>What a grant allows, as a row reads it.</summary>
    /// <param name="access">The granted operations.</param>
    public static string AccessText(TenantGrantAccess access) => (access & TenantGrantAccess.ReadWrite) switch
    {
        TenantGrantAccess.ReadWrite => "Read and write",
        TenantGrantAccess.Write => "Write only",
        TenantGrantAccess.Read => "Read only",
        _ => "No access",
    };

    /// <summary>The note for a head that serves no grant facade.</summary>
    /// <param name="tenant">The tenant whose directory it is.</param>
    public static string NotOfferedNote(string tenant) =>
        $"Trees other tenants share with tenant {tenant} are not listed: this cluster does not let the Explorer list grants.";

    /// <summary>The note for a grant listing that failed, in a fixed sentence (a server message is never shown).</summary>
    /// <param name="exception">The failure.</param>
    /// <param name="tenant">The tenant whose directory it is.</param>
    public static string NoteFor(Exception exception, string tenant)
    {
        ArgumentNullException.ThrowIfNull(exception);
        if (DataErrors.IsDenied(exception))
        {
            return $"Trees other tenants share with tenant {tenant} are not listed: you cannot list its grants. Only its own trees are shown.";
        }

        return DataErrors.IsNotOffered(exception)
            ? NotOfferedNote(tenant)
            : $"Trees other tenants share with tenant {tenant} could not be listed, so only its own trees are shown. Refresh to try again.";
    }

    /// <summary>The note for approved grants whose scope shares nothing this tenant can read.</summary>
    /// <param name="count">How many such grants there are.</param>
    public static string UnreadableNote(int count) => count == 1
        ? "1 approved grant names a scope outside its granting tenant's trees, so it shares nothing and is not listed."
        : $"{count} approved grants name a scope outside their granting tenant's trees, so they share nothing and are not listed.";

    private static bool Covers(string scope, string id) =>
        id.Length > scope.Length
        && id.StartsWith(scope, StringComparison.Ordinal)
        && (scope[^1] == '/' || id[scope.Length] == '/');
}
