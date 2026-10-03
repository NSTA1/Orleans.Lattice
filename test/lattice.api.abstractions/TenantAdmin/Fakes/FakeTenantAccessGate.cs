using Orleans.Lattice;

namespace Orleans.Lattice.Api.TenantAdmin.Fakes;

/// <summary>
/// The guard checks the in-memory delegated tenant-access fakes share: the
/// reserved default tenant, the feature flag, and a scripted denial, applied in
/// the order the real facades apply them. Shared by
/// <see cref="FakeTenantDirectoryAdmin"/> and <see cref="FakeTenantPolicyAdmin"/>
/// and linked, together with them, into every test project that binds to the
/// delegated tenant-access contracts.
/// </summary>
internal sealed class FakeTenantAccessGate
{
    /// <summary>Whether delegated tenant access administration is enabled. Defaults to <see langword="true"/>.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>When <see langword="true"/>, every call is refused as though the caller were not authorized.</summary>
    public bool Denied { get; set; }

    /// <summary>The name of every operation called, in call order.</summary>
    public List<string> Calls { get; } = [];

    /// <summary>
    /// A failure to raise from the next guarded call, then clear - for example a
    /// <see cref="LatticeQuotaExceededException"/> to script a tenant at a cap.
    /// </summary>
    public Exception? NextFailure { get; set; }

    /// <summary>
    /// Records <paramref name="operation"/> and applies the guards: a scripted
    /// <see cref="NextFailure"/>, an invalid tenant id, the reserved default tenant,
    /// a scripted denial, and (unless <paramref name="answersWhileDisabled"/>) the
    /// feature flag.
    /// </summary>
    /// <param name="tenantId">The tenant id the call named.</param>
    /// <param name="operation">The name of the operation being called.</param>
    /// <param name="answersWhileDisabled"><see langword="true"/> for the posture probe, which answers while the feature is off.</param>
    public void Check(string tenantId, string operation, bool answersWhileDisabled = false)
    {
        Calls.Add(operation);
        if (NextFailure is { } failure)
        {
            NextFailure = null;
            throw failure;
        }
        if (!TenantId.TryParse(tenantId, out _))
        {
            throw new ArgumentException($"'{tenantId}' is not a valid tenant id.", nameof(tenantId));
        }

        if (string.Equals(tenantId, TenantId.DefaultId, StringComparison.Ordinal))
        {
            throw new ReservedTenantOperationException(tenantId, operation);
        }

        if (Denied)
        {
            throw new LatticeAuthorizationDeniedException($"The caller may not administer tenant '{tenantId}'.");
        }

        if (!answersWhileDisabled && !Enabled)
        {
            throw new TenantAccessAdministrationDisabledException(tenantId);
        }
    }

    /// <summary>Refuses an entry that names another tenant's group through the cluster-group kind.</summary>
    /// <param name="tenantId">The tenant id the call named.</param>
    /// <param name="subjectId">The entry's id.</param>
    /// <param name="kind">The entry's kind.</param>
    /// <param name="paramName">The offending argument's name.</param>
    public static void RejectForeignTenantGroup(string tenantId, string subjectId, TenantSubjectKind kind, string paramName)
    {
        if (kind == TenantSubjectKind.ClusterGroup && subjectId.StartsWith("t/", StringComparison.Ordinal))
        {
            throw new TenantAccessConfinementException(
                tenantId,
                TenantAccessConfinementRule.ForeignTenantGroup,
                $"'{subjectId}' is a tenant group id; name the tenant's own groups by local name.",
                paramName);
        }
    }

    /// <summary>Cuts one ordinal page from <paramref name="ordered"/>, using the last key as the exclusive cursor.</summary>
    /// <typeparam name="T">The entry type.</typeparam>
    /// <param name="ordered">The entries in ascending ordinal order of <paramref name="key"/>.</param>
    /// <param name="key">Projects an entry to its ordering key.</param>
    /// <param name="page">The paging request.</param>
    /// <returns>The entries on the page and the cursor for the next one.</returns>
    public static (IReadOnlyList<T> Entries, string? NextPageToken) Page<T>(
        IReadOnlyList<T> ordered, Func<T, string> key, TenantAccessPageRequest page)
    {
        ArgumentNullException.ThrowIfNull(page);
        var entries = new List<T>();
        var size = page.EffectivePageSize;
        string? next = null;
        foreach (var entry in ordered)
        {
            if (page.PageToken is not null && string.CompareOrdinal(key(entry), page.PageToken) <= 0)
            {
                continue;
            }

            if (entries.Count == size)
            {
                next = key(entries[^1]);
                break;
            }

            entries.Add(entry);
        }

        return (entries, next);
    }
}
