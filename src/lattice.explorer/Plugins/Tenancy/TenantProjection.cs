using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.Tenancy;

/// <summary>
/// Projects the tenant-administration control API's wire types onto the
/// Explorer's own tenancy domain model. This is the D3 boundary in code: it is
/// the only place in the Explorer that names both vocabularies, so no wire type
/// can reach a tenancy plugin.
/// </summary>
/// <remarks>
/// <para>
/// <b>Nothing is null-coalesced.</b> A quota ceiling of <see langword="null"/>
/// means unbounded and a usage of <see langword="null"/> means unmeasured;
/// substituting <c>0</c> for either would render an unlimited tenant as a full
/// bar or an unsampled rate as idle, so both cross the boundary intact.
/// </para>
/// <para>
/// <b>Every enum is translated member by member, and an unrecognised value
/// fails closed</b> rather than defaulting to the zero member. That matters most
/// for a grant: a value a newer server introduced must never arrive as
/// <see cref="ExplorerTenantGrantState.Active"/>, because that is the one state
/// that authorizes anything.
/// </para>
/// </remarks>
internal static class TenantProjection
{
    /// <summary>Projects a tenant descriptor onto a list summary.</summary>
    /// <param name="descriptor">The wire descriptor. Must not be <see langword="null"/>.</param>
    /// <returns>The Explorer's summary of the tenant.</returns>
    public static ExplorerTenantSummary ToSummary(TenantDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        return new ExplorerTenantSummary(descriptor.TenantId, ToLifecycle(descriptor.Status), descriptor.IsDefault);
    }

    /// <summary>Projects a list of tenant descriptors, preserving order.</summary>
    /// <param name="descriptors">The wire descriptors. Must not be <see langword="null"/>.</param>
    /// <returns>The Explorer's summaries, in the same order.</returns>
    public static IReadOnlyList<ExplorerTenantSummary> ToSummaries(IReadOnlyList<TenantDescriptor> descriptors)
    {
        ArgumentNullException.ThrowIfNull(descriptors);
        if (descriptors.Count == 0)
        {
            return Array.Empty<ExplorerTenantSummary>();
        }

        var mapped = new ExplorerTenantSummary[descriptors.Count];
        for (var i = 0; i < descriptors.Count; i++)
        {
            mapped[i] = ToSummary(descriptors[i]);
        }

        return mapped;
    }

    /// <summary>Projects a tenant status report onto the Explorer's tenant detail.</summary>
    /// <param name="report">The wire report. Must not be <see langword="null"/>.</param>
    /// <returns>The Explorer's tenant detail.</returns>
    public static ExplorerTenantDetail ToDetail(TenantStatusReport report)
    {
        ArgumentNullException.ThrowIfNull(report);
        return new ExplorerTenantDetail
        {
            TenantId = report.TenantId,
            Status = ToLifecycle(report.Status),
            IsDefault = report.IsDefault,
            Regions = ToRegions(report.Regions),
            Quotas = ToLimits(report.Quotas),
        };
    }

    /// <summary>Projects a creation result.</summary>
    /// <param name="result">The wire result. Must not be <see langword="null"/>.</param>
    /// <returns>The Explorer's creation outcome.</returns>
    public static ExplorerTenantCreation ToCreation(TenantCreationResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return new ExplorerTenantCreation
        {
            TenantId = result.TenantId,
            Status = ToLifecycle(result.Status),
            AdminSubjects = result.AdminSubjects,
        };
    }

    /// <summary>Projects a lifecycle transition result.</summary>
    /// <param name="result">The wire result. Must not be <see langword="null"/>.</param>
    /// <returns>The Explorer's transition outcome.</returns>
    public static ExplorerTenantStatusChange ToStatusChange(TenantStatusChangeResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return new ExplorerTenantStatusChange(
            result.TenantId,
            ToLifecycle(result.PreviousStatus),
            ToLifecycle(result.NewStatus),
            result.Changed);
    }

    /// <summary>Projects a deletion result.</summary>
    /// <param name="result">The wire result. Must not be <see langword="null"/>.</param>
    /// <returns>The Explorer's deletion outcome.</returns>
    public static ExplorerTenantDeletion ToDeletion(TenantDeletionResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return new ExplorerTenantDeletion(result.TenantId, result.CascadedTreeCount);
    }

    /// <summary>Projects a residency-change result.</summary>
    /// <param name="result">The wire result. Must not be <see langword="null"/>.</param>
    /// <returns>The Explorer's residency-change outcome.</returns>
    public static ExplorerTenantResidencyChange ToResidencyChange(TenantResidencyChangeResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return new ExplorerTenantResidencyChange
        {
            TenantId = result.TenantId,
            AddedRegions = result.AddedRegions,
            RemovedRegions = result.RemovedRegions,
            Regions = ToRegions(result.Regions),
        };
    }

    /// <summary>Projects an admin-subject report.</summary>
    /// <param name="report">The wire report. Must not be <see langword="null"/>.</param>
    /// <returns>The Explorer's admin-subject set.</returns>
    public static ExplorerTenantAdmins ToAdmins(TenantAdminSubjectReport report)
    {
        ArgumentNullException.ThrowIfNull(report);
        return new ExplorerTenantAdmins { TenantId = report.TenantId, Subjects = report.Subjects };
    }

    /// <summary>Projects an admin-subject change result.</summary>
    /// <param name="result">The wire result. Must not be <see langword="null"/>.</param>
    /// <returns>The Explorer's admin-subject change outcome.</returns>
    public static ExplorerTenantAdminChange ToAdminChange(TenantAdminSubjectChangeResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return new ExplorerTenantAdminChange
        {
            TenantId = result.TenantId,
            SubjectId = result.SubjectId,
            Changed = result.Changed,
            Subjects = result.Subjects,
        };
    }

    /// <summary>Projects a cross-tenant grant report, keeping the two directions apart.</summary>
    /// <param name="report">The wire report. Must not be <see langword="null"/>.</param>
    /// <returns>The Explorer's grant report.</returns>
    public static ExplorerTenantGrants ToGrants(TenantGrantReport report)
    {
        ArgumentNullException.ThrowIfNull(report);
        return new ExplorerTenantGrants
        {
            TenantId = report.TenantId,
            Issued = ToGrantList(report.Issued),
            Received = ToGrantList(report.Received),
        };
    }

    /// <summary>Projects a grant transition result.</summary>
    /// <param name="result">The wire result. Must not be <see langword="null"/>.</param>
    /// <returns>The Explorer's grant transition outcome.</returns>
    public static ExplorerTenantGrantChange ToGrantChange(TenantGrantChangeResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return new ExplorerTenantGrantChange(ToGrant(result.Grant), result.Changed);
    }

    /// <summary>Projects one cross-tenant grant, carrying its state explicitly.</summary>
    /// <param name="descriptor">The wire descriptor. Must not be <see langword="null"/>.</param>
    /// <returns>The Explorer's grant.</returns>
    public static ExplorerTenantGrant ToGrant(TenantGrantDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        return new ExplorerTenantGrant(
            descriptor.GrantId,
            descriptor.GranterTenantId,
            descriptor.GranteeTenantId,
            descriptor.Scope,
            ToGrantAccess(descriptor.Operations),
            ToGrantState(descriptor.State));
    }

    /// <summary>Projects a usage-against-quota report, dimension by dimension.</summary>
    /// <param name="report">The wire report. Must not be <see langword="null"/>.</param>
    /// <returns>The Explorer's usage reading.</returns>
    public static ExplorerTenantQuotaUsage ToUsage(TenantQuotaUsageReport report)
    {
        ArgumentNullException.ThrowIfNull(report);
        return new ExplorerTenantQuotaUsage
        {
            TenantId = report.TenantId,
            IsDefault = report.IsDefault,
            EnforcementScope = ToEnforcement(report.EnforcementScope),
            HasUsage = report.HasUsage,
            Bytes = ToDimension(report.Bytes),
            Keys = ToDimension(report.Keys),
            MemoryBytes = ToDimension(report.MemoryBytes),
            TreeCount = ToDimension(report.TreeCount),
            OpsPerSecond = ToDimension(report.OpsPerSecond),
            BurstPercent = report.BurstPercent,
            Limits = ToLimits(report.Quotas),
        };
    }

    /// <summary>
    /// Projects one quota dimension. Both nullable figures cross unchanged: an
    /// absent ceiling stays absent, and an absent usage stays absent.
    /// </summary>
    /// <param name="usage">The wire dimension.</param>
    /// <returns>The Explorer's dimension.</returns>
    public static ExplorerTenantQuotaDimension ToDimension(TenantQuotaDimensionUsage usage) => new()
    {
        Usage = usage.Usage,
        Limit = usage.Limit,
        BurstLimit = usage.BurstLimit,
        Overage = usage.Overage,
        MeteredOverage = usage.MeteredOverage,
    };

    /// <summary>
    /// Projects a quota descriptor onto the Explorer's ceilings, preserving each
    /// unbounded dimension as absent rather than zero.
    /// </summary>
    /// <param name="quotas">The wire descriptor.</param>
    /// <returns>The Explorer's ceilings.</returns>
    public static ExplorerTenantQuotaLimits ToLimits(TenantQuotasDescriptor quotas) => new()
    {
        MaxBytes = quotas.MaxBytes,
        MaxKeys = quotas.MaxKeys,
        MaxMemoryBytes = quotas.MaxMemoryBytes,
        MaxTreeCount = quotas.MaxTreeCount,
        MaxOpsPerSecond = quotas.MaxOpsPerSecond,
        BurstPercent = quotas.BurstPercent,
    };

    /// <summary>
    /// Projects the Explorer's ceilings back onto a quota descriptor for a write,
    /// again preserving each unbounded dimension as absent.
    /// </summary>
    /// <param name="limits">The Explorer's ceilings.</param>
    /// <returns>The wire descriptor to send.</returns>
    public static TenantQuotasDescriptor ToDescriptor(ExplorerTenantQuotaLimits limits) => new()
    {
        MaxBytes = limits.MaxBytes,
        MaxKeys = limits.MaxKeys,
        MaxMemoryBytes = limits.MaxMemoryBytes,
        MaxTreeCount = limits.MaxTreeCount,
        MaxOpsPerSecond = limits.MaxOpsPerSecond,
        BurstPercent = limits.BurstPercent,
    };

    /// <summary>Projects a list of per-region status descriptors, preserving order.</summary>
    /// <param name="regions">The wire descriptors. Must not be <see langword="null"/>.</param>
    /// <returns>The Explorer's regions, in the same order.</returns>
    public static IReadOnlyList<ExplorerTenantRegion> ToRegions(IReadOnlyList<TenantRegionStatusDescriptor> regions)
    {
        ArgumentNullException.ThrowIfNull(regions);
        if (regions.Count == 0)
        {
            return Array.Empty<ExplorerTenantRegion>();
        }

        var mapped = new ExplorerTenantRegion[regions.Count];
        for (var i = 0; i < regions.Count; i++)
        {
            var region = regions[i];
            mapped[i] = new ExplorerTenantRegion(
                region.RegionId,
                ToRegionLifecycle(region.Status),
                region.IsAllowed);
        }

        return mapped;
    }

    private static IReadOnlyList<ExplorerTenantGrant> ToGrantList(IReadOnlyList<TenantGrantDescriptor> grants)
    {
        if (grants.Count == 0)
        {
            return Array.Empty<ExplorerTenantGrant>();
        }

        var mapped = new ExplorerTenantGrant[grants.Count];
        for (var i = 0; i < grants.Count; i++)
        {
            mapped[i] = ToGrant(grants[i]);
        }

        return mapped;
    }

    /// <summary>
    /// Translates a tenant lifecycle status. An unrecognised value - a state a
    /// newer server introduced - reports as suspended rather than active, so an
    /// Explorer that does not understand a state never presents it as healthy.
    /// </summary>
    private static ExplorerTenantLifecycle ToLifecycle(TenantLifecycleStatus status) => status switch
    {
        TenantLifecycleStatus.Active => ExplorerTenantLifecycle.Active,
        TenantLifecycleStatus.Suspended => ExplorerTenantLifecycle.Suspended,
        _ => ExplorerTenantLifecycle.Suspended,
    };

    /// <summary>
    /// Translates a per-region lifecycle status. An unrecognised value reports as
    /// no residency, which claims the least.
    /// </summary>
    private static ExplorerTenantRegionLifecycle ToRegionLifecycle(TenantRegionLifecycleStatus status) => status switch
    {
        TenantRegionLifecycleStatus.None => ExplorerTenantRegionLifecycle.None,
        TenantRegionLifecycleStatus.Provisioning => ExplorerTenantRegionLifecycle.Provisioning,
        TenantRegionLifecycleStatus.Backfilling => ExplorerTenantRegionLifecycle.Backfilling,
        TenantRegionLifecycleStatus.Online => ExplorerTenantRegionLifecycle.Online,
        TenantRegionLifecycleStatus.Draining => ExplorerTenantRegionLifecycle.Draining,
        TenantRegionLifecycleStatus.Offline => ExplorerTenantRegionLifecycle.Offline,
        TenantRegionLifecycleStatus.Removed => ExplorerTenantRegionLifecycle.Removed,
        _ => ExplorerTenantRegionLifecycle.None,
    };

    /// <summary>
    /// Translates the scope a quota reading was taken under. An unrecognised
    /// value reports as per-cluster, the weaker claim, so the Explorer never
    /// captions a reading as a converged global total it cannot vouch for.
    /// </summary>
    private static ExplorerTenantQuotaEnforcement ToEnforcement(TenantQuotaEnforcementScope scope) => scope switch
    {
        TenantQuotaEnforcementScope.GlobalConverged => ExplorerTenantQuotaEnforcement.GlobalConverged,
        TenantQuotaEnforcementScope.PerCluster => ExplorerTenantQuotaEnforcement.PerCluster,
        _ => ExplorerTenantQuotaEnforcement.PerCluster,
    };

    /// <summary>
    /// Translates a grant's lifecycle state. An unrecognised value reports as
    /// revoked - inert and terminal - because <see cref="ExplorerTenantGrantState.Active"/>
    /// is the only state that authorizes anything and an unknown state must
    /// never be allowed to fail open onto it.
    /// </summary>
    private static ExplorerTenantGrantState ToGrantState(TenantGrantLifecycleState state) => state switch
    {
        TenantGrantLifecycleState.Active => ExplorerTenantGrantState.Active,
        TenantGrantLifecycleState.Pending => ExplorerTenantGrantState.Pending,
        TenantGrantLifecycleState.Rejected => ExplorerTenantGrantState.Rejected,
        TenantGrantLifecycleState.Revoked => ExplorerTenantGrantState.Revoked,
        _ => ExplorerTenantGrantState.Revoked,
    };

    /// <summary>
    /// Translates the operations a grant authorizes, bit by bit. An unrecognised
    /// bit is dropped rather than carried through, so the Explorer never claims
    /// an authority it cannot name.
    /// </summary>
    private static ExplorerTenantGrantAccess ToGrantAccess(TenantGrantAccess operations)
    {
        var access = ExplorerTenantGrantAccess.None;
        if ((operations & TenantGrantAccess.Read) != 0)
        {
            access |= ExplorerTenantGrantAccess.Read;
        }

        if ((operations & TenantGrantAccess.Write) != 0)
        {
            access |= ExplorerTenantGrantAccess.Write;
        }

        return access;
    }

    /// <summary>
    /// Translates the Explorer's grant operations back onto the wire flags for a
    /// write, bit by bit for the same reason.
    /// </summary>
    /// <param name="operations">The operations the offer will authorize.</param>
    /// <returns>The wire flags to send.</returns>
    public static TenantGrantAccess ToWireGrantAccess(ExplorerTenantGrantAccess operations)
    {
        var access = TenantGrantAccess.None;
        if ((operations & ExplorerTenantGrantAccess.Read) != 0)
        {
            access |= TenantGrantAccess.Read;
        }

        if ((operations & ExplorerTenantGrantAccess.Write) != 0)
        {
            access |= TenantGrantAccess.Write;
        }

        return access;
    }
}
