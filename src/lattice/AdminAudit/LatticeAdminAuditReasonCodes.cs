namespace Orleans.Lattice;

/// <summary>
/// The stable reason codes a <see cref="LatticeAdminAuditRecord"/> carries in
/// <see cref="LatticeAdminAuditRecord.ReasonCode"/>. A record never carries an
/// exception message; it carries one of these codes, so a reader can filter on
/// them and a message can never leak request content into the trail.
/// </summary>
public static class LatticeAdminAuditReasonCodes
{
    /// <summary>The caller is not authorized for the call.</summary>
    public const string DeniedNotAuthorized = "denied.not-authorized";

    /// <summary>The call targeted a tenant the caller may not act for.</summary>
    public const string DeniedTenantScope = "denied.tenant-scope";

    /// <summary>The call targeted an object owned by the tenant tier (or by an app) that this surface may not author.</summary>
    public const string DeniedTenantOwned = "denied.tenant-owned";

    /// <summary>The request was invalid.</summary>
    public const string FailedValidation = "failed.validation";

    /// <summary>A precondition or concurrent change refused the call.</summary>
    public const string FailedConflict = "failed.conflict";

    /// <summary>The target did not exist.</summary>
    public const string FailedNotFound = "failed.not-found";

    /// <summary>A quota refused the call.</summary>
    public const string FailedQuota = "failed.quota";

    /// <summary>The call completed without changing anything.</summary>
    public const string FailedNoChange = "failed.no-change";

    /// <summary>The reconciler proved that the interrupted call never committed.</summary>
    public const string FailedNotCommitted = "failed.not-committed";

    /// <summary>The reconciler could not prove whether the interrupted call committed.</summary>
    public const string IndeterminateNoEvidence = "indeterminate.no-evidence";
}
