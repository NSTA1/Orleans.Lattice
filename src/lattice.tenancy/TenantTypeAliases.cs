namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Centralized Orleans serialization alias constants for the
/// <c>Orleans.Lattice.Tenancy</c> package. Mirrors the core <c>TypeAliases</c>
/// table: every constant uses the <c>olt.</c> prefix, is at most 7 characters,
/// and is unique - invariants enforced by <c>TenantTypeAliasesTests</c>. An
/// alias is wire format; never rename or remove one.
/// </summary>
internal static class TenantTypeAliases
{
    /// <summary>Alias for <see cref="TenantRecord"/>.</summary>
    internal const string TenantRecord = "olt.rec";

    /// <summary>Alias for <see cref="TenantQuotas"/>.</summary>
    internal const string TenantQuotas = "olt.qta";

    /// <summary>Alias for <see cref="TenantPlacement"/>.</summary>
    internal const string TenantPlacement = "olt.plc";

    /// <summary>Alias for <see cref="CrossTenantGrant"/>.</summary>
    internal const string CrossTenantGrant = "olt.grt";

    /// <summary>Alias for <see cref="TenantLwwRegister{T}"/>.</summary>
    internal const string TenantLwwRegister = "olt.lww";

    /// <summary>Alias for <see cref="TenantSubjectSlot"/>.</summary>
    internal const string TenantSubjectSlot = "olt.ssl";

    /// <summary>Alias for <see cref="TenantGrantSlot"/>.</summary>
    internal const string TenantGrantSlot = "olt.gsl";

    /// <summary>Alias for <see cref="Tenancy.TenantStatus"/>.</summary>
    internal const string TenantStatus = "olt.sts";

    /// <summary>Alias for <see cref="Tenancy.TenantGranteeKind"/>.</summary>
    internal const string TenantGranteeKind = "olt.gnk";

    /// <summary>Alias for <see cref="Tenancy.TenantGrantOperations"/>.</summary>
    internal const string TenantGrantOperations = "olt.gop";

    /// <summary>Alias for <see cref="Tenancy.LocalUsageSample"/>.</summary>
    internal const string LocalUsageSample = "olt.lus";

    /// <summary>Alias for <see cref="Tenancy.TenantUsageRecord"/>.</summary>
    internal const string TenantUsageRecord = "olt.usg";

    /// <summary>Alias for <see cref="Tenancy.TenantOverageRecord"/>.</summary>
    internal const string TenantOverageRecord = "olt.ovr";
}
