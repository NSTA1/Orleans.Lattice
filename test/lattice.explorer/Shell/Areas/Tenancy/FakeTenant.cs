using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Tenancy;

/// <summary>One tenant of the <see cref="FakeTenancyCluster"/>.</summary>
internal sealed class FakeTenant
{
    /// <summary>The lifecycle state.</summary>
    public TenantLifecycleStatus Status { get; set; }

    /// <summary>Whether self-service lists it for the caller (and reads it).</summary>
    public bool Listed { get; set; } = true;

    /// <summary>The admin subjects.</summary>
    public SortedSet<string> Admins { get; } = new(StringComparer.Ordinal);

    /// <summary>The per-region status, in order.</summary>
    public List<TenantRegionStatusDescriptor> Regions { get; } = [];

    /// <summary>The quotas in effect.</summary>
    public TenantQuotasDescriptor Quotas { get; set; }

    /// <summary>Measured usage per dimension (<c>bytes</c>, <c>keys</c>, <c>memory</c>, <c>trees</c>, <c>ops</c>); absent is not measured.</summary>
    public Dictionary<string, long> Usage { get; } = new(StringComparer.Ordinal);

    /// <summary>How many trees a delete cascades to.</summary>
    public int Trees { get; set; } = 3;
}
