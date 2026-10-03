using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Fakes;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant;

/// <summary>
/// The Access area's tenant facade seam over the delegated tenant-access contract
/// fakes: an in-memory tenant directory and tenant policy sharing one guard, so
/// the feature flag, a scripted denial and the call log apply to both. Either
/// facade can be withdrawn to model a head that serves none.
/// </summary>
internal sealed class FakeTenantAccessFacades : ITenantAccessFacades
{
    /// <summary>Creates the fakes over one shared guard, with delegated administration switched off.</summary>
    public FakeTenantAccessFacades()
    {
        Gate = new FakeTenantAccessGate { Enabled = false };
        PolicyFake = new FakeTenantPolicyAdmin(Gate) { CallerIsTenantAdmin = false };
        DirectoryFake = new FakeTenantDirectoryAdmin(Gate, PolicyFake);
    }

    /// <summary>The guard both fakes apply: the feature flag, a scripted denial, a scripted failure, and the call log.</summary>
    public FakeTenantAccessGate Gate { get; }

    /// <summary>The tenant directory fake.</summary>
    public FakeTenantDirectoryAdmin DirectoryFake { get; }

    /// <summary>The tenant policy fake, which answers the posture probe.</summary>
    public FakeTenantPolicyAdmin PolicyFake { get; }

    /// <summary>Whether the directory facade is served. Defaults to <see langword="true"/>.</summary>
    public bool ServesDirectory { get; set; } = true;

    /// <summary>Whether the policy facade is served. Defaults to <see langword="true"/>.</summary>
    public bool ServesPolicy { get; set; } = true;

    /// <inheritdoc />
    public ILatticeTenantDirectoryAdmin? Directory => ServesDirectory ? DirectoryFake : null;

    /// <inheritdoc />
    public ILatticeTenantPolicyAdmin? Policy => ServesPolicy ? PolicyFake : null;

    /// <summary>The caller administers the tenant, and delegated administration is on.</summary>
    /// <returns>The same fakes, for chaining.</returns>
    public FakeTenantAccessFacades AsTenantAdmin()
    {
        Gate.Enabled = true;
        PolicyFake.CallerIsTenantAdmin = true;
        PolicyFake.CallerIsPlatformOperator = false;
        return this;
    }

    /// <summary>The caller is a platform operator but no tenant admin, and delegated administration is on.</summary>
    /// <returns>The same fakes, for chaining.</returns>
    public FakeTenantAccessFacades AsOperator()
    {
        Gate.Enabled = true;
        PolicyFake.CallerIsTenantAdmin = false;
        PolicyFake.CallerIsPlatformOperator = true;
        return this;
    }

    /// <summary>The caller is a plain member: neither admin nor operator; delegated administration is on.</summary>
    /// <returns>The same fakes, for chaining.</returns>
    public FakeTenantAccessFacades AsMember()
    {
        Gate.Enabled = true;
        PolicyFake.CallerIsTenantAdmin = false;
        PolicyFake.CallerIsPlatformOperator = false;
        return this;
    }

    /// <summary>Seeds a group of <paramref name="tenant"/>.</summary>
    /// <param name="tenant">The tenant.</param>
    /// <param name="name">The group's tenant-local name.</param>
    /// <param name="displayName">Its display name, or <see langword="null"/>.</param>
    /// <returns>The same fakes, for chaining.</returns>
    public FakeTenantAccessFacades WithGroup(string tenant, string name, string? displayName = null)
    {
        var enabled = Gate.Enabled;
        Gate.Enabled = true;
        DirectoryFake.UpsertGroupAsync(tenant, new TenantGroupDescriptor { Name = name, DisplayName = displayName }).GetAwaiter().GetResult();
        Gate.Enabled = enabled;
        Gate.Calls.Clear();
        return this;
    }
}
