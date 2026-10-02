using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Groups;

/// <summary>
/// The tenant facade seam over the Access context's fakes, whose directory a test can
/// replace with a scripted one (a read that never answers, or one that fails) while
/// the fake policy still answers the posture probe.
/// </summary>
/// <param name="inner">The Access context's fakes.</param>
internal sealed class ScriptedTenantAccessFacades(FakeTenantAccessFacades inner) : ITenantAccessFacades
{
    /// <summary>The scripted directory, or <see langword="null"/> to read the fake's.</summary>
    public ILatticeTenantDirectoryAdmin? DirectoryOverride { get; set; }

    /// <inheritdoc />
    public ILatticeTenantDirectoryAdmin? Directory => DirectoryOverride ?? inner.Directory;

    /// <inheritdoc />
    public ILatticeTenantPolicyAdmin? Policy => inner.Policy;
}
