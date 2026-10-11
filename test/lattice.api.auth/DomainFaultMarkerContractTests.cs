using System.Reflection;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>Enrols authorization facade exceptions in the shared domain-refusal marker contract.</summary>
[TestFixture]
public sealed class DomainFaultMarkerContractTests : DomainFaultMarkerContractTestsBase
{
    /// <inheritdoc />
    protected override Assembly PackageAssembly => typeof(LatticeTenantOwnedGroupException).Assembly;

    /// <inheritdoc />
    protected override Type DomainFaultMarkerType => typeof(ILatticeDomainFault);

    /// <inheritdoc />
    protected override string PackageAnchorDescription => nameof(LatticeTenantOwnedGroupException);
}
