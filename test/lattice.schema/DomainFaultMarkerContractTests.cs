using System.Reflection;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>Enrols schema exceptions in the shared domain-refusal marker contract.</summary>
[TestFixture]
public sealed class DomainFaultMarkerContractTests : DomainFaultMarkerContractTestsBase
{
    /// <inheritdoc />
    protected override Assembly PackageAssembly => typeof(LatticeSchemaViolationException).Assembly;

    /// <inheritdoc />
    protected override Type DomainFaultMarkerType => typeof(ILatticeDomainFault);

    /// <inheritdoc />
    protected override string PackageAnchorDescription => nameof(LatticeSchemaViolationException);
}
