using System.Reflection;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Concrete guard for the core <c>Orleans.Lattice</c> assembly: every static
/// grain-key composer it declares (marked with <c>[GrainKeyBuilder]</c>) must
/// produce a key safe to use as an Orleans grain primary key on a keyed storage
/// backend such as Azure Table. Reuses
/// <see cref="GrainKeyStorageSafetyContractTestsBase"/> so a newly added composer
/// that joins its parts with a control-character delimiter - the defect behind
/// <c>LatticeCrossTreeReceiverGrain.ComputeKey</c>'s original ASCII Unit
/// Separator - is caught in CI rather than in a live deployment.
/// </summary>
[TestFixture]
public sealed class GrainKeyStorageSafetyContractTests : GrainKeyStorageSafetyContractTestsBase
{
    /// <inheritdoc />
    protected override Assembly PackageAssembly => typeof(LatticeWriteFencedException).Assembly;
}
