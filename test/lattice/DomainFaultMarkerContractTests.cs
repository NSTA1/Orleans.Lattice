using System.Reflection;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Repository guard for the core <c>Orleans.Lattice</c> assembly: every exception
/// it declares whose base type is foreign - in practice a Base Class Library
/// exception subclass such as <see cref="InvalidOperationException"/> - must
/// implement <see cref="ILatticeDomainFault"/>. The contract, its two-layer
/// anti-vacuity, and its detection self-tests live in
/// <see cref="DomainFaultMarkerContractTestsBase"/>, shared with every other
/// enrolled package.
/// <para>
/// Re-parenting such an exception onto <see cref="Exception"/> was rejected as a
/// remedy: the base type of a public exception is shipped contract, and changing
/// it would break every caller that catches the BCL base today. The marker is
/// additive and leaves those callers working.
/// </para>
/// </summary>
[TestFixture]
public sealed class DomainFaultMarkerContractTests : DomainFaultMarkerContractTestsBase
{
    /// <inheritdoc />
    protected override Assembly PackageAssembly => typeof(LatticeWriteFencedException).Assembly;

    /// <inheritdoc />
    protected override Type DomainFaultMarkerType => typeof(ILatticeDomainFault);

    /// <inheritdoc />
    protected override string PackageAnchorDescription => nameof(LatticeWriteFencedException);

    [Test]
    public void Domain_fault_marker_is_public_so_callers_outside_the_package_can_decline_a_refusal()
    {
        // The marker only removes the defect if a handler in another package can
        // name it. An internal marker would leave every caller above the core with
        // the same unusable choice it had before.
        Assert.That(typeof(ILatticeDomainFault).IsPublic, Is.True);
        Assert.That(typeof(ILatticeDomainFault).IsInterface, Is.True);
        Assert.That(typeof(ILatticeDomainFault).Assembly, Is.SameAs(PackageAssembly));
    }
}
