using System.Reflection;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Repository guard for the core <c>Orleans.Lattice</c> assembly: every exception
/// it declares whose base type is <b>foreign</b> - declared outside this package,
/// which in practice means a Base Class Library exception subclass such as
/// <see cref="InvalidOperationException"/> - must implement
/// <see cref="ILatticeDomainFault"/>.
/// <para>
/// Such an exception is caught by any broad <c>catch</c> clause naming its
/// foreign base, which then applies remediation chosen for a framework failure
/// rather than for the domain refusal that actually occurred. The marker is what
/// lets a handler decline it with
/// <c>when (ex is not ILatticeDomainFault)</c>; this guard is what stops a newly
/// added exception from reintroducing the defect by omitting the marker.
/// </para>
/// <para>
/// The population is enumerated from the live assembly on every run, never from
/// a checked-in list of type names or an expected count, so the guard neither
/// needs editing when an exception is added nor depends on the order in which
/// concurrent work lands.
/// </para>
/// <para>
/// Anti-vacuity is asserted in two layers, because the two ways this guard can
/// silently stop guarding are different. The outer layer asserts that the scan
/// saw exception types at all; the inner layer asserts that it resolved some of
/// them as foreign-based, which is the population the contract actually ranges
/// over. Only the inner layer catches a change to base resolution that leaves
/// the outer count healthy and the offender list empty for the wrong reason.
/// </para>
/// </summary>
[TestFixture]
public sealed class DomainFaultMarkerContractTests
{
    /// <summary>
    /// The assembly under audit. Anchored on a concrete exception rather than on
    /// the marker so that deleting the marker fails compilation here instead of
    /// quietly emptying the guard's population.
    /// </summary>
    private static Assembly PackageAssembly => typeof(LatticeWriteFencedException).Assembly;

    /// <summary>
    /// Every concrete or abstract exception type declared by the package, ordered
    /// deterministically so a failure message reads the same way on every run.
    /// </summary>
    private static IReadOnlyList<Type> DeclaredExceptionTypes() =>
        [.. PackageAssembly
            .GetTypes()
            .Where(t => typeof(Exception).IsAssignableFrom(t) && !t.ContainsGenericParameters)
            .OrderBy(t => t.FullName, StringComparer.Ordinal)];

    /// <summary>
    /// Reports whether <paramref name="type"/> inherits from an exception declared
    /// outside <paramref name="packageAssembly"/>, walking the base chain up to but
    /// not including <see cref="Exception"/> itself. Deriving directly from
    /// <see cref="Exception"/> is the one shape no foreign <c>catch</c> clause can
    /// single out, so it is never a hazard.
    /// </summary>
    private static bool HasForeignExceptionBase(Type type, Assembly packageAssembly)
    {
        for (var ancestor = type.BaseType;
             ancestor is not null && ancestor != typeof(Exception);
             ancestor = ancestor.BaseType)
        {
            if (ancestor.Assembly != packageAssembly)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>An exception deriving from a foreign base is detected as a hazard.</summary>
    private sealed class PlantedForeignBasedException : InvalidOperationException;

    /// <summary>An exception deriving directly from <see cref="Exception"/> is not.</summary>
    private sealed class PlantedDirectlyDerivedException : Exception;

    [Test]
    public void Every_exception_with_a_foreign_base_implements_the_domain_fault_marker()
    {
        var declared = DeclaredExceptionTypes();

        // Anti-vacuity, first layer: assert the DENOMINATOR of the scan, never the
        // size of the match set. An empty offender list is the desired outcome and
        // must stay legal, so the only way to tell a working guard from one that
        // reflected over nothing is to assert what it examined.
        HygieneDenominator.RequireExamined(
            declared.Count,
            nameof(DomainFaultMarkerContractTests),
            "exception types",
            $"assembly '{PackageAssembly.GetName().Name}', anchored on {nameof(LatticeWriteFencedException)}");

        var foreignBased = declared
            .Where(t => HasForeignExceptionBase(t, PackageAssembly))
            .ToList();

        // Anti-vacuity, second layer: the population the contract actually ranges
        // over is not every exception, it is every exception with a foreign base.
        // The likeliest way to silently empty THAT set is not a refactor of this
        // fixture but a change to how the base is resolved - a reflection detail
        // that would leave the outer count healthy, the offender list empty, and
        // this test green while asserting nothing. Re-parenting was rejected for
        // this package (the base type of a public exception is shipped contract),
        // so a zero here means the detection broke, never that the hazard is gone.
        HygieneDenominator.RequireExamined(
            foreignBased.Count,
            nameof(DomainFaultMarkerContractTests),
            "exception types with a foreign base",
            $"assembly '{PackageAssembly.GetName().Name}', resolved by {nameof(HasForeignExceptionBase)}");

        var offenders = foreignBased
            .Where(t => !typeof(ILatticeDomainFault).IsAssignableFrom(t))
            .Select(t => $"  {t.FullName} : {t.BaseType?.Name}")
            .ToList();

        Assert.That(
            offenders,
            Is.Empty,
            $"These exceptions derive from a foreign base, so a broad catch clause naming that base "
            + $"absorbs them and applies remediation chosen for a framework failure. Implement "
            + $"{nameof(ILatticeDomainFault)} on each so a handler can decline it with "
            + $"'when (ex is not {nameof(ILatticeDomainFault)})':{Environment.NewLine}"
            + string.Join(Environment.NewLine, offenders));
    }

    [Test]
    public void Domain_fault_detection_flags_a_planted_foreign_based_exception()
    {
        // The guard's own smoke test. A change that narrows the detection until it
        // matches nothing fails here, so the contract test above cannot go vacuous
        // while still reporting success.
        Assert.That(
            HasForeignExceptionBase(typeof(PlantedForeignBasedException), typeof(PlantedForeignBasedException).Assembly),
            Is.True,
            "Detection missed an exception deriving from InvalidOperationException.");
    }

    [Test]
    public void Domain_fault_detection_ignores_an_exception_derived_directly_from_exception()
    {
        Assert.That(
            HasForeignExceptionBase(typeof(PlantedDirectlyDerivedException), typeof(PlantedDirectlyDerivedException).Assembly),
            Is.False,
            "Detection flagged an exception deriving directly from Exception, which no foreign catch clause can single out.");
    }

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
