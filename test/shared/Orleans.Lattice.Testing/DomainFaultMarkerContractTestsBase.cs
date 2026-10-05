using System.Reflection;
using NUnit.Framework;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Reusable repository guard: every exception a package declares whose base type
/// is <b>foreign</b> - declared outside that package, which in practice means a
/// Base Class Library exception subclass such as
/// <see cref="InvalidOperationException"/> - must implement the domain-fault
/// marker interface named by <see cref="DomainFaultMarkerType"/>.
/// <para>
/// Such an exception is caught by any broad <c>catch</c> clause naming its
/// foreign base, which then applies remediation chosen for a framework failure
/// rather than for the domain refusal that actually occurred. The marker is what
/// lets a handler decline it (<c>when (ex is not ILatticeDomainFault)</c>); this
/// guard is what stops a newly added exception from reintroducing the defect by
/// omitting the marker.
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
/// the outer count healthy and the offender list empty for the wrong reason. A
/// package that declares no foreign-based exception therefore cannot enrol: it
/// would fail the inner layer, which is the intended outcome, because a guard
/// over an empty population is green for no reason.
/// </para>
/// <para>
/// This library has no compile-time reference to the core assembly, so the marker
/// is supplied by the consumer. The base is <see langword="abstract"/> so it is
/// never discovered on its own; the inherited <c>[Test]</c> methods run through
/// the concrete subclass in the consuming assembly.
/// </para>
/// </summary>
public abstract class DomainFaultMarkerContractTestsBase
{
    /// <summary>
    /// The package assembly under audit. Only types <em>declared</em> in this
    /// assembly are considered, so each package audits exactly its own exceptions.
    /// Anchor it on a concrete exception rather than on the marker, so that
    /// deleting that exception fails compilation instead of quietly emptying the
    /// guard's population.
    /// </summary>
    protected abstract Assembly PackageAssembly { get; }

    /// <summary>
    /// The domain-fault marker interface every foreign-based exception must
    /// implement, i.e. <c>typeof(ILatticeDomainFault)</c>.
    /// </summary>
    protected abstract Type DomainFaultMarkerType { get; }

    /// <summary>
    /// A short description of how <see cref="PackageAssembly"/> is anchored, for
    /// example the name of the concrete exception it is resolved from. Quoted in
    /// the anti-vacuity failure message so a zero count names its source.
    /// </summary>
    protected abstract string PackageAnchorDescription { get; }

    /// <summary>
    /// Every concrete or abstract exception type declared by the package, ordered
    /// deterministically so a failure message reads the same way on every run.
    /// </summary>
    private IReadOnlyList<Type> DeclaredExceptionTypes() =>
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
    /// <param name="type">The exception type to classify.</param>
    /// <param name="packageAssembly">The assembly that counts as "own" for the classification.</param>
    /// <returns><see langword="true"/> when some ancestor below <see cref="Exception"/> is declared elsewhere.</returns>
    protected static bool HasForeignExceptionBase(Type type, Assembly packageAssembly)
    {
        ArgumentNullException.ThrowIfNull(type);
        ArgumentNullException.ThrowIfNull(packageAssembly);

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

    /// <summary>
    /// The contract: every foreign-based exception in the package implements the marker.
    /// </summary>
    [Test]
    public void Every_exception_with_a_foreign_base_implements_the_domain_fault_marker()
    {
        var gate = GetType().Name;
        var assemblyName = PackageAssembly.GetName().Name;
        var declared = DeclaredExceptionTypes();

        // Anti-vacuity, first layer: assert the DENOMINATOR of the scan, never the
        // size of the match set. An empty offender list is the desired outcome and
        // must stay legal, so the only way to tell a working guard from one that
        // reflected over nothing is to assert what it examined.
        HygieneDenominator.RequireExamined(
            declared.Count,
            gate,
            "exception types",
            $"assembly '{assemblyName}', anchored on {PackageAnchorDescription}");

        var foreignBased = declared
            .Where(t => HasForeignExceptionBase(t, PackageAssembly))
            .ToList();

        // Anti-vacuity, second layer: the population the contract actually ranges
        // over is every exception with a foreign base. The likeliest way to silently
        // empty THAT set is a change to how the base is resolved, which would leave
        // the outer count healthy, the offender list empty, and this test green
        // while asserting nothing. A zero here means the detection broke, or the
        // package should not be enrolled - never that the guard passed.
        HygieneDenominator.RequireExamined(
            foreignBased.Count,
            gate,
            "exception types with a foreign base",
            $"assembly '{assemblyName}', resolved by {nameof(HasForeignExceptionBase)}");

        var marker = DomainFaultMarkerType;
        var offenders = foreignBased
            .Where(t => !marker.IsAssignableFrom(t))
            .Select(t => $"  {t.FullName} : {t.BaseType?.Name}")
            .ToList();

        Assert.That(
            offenders,
            Is.Empty,
            $"These exceptions in '{assemblyName}' derive from a foreign base, so a broad catch clause "
            + $"naming that base absorbs them and applies remediation chosen for a framework failure. "
            + $"Implement {marker.Name} on each so a handler can decline it with "
            + $"'when (ex is not {marker.Name})':{Environment.NewLine}"
            + string.Join(Environment.NewLine, offenders));
    }

    /// <summary>
    /// The marker supplied by the consumer is a public interface, so a handler in
    /// any package can name it.
    /// </summary>
    [Test]
    public void Domain_fault_marker_is_a_public_interface()
    {
        Assert.That(DomainFaultMarkerType.IsInterface, Is.True, $"{DomainFaultMarkerType.FullName} is not an interface.");
        Assert.That(DomainFaultMarkerType.IsPublic, Is.True, $"{DomainFaultMarkerType.FullName} is not public.");
    }

    /// <summary>
    /// The guard's own smoke test: detection flags an exception deriving from a
    /// foreign BCL subclass.
    /// </summary>
    [Test]
    public void Domain_fault_detection_flags_a_planted_foreign_based_exception()
    {
        // A change that narrows the detection until it matches nothing fails here,
        // so the contract test above cannot go vacuous while still reporting success.
        Assert.That(
            HasForeignExceptionBase(typeof(PlantedForeignBasedException), typeof(PlantedForeignBasedException).Assembly),
            Is.True,
            "Detection missed an exception deriving from InvalidOperationException.");
    }

    /// <summary>
    /// Detection does not flag an exception deriving directly from <see cref="Exception"/>.
    /// </summary>
    [Test]
    public void Domain_fault_detection_ignores_an_exception_derived_directly_from_exception()
    {
        Assert.That(
            HasForeignExceptionBase(typeof(PlantedDirectlyDerivedException), typeof(PlantedDirectlyDerivedException).Assembly),
            Is.False,
            "Detection flagged an exception deriving directly from Exception, which no foreign catch clause can single out.");
    }
}
