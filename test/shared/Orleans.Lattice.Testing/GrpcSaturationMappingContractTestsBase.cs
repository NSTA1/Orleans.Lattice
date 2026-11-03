using System.Reflection;
using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Reusable, product-agnostic guard proving that every gRPC facade service in a
/// package handles storage back-pressure (<c>LatticeSaturatedException</c>)
/// explicitly, instead of letting it reach a generic handler (#5722, #5723).
/// </summary>
/// <remarks>
/// <para>
/// The exception is the store's deliberate "busy, retry later" refusal, and it
/// derives from <see cref="InvalidOperationException"/>. A binding with no arm
/// for it reports it as whatever its generic ladder says. That was either an
/// opaque <c>Internal</c>, logged as a server error, or - where a ladder maps
/// <see cref="InvalidOperationException"/> to <c>FailedPrecondition</c> - a
/// non-retryable precondition failure, which tells a well-behaved client never
/// to retry a refusal that clears by itself. Nothing faults when the arm is
/// missing, so only a structural guard keeps a new binding from reintroducing it.
/// </para>
/// <para>
/// The check reads the compiled exception-handling clauses, which is the only way
/// to observe a <c>catch</c> by reflection. Async dispatch compiles its try/catch
/// into a generated state machine, so the service's nested types are scanned
/// alongside its own methods. Matching is by name, so this library needs no
/// compile-time reference to the product assemblies.
/// </para>
/// </remarks>
public abstract class GrpcSaturationMappingContractTestsBase
{
    private const string SaturationTypeName = "LatticeSaturatedException";

    /// <summary>The package assembly whose gRPC facade services are audited.</summary>
    protected abstract Assembly PackageAssembly { get; }

    /// <summary>
    /// Every gRPC facade service carries a catch clause for the saturation
    /// refusal, so it maps to a retryable status of its own choosing.
    /// </summary>
    [Test]
    public void Every_grpc_facade_service_maps_storage_back_pressure()
    {
        var services = GrpcFacadeCatchClauses.DiscoverServices(PackageAssembly);

        var offenders = services
            .Where(service => !GrpcFacadeCatchClauses.Catches(service, SaturationTypeName))
            .Select(service => $"{service.FullName}: no catch clause for {SaturationTypeName}.")
            .ToList();

        Assert.That(offenders, Is.Empty,
            "Storage back-pressure is a deliberate, retryable refusal and must be mapped explicitly on every "
            + "binding (ResourceExhausted for a facade), never left to a generic handler that reports it as "
            + "Internal or FailedPrecondition. Offenders:"
            + Environment.NewLine + string.Join(Environment.NewLine, offenders));
    }
}
