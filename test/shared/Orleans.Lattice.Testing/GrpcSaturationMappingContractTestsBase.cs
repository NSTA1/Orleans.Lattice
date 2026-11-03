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
    private const string ServiceTypeSuffix = "GrpcService";

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
        var services = PackageAssembly.GetTypes()
            .Where(t => t.IsClass && !t.IsAbstract && t.Name.EndsWith(ServiceTypeSuffix, StringComparison.Ordinal))
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .ToList();

        Assert.That(services, Is.Not.Empty,
            $"No '*{ServiceTypeSuffix}' types were discovered in {PackageAssembly.GetName().Name}; "
            + "the guard would be inert. Verify PackageAssembly.");

        var offenders = services
            .Where(service => !Catches(service, SaturationTypeName))
            .Select(service => $"{service.FullName}: no catch clause for {SaturationTypeName}.")
            .ToList();

        Assert.That(offenders, Is.Empty,
            "Storage back-pressure is a deliberate, retryable refusal and must be mapped explicitly on every "
            + "binding (ResourceExhausted for a facade), never left to a generic handler that reports it as "
            + "Internal or FailedPrecondition. Offenders:"
            + Environment.NewLine + string.Join(Environment.NewLine, offenders));
    }

    private static bool Catches(Type service, string exceptionTypeName)
    {
        const BindingFlags Members = BindingFlags.Instance | BindingFlags.Static
            | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly;

        var candidates = new List<Type> { service };
        candidates.AddRange(service.GetNestedTypes(BindingFlags.Public | BindingFlags.NonPublic));

        foreach (var type in candidates)
        {
            foreach (var method in type.GetMethods(Members))
            {
                MethodBody? body;
                try
                {
                    body = method.GetMethodBody();
                }
                catch (Exception)
                {
                    continue;
                }

                if (body is null)
                {
                    continue;
                }

                foreach (var clause in body.ExceptionHandlingClauses)
                {
                    if (clause.Flags != ExceptionHandlingClauseOptions.Clause)
                    {
                        continue;
                    }

                    Type? caught;
                    try
                    {
                        caught = clause.CatchType;
                    }
                    catch (Exception)
                    {
                        continue;
                    }

                    if (caught is not null && string.Equals(caught.Name, exceptionTypeName, StringComparison.Ordinal))
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }
}
