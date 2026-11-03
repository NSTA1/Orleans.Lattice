using System.Reflection;
using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Reusable, product-agnostic guard proving that every gRPC facade service in a
/// package maps a transient cluster fault explicitly, instead of letting it reach a
/// generic handler that reports it as an opaque <c>Internal</c> (#5742).
/// </summary>
/// <remarks>
/// <para>
/// Under load an Orleans grain call can fail with a <see cref="TimeoutException"/>
/// (the response did not arrive in time, so the call's outcome is unknown) or an
/// <c>OrleansMessageRejectionException</c>, whose subtypes include
/// <c>SiloUnavailableException</c> (the target silo was unavailable, or the message
/// was rejected before it ran). Neither is a server bug. A binding with no arm for
/// them reports them as <c>Internal</c>, logged as a server error, so a client
/// cannot tell a transient fault it should retry or reconcile from a defect.
/// </para>
/// <para>
/// A facade maps the timeout to <c>DeadlineExceeded</c> and the rejection to
/// <c>Unavailable</c>. Nothing faults when an arm is missing, so only a structural
/// guard keeps a new binding from reintroducing the opaque mapping. A filtered
/// <c>catch (Exception) when (...)</c> does not count: the clause must name the type.
/// </para>
/// </remarks>
public abstract class GrpcTransientFaultMappingContractTestsBase
{
    private static readonly string[] TransientFaultTypeNames =
    [
        nameof(TimeoutException),
        "OrleansMessageRejectionException",
    ];

    /// <summary>The package assembly whose gRPC facade services are audited.</summary>
    protected abstract Assembly PackageAssembly { get; }

    /// <summary>
    /// Every gRPC facade service carries a catch clause for each transient cluster
    /// fault, so it maps to a status a client can act on.
    /// </summary>
    [Test]
    public void Every_grpc_facade_service_maps_transient_cluster_faults()
    {
        var services = GrpcFacadeCatchClauses.DiscoverServices(PackageAssembly);

        var offenders = new List<string>();
        foreach (var service in services)
        {
            foreach (var typeName in TransientFaultTypeNames)
            {
                if (!GrpcFacadeCatchClauses.Catches(service, typeName))
                {
                    offenders.Add($"{service.FullName}: no catch clause for {typeName}.");
                }
            }
        }

        Assert.That(offenders, Is.Empty,
            "A grain-call timeout (outcome unknown) and a silo-unavailable or rejected message (retryable) are "
            + "transient cluster faults and must be mapped explicitly on every binding (DeadlineExceeded and "
            + "Unavailable for a facade), never left to a generic handler that reports them as Internal. Offenders:"
            + Environment.NewLine + string.Join(Environment.NewLine, offenders));
    }
}
