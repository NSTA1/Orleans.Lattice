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
/// was rejected before it ran), or an <c>EnumerationAbortedException</c> (a remote
/// enumerator was lost because its grain deactivated mid-read). None is a server bug.
/// A binding with no arm for
/// them reports them as <c>Internal</c>, logged as a server error, so a client
/// cannot tell a transient fault it should retry or reconcile from a defect.
/// </para>
/// <para>
/// A facade maps the timeout to <c>DeadlineExceeded</c> and the other two to
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
        "EnumerationAbortedException",
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
            "A grain-call timeout (outcome unknown), a silo-unavailable or rejected message and an aborted remote "
            + "enumeration (retryable) are transient cluster faults and must be mapped explicitly on every binding "
            + "(DeadlineExceeded and Unavailable for a facade), never left to a generic handler that reports them as "
            + "Internal. Offenders:"
            + Environment.NewLine + string.Join(Environment.NewLine, offenders));
    }

    /// <summary>
    /// Every catch chain that maps the transient cluster faults also maps a WAL shard
    /// quiesced for a placement move (#5800), ahead of any broader clause.
    /// </summary>
    /// <remarks>
    /// <c>LatticeWalQuiescingException</c> is transient, self-healing back-pressure: the
    /// refused write was never committed and the caller should retry shortly. It derives
    /// from <see cref="InvalidOperationException"/>, so a chain without its own arm, or
    /// with one below a <c>catch (InvalidOperationException)</c> or <c>catch (Exception)</c>,
    /// reports it as <c>FailedPrecondition</c> or <c>Internal</c> and the client does not
    /// retry a write that would succeed moments later.
    /// </remarks>
    [Test]
    public void Every_grpc_facade_chain_maps_a_wal_quiesce_ahead_of_broader_catches()
    {
        var offenders = GrpcFacadeCatchClauses.DiscoverServices(PackageAssembly)
            .SelectMany(service => GrpcFacadeCatchClauses.ChainsMissingOrShadowing(
                service,
                "OrleansMessageRejectionException",
                "LatticeWalQuiescingException",
                [nameof(InvalidOperationException), nameof(Exception)]))
            .ToList();

        Assert.That(offenders, Is.Empty,
            "A WAL shard quiesced for a placement move refuses appends briefly and commits nothing; every facade "
            + "chain must map it to the retryable Unavailable before a broader clause reports it as a precondition "
            + "failure or a server error. Offenders:"
            + Environment.NewLine + string.Join(Environment.NewLine, offenders));
    }
}
