using System.Reflection;
using NUnit.Framework;

namespace Orleans.Lattice.Testing;

/// <summary>
/// Reusable, product-agnostic guard proving that every gRPC facade service in a
/// package lifts the caller's asserted active tenant onto the ambient
/// active-tenant context.
/// </summary>
/// <remarks>
/// <para>
/// A binding that omits the stamp does not fault, which is what makes the gap so
/// easy to ship: with no ambient tenant the facade's tenant-scoped name
/// resolution silently resolves the reserved default tenant, so a caller that
/// asserted a tenant is served the shared cluster-global namespace instead of its
/// own. That is the isolation failure the scoping exists to close, reintroduced
/// by omission rather than by a visible error.
/// </para>
/// <para>
/// Facade-level unit tests cannot catch it, because they inject a substituted
/// tenant resolver directly and therefore pass whether or not the transport
/// binding ever established the tenant. Only an end-to-end run through the real
/// binding, or a structural guard like this one, closes the loop.
/// </para>
/// <para>
/// Discovery is by reflection so a newly added binding is audited automatically:
/// a concrete subclass names only its assembly, and every gRPC service type in it
/// is required to route its header lookup through the shared
/// <c>LatticeActiveTenantAssertion</c> helper. Matching is by name so this
/// library needs no compile-time reference to the product assemblies, exactly as
/// the serializable-exception and grain-key guards do.
/// </para>
/// <para>
/// The guard is bound per package, and two families of gRPC service are
/// deliberately excluded rather than overlooked. The cross-cluster replication
/// transports (<c>Orleans.Lattice.Replication.Grpc</c>) carry silo-to-silo
/// traffic, not caller traffic: lifting a tenant asserted by a peer would be
/// trusting wire-supplied classification, which the security rules forbid. The
/// authorization control API (<c>Orleans.Lattice.Api.Auth.Grpc</c>) is
/// cluster-global and has no tenant-scoped name resolution at all - it treats
/// tree ids as opaque strings already composed by the data-plane facades - so a
/// stamp there would be inert. Bind this guard to a package only when its facade
/// resolves tenant-scoped names.
/// </para>
/// </remarks>
public abstract class GrpcActiveTenantStampContractTestsBase
{
    /// <summary>Simple name of the shared helper every binding must stamp through.</summary>
    private const string HelperTypeName = "LatticeActiveTenantAssertion";

    /// <summary>Conventional suffix identifying a gRPC facade service type.</summary>
    private const string ServiceTypeSuffix = "GrpcService";

    /// <summary>
    /// The package assembly whose gRPC facade services are audited.
    /// </summary>
    protected abstract Assembly PackageAssembly { get; }

    /// <summary>
    /// Every gRPC facade service in the package must expose a method that stamps
    /// the caller's asserted active tenant, and must route it through the shared
    /// helper rather than re-implementing the parsing and fail-closed rules.
    /// </summary>
    [Test]
    public void Every_grpc_facade_service_stamps_the_active_tenant()
    {
        var services = PackageAssembly.GetTypes()
            .Where(t => t.IsClass && t.Name.EndsWith(ServiceTypeSuffix, StringComparison.Ordinal))
            .Where(t => !t.IsAbstract)
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .ToList();

        Assert.That(services, Is.Not.Empty,
            $"No '*{ServiceTypeSuffix}' types were discovered in {PackageAssembly.GetName().Name}; "
            + "the guard would be inert. Verify PackageAssembly.");

        var offenders = new List<string>();
        foreach (var service in services)
        {
            var stamper = service
                .GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly)
                .FirstOrDefault(m => m.Name.Contains("ActiveTenant", StringComparison.Ordinal));

            if (stamper is null)
            {
                offenders.Add(
                    $"{service.FullName}: no active-tenant stamping method. A facade that never stamps the "
                    + "asserted tenant silently serves every tenant the shared cluster-global namespace.");
            }
        }

        Assert.That(offenders, Is.Empty,
            "Every gRPC facade service must lift the caller's asserted active tenant onto the ambient "
            + $"context for the duration of the call, via the shared {HelperTypeName} helper, so the facade's "
            + "tenant-scoped name resolution sees the caller's tenant rather than the reserved default. "
            + "Offenders:" + Environment.NewLine + string.Join(Environment.NewLine, offenders));
    }
}
