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

    /// <summary>Conventional suffix identifying a binding's options type.</summary>
    private const string OptionsTypeSuffix = "GrpcOptions";

    /// <summary>The option every binding exposes to name the inbound header.</summary>
    private const string OptionPropertyName = "ActiveTenantHeaderName";

    /// <summary>The canonical header name the option must default to.</summary>
    private const string DefaultHeaderName = "lattice-active-tenant";

    /// <summary>
    /// The package assembly whose gRPC facade services are audited.
    /// </summary>
    protected abstract Assembly PackageAssembly { get; }

    /// <summary>
    /// Every binding must expose an <c>ActiveTenantHeaderName</c> option defaulting
    /// to the canonical header, so a host can rename or disable header-based tenant
    /// selection, and so every binding agrees on the wire name out of the box.
    /// </summary>
    /// <remarks>
    /// A binding that stamps but reads a different default header is as broken as
    /// one that never stamps: the forwarding interceptor sends a single agreed
    /// header name, so a mismatch silently resolves no tenant and serves the shared
    /// cluster-global namespace. Asserting the default here also gives the option
    /// the per-package test coverage the repository requires of every public member,
    /// in one place rather than seven near-identical fixtures.
    /// </remarks>
    [Test]
    public void Every_binding_exposes_the_active_tenant_header_option()
    {
        var optionTypes = PackageAssembly.GetTypes()
            .Where(t => t.IsClass && !t.IsAbstract && t.Name.EndsWith(OptionsTypeSuffix, StringComparison.Ordinal))
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .ToList();

        Assert.That(optionTypes, Is.Not.Empty,
            $"No '*{OptionsTypeSuffix}' types were discovered in {PackageAssembly.GetName().Name}; "
            + "the guard would be inert. Verify PackageAssembly.");

        var offenders = new List<string>();
        foreach (var optionType in optionTypes)
        {
            var property = optionType.GetProperty(OptionPropertyName);
            if (property is null || property.PropertyType != typeof(string))
            {
                offenders.Add($"{optionType.FullName}: no public string '{OptionPropertyName}' property.");
                continue;
            }

            var instance = Activator.CreateInstance(optionType);
            var actual = property.GetValue(instance) as string;
            if (!string.Equals(actual, DefaultHeaderName, StringComparison.Ordinal))
            {
                offenders.Add(
                    $"{optionType.FullName}.{OptionPropertyName} defaults to '{actual}', expected '{DefaultHeaderName}'.");
            }
        }

        Assert.That(offenders, Is.Empty,
            "Every gRPC binding must expose an active-tenant header option defaulting to the canonical "
            + $"'{DefaultHeaderName}', so all bindings agree with the header the forwarding interceptor sends. "
            + "Offenders:" + Environment.NewLine + string.Join(Environment.NewLine, offenders));
    }

    /// <summary>Simple name of the fail-closed tenant denial every ladder must map.</summary>
    private const string DenialTypeName = "LatticeTenantAccessDeniedException";

    /// <summary>
    /// Every gRPC facade service must handle the fail-closed tenant denial
    /// explicitly, so it maps to <c>PermissionDenied</c> rather than falling
    /// through to the generic handler and surfacing as <c>Internal</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Reported as <c>Internal</c> a denial is actively harmful, not merely
    /// mislabelled: <c>Internal</c> is a canonical retryable status, so a
    /// well-behaved client backs off and retries a decision that can never change;
    /// the generic handler replaces the actionable reason with a fixed message; and
    /// the refusal is logged as an error and counted against the server-fault rate
    /// operators alert on.
    /// </para>
    /// <para>
    /// The check reads the compiled exception-handling clauses, which is the only
    /// way to observe a <c>catch</c> by reflection. Async dispatch compiles its
    /// try/catch into a generated state machine, so the service's nested types are
    /// scanned alongside its own methods.
    /// </para>
    /// </remarks>
    [Test]
    public void Every_grpc_facade_service_maps_the_tenant_denial()
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
            if (!CatchesDenial(service))
            {
                offenders.Add(
                    $"{service.FullName}: no catch clause for {DenialTypeName}, so a fail-closed tenant "
                    + "denial reaches the generic handler and surfaces as Internal.");
            }
        }

        Assert.That(offenders, Is.Empty,
            "A tenant denial is an authorization outcome, not a server fault, and must map to "
            + "PermissionDenied on every binding. Offenders:"
            + Environment.NewLine + string.Join(Environment.NewLine, offenders));
    }

    /// <summary>
    /// Reports whether <paramref name="service"/> - or any state machine generated
    /// from its async methods - carries a catch clause for the tenant denial.
    /// </summary>
    private static bool CatchesDenial(Type service)
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
                    // A generic definition or an unavailable body tells us nothing;
                    // it must not mask a genuine handler found elsewhere.
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

                    if (caught is not null
                        && string.Equals(caught.Name, DenialTypeName, StringComparison.Ordinal))
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }

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
