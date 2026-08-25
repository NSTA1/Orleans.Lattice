using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Extension methods for registering the optional tenant-scoped whole-tree
/// lifecycle and schema control facade
/// (<see cref="ILatticeTenantScopedTreeAdmin"/>) on an Orleans silo.
/// </summary>
public static class LatticeApiTenantScopedTreeAdminServiceCollectionExtensions
{
    /// <summary>
    /// Adds the transport-agnostic tenant-scoped tree/schema control facade to the
    /// silo: registers the <see cref="ILatticeTenantScopedTreeAdmin"/> singleton
    /// that composes the existing <see cref="ILatticeTreeAdmin"/> and
    /// <see cref="ILatticeSchemaAdmin"/> surfaces, binding every tenant-local tree
    /// name to the active tenant's namespace and quota. It adds no transport
    /// behaviour of its own and registers no new lifecycle store; it strictly layers
    /// over the two wrapped facades and the core
    /// <see cref="ITenantAdmissionController"/> (always registered).
    /// <para>
    /// Must be called <i>after</i> both wrapped facades are registered
    /// (<c>siloBuilder.AddLatticeTreeAdminApi(...)</c> and the schema enforcement /
    /// API add-ons that provide <see cref="ILatticeSchemaAdmin"/>). The facade
    /// delegates to those surfaces, so their absence means it would have nothing to
    /// compose; calling it out of order fails fast with a clear message, mirroring
    /// how the sibling control-API add-ons guard their ordering.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when either wrapped facade has not been registered on the same builder
    /// before this call.
    /// </exception>
    public static ISiloBuilder AddLatticeTenantScopedTreeAdminApi(this ISiloBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Ordering guard: the facade composes the whole-tree lifecycle surface, so
        // ILatticeTreeAdmin must already be registered (by AddLatticeTreeAdminApi).
        // Its absence means this facade would have no lifecycle surface to delegate
        // to, so fail fast at registration with an actionable message rather than
        // failing obscurely at silo start.
        if (!builder.Services.Any(d => d.ServiceType == typeof(ILatticeTreeAdmin)))
        {
            throw new InvalidOperationException(
                "AddLatticeTenantScopedTreeAdminApi() must be called after AddLatticeTreeAdminApi(). Register the " +
                "whole-tree admin control API (siloBuilder.AddLatticeTreeAdminApi(...)) before adding the " +
                "tenant-scoped facade, which delegates to it.");
        }

        // Ordering guard: the facade composes the per-tree schema surface, so
        // ILatticeSchemaAdmin must already be registered (by
        // AddLatticeSchemaEnforcement / the schema API add-on).
        if (!builder.Services.Any(d => d.ServiceType == typeof(ILatticeSchemaAdmin)))
        {
            throw new InvalidOperationException(
                "AddLatticeTenantScopedTreeAdminApi() must be called after the schema control surface is " +
                "registered (siloBuilder.AddLatticeSchemaEnforcement(...)/AddLatticeSchemaApi(...)). Register it " +
                "before adding the tenant-scoped facade, which delegates to it.");
        }

        // The transport-agnostic tenant-scoped control facade. Registered as a silo
        // singleton that every transport binding (for example gRPC, MCP) adapts
        // over. ITenantAdmissionController is always resolvable from core (the no-op
        // controller when tenancy is off), so no guard is needed for it.
        builder.Services.TryAddSingleton<ILatticeTenantScopedTreeAdmin, LatticeTenantScopedTreeAdmin>();

        // Idempotency marker: the structural wiring runs once regardless of how
        // many times the host calls this method, matching how the sibling add-ons
        // treat repeated registration.
        builder.Services.TryAddSingleton<LatticeApiTenantScopedTreeAdminMarker>();

        return builder;
    }

    /// <summary>
    /// Internal singleton whose sole purpose is to make a repeated
    /// <see cref="AddLatticeTenantScopedTreeAdminApi"/> call a no-op for the
    /// structural wiring.
    /// </summary>
    internal sealed class LatticeApiTenantScopedTreeAdminMarker
    {
    }
}
