using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice;
using Orleans.Lattice.Api.Schema;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// Extension methods for registering the optional
/// <c>Orleans.Lattice.Api.TreeAdmin</c> tree-administration control facade on an
/// Orleans silo.
/// </summary>
public static class LatticeApiTreeAdminServiceCollectionExtensions
{
    /// <summary>
    /// Adds the transport-agnostic tree-administration control facade to the silo:
    /// binds <see cref="LatticeApiTreeAdminOptions"/>, registers the
    /// <see cref="ILatticeTreeAdmin"/> singleton every transport binding (for example
    /// gRPC, MCP) adapts over, and registers an idempotency marker. It adds no
    /// transport behaviour of its own.
    /// <para>
    /// Must be called <i>after</i>
    /// <see cref="LatticeApiSchemaServiceCollectionExtensions.AddLatticeSchemaApi(ISiloBuilder, Action{LatticeApiSchemaOptions})"/>:
    /// the facade composes the schema control facade
    /// (<see cref="ILatticeSchemaControl"/>) by delegation, so that facade must be
    /// registered first. Calling it out of order fails fast with a clear message,
    /// mirroring how the sibling control-API add-ons guard their ordering.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">
    /// Optional delegate that populates <see cref="LatticeApiTreeAdminOptions"/>.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when <c>AddLatticeSchemaApi(...)</c> has not been called on the same
    /// builder before this call.
    /// </exception>
    public static ISiloBuilder AddLatticeTreeAdminApi(
        this ISiloBuilder builder,
        Action<LatticeApiTreeAdminOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Ordering guard: the facade composes the schema control facade by
        // delegation, so ILatticeSchemaControl must already be registered (by
        // AddLatticeSchemaApi). Its absence means this facade would have nothing to
        // wrap, so fail fast at registration with an actionable message rather than
        // failing obscurely at silo start.
        if (!builder.Services.Any(d => d.ServiceType == typeof(ILatticeSchemaControl)))
        {
            throw new InvalidOperationException(
                "AddLatticeTreeAdminApi() must be called after AddLatticeSchemaApi(). Register the schema " +
                "control facade (siloBuilder.AddLatticeSchemaApi(...)) before adding the tree-administration " +
                "control API, which composes it.");
        }

        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        // Ensure the options instance is always resolvable even when the caller
        // passes no configure delegate.
        builder.Services.AddOptions<LatticeApiTreeAdminOptions>();

        // The transport-agnostic control facade. Registered as a silo singleton that
        // every transport binding (for example gRPC, MCP) adapts over.
        builder.Services.TryAddSingleton<ILatticeTreeAdmin, LatticeTreeAdmin>();

        // The fail-closed diagnostics authorization seam the facade consults before
        // every read-only diagnostics operation. It resolves the core access gate
        // (the no-op gate when no auth add-on is registered, so it is zero cost) and
        // the optional membership context.
        builder.Services.TryAddSingleton(sp => new TreeAdminAccessAuthorizer(
            sp.GetRequiredService<ILatticeAccessGate>(),
            sp.GetService<ILatticeMembershipContext>()));

        // Idempotency marker: the structural wiring runs once regardless of how many
        // times the host calls this method. A repeat call still layers any supplied
        // configure delegate above, matching how the sibling add-ons treat repeated
        // registration.
        builder.Services.TryAddSingleton<LatticeApiTreeAdminMarker>();

        return builder;
    }

    /// <summary>
    /// Internal singleton whose sole purpose is to make a repeated
    /// <see cref="AddLatticeTreeAdminApi"/> call a no-op for the structural wiring
    /// while still layering any supplied options delegate.
    /// </summary>
    internal sealed class LatticeApiTreeAdminMarker
    {
    }
}
