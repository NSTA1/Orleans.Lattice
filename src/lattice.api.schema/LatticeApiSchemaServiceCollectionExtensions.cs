using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema;

/// <summary>
/// Extension methods for registering the optional
/// <c>Orleans.Lattice.Api.Schema</c> schema-management control facade on an
/// Orleans silo.
/// </summary>
public static class LatticeApiSchemaServiceCollectionExtensions
{
    /// <summary>
    /// Adds the transport-agnostic schema-management control facade to the silo:
    /// binds <see cref="LatticeApiSchemaOptions"/>, registers the
    /// <see cref="ILatticeSchemaControl"/> singleton every transport binding (for
    /// example gRPC) adapts over, and registers an idempotency marker. It adds no
    /// transport behaviour of its own.
    /// <para>
    /// Must be called <i>after</i>
    /// <see cref="LatticeSchemaEnforcementServiceCollectionExtensions.AddLatticeSchemaEnforcement(ISiloBuilder, Action{LatticeSchemaEnforcementOptions})"/>:
    /// the schema enforcement layer is the source of truth for the admin, provider,
    /// and authorization seams this facade drives. Calling it first fails fast with
    /// a clear message, mirroring how the sibling control-API add-on guards its
    /// ordering relative to the core registration.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">
    /// Optional delegate that populates <see cref="LatticeApiSchemaOptions"/>.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when <c>AddLatticeSchemaEnforcement(...)</c> has not been called on
    /// the same builder before this call.
    /// </exception>
    public static ISiloBuilder AddLatticeSchemaApi(
        this ISiloBuilder builder,
        Action<LatticeApiSchemaOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Ordering guard: AddLatticeSchemaEnforcement registers the schema admin
        // surface and the authorization seam. Their absence means the facade would
        // have nothing to drive, so fail fast at registration with an actionable
        // message rather than failing obscurely at silo start.
        if (!builder.Services.Any(d => d.ServiceType == typeof(ILatticeSchemaAdmin)))
        {
            throw new InvalidOperationException(
                "AddLatticeSchemaApi() must be called after AddLatticeSchemaEnforcement(). Register the " +
                "schema enforcement layer (siloBuilder.AddLatticeSchemaEnforcement(...)) before adding the " +
                "schema control API.");
        }

        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        // Ensure the options instance is always resolvable even when the caller
        // passes no configure delegate.
        builder.Services.AddOptions<LatticeApiSchemaOptions>();

        // The transport-agnostic control facade. Registered as a silo singleton
        // that every transport binding (for example gRPC) adapts over.
        builder.Services.TryAddSingleton<ILatticeSchemaControl, LatticeSchemaControl>();

        // Idempotency marker: the structural wiring runs once regardless of how
        // many times the host calls this method. A repeat call still layers any
        // supplied configure delegate above, matching how the sibling add-ons treat
        // repeated registration.
        builder.Services.TryAddSingleton<LatticeApiSchemaMarker>();

        return builder;
    }

    /// <summary>
    /// Internal singleton whose sole purpose is to make a repeated
    /// <see cref="AddLatticeSchemaApi"/> call a no-op for the structural wiring
    /// while still layering any supplied options delegate.
    /// </summary>
    internal sealed class LatticeApiSchemaMarker
    {
    }
}
