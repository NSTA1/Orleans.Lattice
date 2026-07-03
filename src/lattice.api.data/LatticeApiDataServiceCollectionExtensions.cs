using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Data;

/// <summary>
/// Extension methods for registering the optional
/// <c>Orleans.Lattice.Api.Data</c> add-on - the opt-in read-write external
/// data-plane API - on an Orleans silo.
/// </summary>
public static class LatticeApiDataServiceCollectionExtensions
{
    /// <summary>
    /// Adds the read-write data-plane API to the silo. Binds
    /// <see cref="LatticeApiDataOptions"/> and registers the transport-agnostic
    /// facade (<c>ILatticeDataApi</c>). It adds no transport behaviour of its own
    /// (a sibling binding maps the gRPC surface) and no authorization path: every
    /// facade operation routes through the gated <see cref="ILattice"/> surface,
    /// so the cluster's access gate is the single source of enforcement.
    /// <para>
    /// The API is <b>opt-in and absent by default</b>: nothing is registered
    /// unless the host calls this method. Must be called <i>after</i>
    /// <c>AddLattice(...)</c>: the core registration is the source of truth for
    /// the tree registry and options system this API dials. Calling it first
    /// fails fast with a clear message.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">
    /// Optional delegate that populates <see cref="LatticeApiDataOptions"/>.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when <c>AddLattice(...)</c> has not been called on the same
    /// builder before this call.
    /// </exception>
    public static ISiloBuilder AddLatticeDataApi(
        this ISiloBuilder builder,
        Action<LatticeApiDataOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Ordering guard: AddLattice registers the core options validator
        // (IValidateOptions<LatticeOptions>). Its absence means the data API
        // would have no tree registry to dial, so fail fast at registration with
        // an actionable message rather than failing obscurely at silo start.
        if (!builder.Services.Any(d => d.ServiceType == typeof(IValidateOptions<LatticeOptions>)))
        {
            throw new InvalidOperationException(
                "AddLatticeDataApi() must be called after AddLattice(). Register the core " +
                "lattice (siloBuilder.AddLattice(...)) before adding the data API.");
        }

        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        // Ensure the options instance is always resolvable even when the caller
        // passes no configure delegate.
        builder.Services.AddOptions<LatticeApiDataOptions>();

        // The transport-agnostic read-write facade. Registered as a silo
        // singleton that every transport binding (gRPC now) adapts over.
        builder.Services.TryAddSingleton<ILatticeDataApi, LatticeDataApi>();

        // Idempotency marker: a repeat call still layers any supplied configure
        // delegate above, matching how the sibling add-ons treat repeated
        // registration.
        builder.Services.TryAddSingleton<LatticeApiDataMarker>();

        return builder;
    }

    /// <summary>
    /// Internal singleton whose sole purpose is to make a repeated
    /// <see cref="AddLatticeDataApi"/> call a no-op for the structural wiring
    /// while still layering any supplied options delegate.
    /// </summary>
    internal sealed class LatticeApiDataMarker
    {
    }
}
