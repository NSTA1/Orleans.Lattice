using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// Extension methods for registering the optional
/// <c>Orleans.Lattice.Api.Auth</c> add-on - the opt-in configuration and control
/// facade for membership and authorization policy - on an Orleans silo.
/// </summary>
public static class LatticeApiAuthServiceCollectionExtensions
{
    /// <summary>
    /// Adds the membership and policy control facade to the silo. Binds
    /// <see cref="LatticeApiAuthOptions"/> and registers the transport-agnostic
    /// facade (<c>ILatticeAuthAdmin</c>). It adds no transport behaviour of its
    /// own (a sibling binding maps the gRPC surface) and no bespoke authorization
    /// path: every facade operation first authorizes the caller as an
    /// administrator through the same enforcement the in-cluster mutation path
    /// uses, so the cluster's access gate remains the single source of
    /// enforcement.
    /// <para>
    /// The API is <b>opt-in and absent by default</b>: nothing is registered
    /// unless the host calls this method, and once registered it performs no
    /// background work until a facade method is called. Must be called
    /// <i>after</i> <c>AddLatticeAuth(...)</c>: the authorization registration is
    /// the source of truth for the policy store and membership directory this API
    /// administers. Calling it first fails fast with a clear message.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">
    /// Optional delegate that populates <see cref="LatticeApiAuthOptions"/>.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when <c>AddLatticeAuth(...)</c> has not been called on the same
    /// builder before this call.
    /// </exception>
    public static ISiloBuilder AddLatticeAuthApi(
        this ISiloBuilder builder,
        Action<LatticeApiAuthOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Ordering guard: AddLatticeAuth registers the policy store this facade
        // administers and introspects. Its absence means the facade would have no
        // policy store or gate to dial, so fail fast at registration with an
        // actionable message rather than failing obscurely at silo start.
        if (!builder.Services.Any(d => d.ServiceType == typeof(ILatticeAuthorizationPolicyStore)))
        {
            throw new InvalidOperationException(
                "AddLatticeAuthApi() must be called after AddLatticeAuth(). Register the " +
                "authorization package (siloBuilder.AddLatticeAuth(...)) before adding the auth API.");
        }

        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        // Ensure the options instance is always resolvable even when the caller
        // passes no configure delegate.
        builder.Services.AddOptions<LatticeApiAuthOptions>();

        // The transport-agnostic control facade. Registered as a silo singleton
        // that every transport binding (gRPC now) adapts over.
        builder.Services.TryAddSingleton<ILatticeAuthAdmin, LatticeAuthAdmin>();

        // Idempotency marker: a repeat call still layers any supplied configure
        // delegate above, matching how the sibling add-ons treat repeated
        // registration.
        builder.Services.TryAddSingleton<LatticeApiAuthMarker>();

        return builder;
    }

    /// <summary>
    /// Internal singleton whose sole purpose is to make a repeated
    /// <see cref="AddLatticeAuthApi"/> call a no-op for the structural wiring
    /// while still layering any supplied options delegate.
    /// </summary>
    internal sealed class LatticeApiAuthMarker
    {
    }
}
