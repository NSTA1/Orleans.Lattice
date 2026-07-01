using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Public DI extension methods that customise the
/// <c>Orleans.Lattice.Replication</c> secret-source surface. The default
/// secret source (registered automatically by
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>)
/// reads from the environment-variable surface documented on
/// <see cref="LatticeReplicationEnvironmentVariables"/>. Hosts replace it
/// with one of the helpers on this type.
/// </summary>
public static class LatticeReplicationSecurityServiceCollectionExtensions
{
    /// <summary>
    /// Replaces the default <see cref="ILatticeReplicationSecretSource"/>
    /// with the supplied implementation type. The implementation is
    /// activated through DI and registered as a singleton, so it can
    /// declare constructor dependencies on any other service in the
    /// container (an Azure Key Vault client, an
    /// <c>IHttpClientFactory</c>, etc.).
    /// </summary>
    /// <typeparam name="TSource">The custom secret source type.</typeparam>
    public static ISiloBuilder AddLatticeReplicationSecrets<TSource>(this ISiloBuilder builder)
        where TSource : class, ILatticeReplicationSecretSource
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddSingleton<TSource>();
        builder.Services.Replace(ServiceDescriptor.Singleton<ILatticeReplicationSecretSource>(
            sp => sp.GetRequiredService<TSource>()));
        return builder;
    }

    /// <summary>
    /// Replaces the default <see cref="ILatticeReplicationSecretSource"/>
    /// with the instance returned by <paramref name="factory"/>. The
    /// factory variant is the right tool when the custom source has
    /// non-trivial construction logic or wraps a pre-existing
    /// configured client.
    /// </summary>
    /// <typeparam name="TSource">The custom secret source type.</typeparam>
    /// <param name="builder">The silo builder.</param>
    /// <param name="factory">Resolves the secret source from the service provider.</param>
    public static ISiloBuilder AddLatticeReplicationSecrets<TSource>(
        this ISiloBuilder builder,
        Func<IServiceProvider, TSource> factory)
        where TSource : class, ILatticeReplicationSecretSource
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(factory);
        builder.Services.Replace(ServiceDescriptor.Singleton<ILatticeReplicationSecretSource>(
            sp => factory(sp)));
        return builder;
    }

    /// <summary>
    /// Replaces the default <see cref="ILatticeReplicationSecretSource"/>
    /// with a <see cref="ConfigurationBindingSecretSource"/> bound to
    /// the supplied configuration section. Intended for hosts that
    /// inject secrets via a non-file configuration provider - the
    /// startup hostile-config scan still runs and will reject
    /// <c>appsettings.json</c>-backed secrets under the app directory.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="section">
    /// The configuration section that contains <c>Secret</c>,
    /// <c>AcceptedSecrets</c>, and <c>PeerSecrets</c>. Typically
    /// <c>configuration.GetSection("LatticeReplication:Secrets")</c>.
    /// </param>
    public static ISiloBuilder AddLatticeReplicationSecretsFromConfiguration(
        this ISiloBuilder builder,
        IConfiguration section)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(section);
        builder.Services.Replace(ServiceDescriptor.Singleton<ILatticeReplicationSecretSource>(
            _ => new ConfigurationBindingSecretSource(section)));
        return builder;
    }

    /// <summary>
    /// Configures the security-related options (refresh interval,
    /// authenticator policy, hostile-config scan toggle). The secret
    /// material itself flows through <see cref="ILatticeReplicationSecretSource"/>
    /// rather than through these options.
    /// </summary>
    public static ISiloBuilder ConfigureLatticeReplicationSecurity(
        this ISiloBuilder builder,
        Action<LatticeReplicationSecurityOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        builder.Services.Configure(configure);
        return builder;
    }
}
