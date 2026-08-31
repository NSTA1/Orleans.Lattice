using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Orleans.Hosting;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Extension methods for declaring grain indexes on an Orleans silo, following
/// the same shape as the core <c>AddLattice</c> / <c>ConfigureLattice</c> pair:
/// <c>AddGrainIndex</c> declares an index, <c>ConfigureGrainIndex</c> overrides
/// its settings.
/// </summary>
public static class GrainIndexServiceCollectionExtensions
{
    /// <summary>
    /// Declares a grain index over <typeparamref name="TGrain"/>, projecting the
    /// properties of <typeparamref name="TState"/> that
    /// <paramref name="configure"/> opts in.
    /// <para>Example:</para>
    /// <code>
    /// silo.AddGrainIndex&lt;IUserGrain, UserState&gt;(cfg =&gt; cfg
    ///     .WithName("users")
    ///     .Include(x =&gt; x.Age)
    ///     .Include(x =&gt; x.Country));
    /// </code>
    /// </summary>
    /// <typeparam name="TGrain">The indexed grain interface type.</typeparam>
    /// <typeparam name="TState">The grain-state type the index projects from.</typeparam>
    /// <param name="builder">The silo builder. Must not be <c>null</c>.</param>
    /// <param name="configure">Declares the index. Must not be <c>null</c>.</param>
    /// <returns>The silo builder, for chaining.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    /// <exception cref="GrainIndexKeyEncodingException">
    /// The declaration supplied no key codec and no built-in codec can encode
    /// <typeparamref name="TGrain"/>'s key, so the grain is not indexable.
    /// </exception>
    public static ISiloBuilder AddGrainIndex<TGrain, TState>(
        this ISiloBuilder builder,
        Action<GrainIndexBuilder<TGrain, TState>> configure)
        where TGrain : IGrain
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var indexBuilder = new GrainIndexBuilder<TGrain, TState>();
        configure(indexBuilder);

        var definition = indexBuilder.Build();
        var indexName = definition.Name;
        var treeName = indexBuilder.TreeNameOverride ?? GrainIndexTreeNames.ForIndex(indexName);
        var allowReplication = indexBuilder.AllowReplicationValue;
        var backfillBatchSize = indexBuilder.BackfillBatchSizeOverride;
        var backfillInterval = indexBuilder.BackfillIntervalOverride;

        var services = builder.Services;

        // Seed the named options from the declaration. A later
        // ConfigureGrainIndex call is registered after this one and therefore
        // wins, which is what makes the mirror an override rather than a
        // suggestion. Knobs the declaration did not set are left alone so a
        // global ConfigureGrainIndex applied first is not silently undone.
        services.Configure<GrainIndexOptions>(indexName, options =>
        {
            options.TreeName = treeName;
            options.AllowReplication = allowReplication;
            if (backfillBatchSize is { } batchSize)
            {
                options.BackfillBatchSize = batchSize;
            }

            if (backfillInterval is { } interval)
            {
                options.BackfillInterval = interval;
            }
        });

        services.Configure<GrainIndexDeclarationOptions>(
            declarations => declarations.Definitions.Add(definition));

        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<GrainIndexOptions>, GrainIndexOptionsValidator>());
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<GrainIndexDeclarationOptions>, GrainIndexDeclarationOptionsValidator>());
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, GrainIndexStartupValidator>());

        return builder;
    }

    /// <summary>
    /// Configures global <see cref="GrainIndexOptions"/> that apply to every
    /// declared index unless a per-index override is registered afterwards.
    /// </summary>
    /// <param name="builder">The silo builder. Must not be <c>null</c>.</param>
    /// <param name="configure">Mutates every named options instance. Must not be <c>null</c>.</param>
    /// <returns>The silo builder, for chaining.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public static ISiloBuilder ConfigureGrainIndex(
        this ISiloBuilder builder,
        Action<GrainIndexOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        builder.Services.ConfigureAll(configure);
        return builder;
    }

    /// <summary>
    /// Configures <see cref="GrainIndexOptions"/> for the single index named
    /// <paramref name="indexName"/>, overriding both the declaration's seeded
    /// values and any global defaults.
    /// </summary>
    /// <param name="builder">The silo builder. Must not be <c>null</c>.</param>
    /// <param name="indexName">The index to configure. Must not be <c>null</c>, empty, or white space.</param>
    /// <param name="configure">Mutates that index's options. Must not be <c>null</c>.</param>
    /// <returns>The silo builder, for chaining.</returns>
    /// <exception cref="ArgumentException"><paramref name="indexName"/> is empty or white space.</exception>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public static ISiloBuilder ConfigureGrainIndex(
        this ISiloBuilder builder,
        string indexName,
        Action<GrainIndexOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(indexName);
        ArgumentNullException.ThrowIfNull(configure);
        builder.Services.Configure(indexName, configure);
        return builder;
    }
}
