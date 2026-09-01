using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex.Enrollment;

/// <summary>
/// Binds an <see cref="IndexedAttribute"/> parameter to the persistent-state
/// object that keeps the grain's index entries in step with its state.
/// </summary>
/// <remarks>
/// <para>
/// This is Orleans' own facet extension point - the same one
/// <c>[PersistentState]</c> is built on - so an indexed grain is constructed by
/// exactly the mechanism an ordinary persistent grain is, with no interception
/// of grain calls and no wrapping of a storage provider that untracked grains
/// share.
/// </para>
/// <para>
/// The generic work happens in <see cref="GetFactory"/>, which Orleans calls
/// once per annotated constructor parameter when it builds a grain type's
/// activator, so an activation pays for one reflective invoke and no type
/// resolution.
/// </para>
/// <para>
/// When no declared index projects the annotated state type, the real state
/// object is returned unwrapped: an attribute that currently matches nothing
/// costs the grain nothing at all, which is what makes it safe to annotate a
/// grain before its index is declared.
/// </para>
/// </remarks>
internal sealed class IndexedAttributeMapper : IAttributeToFactoryMapper<IndexedAttribute>
{
    private static readonly MethodInfo CreateMethod =
        typeof(IndexedAttributeMapper).GetMethod(nameof(Create), BindingFlags.NonPublic | BindingFlags.Static)!;

    /// <inheritdoc />
    public Factory<IGrainContext, object> GetFactory(ParameterInfo parameter, IndexedAttribute metadata)
    {
        ArgumentNullException.ThrowIfNull(parameter);
        ArgumentNullException.ThrowIfNull(metadata);

        var parameterType = parameter.ParameterType;
        if (!parameterType.IsGenericType
            || parameterType.GetGenericTypeDefinition() != typeof(IPersistentState<>))
        {
            throw new ArgumentException(
                $"[{nameof(IndexedAttribute)}] must annotate an IPersistentState<TState> parameter, but "
                + $"'{parameter.Name}' is of type '{parameterType}'. Declare the parameter as "
                + "IPersistentState<TState> so the grain's state can be both persisted and indexed.",
                nameof(parameter));
        }

        // Mirrors [PersistentState]: an unnamed state takes the parameter's own
        // name, so the storage key of an indexed grain is identical to the one it
        // would have had without the attribute.
        IPersistentStateConfiguration configuration = string.IsNullOrEmpty(metadata.StateName)
            ? new IndexedStateConfiguration(parameter.Name ?? string.Empty, metadata.StorageName)
            : metadata;

        var create = CreateMethod.MakeGenericMethod(parameterType.GetGenericArguments());
        return context => create.Invoke(null, [context, configuration])!;
    }

    private static object Create<TState>(IGrainContext context, IPersistentStateConfiguration configuration)
    {
        var services = context.ActivationServices;
        var inner = services
            .GetRequiredService<IPersistentStateFactory>()
            .Create<TState>(context, configuration);

        var set = services.GetRequiredService<GrainIndexEnrollmentSet<TState>>();
        if (set.IsEmpty)
            return inner;

        var indexed = new IndexedPersistentState<TState>(
            inner,
            context,
            set,
            services.GetRequiredService<ILogger<IndexedPersistentState<TState>>>());

        indexed.Participate(context.ObservableLifecycle);
        return indexed;
    }

    /// <summary>
    /// The persistence configuration used when the attribute names no state,
    /// carrying the annotated parameter's name in its place.
    /// </summary>
    private sealed class IndexedStateConfiguration(string stateName, string storageName)
        : IPersistentStateConfiguration
    {
        public string StateName { get; } = stateName;

        public string StorageName { get; } = storageName;
    }
}
