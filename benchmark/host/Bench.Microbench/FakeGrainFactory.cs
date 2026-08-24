using System;
using System.Collections.Generic;
using Orleans;
using Orleans.Runtime;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Allocation-free routing <see cref="IGrainFactory"/> for the microbench
/// harness. It replaces the previous <c>Substitute.For&lt;IGrainFactory&gt;()</c>
/// mock so the measured allocation profile reflects product code only. The
/// mocked factory was the single largest allocator in the harness: every
/// <c>GetGrain&lt;T&gt;(...)</c> the sagas fan out to routed through
/// NSubstitute's configured-return pipeline, so the BenchmarkDotNet
/// <c>Allocated</c> figure grew with the mock library's per-call overhead
/// (which changed sharply between NSubstitute 5.3.0 and 6.2.0) rather than with
/// Orleans.Lattice. This fake resolves each grain by a dictionary lookup into
/// the harness's real cached grain instances, allocating nothing per call.
/// </summary>
/// <remarks>
/// Routes are registered per grain-interface type, keyed by the primary-key
/// shape the callers use: <see cref="RouteByGuid{T}"/> and
/// <see cref="RouteByGrainId{T}"/> for the leaf / internal grains addressed by
/// GUID, and <see cref="RouteByString{T}"/> for the string-keyed grains
/// (shard, leaf-cache, saga, registry, lattice, and the fire-and-forget
/// auxiliaries). <see cref="RouteKeyedString{T}"/> registers an exact-key
/// override that wins over the type-level string route, mirroring how the
/// prior mock layered a specific-argument <c>.Returns(...)</c> over an
/// <c>Arg.Any&lt;string&gt;()</c> default (used by the multi-tree benchmarks to
/// bind a distinct <see cref="ILattice"/> per tree name). Any unregistered
/// route throws, so an unwired grain type surfaces loudly during a bench run.
/// </remarks>
internal sealed class FakeGrainFactory : IGrainFactory
{
    private readonly Dictionary<Type, Func<Guid, object>> _guidRoutes = [];
    private readonly Dictionary<Type, Func<GrainId, object>> _grainIdRoutes = [];
    private readonly Dictionary<Type, Func<string, object>> _stringRoutes = [];
    private readonly Dictionary<(Type Interface, string Key), object> _keyedStringRoutes = [];

    /// <summary>Routes <c>GetGrain&lt;T&gt;(Guid)</c> through <paramref name="resolver"/>.</summary>
    public void RouteByGuid<T>(Func<Guid, T> resolver) where T : class =>
        _guidRoutes[typeof(T)] = key => resolver(key);

    /// <summary>Routes <c>GetGrain&lt;T&gt;(GrainId)</c> through <paramref name="resolver"/>.</summary>
    public void RouteByGrainId<T>(Func<GrainId, T> resolver) where T : class =>
        _grainIdRoutes[typeof(T)] = key => resolver(key);

    /// <summary>Routes <c>GetGrain&lt;T&gt;(string)</c> through <paramref name="resolver"/>.</summary>
    public void RouteByString<T>(Func<string, T> resolver) where T : class =>
        _stringRoutes[typeof(T)] = key => resolver(key);

    /// <summary>Binds a specific <c>GetGrain&lt;T&gt;(key)</c> to <paramref name="instance"/>,
    /// overriding any type-level string route for that exact key.</summary>
    public void RouteKeyedString<T>(string key, T instance) where T : class =>
        _keyedStringRoutes[(typeof(T), key)] = instance;

    public TGrainInterface GetGrain<TGrainInterface>(Guid primaryKey, string? grainClassNamePrefix = null)
        where TGrainInterface : IGrainWithGuidKey =>
        _guidRoutes.TryGetValue(typeof(TGrainInterface), out var resolver)
            ? (TGrainInterface)resolver(primaryKey)
            : throw NoRoute(typeof(TGrainInterface), primaryKey.ToString());

    public TGrainInterface GetGrain<TGrainInterface>(string primaryKey, string? grainClassNamePrefix = null)
        where TGrainInterface : IGrainWithStringKey =>
        ResolveString<TGrainInterface>(primaryKey);

    public TGrainInterface GetGrain<TGrainInterface>(GrainId grainId)
        where TGrainInterface : IAddressable =>
        _grainIdRoutes.TryGetValue(typeof(TGrainInterface), out var resolver)
            ? (TGrainInterface)resolver(grainId)
            : throw NoRoute(typeof(TGrainInterface), grainId.ToString());

    private TGrainInterface ResolveString<TGrainInterface>(string primaryKey)
    {
        if (_keyedStringRoutes.TryGetValue((typeof(TGrainInterface), primaryKey), out var keyed))
        {
            return (TGrainInterface)keyed;
        }

        return _stringRoutes.TryGetValue(typeof(TGrainInterface), out var resolver)
            ? (TGrainInterface)resolver(primaryKey)
            : throw NoRoute(typeof(TGrainInterface), primaryKey);
    }

    private static NotSupportedException NoRoute(Type grainInterface, string key) =>
        new($"FakeGrainFactory has no route for {grainInterface} with key '{key}'. " +
            "Register one in GlobalSetup (RouteByGuid / RouteByGrainId / RouteByString / RouteKeyedString) " +
            "if a new benchmark path reaches this grain type.");

    // ----- Unused IGrainFactory surface: never reached on any measured path. -----

    public TGrainInterface GetGrain<TGrainInterface>(long primaryKey, string? grainClassNamePrefix = null)
        where TGrainInterface : IGrainWithIntegerKey => throw NotUsed();

    public TGrainInterface GetGrain<TGrainInterface>(Guid primaryKey, string keyExtension, string? grainClassNamePrefix = null)
        where TGrainInterface : IGrainWithGuidCompoundKey => throw NotUsed();

    public TGrainInterface GetGrain<TGrainInterface>(long primaryKey, string keyExtension, string? grainClassNamePrefix = null)
        where TGrainInterface : IGrainWithIntegerCompoundKey => throw NotUsed();

    public IGrain GetGrain(Type grainInterfaceType, Guid grainPrimaryKey) => throw NotUsed();

    public IGrain GetGrain(Type grainInterfaceType, long grainPrimaryKey) => throw NotUsed();

    public IGrain GetGrain(Type grainInterfaceType, string grainPrimaryKey) => throw NotUsed();

    public IGrain GetGrain(Type grainInterfaceType, Guid grainPrimaryKey, string keyExtension) => throw NotUsed();

    public IGrain GetGrain(Type grainInterfaceType, long grainPrimaryKey, string keyExtension) => throw NotUsed();

    public TGrainObserverInterface CreateObjectReference<TGrainObserverInterface>(IGrainObserver obj)
        where TGrainObserverInterface : IGrainObserver => throw NotUsed();

    public void DeleteObjectReference<TGrainObserverInterface>(IGrainObserver obj)
        where TGrainObserverInterface : IGrainObserver => throw NotUsed();

    public IAddressable GetGrain(GrainId grainId) => throw NotUsed();

    public IAddressable GetGrain(GrainId grainId, GrainInterfaceType interfaceType) => throw NotUsed();

    public IAddressable GetGrain(Type grainInterfaceType, IdSpan grainKey, string grainClassNamePrefix) => throw NotUsed();

    public IAddressable GetGrain(Type grainInterfaceType, IdSpan grainKey) => throw NotUsed();

    private static NotSupportedException NotUsed() =>
        new("FakeGrainFactory only implements the keyed GetGrain<T> overloads the microbench uses.");
}
