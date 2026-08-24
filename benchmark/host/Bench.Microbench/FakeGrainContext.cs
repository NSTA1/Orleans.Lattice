using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Allocation-free in-memory <see cref="IGrainContext"/> for the microbench
/// harness. It replaces the previous <c>Substitute.For&lt;IGrainContext&gt;()</c>
/// mocks so the measured allocation profile reflects product code only, not the
/// mocking framework's per-call dynamic-proxy machinery (NSubstitute /
/// Castle DynamicProxy), which otherwise dominated the BenchmarkDotNet
/// <c>Allocated</c> figure and made it a function of the mock library version
/// rather than of Orleans.Lattice.
/// </summary>
/// <remarks>
/// Only the seams the lattice grains actually touch at runtime are
/// implemented: <see cref="GrainId"/> (self-identification, metric tags, key
/// derivation), <see cref="ActivationServices"/> (optional-service resolution,
/// which returns <see langword="null"/> for anything unwired exactly as the
/// prior auto-substituted provider did), and a no-op <see cref="Deactivate"/>
/// (the purge path). Every other member throws, so any future reliance on an
/// unimplemented seam surfaces loudly during a bench run instead of silently
/// returning a mock value.
/// </remarks>
internal sealed class FakeGrainContext(GrainId grainId, IServiceProvider activationServices) : IGrainContext
{
    /// <summary>A service provider that resolves nothing - the default backing
    /// for a context whose grain wires no optional services.</summary>
    internal sealed class EmptyServiceProvider : IServiceProvider
    {
        public static readonly EmptyServiceProvider Instance = new();
        public object? GetService(Type serviceType) => null;
    }

    /// <summary>A service provider backed by a fixed type-to-instance map;
    /// returns <see langword="null"/> for any unmapped service, matching the
    /// prior auto-substituted provider's behaviour.</summary>
    internal sealed class MapServiceProvider(IReadOnlyDictionary<Type, object?> services) : IServiceProvider
    {
        public object? GetService(Type serviceType) =>
            services.TryGetValue(serviceType, out var instance) ? instance : null;
    }

    public FakeGrainContext(GrainId grainId)
        : this(grainId, EmptyServiceProvider.Instance)
    {
    }

    public GrainId GrainId { get; } = grainId;

    public IServiceProvider ActivationServices { get; } = activationServices;

    // Purge path only (DeactivateOnIdle-style); never on the measured hot path.
    // A no-op is a faithful, allocation-free stand-in for the mock.
    public void Deactivate(DeactivationReason deactivationReason, CancellationToken cancellationToken = default)
    {
    }

    public GrainReference GrainReference => throw NotUsed();

    public object GrainInstance => throw NotUsed();

    public ActivationId ActivationId => throw NotUsed();

    public GrainAddress Address => throw NotUsed();

    public IGrainLifecycle ObservableLifecycle => throw NotUsed();

    public IWorkItemScheduler Scheduler => throw NotUsed();

    public Task Deactivated => throw NotUsed();

    public void SetComponent<TComponent>(TComponent? value) where TComponent : class => throw NotUsed();

    public object GetComponent(Type componentType) => throw NotUsed();

    public object GetTarget() => throw NotUsed();

    public void ReceiveMessage(object message) => throw NotUsed();

    public void Activate(Dictionary<string, object>? requestContext, CancellationToken cancellationToken = default) =>
        throw NotUsed();

    public void Rehydrate(IRehydrationContext context) => throw NotUsed();

    public void Migrate(Dictionary<string, object>? requestContext, CancellationToken cancellationToken = default) =>
        throw NotUsed();

    public bool Equals(IGrainContext? other) => ReferenceEquals(this, other);

    private static NotSupportedException NotUsed() =>
        new("FakeGrainContext only implements GrainId, ActivationServices, and Deactivate; " +
            "the microbench never exercises this member. Implement it here if a new benchmark path needs it.");
}
