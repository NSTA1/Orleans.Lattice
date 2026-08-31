using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.GrainIndex.Enrollment;
using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex.Tests.Enrollment;

/// <summary>
/// Builds an <see cref="IndexedPersistentState{TState}"/> around a substituted
/// grain context, so the enrolment path can be driven end to end - activate,
/// write, clear - without a silo.
/// </summary>
/// <typeparam name="TState">The state type.</typeparam>
internal sealed class IndexedStateHarness<TState>
{
    /// <summary>Builds the harness.</summary>
    /// <param name="inner">The state object the wrapper wraps.</param>
    /// <param name="grainKey">The grain's primary key.</param>
    /// <param name="grainInstance">
    /// The instance the enrollers test their grain interface against. A
    /// substitute is used rather than a real class so the Orleans code generator
    /// never mistakes the test double for a second implementation of the grain
    /// interface.
    /// </param>
    /// <param name="enrollers">The indexes tracking the grain.</param>
    public IndexedStateHarness(
        IPersistentState<TState> inner,
        string grainKey,
        object grainInstance,
        params GrainIndexEnroller<TState>[] enrollers)
    {
        Inner = inner;

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("harness", grainKey));
        context.GrainInstance.Returns(grainInstance);
        Context = context;

        Indexed = new IndexedPersistentState<TState>(
            inner,
            context,
            new GrainIndexEnrollmentSet<TState>(enrollers),
            NullLogger.Instance);
    }

    /// <summary>The wrapped state object.</summary>
    public IPersistentState<TState> Inner { get; }

    /// <summary>The substituted grain context.</summary>
    public IGrainContext Context { get; }

    /// <summary>The state object under test.</summary>
    public IndexedPersistentState<TState> Indexed { get; }

    /// <summary>Runs the activation hook, as the grain lifecycle would.</summary>
    /// <param name="cancellationToken">Cancels the activation.</param>
    /// <returns>A task that completes when enrolment has run.</returns>
    public Task ActivateAsync(CancellationToken cancellationToken = default) =>
        Indexed.OnStart(cancellationToken);
}
