using System.Collections.Frozen;
using System.Diagnostics;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice;

/// <summary>
/// A silo-wide <see cref="IOutgoingGrainCallFilter"/> that records, for every
/// <see cref="ILatticeRegistry"/> call dispatched from this silo, how long the
/// caller waited and how the call ended, tagged by interface method. Installed by
/// <see cref="LatticeServiceCollectionExtensions.AddLattice"/> on every silo; it
/// is not opt-in.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists (issue #3088).</b> Registry contention was diagnosed from
/// the <c>Diagnostics: [... CurrentlyExecuting=...]</c> block Orleans appends to a
/// <c>Response did not arrive on time</c> timeout. Orleans stops emitting that
/// block silo-wide once the silo saturates, and its absence is indistinguishable
/// from "the registry was idle", so the one diagnostic that named the registry's
/// occupant produced a confident all-clear exactly when it had stopped measuring.
/// This filter is the first-party replacement: a sample for every dispatched
/// call, with no dependency on any log field. See
/// <see cref="LatticeMetrics.RegistryCallerDuration"/> for how the three readings
/// (fine, slow, unreachable) separate.
/// </para>
/// <para>
/// <b>Why the outgoing seam.</b> The grain-body census
/// (<see cref="LatticeMetrics.RegistryCallDuration"/>) can only see calls the
/// registry admitted. A registry nobody can reach produces no body samples at
/// all, which is the same silence the vanished log field produced. Only the
/// caller observes a call that is never served, so the caller is where this is
/// recorded.
/// </para>
/// <para>
/// <b>Cost.</b> Every outgoing call on the silo pays one reference comparison on
/// its interface type. A registry call additionally pays a frozen-dictionary
/// lookup and one histogram record; the tag pairs are precomputed, so nothing is
/// allocated per call beyond the async state machine of the awaited call itself.
/// </para>
/// </remarks>
internal sealed class LatticeRegistryCallObservationFilter : IOutgoingGrainCallFilter
{
    /// <summary>The <see cref="LatticeMetrics.TagOutcome"/> value for a call that returned.</summary>
    internal const string CompletedOutcome = "completed";

    /// <summary>
    /// The <see cref="LatticeMetrics.TagOutcome"/> value for a call that ended in
    /// a <see cref="TimeoutException"/> - the Orleans response deadline, which is
    /// how an unreachable or wedged registry presents at its caller.
    /// </summary>
    internal const string TimeoutOutcome = "timeout";

    /// <summary>The <see cref="LatticeMetrics.TagOutcome"/> value for a call that threw anything else.</summary>
    internal const string FaultedOutcome = "faulted";

    /// <summary>The <see cref="LatticeMetrics.TagMethod"/> value for a method name not declared on the interface.</summary>
    internal const string UnknownMethod = "other";

    private static readonly KeyValuePair<string, object?> UnknownMethodTag = new(LatticeMetrics.TagMethod, UnknownMethod);

    /// <summary>
    /// One frozen <see cref="LatticeMetrics.TagMethod"/> pair per member of
    /// <see cref="ILatticeRegistry"/>, so the tag domain is exactly the interface's
    /// member set and the hot path never builds a string.
    /// </summary>
    private static readonly FrozenDictionary<string, KeyValuePair<string, object?>> MethodTags =
        MethodNames().ToFrozenDictionary(
            static name => name,
            static name => new KeyValuePair<string, object?>(LatticeMetrics.TagMethod, name),
            StringComparer.Ordinal);

    /// <summary>
    /// The distinct member names declared on <see cref="ILatticeRegistry"/> and
    /// the interfaces it extends: the complete <see cref="LatticeMetrics.TagMethod"/>
    /// domain of <see cref="LatticeMetrics.RegistryCallerDuration"/>.
    /// </summary>
    /// <returns>The distinct method names, in ordinal order.</returns>
    internal static IReadOnlyList<string> MethodNames() =>
        typeof(ILatticeRegistry).GetMethods()
            .Concat(typeof(ILatticeRegistry).GetInterfaces().SelectMany(static i => i.GetMethods()))
            .Select(static m => m.Name)
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToArray();

    /// <inheritdoc />
    public Task Invoke(IOutgoingGrainCallContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        // Every other outgoing call on the silo leaves through this branch, so it
        // must stay a single comparison with no await and no allocation. The
        // generated invokable returns a statically cached MethodInfo, so reading
        // its declaring type allocates nothing.
        return context.InterfaceMethod?.DeclaringType == typeof(ILatticeRegistry)
            ? ObserveAsync(context)
            : context.Invoke();
    }

    /// <summary>Awaits one registry call and records its caller-observed duration and outcome.</summary>
    /// <param name="context">The outgoing registry call.</param>
    private static async Task ObserveAsync(IOutgoingGrainCallContext context)
    {
        var method = MethodTag(context.MethodName);
        Exception? fault = null;
        var start = Stopwatch.GetTimestamp();
        try
        {
            await context.Invoke().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            fault = ex;
            throw;
        }
        finally
        {
            LatticeMetrics.RegistryCallerDuration.Record(
                Stopwatch.GetElapsedTime(start).TotalMilliseconds,
                method,
                RegistryCallerOutcomeTag(fault),
                LatticeTenantLabel.Platform);
        }
    }

    /// <summary>Resolves the frozen method tag for <paramref name="methodName"/>.</summary>
    /// <param name="methodName">The invoked member name, as reported by Orleans.</param>
    /// <returns>The member's tag, or the <see cref="UnknownMethod"/> tag for a name not on the interface.</returns>
    internal static KeyValuePair<string, object?> MethodTag(string? methodName) =>
        methodName is not null && MethodTags.TryGetValue(methodName, out var tag) ? tag : UnknownMethodTag;

    /// <summary>Maps how a registry call ended to its frozen <see cref="LatticeMetrics.TagOutcome"/> pair.</summary>
    /// <param name="fault">The exception the call ended with, or <see langword="null"/> when it returned.</param>
    /// <returns>The <c>completed</c>, <c>timeout</c>, or <c>faulted</c> tag.</returns>
    /// <remarks>
    /// The arms are referenced as <c>OutcomeTags.X</c> so the dashboard tag-domain
    /// guard can read the emittable <c>outcome</c> set straight from source.
    /// </remarks>
    internal static KeyValuePair<string, object?> RegistryCallerOutcomeTag(Exception? fault) => fault switch
    {
        null => OutcomeTags.Completed,
        TimeoutException => OutcomeTags.Timeout,
        _ => OutcomeTags.Faulted,
    };

    /// <summary>The precomputed <see cref="LatticeMetrics.TagOutcome"/> pairs, one per arm.</summary>
    private static class OutcomeTags
    {
        /// <summary>The pair for <see cref="CompletedOutcome"/>.</summary>
        public static readonly KeyValuePair<string, object?> Completed = new(LatticeMetrics.TagOutcome, CompletedOutcome);

        /// <summary>The pair for <see cref="TimeoutOutcome"/>.</summary>
        public static readonly KeyValuePair<string, object?> Timeout = new(LatticeMetrics.TagOutcome, TimeoutOutcome);

        /// <summary>The pair for <see cref="FaultedOutcome"/>.</summary>
        public static readonly KeyValuePair<string, object?> Faulted = new(LatticeMetrics.TagOutcome, FaultedOutcome);
    }
}
