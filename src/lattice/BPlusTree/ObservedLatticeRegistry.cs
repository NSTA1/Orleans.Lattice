using System.Collections.Frozen;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Caller-side timing for <see cref="ILatticeRegistry"/> calls. <see cref="Wrap"/>
/// returns a decorator over the registry grain reference that records, for every
/// registry call made through it, how long the caller waited and how the call
/// ended, tagged by interface method, on
/// <see cref="LatticeMetrics.RegistryCallerDuration"/>. Every production caller
/// obtains the registry through
/// <see cref="LatticeRegistryGrainFactoryExtensions.GetLatticeRegistry"/>, which
/// wraps it, so the recording is not opt-in.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists (issue #3088).</b> Registry contention was diagnosed from
/// the <c>Diagnostics: [... CurrentlyExecuting=...]</c> block Orleans appends to a
/// <c>Response did not arrive on time</c> timeout. Orleans stops emitting that
/// block silo-wide once the silo saturates, and its absence is indistinguishable
/// from "the registry was idle", so the one diagnostic that named the registry's
/// occupant produced a confident all-clear exactly when it had stopped measuring.
/// This decorator is the first-party replacement: a sample for every call, with
/// no dependency on any log field.
/// </para>
/// <para>
/// <b>Why the caller.</b> The grain-body census
/// (<see cref="LatticeMetrics.RegistryCallDuration"/>) can only see calls the
/// registry admitted. A registry nobody can reach produces no body samples at
/// all, which is the same silence the vanished log field produced. Only the
/// caller observes a call that is never served.
/// </para>
/// <para>
/// <b>Why a decorator and not an outgoing grain call filter.</b> A silo-wide
/// <see cref="IOutgoingGrainCallFilter"/> was measured and rejected: registering
/// any outgoing filter makes Orleans run the filter chain on every outgoing call
/// on the silo, which cost roughly 520 bytes per call on WAL, leaf, shard and
/// replication traffic that has nothing to do with the registry, even with a
/// filter body that only compared the interface type. The decorator is paid for
/// only on the registry path: one small object per acquisition and one async
/// state machine per call, with every tag pair precomputed.
/// </para>
/// <para>
/// <b>Why the decorator is a private nested class.</b> The Orleans code generator
/// registers every public or internal concrete class that implements a grain
/// interface as a grain class. An internal decorator implementing
/// <see cref="ILatticeRegistry"/> therefore became a second candidate grain type,
/// and every <c>GetGrain&lt;ILatticeRegistry&gt;</c> failed with "Unable to
/// identify a single appropriate grain type". A private nested type is invisible
/// to that scan, so it must stay private.
/// </para>
/// </remarks>
internal static class ObservedLatticeRegistry
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

    /// <summary>Resolves the frozen method tag for <paramref name="methodName"/>.</summary>
    /// <param name="methodName">The invoked member name.</param>
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

    /// <summary>
    /// Wraps a registry grain reference in the caller-side timing decorator.
    /// </summary>
    /// <param name="inner">The registry grain reference every call is forwarded to.</param>
    /// <returns>A registry whose every call records one <see cref="LatticeMetrics.RegistryCallerDuration"/> sample.</returns>
    internal static ILatticeRegistry Wrap(ILatticeRegistry inner)
    {
        ArgumentNullException.ThrowIfNull(inner);
        return new Decorator(inner);
    }

    /// <summary>Unwraps a registry produced by <see cref="Wrap"/>.</summary>
    /// <param name="registry">The registry to inspect.</param>
    /// <param name="inner">The wrapped reference, when <paramref name="registry"/> is a decorator.</param>
    /// <returns><see langword="true"/> when <paramref name="registry"/> came from <see cref="Wrap"/>.</returns>
    internal static bool TryGetInner(ILatticeRegistry registry, [NotNullWhen(true)] out ILatticeRegistry? inner)
    {
        inner = (registry as Decorator)?.Inner;
        return inner is not null;
    }

    /// <summary>Awaits one registry call that returns no value and records it.</summary>
    /// <param name="start">The <see cref="Stopwatch"/> timestamp taken before the call was dispatched.</param>
    /// <param name="call">The dispatched call.</param>
    /// <param name="method">The call's frozen method tag.</param>
    private static async Task Observe(long start, Task call, KeyValuePair<string, object?> method)
    {
        Exception? fault = null;
        try
        {
            await call;
        }
        catch (Exception ex)
        {
            fault = ex;
            throw;
        }
        finally
        {
            RecordRegistryCallerSample(start, method, fault);
        }
    }

    /// <summary>Awaits one registry call that returns a value and records it.</summary>
    /// <typeparam name="T">The call's result type.</typeparam>
    /// <param name="start">The <see cref="Stopwatch"/> timestamp taken before the call was dispatched.</param>
    /// <param name="call">The dispatched call.</param>
    /// <param name="method">The call's frozen method tag.</param>
    /// <returns>The call's result.</returns>
    private static async Task<T> Observe<T>(long start, Task<T> call, KeyValuePair<string, object?> method)
    {
        Exception? fault = null;
        try
        {
            return await call;
        }
        catch (Exception ex)
        {
            fault = ex;
            throw;
        }
        finally
        {
            RecordRegistryCallerSample(start, method, fault);
        }
    }

    /// <summary>Records one caller-observed registry sample.</summary>
    /// <param name="start">The <see cref="Stopwatch"/> timestamp taken before the call was dispatched.</param>
    /// <param name="method">The call's frozen method tag.</param>
    /// <param name="fault">The exception the call ended with, or <see langword="null"/> when it returned.</param>
    private static void RecordRegistryCallerSample(long start, KeyValuePair<string, object?> method, Exception? fault) =>
        LatticeMetrics.RegistryCallerDuration.Record(
            Stopwatch.GetElapsedTime(start).TotalMilliseconds,
            method,
            RegistryCallerOutcomeTag(fault),
            LatticeTenantLabel.Platform);

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

    /// <summary>
    /// The forwarding decorator. Private so the Orleans code generator does not
    /// register it as a grain class; see the type remarks.
    /// </summary>
    /// <param name="inner">The registry grain reference every call is forwarded to.</param>
    private sealed class Decorator(ILatticeRegistry inner) : ILatticeRegistry
    {
        /// <summary>The registry grain reference this decorator forwards to.</summary>
        internal ILatticeRegistry Inner { get; } = inner;

        /// <inheritdoc />
        public Task RegisterAsync(string treeId, TreeRegistryEntry? entry = null) =>
            Observe(Stopwatch.GetTimestamp(), Inner.RegisterAsync(treeId, entry), MethodTag(nameof(RegisterAsync)));

        /// <inheritdoc />
        public Task UpdateAsync(string treeId, TreeRegistryEntry entry) =>
            Observe(Stopwatch.GetTimestamp(), Inner.UpdateAsync(treeId, entry), MethodTag(nameof(UpdateAsync)));

        /// <inheritdoc />
        public Task UnregisterAsync(string treeId) =>
            Observe(Stopwatch.GetTimestamp(), Inner.UnregisterAsync(treeId), MethodTag(nameof(UnregisterAsync)));

        /// <inheritdoc />
        public Task<bool> ExistsAsync(string treeId) =>
            Observe(Stopwatch.GetTimestamp(), Inner.ExistsAsync(treeId), MethodTag(nameof(ExistsAsync)));

        /// <inheritdoc />
        public Task<TreeRegistryEntry?> GetEntryAsync(string treeId) =>
            Observe(Stopwatch.GetTimestamp(), Inner.GetEntryAsync(treeId), MethodTag(nameof(GetEntryAsync)));

        /// <inheritdoc />
        public Task<Dictionary<string, TreeRegistryEntry>> GetEntriesAsync(IReadOnlyList<string> treeIds) =>
            Observe(Stopwatch.GetTimestamp(), Inner.GetEntriesAsync(treeIds), MethodTag(nameof(GetEntriesAsync)));

        /// <inheritdoc />
        public Task<IReadOnlyList<string>> GetAllTreeIdsAsync() =>
            Observe(Stopwatch.GetTimestamp(), Inner.GetAllTreeIdsAsync(), MethodTag(nameof(GetAllTreeIdsAsync)));

        /// <inheritdoc />
        public Task<IReadOnlyList<string>> GetAllTreeIdsAsync(string? prefix) =>
            Observe(Stopwatch.GetTimestamp(), Inner.GetAllTreeIdsAsync(prefix), MethodTag(nameof(GetAllTreeIdsAsync)));

        /// <inheritdoc />
        public Task SetAliasAsync(string treeId, string physicalTreeId) =>
            Observe(Stopwatch.GetTimestamp(), Inner.SetAliasAsync(treeId, physicalTreeId), MethodTag(nameof(SetAliasAsync)));

        /// <inheritdoc />
        public Task RemoveAliasAsync(string treeId) =>
            Observe(Stopwatch.GetTimestamp(), Inner.RemoveAliasAsync(treeId), MethodTag(nameof(RemoveAliasAsync)));

        /// <inheritdoc />
        public Task<string> ResolveAsync(string treeId) =>
            Observe(Stopwatch.GetTimestamp(), Inner.ResolveAsync(treeId), MethodTag(nameof(ResolveAsync)));

        /// <inheritdoc />
        public Task<ShardMap?> GetShardMapAsync(string treeId) =>
            Observe(Stopwatch.GetTimestamp(), Inner.GetShardMapAsync(treeId), MethodTag(nameof(GetShardMapAsync)));

        /// <inheritdoc />
        public Task SetShardMapAsync(string treeId, ShardMap map) =>
            Observe(Stopwatch.GetTimestamp(), Inner.SetShardMapAsync(treeId, map), MethodTag(nameof(SetShardMapAsync)));

        /// <inheritdoc />
        public Task<ShardMap> ReassignSlotsAsync(string treeId, int[] slots, int targetShardIndex, ShardMap fallbackMap) =>
            Observe(
                Stopwatch.GetTimestamp(),
                Inner.ReassignSlotsAsync(treeId, slots, targetShardIndex, fallbackMap),
                MethodTag(nameof(ReassignSlotsAsync)));

        /// <inheritdoc />
        public Task<int> AllocateNextShardIndexAsync(string treeId, int currentMaxFromMap) =>
            Observe(
                Stopwatch.GetTimestamp(),
                Inner.AllocateNextShardIndexAsync(treeId, currentMaxFromMap),
                MethodTag(nameof(AllocateNextShardIndexAsync)));

        /// <inheritdoc />
        public Task SetPublishEventsAsync(string treeId, bool? enabled) =>
            Observe(Stopwatch.GetTimestamp(), Inner.SetPublishEventsAsync(treeId, enabled), MethodTag(nameof(SetPublishEventsAsync)));

        /// <inheritdoc />
        public Task SetHistoryRetentionAsync(string treeId, HistoryRetentionMode? mode, TimeSpan? window) =>
            Observe(
                Stopwatch.GetTimestamp(),
                Inner.SetHistoryRetentionAsync(treeId, mode, window),
                MethodTag(nameof(SetHistoryRetentionAsync)));

        /// <inheritdoc />
        public Task SetMaintainProjectionDigestAsync(string treeId, bool? enabled) =>
            Observe(
                Stopwatch.GetTimestamp(),
                Inner.SetMaintainProjectionDigestAsync(treeId, enabled),
                MethodTag(nameof(SetMaintainProjectionDigestAsync)));

        /// <inheritdoc />
        public Task SetMaxCacheValueBytesAsync(string treeId, long? maxCacheValueBytes) =>
            Observe(
                Stopwatch.GetTimestamp(),
                Inner.SetMaxCacheValueBytesAsync(treeId, maxCacheValueBytes),
                MethodTag(nameof(SetMaxCacheValueBytesAsync)));

        /// <inheritdoc />
        public Task SetWalMaxRetainedBytesAsync(string treeId, long? walMaxRetainedBytes) =>
            Observe(
                Stopwatch.GetTimestamp(),
                Inner.SetWalMaxRetainedBytesAsync(treeId, walMaxRetainedBytes),
                MethodTag(nameof(SetWalMaxRetainedBytesAsync)));

        /// <inheritdoc />
        public Task LatchProjectionDigestPermanentlyDisabledAsync(string treeId) =>
            Observe(
                Stopwatch.GetTimestamp(),
                Inner.LatchProjectionDigestPermanentlyDisabledAsync(treeId),
                MethodTag(nameof(LatchProjectionDigestPermanentlyDisabledAsync)));

        /// <inheritdoc />
        public Task<WalPlacementPin> GetWalPlacementAsync(string treeId) =>
            Observe(Stopwatch.GetTimestamp(), Inner.GetWalPlacementAsync(treeId), MethodTag(nameof(GetWalPlacementAsync)));

        /// <inheritdoc />
        public Task<WalPlacementPin> UpdateWalPlacementAsync(string treeId, long expectedVersion, int partition, string providerKey) =>
            Observe(
                Stopwatch.GetTimestamp(),
                Inner.UpdateWalPlacementAsync(treeId, expectedVersion, partition, providerKey),
                MethodTag(nameof(UpdateWalPlacementAsync)));

        /// <inheritdoc />
        public Task<WalPlacementPin> UpdateWalPlacementAsync(
            string treeId,
            long expectedVersion,
            IReadOnlyCollection<(int Partition, string ProviderKey)> moves) =>
            Observe(
                Stopwatch.GetTimestamp(),
                Inner.UpdateWalPlacementAsync(treeId, expectedVersion, moves),
                MethodTag(nameof(UpdateWalPlacementAsync)));
    }
}
