using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Measures the tree registry's own service time and concurrency, from inside
/// the grain body.
/// <para>
/// <b>Why this exists.</b> The registry is a cluster singleton addressed by
/// every per-tree background service on every silo, so a cold start fans a whole
/// estate onto one activation. When that saturates, callers see nothing but a
/// response-deadline <see cref="TimeoutException"/>, and a timeout is the same
/// observation whether the call was <em>served slowly</em> or <em>never served
/// at all</em>. Those two have opposite remedies, and no caller-side signal
/// separates them.
/// </para>
/// <para>
/// <b>What it discriminates.</b> These instruments are recorded from inside the
/// grain body, so a call only appears here once it has been admitted. Read them
/// against the caller-side timeout population:
/// </para>
/// <list type="bullet">
/// <item><description>
/// Callers time out while this histogram stays short and its call count stays
/// far below the offered load - the calls were <b>never admitted</b>. The
/// registry body is not where the time went, so the block is upstream of it: the
/// activation had not completed, or the calls were queued behind a turn that
/// never yielded. Interleaving attributes cannot fix that, because
/// <c>[AlwaysInterleave]</c> admits a call past a running turn and does not admit
/// one during activation, which is not interleavable.
/// </description></item>
/// <item><description>
/// Callers time out and this histogram's tail approaches the caller's deadline
/// with a matching call count - the calls <b>were admitted and served slowly</b>.
/// The time went inside the body, on the awaited hop to the backing system tree,
/// and the remedy is a scheduling or caching one at that hop.
/// </description></item>
/// </list>
/// <para>
/// <b>Why not a storage instrument.</b> Orleans tags
/// <c>orleans-storage-read-latency</c> with <c>state_name</c> taken from a
/// grain's <c>[PersistentState]</c> declaration. This grain declares none - it is
/// a POCO grain with no persistent state and no activation-time state load at
/// all, reading everything through the backing <c>_lattice_trees</c> Lattice
/// tree instead. So no <c>state_name</c> series for the registry can exist, and
/// manufacturing one would mean giving the grain state it does not have in order
/// to measure a load that does not happen. The service-time pair above measures
/// the delay that is actually there.
/// </para>
/// <para>
/// The in-flight counter is static because the registry is a single activation
/// per cluster, so per-process is per-activation. A host that activates more than
/// one registry - only an in-process multi-silo test cluster does - aggregates
/// them, which inflates the reading rather than losing it.
/// </para>
/// </summary>
internal static class RegistryCallCensus
{
    /// <summary><see cref="ILatticeRegistry.ExistsAsync"/>.</summary>
    internal const string Exists = "exists";

    /// <summary><see cref="ILatticeRegistry.GetEntryAsync"/>.</summary>
    internal const string GetEntry = "get_entry";

    /// <summary><see cref="ILatticeRegistry.ResolveAsync"/>.</summary>
    internal const string Resolve = "resolve";

    /// <summary><see cref="ILatticeRegistry.GetShardMapAsync"/>.</summary>
    internal const string GetShardMap = "get_shard_map";

    /// <summary><see cref="ILatticeRegistry.GetAllTreeIdsAsync(string?)"/>.</summary>
    internal const string GetAllTreeIds = "get_all_tree_ids";

    /// <summary><see cref="ILatticeRegistry.RegisterAsync"/>.</summary>
    internal const string Register = "register";

    /// <summary><see cref="ILatticeRegistry.UnregisterAsync"/>.</summary>
    internal const string Unregister = "unregister";

    /// <summary>
    /// The arms that are <b>not</b> <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/>,
    /// and on which the admitted-call reading therefore means something different.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This distinction is load-bearing for any attempt to read these instruments,
    /// so it is recorded in source rather than left to the reader to rederive from
    /// <see cref="ILatticeRegistry"/>.
    /// </para>
    /// <para>
    /// On an interleaved point read, a short service time beside an admitted count
    /// far below the offered load does mean calls were never admitted. On a
    /// <b>non-interleaved</b> member it means no such thing: callers serialise
    /// behind the singleton's turn token, so a slow downstream hop produces a
    /// short <i>per-call</i> service time and a low admitted count as well. A
    /// mechanism that is really downstream cost therefore wears the signature of
    /// one that is really admission failure, and the two are not separable from
    /// the duration arm alone.
    /// </para>
    /// <para>
    /// <see cref="LatticeMetrics.RegistryCallInFlight"/> is what separates them,
    /// but read it for what it actually counts: the in-flight total is GLOBAL
    /// across every arm, so it answers "how many registry calls of any kind were
    /// in the grain body when this one was admitted" - the fan-in width - and
    /// <b>not</b> "was this member interleaving".
    /// </para>
    /// <para>
    /// Those are easy to conflate, and the conflation fails in the permissive
    /// direction. A non-interleaved member can report a width well above zero:
    /// an <c>[AlwaysInterleave]</c> read admitted earlier and still awaiting its
    /// downstream hop counts as in flight, and Orleans may start a new turn once
    /// the running turn yields at an await. Measured directly on this surface,
    /// <see cref="Register"/> reported a mean width of 1.44 while being
    /// non-interleaved throughout. Treating that as evidence of interleaving
    /// would license exactly the attribution this list exists to forbid.
    /// </para>
    /// <para>
    /// Which members interleave is therefore taken from the attribute, which is
    /// known statically and recorded in this list, rather than inferred from a
    /// reading that cannot carry it. No (a)/(b) attribution may be made from the
    /// duration arm of a member named here, whatever its observed width.
    /// </para>
    /// </remarks>
    internal static readonly IReadOnlyList<string> NonInterleavedOperations =
        [GetAllTreeIds, Register, Unregister];

    /// <summary>Every operation arm this census can record.</summary>
    /// <remarks>
    /// Exposed so the arms are enumerable from one place in source rather than
    /// restated at each emission site, which is what lets every arm share a
    /// single attribution rule.
    /// </remarks>
    internal static readonly IReadOnlyList<string> Operations =
        [Exists, GetEntry, Resolve, GetShardMap, GetAllTreeIds, Register, Unregister];

    private static int _inFlight;

    /// <summary>
    /// Runs <paramref name="body"/>, recording the in-flight registry-call count
    /// at the moment it was admitted and how long it then took.
    /// </summary>
    /// <typeparam name="T">The member's return type.</typeparam>
    /// <param name="operation">The <see cref="ILatticeRegistry"/> member being served.</param>
    /// <param name="body">The member's work.</param>
    /// <returns>The member's result.</returns>
    internal static async Task<T> MeasureAsync<T>(string operation, Func<Task<T>> body)
    {
        var inFlight = Interlocked.Increment(ref _inFlight) - 1;
        LatticeMetrics.RegistryCallInFlight.Record(
            inFlight,
            new KeyValuePair<string, object?>(LatticeMetrics.TagOperation, operation),
            LatticeTenantLabel.Platform);

        var from = Stopwatch.GetTimestamp();
        try
        {
            return await body().ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Decrement(ref _inFlight);
            LatticeMetrics.RegistryCallDuration.Record(
                Stopwatch.GetElapsedTime(from).TotalMilliseconds,
                new KeyValuePair<string, object?>(LatticeMetrics.TagOperation, operation),
                LatticeTenantLabel.Platform);
        }
    }

    /// <summary>
    /// Runs <paramref name="body"/>, recording the same two readings as
    /// <see cref="MeasureAsync{T}"/> for a member that returns no value.
    /// </summary>
    /// <param name="operation">The <see cref="ILatticeRegistry"/> member being served.</param>
    /// <param name="body">The member's work.</param>
    /// <returns>A task that completes when the member's work completes.</returns>
    /// <remarks>
    /// The mutators need this overload, and they are the members for which the
    /// readings matter most: each is a read-then-write that holds the singleton's
    /// turn token for its whole duration, so a cold start registering K trees
    /// costs K serialised turns whose individual cost grows with the estate.
    /// </remarks>
    internal static async Task MeasureAsync(string operation, Func<Task> body)
    {
        // Deliberately not expressed as MeasureAsync(operation, async () => { await body(); return true; }).
        // That shape compiles, but the lambda's inferred type sends overload
        // resolution back to this same non-generic overload rather than to the
        // generic one, so the method recurses into itself - and it does so
        // silently, as unbounded recursion rather than a compile error.
        var inFlight = Interlocked.Increment(ref _inFlight) - 1;
        LatticeMetrics.RegistryCallInFlight.Record(
            inFlight,
            new KeyValuePair<string, object?>(LatticeMetrics.TagOperation, operation),
            LatticeTenantLabel.Platform);

        var from = Stopwatch.GetTimestamp();
        try
        {
            await body().ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Decrement(ref _inFlight);
            LatticeMetrics.RegistryCallDuration.Record(
                Stopwatch.GetElapsedTime(from).TotalMilliseconds,
                new KeyValuePair<string, object?>(LatticeMetrics.TagOperation, operation),
                LatticeTenantLabel.Platform);
        }
    }
}
