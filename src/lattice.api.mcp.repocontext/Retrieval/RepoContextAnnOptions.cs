using Orleans.Lattice.Vector;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// How the approximate retrieval plane shapes, builds, and maintains one
/// persisted index. Every default is chosen so an existing deployment picks the
/// plane up with no configuration at all: the index shapes itself from the corpus
/// (an automatic partition count and probe budget), builds itself in bounded
/// slices behind live traffic, and persists itself in bounded records.
/// </summary>
internal sealed class RepoContextAnnOptions
{
    /// <summary>
    /// The Lattice key prefix, inside the dedicated index tree, that every
    /// repository's index sits under. Each <c>(repository, embedding space)</c>
    /// gets its own sub-prefix beneath it, because the index's recovery path
    /// deletes whole key ranges under its own prefix and must never be able to
    /// reach another index - or any store of record.
    /// </summary>
    internal const string KeyPrefixRoot = "vidx/";

    /// <summary>
    /// How many source vectors one background build slice consumes before it
    /// checkpoints and yields.
    /// <para>
    /// This is the work half of the slice bound only. It does NOT on its own
    /// bound how long a slice runs, and reading it as though it did is what
    /// issue #2483 measured: this repository's source streams over grain calls,
    /// so on an 8,158-file corpus a 4,096-vector slice held the coordinator's
    /// turn for over twenty minutes and every caller queued behind it.
    /// <see cref="IngestSliceBudget"/> is the bound that actually keeps a slice
    /// short.
    /// </para>
    /// </summary>
    public int IngestBatchSize { get; init; } = 4_096;

    /// <summary>
    /// The wall-clock ceiling on one background build slice, checked after each
    /// vector so a slice always makes progress. This is what keeps the build
    /// coordinator's turn available: a slice ends on whichever of this and
    /// <see cref="IngestBatchSize"/> is reached first, so the keep-alive reminder
    /// is delivered and an arming call is answered while a build is running.
    /// </summary>
    public TimeSpan IngestSliceBudget { get; init; } =
        DurableVectorIndexOptions.DefaultIngestSliceBudget;

    /// <summary>
    /// The wall-clock ceiling on one attempt to OPEN the durable index, which is
    /// the phase before any ingest budget is consulted. A non-positive value
    /// removes the bound.
    /// <para>
    /// <b>This is a different bound from <see cref="IngestSliceBudget"/>, and the
    /// absence of it is what issue #3130 is.</b> That budget governs the ingest,
    /// and the ingest is read only once the index is already open. The open itself
    /// restores the whole durable index - an O(corpus) walk of the identifier key
    /// map followed by a partition-by-partition restore - and carried no bound of
    /// any kind, inside a single non-reentrant coordinator turn. On the acceptance
    /// rig that held the turn for over thirty minutes with the keep-alive reminder
    /// and every arming call queued behind it.
    /// </para>
    /// <para>
    /// <b>The bound is safe only because the load resumes.</b> An attempt stopped
    /// by this budget banks what it walked and the next attempt continues past it,
    /// so the ceiling slices one long open into several short ones rather than
    /// restarting it. Without that property a bound would be strictly worse than
    /// none - it would convert a slow open into one that never completes, which is
    /// the trap issue #2953 names. Do not raise this above the coordinator's tick
    /// interval expecting faster convergence; it is a turn-yield interval, not a
    /// work quota.
    /// </para>
    /// <para>
    /// Defaults to <see cref="DurableVectorIndexOptions.DefaultIngestSliceBudget"/>
    /// so the open yields on the same cadence a slice does. There is no reason for
    /// the two to differ: both exist to return the coordinator's turn, and a caller
    /// blocked behind the handle cannot tell which phase is holding it.
    /// </para>
    /// </summary>
    public TimeSpan OpenSliceBudget { get; init; } =
        DurableVectorIndexOptions.DefaultIngestSliceBudget;

    /// <summary>
    /// The clock the <see cref="OpenSliceBudget"/> is measured against. Present so
    /// a test can expire the budget deterministically rather than by sleeping.
    /// </summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    /// <summary>
    /// How many further <see cref="OpenSliceBudget"/> periods an open slice that
    /// has banked <b>nothing</b> may be granted before the budget fires anyway.
    /// Zero reproduces the historical elapsed-only bound exactly.
    /// <para>
    /// <b>This exists because the budget above measures wall-clock that includes
    /// time in which progress is impossible, and that is issue #3284.</b> The open
    /// walks the identifier key map, which activates cold leaves, which queue for a
    /// per-silo WAL replay permit. The walk banks position per entry, so a slice
    /// that cannot complete one entry banks nothing at all. With a measured mean
    /// permit wait of twelve seconds against a five second budget, every slice was
    /// guaranteed to expire having banked zero - and because each expiry re-enqueued
    /// a waiter, the budget lengthened the very queue that caused it. It was
    /// regenerative, not merely ineffective.
    /// </para>
    /// <para>
    /// <b>The cap is what keeps the loud failure reachable, and removing it would be
    /// the opposite error.</b> An open extended without limit is an unbounded open,
    /// which is the thirty-minute coordinator wedge issue #3130 removed. With the
    /// cap, storage that genuinely answers nothing still exhausts its extensions,
    /// still banks zero, and still trips the empty-deferral escalation. The default
    /// of six bounds one attempt at roughly seven budget periods - about thirty-five
    /// seconds at the default budget - which is two orders of magnitude below the
    /// wedge and comfortably above the worst permit wait measured on the incident.
    /// </para>
    /// </summary>
    public int MaxOpenSliceExtensions { get; init; } = 6;

    /// <summary>
    /// How many <b>consecutive</b> admission refusals an open may take before the
    /// handle declares itself terminally saturated. Zero removes the count bound,
    /// leaving <see cref="OpenRefusalTerminalPeriod"/> as the only one.
    /// <para>
    /// <b>This exists because a refusal loop had no terminal state at all, and that
    /// is issue #3286.</b> Measured live: <c>ann.index.load{outcome="refused"}</c>
    /// climbing at 0.66 per minute while <c>fresh</c> and <c>resumed</c> stayed at
    /// zero indefinitely, <c>ann.build.step.in_flight_seconds{phase="opening"}</c>
    /// pinned at 273 seconds with every later phase at zero, and
    /// <c>/health/ready</c> returning 503 with nothing anywhere distinguishing
    /// "still arming" from "will never arm". Those two readings need different
    /// actions - wait, versus add capacity - and the plane emitted the same thing
    /// for both.
    /// </para>
    /// <para>
    /// <b>The bound is on DECLARING, not on trying.</b> Reaching it makes the state
    /// readable and does not stop the open, so a plane whose saturation clears
    /// still self-heals with no operator action. A terminal state that also stopped
    /// retrying would turn a transient heap excursion into a permanent outage
    /// requiring a restart, which is a worse failure than the silence it replaces.
    /// </para>
    /// <para>
    /// The default of twelve is derived from that measured refusal rate: at roughly
    /// one refusal every ninety seconds the state is reached in about a quarter of
    /// an hour, which is long enough that an ordinary cold open over a large plane
    /// can never reach it (such an open banks progress, which clears the counter)
    /// and short enough that an operator watching a deploy is not left guessing.
    /// </para>
    /// </summary>
    public int MaxConsecutiveOpenRefusals { get; init; } = 12;

    /// <summary>
    /// How long an unbroken run of admission refusals may persist before the handle
    /// declares itself terminally saturated, whichever bound is reached first.
    /// <see cref="TimeSpan.Zero"/> removes the elapsed bound, leaving
    /// <see cref="MaxConsecutiveOpenRefusals"/> as the only one.
    /// <para>
    /// <b>Both bounds exist because either alone is reachable only on some
    /// deployments.</b> A count bound alone is never reached by a host whose
    /// coordinator ticks slowly, which is exactly the host that most needs the
    /// signal; an elapsed bound alone is reached by a host that took two refusals
    /// during a long, slow, healthy startup. The counter is cleared by any open
    /// that banks progress, so neither bound is reachable by a converging walk.
    /// </para>
    /// </summary>
    public TimeSpan OpenRefusalTerminalPeriod { get; init; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Environment variable that overrides <see cref="OpenSliceBudget"/>, in
    /// seconds. Zero removes the bound.
    /// </summary>
    internal const string OpenSliceBudgetSecondsVariable =
        "LATTICE_REPOCONTEXT_ANN_OPEN_SLICE_BUDGET_SECONDS";

    /// <summary>
    /// Environment variable that overrides <see cref="MaxOpenSliceExtensions"/>.
    /// </summary>
    internal const string MaxOpenSliceExtensionsVariable =
        "LATTICE_REPOCONTEXT_ANN_OPEN_SLICE_MAX_EXTENSIONS";

    /// <summary>
    /// Environment variable that overrides <see cref="IngestSliceBudget"/>, in
    /// seconds.
    /// </summary>
    internal const string IngestSliceBudgetSecondsVariable =
        "LATTICE_REPOCONTEXT_ANN_INGEST_SLICE_BUDGET_SECONDS";

    /// <summary>
    /// Environment variable that overrides <see cref="MaxConsecutiveOpenRefusals"/>.
    /// Zero removes the count bound.
    /// </summary>
    internal const string MaxConsecutiveOpenRefusalsVariable =
        "LATTICE_REPOCONTEXT_ANN_OPEN_MAX_CONSECUTIVE_REFUSALS";

    /// <summary>
    /// Environment variable that overrides <see cref="OpenRefusalTerminalPeriod"/>,
    /// in seconds. Zero removes the elapsed bound.
    /// </summary>
    internal const string OpenRefusalTerminalPeriodSecondsVariable =
        "LATTICE_REPOCONTEXT_ANN_OPEN_REFUSAL_TERMINAL_SECONDS";

    /// <summary>
    /// Resolves the open-slice bounds from the environment, falling back to the
    /// defaults for any variable that is absent or malformed.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>This type had no configuration surface at all until issue #3284</b>, and
    /// the absence was not an oversight so much as an unexamined consequence: the
    /// container registration was a bare
    /// <c>TryAddSingleton&lt;RepoContextAnnOptions&gt;()</c>, which binds the
    /// parameterless constructor, so every value was a compile-time constant and no
    /// operator could move any of them. That is tolerable while every default is
    /// right and intolerable the moment one is wrong, because the only remedy left
    /// is a redeploy of the library.
    /// </para>
    /// <para>
    /// Deliberately narrow: only the bounds an operator might have to move during
    /// an incident are exposed. The shape parameters below stay fixed, because a
    /// wrong partition count degrades recall silently rather than wedging a plane,
    /// and an environment surface invites exactly the fixed-partition-count
    /// misconfiguration those defaults exist to prevent.
    /// </para>
    /// </remarks>
    /// <returns>The resolved options.</returns>
    internal static RepoContextAnnOptions FromEnvironment()
    {
        var defaults = new RepoContextAnnOptions();
        return new RepoContextAnnOptions
        {
            OpenSliceBudget = ReadSeconds(OpenSliceBudgetSecondsVariable, defaults.OpenSliceBudget),
            IngestSliceBudget = ReadSeconds(IngestSliceBudgetSecondsVariable, defaults.IngestSliceBudget),
            MaxOpenSliceExtensions = ReadCount(
                MaxOpenSliceExtensionsVariable, defaults.MaxOpenSliceExtensions),
            MaxConsecutiveOpenRefusals = ReadCount(
                MaxConsecutiveOpenRefusalsVariable, defaults.MaxConsecutiveOpenRefusals),
            OpenRefusalTerminalPeriod = ReadSeconds(
                OpenRefusalTerminalPeriodSecondsVariable, defaults.OpenRefusalTerminalPeriod),
        };
    }

    /// <summary>
    /// Reads a non-negative seconds value, falling back for anything absent,
    /// malformed, or negative - so a typo can never remove a bound silently.
    /// </summary>
    private static TimeSpan ReadSeconds(string key, TimeSpan fallback)
    {
        var raw = Environment.GetEnvironmentVariable(key);
        if (!string.IsNullOrWhiteSpace(raw)
            && double.TryParse(
                raw,
                System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture,
                out var seconds)
            && seconds >= 0)
        {
            return TimeSpan.FromSeconds(seconds);
        }

        return fallback;
    }

    /// <summary>
    /// Reads a non-negative count, falling back for anything absent, malformed, or
    /// negative.
    /// </summary>
    private static int ReadCount(string key, int fallback)
    {
        var raw = Environment.GetEnvironmentVariable(key);
        if (!string.IsNullOrWhiteSpace(raw)
            && int.TryParse(
                raw,
                System.Globalization.NumberStyles.Integer,
                System.Globalization.CultureInfo.InvariantCulture,
                out var count)
            && count >= 0)
        {
            return count;
        }

        return fallback;
    }

    /// <summary>
    /// The largest number of centroids or vectors one persisted record carries,
    /// so no record grows with the corpus.
    /// </summary>
    public int MaxItemsPerChunk { get; init; } = 1_024;

    /// <summary>
    /// How many applied maintenance updates accumulate before the plane flushes
    /// the dirty cells to durable storage. A flush costs one write per dirty cell
    /// plus the manifest, so batching keeps a bulk re-embed from rewriting the
    /// same cell once per vector.
    /// </summary>
    public int FlushAfterUpdates { get; init; } = 256;

    /// <summary>
    /// How many updates may accumulate since the partitioning was last computed,
    /// as a fraction of the corpus, before the plane retrains off the request
    /// path. Incremental maintenance keeps the index correct forever but cannot
    /// keep the cells descriptive once the corpus drifts away from the
    /// distribution they were trained on, and that loss is quiet - every record
    /// stays valid. This is the repair trigger for it.
    /// </summary>
    public double RetrainAfterUpdateFraction { get; init; } = 0.25;

    /// <summary>
    /// The distance metric the index ranks with. Cosine reproduces the exact
    /// path's ordering under both normalization conventions: for a unit-L2 space
    /// the cosine similarity and the dot product the exact ranker uses are the
    /// same quantity, and for an unnormalized space the exact ranker computes the
    /// cosine similarity too.
    /// </summary>
    public VectorDistanceMetric Metric { get; init; } = VectorDistanceMetric.Cosine;

    /// <summary>
    /// The number of partitions the index trains, or <c>0</c> to let it choose
    /// from the corpus size. Leave it at the default: a fixed partition count
    /// makes query cost linear in the corpus again for a corpus large enough,
    /// which is the whole property this plane exists to break.
    /// </summary>
    public int PartitionCount { get; init; }

    /// <summary>
    /// The number of partitions a query probes, or <c>0</c> to let the index
    /// choose. Leave it at the default: the automatic budget deliberately scans a
    /// <i>shrinking</i> fraction of the corpus as the corpus grows, which a fixed
    /// fraction of partitions would not.
    /// </summary>
    public int Probes { get; init; }

    /// <summary>
    /// The seed for the training pass, so a rebuild of the same corpus produces
    /// the same partitioning and a recall measurement is reproducible.
    /// </summary>
    public ulong Seed { get; init; } = 20_260_101;

    /// <summary>
    /// The smallest corpus the index will train a partitioning for. Below it the
    /// index legitimately finishes its build with no partitioning and answers
    /// exactly by exhaustive scan, which is correct and is reported as
    /// <see cref="RepoContextAnnServingState.Exhaustive"/> rather than as
    /// approximate.
    /// </summary>
    public int MinimumTrainingCount { get; init; } = 1_024;

    /// <summary>
    /// Projects these options onto the durable index configuration for one
    /// embedding space, whose dimensionality fixes the index's own.
    /// </summary>
    /// <param name="space">The embedding space the index covers.</param>
    /// <param name="keyPrefix">The key prefix this index owns exclusively. Must not be <see langword="null"/>.</param>
    /// <returns>The durable index configuration.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="keyPrefix"/> is null.</exception>
    internal DurableVectorIndexOptions ToDurableOptions(EmbeddingSpaceTag space, string keyPrefix)
    {
        ArgumentNullException.ThrowIfNull(keyPrefix);

        return new DurableVectorIndexOptions
        {
            KeyPrefix = keyPrefix,
            IngestBatchSize = IngestBatchSize,
            IngestSliceBudget = IngestSliceBudget,
            MaxItemsPerChunk = MaxItemsPerChunk,
            Index = new VectorIndexOptions
            {
                Dimensions = space.Dimension,
                Metric = Metric,
                PartitionCount = PartitionCount,
                Probes = Probes,
                Seed = Seed,
                MinimumTrainingCount = MinimumTrainingCount,
            },
        };
    }
}
