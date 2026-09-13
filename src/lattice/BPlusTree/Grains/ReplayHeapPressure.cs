namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The <b>proactive</b> heap dimension of the per-silo WAL replay concurrency
/// gate (issue #2862). Answers one question at one seam: given how much managed
/// memory this process is currently holding, and the ceiling above which the
/// runtime will throw rather than grow, should the replay gate be putting fewer
/// permits into circulation?
/// <para>
/// <b>Why this exists at all, when issue #2781 already gave the gate a memory
/// dimension.</b> #2781's trigger is a fault: a permit is withheld when a replay
/// escapes its guarded region with an exception that
/// <c>BPlusLeafGrain.IsReadMemoryPressure</c> recognises. Acceptance run 10
/// measured that trigger firing <b>zero</b> times across 625
/// <see cref="OutOfMemoryException"/>s, 129 fatal escalations and two process
/// restarts, with both arms of
/// <c>orleans.lattice.wal.replay.permit_adaptations</c> zero-primed - so that was
/// a measured zero, not an absent series. Three structural reasons, none of which
/// a different threshold on the same trigger would have fixed:
/// </para>
/// <para>
/// <b>(1) The replay absorbs the very fault the gate watches for.</b> The
/// slice-budget narrowing introduced by issue #2742 catches
/// <c>IsReadMemoryPressure</c> inside the partition replay loop and retries the
/// same range at a quarter of the width. When the narrowed read succeeds - which
/// is the case it was built for - the fault never leaves the loop, the guarded
/// region completes, and the activation reports a <i>clean</i> replay. A process
/// can therefore throw hundreds of <see cref="OutOfMemoryException"/>s while every
/// replay the gate observes looks healthy, and the recovery arm then <i>restores</i>
/// permits into a heap that is already at its ceiling.
/// </para>
/// <para>
/// <b>(2) A large share of the faults happen before a permit exists.</b> The
/// snapshot rehydrate runs in <c>ReplayAdmissionPhase.RehydratingSnapshot</c>,
/// ahead of the permit acquisition, so a fault there is guarded out of the
/// withholding path by the <c>replayPermit is not null</c> test - correctly, since
/// a permit that was never taken cannot be withheld. Run 10 attributed 63 of its
/// OOMs to <c>leafsnapshotstorage</c>, which is exactly that phase.
/// </para>
/// <para>
/// <b>(3) A fault-driven trigger is too late by construction.</b> Its precondition
/// is that the heap has <i>already</i> been exhausted, so it can never satisfy
/// "withhold before the process reaches its GC hard limit" at any threshold. That
/// is a property of the trigger's shape, not of its tuning.
/// </para>
/// <para>
/// This type supplies the missing dimension: an <i>occupancy</i> reading taken on
/// a path the replays always reach whether they fault, recover, or succeed. The
/// reactive arm is kept as a backstop rather than replaced - a fault that does
/// escape is still evidence, and the two triggers fail independently.
/// </para>
/// <para>
/// <b>The ceiling is denominated in
/// <see cref="GCMemoryInfo.TotalAvailableMemoryBytes"/>, deliberately.</b> That is
/// the figure the runtime throws against, and on the deployment this issue was
/// raised from it is 75% of the container's 12 GiB grant (.NET's default
/// <c>GCHeapHardLimitPercent</c>), i.e. 9 GiB. A threshold expressed against the
/// <i>grant</i> would therefore sit 33% above the limit that actually bites and
/// could never be crossed - the gate would look correct, be measurable, and be
/// unreachable. Observed RSS on that run peaked at 9.293 GiB: past the 9 GiB
/// ceiling, nowhere near the 12 GiB grant.
/// </para>
/// <para>
/// The grant is still consulted, but only to <b>lower</b> the ceiling, never to
/// raise it. <see cref="GCMemoryInfo.TotalAvailableMemoryBytes"/> reports host
/// physical memory - not zero - when no heap hard limit is configured, so taken
/// alone it would make this mechanism silently inert on exactly the hosts that
/// are memory-constrained without a configured hard limit. That is the same trap
/// issue #2788 corrected for <see cref="LeafResidentWorkingSet"/>, and the
/// resolution is the same: take the smaller of the two <i>known</i> figures, and
/// treat a non-positive or sentinel value as unknown rather than as a ceiling of
/// zero.
/// </para>
/// </summary>
internal static class ReplayHeapPressure
{
    /// <summary>
    /// Occupancy, as a percentage of the resolved heap ceiling, at or above which
    /// a returning replay permit is withheld instead.
    /// <para>
    /// Strictly below 100 is the load-bearing property, not the particular value:
    /// a threshold at or above the ceiling could only ever be crossed by a process
    /// that had already reached the limit it is meant to stay under, which is the
    /// #2781 trigger's defect restated as a constant. 75% leaves a quarter of the
    /// heap as headroom for the replays already admitted - each of which holds
    /// multi-MiB deserialisation buffers it has not finished with - so the
    /// reduction takes effect while there is still room for the in-flight set to
    /// complete.
    /// </para>
    /// <para>
    /// Erring low costs replay concurrency, which is bounded, recoverable work on
    /// an already-slow path. Erring high re-admits the exhaustion this exists to
    /// stop, which is unbounded and kills the process. The asymmetry is why this
    /// is three quarters rather than, say, nine tenths.
    /// </para>
    /// </summary>
    internal const int WithholdOccupancyPercent = 75;

    /// <summary>
    /// Occupancy, as a percentage of the resolved heap ceiling, strictly below
    /// which a cleanly-completed replay may return a previously withheld permit.
    /// <para>
    /// This is <b>not</b> the same number as
    /// <see cref="WithholdOccupancyPercent"/>, and the gap is the point. With a
    /// single threshold, a process sitting on the boundary would withhold on one
    /// replay and restore on the next for as long as the condition lasted,
    /// converting a sustained reduction into a permit that flaps and a counter
    /// whose two arms rise together while the effective ceiling never moves. The
    /// hysteresis band means concurrency is only handed back once occupancy has
    /// actually receded, not merely stopped rising.
    /// </para>
    /// </summary>
    internal const int RestoreOccupancyPercent = 60;

    /// <summary>
    /// At or above this, a reported ceiling is read as "unlimited" rather than as
    /// a bound. cgroup v1 spells unlimited as a page-aligned saturation of the
    /// page counter near <see cref="long.MaxValue"/>, which is a well-formed
    /// positive number and would otherwise be believed. 4 EiB is not a boundary
    /// any real deployment sits near, so this does not trade a false positive for
    /// a false negative.
    /// </summary>
    internal const long UnlimitedSentinelFloor = 1L << 62;

    private static readonly Lazy<long> ResolvedCeilingBytes =
        new(
            () => ResolveCeilingBytes(
                GC.GetGCMemoryInfo().TotalAvailableMemoryBytes,
                LeafResidentWorkingSet.ReadContainerMemoryLimitBytes()),
            LazyThreadSafetyMode.ExecutionAndPublication);

    /// <summary>
    /// Test-only replacement for <see cref="Read"/>. Non-null diverts the reading
    /// to the fixture's own figures, so heap pressure can be simulated
    /// deterministically without arranging for a real process to approach its
    /// hard limit - which no unit test can do reproducibly, and which a test that
    /// tried would only manage by destabilising the runner.
    /// </summary>
    internal static Func<ReplayHeapReading>? ReaderForTest;

    /// <summary>
    /// Reads current heap occupancy against the resolved ceiling.
    /// <para>
    /// The occupancy term is <see cref="GC.GetTotalMemory(bool)"/> with
    /// <c>forceFullCollection: false</c> rather than
    /// <see cref="GCMemoryInfo.HeapSizeBytes"/>, and the choice matters. The
    /// latter is a snapshot taken at the last collection, so it is precisely
    /// blind during the interval this mechanism exists to catch: a burst of
    /// concurrent replays each allocating multi-MiB buffers between collections.
    /// The former is a live approximation, which can read high because it counts
    /// garbage the collector has not reclaimed yet - and that error is in the
    /// safe direction, since the cost of a false positive is one permit withheld
    /// and returned by the next relieved replay.
    /// </para>
    /// <para>
    /// The ceiling is resolved once per process and cached. It is a runtime and
    /// cgroup constant, and the cgroup half of it reads the filesystem, which is
    /// not something to do on every permit release.
    /// </para>
    /// </summary>
    internal static ReplayHeapReading Read()
    {
        var reader = Volatile.Read(ref ReaderForTest);
        return reader is not null
            ? reader()
            : new ReplayHeapReading(GC.GetTotalMemory(forceFullCollection: false), ResolvedCeilingBytes.Value);
    }

    /// <summary>
    /// Resolves the ceiling occupancy is measured against: the smaller of the two
    /// <i>known</i> figures, or a non-positive value when neither is known.
    /// </summary>
    /// <param name="totalAvailableMemoryBytes">
    /// <see cref="GCMemoryInfo.TotalAvailableMemoryBytes"/> - the limit the
    /// runtime throws against when a heap hard limit is configured, and host
    /// physical memory when one is not.
    /// </param>
    /// <param name="containerMemoryLimitBytes">
    /// The enforced cgroup memory limit, or a non-positive value when there is
    /// none or none could be read.
    /// </param>
    /// <returns>
    /// The resolved ceiling in bytes, or a non-positive value meaning <i>unknown</i>
    /// - which <see cref="IsPressured"/> and <see cref="IsRelieved"/> both read as
    /// "this mechanism has no verdict", leaving the gate exactly as it behaved
    /// before this change.
    /// </returns>
    /// <remarks>
    /// Pure and parameterised because both inputs are environmental. A rule that
    /// can only be exercised by arranging a container is a rule that is never
    /// exercised, and this one has already been got wrong once elsewhere in the
    /// same codebase (issue #2788).
    /// </remarks>
    internal static long ResolveCeilingBytes(long totalAvailableMemoryBytes, long containerMemoryLimitBytes)
    {
        var runtime = KnownLimitOrUnknown(totalAvailableMemoryBytes);
        var container = KnownLimitOrUnknown(containerMemoryLimitBytes);

        if (runtime <= 0)
            return container;

        return container <= 0 ? runtime : Math.Min(runtime, container);
    }

    /// <summary>
    /// Reports whether occupancy has reached the withholding threshold, so a
    /// replay permit that is about to be returned should be withheld instead.
    /// </summary>
    /// <param name="reading">The occupancy reading to judge.</param>
    /// <returns>
    /// <see langword="true"/> only when the ceiling is known <b>and</b> occupancy
    /// is at or above <see cref="WithholdOccupancyPercent"/> of it.
    /// </returns>
    internal static bool IsPressured(ReplayHeapReading reading)
        => reading.CeilingBytes > 0
            && reading.InUseBytes >= ThresholdBytes(reading.CeilingBytes, WithholdOccupancyPercent);

    /// <summary>
    /// Reports whether occupancy has receded far enough for a cleanly-completed
    /// replay to hand a withheld permit back.
    /// </summary>
    /// <param name="reading">The occupancy reading to judge.</param>
    /// <returns>
    /// <see langword="true"/> when occupancy is strictly below
    /// <see cref="RestoreOccupancyPercent"/> of a known ceiling, and also when the
    /// ceiling is <b>unknown</b>.
    /// </returns>
    /// <remarks>
    /// The unknown case returning <see langword="true"/> is deliberate and is the
    /// asymmetry that keeps this change additive. An unreadable ceiling must not
    /// silently disable the recovery half of issue #2781's mechanism, because
    /// that half is reached by every clean replay on every host - including the
    /// many with no heap hard limit at all, where this type has nothing to say.
    /// Withholding under an unknown ceiling is refused for the mirror reason: a
    /// reduction taken on no evidence is not backpressure.
    /// </remarks>
    internal static bool IsRelieved(ReplayHeapReading reading)
        => reading.CeilingBytes <= 0
            || reading.InUseBytes < ThresholdBytes(reading.CeilingBytes, RestoreOccupancyPercent);

    /// <summary>
    /// The byte figure <paramref name="percent"/> of <paramref name="ceilingBytes"/>
    /// corresponds to.
    /// </summary>
    /// <remarks>
    /// Divides before multiplying because a ceiling near the cgroup v1 unlimited
    /// sentinel would overflow the other way round. The cost is that the computed
    /// trigger point is low by at most <c>percent</c> bytes, which moves both
    /// thresholds fractionally <i>earlier</i> - the safe direction, and immaterial
    /// against any real heap.
    /// </remarks>
    private static long ThresholdBytes(long ceilingBytes, int percent)
        => ceilingBytes / 100L * percent;

    /// <summary>
    /// Normalises a reported limit to a usable ceiling, mapping a non-positive
    /// figure and the cgroup "unlimited" saturation alike onto <c>0</c>, meaning
    /// unknown.
    /// </summary>
    private static long KnownLimitOrUnknown(long limitBytes)
        => limitBytes <= 0 || limitBytes >= UnlimitedSentinelFloor ? 0L : limitBytes;
}
