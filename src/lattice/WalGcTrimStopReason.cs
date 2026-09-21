namespace Orleans.Lattice;

/// <summary>
/// Why a WAL GC pass's per-shard trim scan stopped advancing, and therefore why
/// it reclaimed the entries it did - or, in the case that matters, none at all.
/// <para>
/// The trim scan walks a shard's entries in ascending offset order and stops at
/// the first entry it may not trim, because offsets are dense and the
/// conservative shape forbids jumping over a retained entry to reclaim a later
/// one. When that first entry is the very first one scanned the pass reclaims
/// nothing, and before this enum existed the two reasons it could happen were
/// indistinguishable from each other and from a shard that had legitimately
/// nothing to do (issue #3149).
/// </para>
/// <para>
/// That ambiguity is not the same one <see cref="WalGcCursorFloorState"/>
/// resolves, and reading the two as interchangeable is what made this defect
/// hard to see. <see cref="WalGcCursorFloorState"/> describes the
/// <i>consumer-cursor</i> floor only. The offset floor - the lowest durably
/// applied leaf-checkpoint offset - is an independent gate applied in the same
/// scan, so a tree can report <see cref="WalGcCursorFloorState.Available"/>,
/// classify every pass as <see cref="LatticeMetrics.OutcomeOverCeiling"/>, and
/// still reclaim nothing because the offset floor sits below its whole retained
/// range. On that path the blocking-pin diagnostic is never written, so the tree
/// presents as breaching its ceiling with a healthy floor and no stated cause.
/// </para>
/// <para>
/// <b>Almost entirely diagnostic.</b> Every arm but one names a decision the
/// scan would have taken anyway, so it neither widens nor narrows the trim
/// predicate: the pass reclaims exactly the entries it would have reclaimed
/// before the value existed. The exception is
/// <see cref="DurabilityHold"/>, added by issue #3300, which reports a stop the
/// collector performs <i>because</i> of a configured durability hold and which
/// therefore does narrow the predicate. It is reachable by DEFAULT as of the
/// issue #3300 default-on change, but only on a tree whose every admitting
/// cursor is a leaf materialiser with no durable offset coverage - a tree on
/// which the collector otherwise releases records nothing outside the process
/// has attested to. A deployment with a materialiser wired, or any consumer
/// that reports a cursor from outside this process (a replication shipper, a
/// view maintainer, a log subscriber, the backup capture service), never
/// reaches it. Setting
/// <see cref="LatticeOptions.WalDurabilityHoldCeilingBytes"/> to zero disables
/// it outright. This distinction is called out rather than left implicit
/// because "diagnostic only" was a load-bearing guarantee of this enum, and a
/// member that quietly stopped honouring it would be exactly the kind of
/// unstated behaviour change the rest of these comments exist to prevent.
/// </para>
/// </summary>
[InstrumentedEnum(
    typeof(LatticeWalGc),
    "orleans.lattice.wal.gc.trim_stop",
    LatticeMetrics.TagReason)]
internal enum WalGcTrimStopReason
{
    /// <summary>
    /// The scan consumed every entry the provider offered without meeting one it
    /// had to retain, AND a durable offset floor was available to judge them
    /// against. The healthy value: everything present was eligible, so the shard
    /// trimmed as far as it physically could.
    /// <para>
    /// Narrowed by issue #3300. This arm previously also absorbed the scan that
    /// consumed a non-empty shard with NO durable offset floor at all, which is
    /// not the same statement: the first says "I checked, and everything was
    /// releasable", the second says "I could not check, so I released
    /// everything". Those were byte-identical readings, and the conflation is
    /// why a tree trimming live, never-checkpointed records reported a healthy
    /// arm on every pass. The second case is now
    /// <see cref="DurabilityUnverified"/>.
    /// </para>
    /// </summary>
    Exhausted = 0,

    /// <summary>
    /// The shard held no entries at all, so there was nothing to trim and
    /// nothing to retain. Distinct from <see cref="Exhausted"/> because a shard
    /// that is empty has no backlog to explain, whereas one that exhausted a
    /// non-empty log has just reclaimed something.
    /// </summary>
    Empty = 1,

    /// <summary>
    /// The scan stopped because the first entry it could not trim lay strictly
    /// above the durable materialiser <i>offset</i> floor - the lowest
    /// last-applied leaf-checkpoint offset across the tree's reporting leaves.
    /// <para>
    /// This <b>was</b> the defect state and is no longer one (issue #3172).
    /// While entitlement was HLC-only the offset floor could only ever subtract
    /// trim rights: the scan stopped at the floor and the entries below it still
    /// had to win the cursor clause on their own. A floor that is a
    /// <b>minimum over leaves</b> - so a single leaf whose checkpoint offset
    /// never advances holds it down for the entire tree - then produced a pass
    /// that trimmed nothing while every published floor-state series read
    /// healthy, classifying as <see cref="LatticeMetrics.OutcomeOverCeiling"/>
    /// rather than <see cref="LatticeMetrics.OutcomeBlocked"/> because the
    /// consumer-cursor floor was untouched and therefore still
    /// <see cref="WalGcCursorFloorState.Available"/>.
    /// </para>
    /// <para>
    /// Now that the floor also GRANTS entitlement, this reason means the scan
    /// reclaimed everything the durable floor entitles it to and the floor is the
    /// binding constraint - an honest boundary rather than a silent stall. It is
    /// also the reason a healthy tree is expected to report once the offset axis
    /// is doing the work, because a pass whose cursor is behind its floor now
    /// walks to the floor instead of stopping at its first entry. What remains
    /// diagnostic is the SUSTAINED case with zero bytes reclaimed, which still
    /// identifies a floor that covers none of the retained range - the leaf-level
    /// stall, not the entitlement rule.
    /// </para>
    /// </summary>
    OffsetFloor = 2,

    /// <summary>
    /// The scan stopped because the first entry it could not trim failed the
    /// entitlement clause of the eligibility predicate: neither the minimum
    /// consumer cursor, nor the retention TTL ceiling, nor the durable
    /// materialiser offset floor accepted it.
    /// <para>
    /// Indicts the <b>consumer-cursor</b> subsystem - a reader that has not
    /// acknowledged this far, or a retention window that has not aged the entry
    /// out. Separated from <see cref="OffsetFloor"/> because the two demand
    /// different investigations entirely. This one points at a consumer that has
    /// not advanced its cursor; the other points at a leaf that has not advanced
    /// its durable checkpoint. Collapsing them would send a diagnosis to the wrong
    /// subsystem, which is the failure mode this enum exists to prevent.
    /// </para>
    /// <para>
    /// Since issue #3172 this reason additionally means the offset axis did not
    /// rescue the entry, which narrows rather than muddies what it indicts: a
    /// tree reporting it either has no durable offset floor at all (an
    /// unreachable pin store, an all-sentinel pin set, a blocked partition), or
    /// has one and is held by a consumer that reports cursors but no offsets -
    /// a view maintainer, a log subscriber, the backup capture service, the
    /// replication shipper. Both are consumer-side, which is why the name still
    /// fits.
    /// </para>
    /// <para>
    /// Separated from <see cref="CausalFrontier"/> and <see cref="BlockPin"/> on
    /// exactly that same reasoning, applied to the boundary inside the predicate
    /// rather than the one around it (issue #3155). Those three were one arm,
    /// <c>not_eligible</c>, until a tree stopping every scan on it could be shown
    /// to be stranded with no way to say by what.
    /// </para>
    /// </summary>
    CursorFloor = 3,

    /// <summary>
    /// The scan stopped because the first entry it could not trim failed the
    /// causal-stable clause: a per-origin frontier has been reported and it does
    /// not dominate that entry's version vector.
    /// <para>
    /// Indicts the <b>causal frontier</b> - a replication origin whose stability
    /// has not advanced. It is not a restatement of <see cref="CursorFloor"/>: a
    /// consumer's HLC cursor can be arbitrarily far ahead while one origin's entry
    /// in its stable vector stays behind, so a tree held here has a healthy cursor
    /// and an unhealthy origin, and looking at the cursor would clear the wrong
    /// subsystem.
    /// </para>
    /// </summary>
    CausalFrontier = 4,

    /// <summary>
    /// The scan stopped because the first entry it could not trim sits at or above
    /// a consumer's buffer-pin floor.
    /// <para>
    /// Indicts a <b>buffering receiver</b> that published a non-null pin so it can
    /// recover from buffer state. Distinct from the other two arms in kind as well
    /// as in subsystem: this is a deliberate hold rather than a lag, so the
    /// question it raises is whether the pin is still live, not whether something
    /// is still moving. A sustained run of this arm against a consumer that is no
    /// longer buffering is a leaked pin.
    /// </para>
    /// </summary>
    BlockPin = 5,

    /// <summary>
    /// The scan consumed a NON-EMPTY shard while no durable materialiser offset
    /// floor could be established for the tree at all, so every entry it
    /// released was released without any check that the data had been durably
    /// applied anywhere.
    /// <para>
    /// This is the arm that names the state issue #3300 was lost in. It is NOT
    /// an error and it is not always a defect: a tree with no materialiser
    /// wired, or one whose leaves legitimately owe nothing, reaches it
    /// harmlessly. What it removes is the CONFLATION. Before it existed such a
    /// pass reported <see cref="Exhausted"/> - the same value as a shard that
    /// had a floor and cleared it - so "I trimmed everything because it was all
    /// releasable" and "I trimmed everything because I could not tell whether
    /// any of it was" were one indistinguishable reading, on the arm documented
    /// as the healthy one. A tree silently discarding live records therefore
    /// published a clean bill of health on every pass.
    /// </para>
    /// <para>
    /// This is this repository's signature defect shape - a component that
    /// cannot establish a fact reporting the reassuring value rather than
    /// reporting that it could not establish it (see issues #3149, #3155,
    /// #3301). The remedy is the same one those issues applied: make the
    /// unknown state NAMEABLE, so the next instance is findable rather than
    /// invisible. Handling it silently is not enough.
    /// </para>
    /// <para>
    /// Deliberately distinct from <see cref="Empty"/>. An empty shard has
    /// nothing to lose, so a missing floor over it is genuinely uninteresting;
    /// the concerning case is exactly the one where entries WERE released. It is
    /// also distinct from the <c>offset_floor_unavailable</c> counter, which
    /// records only the pin-store read THROWING (issue #2314). The population
    /// that matters here is larger and mostly silent: a reachable store that
    /// reports no offsets, an all-sentinel pin set, or no pin store wired at
    /// all, none of which reach that counter's catch.
    /// </para>
    /// <para>
    /// Diagnostic only, exactly like every other arm: it is chosen AFTER the
    /// scan has finished and changes nothing about which entries that scan was
    /// allowed to release. Making trim fail closed on this state is a separate,
    /// behavioural change that must not be conflated with naming it, because a
    /// fail-closed gate with no working path to open it grows the WAL without
    /// bound (issue #3094).
    /// </para>
    /// </summary>
    DurabilityUnverified = 6,

    /// <summary>
    /// The scan stopped at the shard's first entry because the durability hold
    /// engaged: every cursor admitting the trim was a leaf materialiser the
    /// durable offset floor does not speak for, and the hold
    /// (<see cref="LatticeOptions.WalDurabilityHoldCeilingBytes"/>) is not yet
    /// exhausted, so nothing was reclaimed (issue #3300).
    /// <para>
    /// <b>The one arm on this enum that reports a stop the collector chose
    /// rather than one it merely observed.</b> Every other member labels a
    /// decision the scan would have taken regardless; this one exists only
    /// because the hold engaged. Setting the hold ceiling to zero makes it
    /// unreachable and the pass behaves exactly as before.
    /// </para>
    /// <para>
    /// The arm does not say WHICH durability condition held the scan. A tree
    /// that has never pinned a durable floor is stalled and holds until its
    /// ceiling forces it; a tree whose floor regressed is mid-upgrade and
    /// clears itself. Those need opposite operator responses, so the
    /// distinction is carried by
    /// <c>orleans.lattice.wal.gc.durability_hold_engaged</c>'s <c>reason</c>
    /// tag rather than smuggled into this enum, whose members name where a scan
    /// stopped and not why the tree is in that state.
    /// </para>
    /// <para>
    /// Distinct from <see cref="DurabilityUnverified"/>, and the pair is
    /// deliberate: both describe a tree with no durable floor, but this one is
    /// the collector <i>retaining</i> those entries and that one is the
    /// collector <i>releasing</i> them. Reading a tree's trim-stop series should
    /// answer which of the two happened without inference, because they differ
    /// in whether data survived.
    /// </para>
    /// <para>
    /// Counted as a retention stop, so a tree held here reports a WAL backlog
    /// and is visible to the byte-pressure advisory rather than presenting as
    /// idle. That is the honest reading: bytes really are being retained, and a
    /// tree sitting on this arm is accumulating toward the ceiling that will
    /// eventually force it onto <see cref="DurabilityUnverified"/>.
    /// </para>
    /// </summary>
    DurabilityHold = 7,

    /// <summary>
    /// The scan stopped because the first entry it could not trim was one the
    /// consumer cursor would have admitted and the durable materialiser offset
    /// floor overruled: the cursor dominates the entry, no retention TTL ceiling
    /// covers it, and the partition's <see cref="WalGcOffsetAdmission"/> declined
    /// it (issue #3300).
    /// <para>
    /// Indicts the <b>durable materialiser</b>. Every other stop on this enum
    /// names something that has not advanced far enough; this one names a tree
    /// where the in-memory consumer cursor HAS advanced past what any durable
    /// evidence supports. That cursor tracks what a leaf folded into its cache,
    /// so it moves the instant a write lands - and while the offset axis could
    /// only ever ADD entitlement, the collector released the entry on that alone,
    /// destroying the only copy. The rows then survived in memory until the next
    /// process boundary and vanished there, with every published series reading
    /// healthy.
    /// </para>
    /// <para>
    /// Like <see cref="DurabilityHold"/> and unlike every other arm, this reports
    /// a stop the predicate CHOSE rather than one it merely observed. It is
    /// reachable only where a durable offset floor was actually established for
    /// the partition, so a tree that reports no offsets never sees it, and a
    /// configured retention TTL still admits independently, so an operator's
    /// retention window is honoured against a floor that has stalled.
    /// </para>
    /// <para>
    /// Distinct from <see cref="OffsetFloor"/>, and the pair is easy to conflate.
    /// That one means the entry lay strictly ABOVE the floor - the scan reached
    /// the floor's edge and stopped, which is an honest boundary. This one means
    /// the entry lay at or below the floor and was still refused, because a
    /// consumer the floor does not speak for still needs it. Distinct from
    /// <see cref="CursorFloor"/> for the converse reason: there the cursor
    /// refused, here the cursor accepted and was overruled.
    /// </para>
    /// <para>
    /// Counted as a retention stop, so a tree held here reports a WAL backlog and
    /// is visible to the byte-pressure advisory rather than presenting as idle.
    /// A sustained run with nothing reclaimed is a leaf whose durable checkpoint
    /// or snapshot coverage has stopped advancing; naming it is the whole point,
    /// because a silent hold would be indistinguishable from the stalled-floor
    /// state of issue #3094 and the two demand opposite responses.
    /// </para>
    /// </summary>
    DurableOffsetRefusal = 8,
}
