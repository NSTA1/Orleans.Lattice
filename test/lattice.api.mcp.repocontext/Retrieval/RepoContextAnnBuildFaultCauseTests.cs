using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// A faulted approximate-index build step must record WHY it faulted, not merely
/// that it did (issue #2880).
/// <para>
/// <b>The blindness these pin.</b> <c>repocontext.ann.build.slice</c> gained a
/// <c>faulted</c> arm on issue #2737, which separated "stepping and throwing" from
/// "never stepping" and was a real improvement. What it could not do is say what
/// threw. On run 12 of epic #2368 the arm rose on every single tick for the whole
/// run, and establishing the reason took dumping an eight-megabyte container log
/// and reading it by line index: 39 of 39 faults were a
/// <see cref="TimeoutException"/> on a vector-index leaf read, none on the content
/// or symbol trees. Every one of those 39 ticks had already emitted a measurement
/// that could have carried that fact and did not.
/// </para>
/// <para>
/// <b>Why the distinction is not cosmetic.</b> The causes have opposite remedies. A
/// leaf read that cannot be paged in one grain call is a tree-shape problem; a
/// cluster that has not settled is a deployment problem; a stale projection
/// checkpoint needs an operator rebuild; a rejected embedding space needs the
/// plane re-derived. A single undifferentiated <c>faulted</c> count is compatible
/// with all four, so it can start an investigation and cannot direct one.
/// </para>
/// <para>
/// NonParallelizable because a <see cref="MeterListener"/> is process-wide, so it
/// observes instruments published by any fixture running beside it.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class RepoContextAnnBuildFaultCauseTests
{
    /// <summary>One measurement seen on the slice instrument.</summary>
    /// <param name="Progress">The <c>progress</c> tag, or <c>null</c> when untagged.</param>
    /// <param name="Cause">The <c>cause</c> tag, or <c>null</c> when the measurement carries none.</param>
    /// <param name="Phase">The <c>phase</c> tag, or <c>null</c> when the measurement carries none.</param>
    /// <param name="Repository">The <c>repository</c> tag, or <c>null</c>.</param>
    /// <param name="Space">The <c>space</c> tag, or <c>null</c>.</param>
    /// <param name="Value">The value added.</param>
    private readonly record struct SliceMeasurement(
        string? Progress,
        string? Cause,
        string? Phase,
        string? Repository,
        string? Space,
        long Value);

    /// <summary>The repository every measurement in this fixture is recorded under.</summary>
    private const string Repo = "acme/widgets";

    /// <summary>The embedding space every measurement in this fixture is recorded under.</summary>
    private static readonly EmbeddingSpaceTag TestSpace =
        new("test-model", 8, VectorNormalization.UnitL2);

    /// <summary>
    /// Runs <paramref name="act"/> against a freshly constructed reporter with a
    /// listener already attached, and returns every measurement the slice
    /// instrument emitted - plane priming included.
    /// </summary>
    /// <param name="act">What to drive through the reporter.</param>
    /// <returns>The measurements, in order.</returns>
    /// <remarks>
    /// The listener is started BEFORE the reporter is constructed, because an
    /// instrument cannot be passed by reference until it exists and the first
    /// recording call primes its plane in the same breath. Matching by meter and
    /// instrument name is the only way to be attached in time.
    /// </remarks>
    private static List<SliceMeasurement> Observe(Action<RepoContextAnnBuildSliceReporter> act)
    {
        var observed = new List<SliceMeasurement>();
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (string.Equals(
                        instrument.Meter.Name, RepoContextUsageRecorder.MeterName, StringComparison.Ordinal)
                    && string.Equals(
                        instrument.Name,
                        RepoContextAnnBuildSliceReporter.SliceInstrumentName,
                        StringComparison.Ordinal))
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };

        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            string? progress = null;
            string? cause = null;
            string? phase = null;
            string? repository = null;
            string? space = null;
            foreach (var tag in tags)
            {
                if (string.Equals(tag.Key, RepoContextAnnBuildSliceReporter.ProgressTagKey, StringComparison.Ordinal))
                {
                    progress = tag.Value?.ToString();
                }
                else if (string.Equals(tag.Key, RepoContextAnnBuildSliceReporter.CauseTagKey, StringComparison.Ordinal))
                {
                    cause = tag.Value?.ToString();
                }
                else if (string.Equals(tag.Key, RepoContextAnnBuildSliceReporter.PhaseTagKey, StringComparison.Ordinal))
                {
                    phase = tag.Value?.ToString();
                }
                else if (string.Equals(
                    tag.Key, RepoContextAnnBuildSliceReporter.RepositoryTagKey, StringComparison.Ordinal))
                {
                    repository = tag.Value?.ToString();
                }
                else if (string.Equals(tag.Key, RepoContextAnnBuildSliceReporter.SpaceTagKey, StringComparison.Ordinal))
                {
                    space = tag.Value?.ToString();
                }
            }

            lock (observed)
            {
                observed.Add(new SliceMeasurement(progress, cause, phase, repository, space, value));
            }
        });

        listener.Start();

        using (var reporter = new RepoContextAnnBuildSliceReporter())
        {
            act(reporter);
        }

        listener.Dispose();

        lock (observed)
        {
            return [.. observed];
        }
    }

    [Test]
    public void Every_faulted_measurement_carries_the_cause_it_was_recorded_under()
    {
        // THE REMEDY ITSELF, asserted on the meter rather than on the in-process
        // tally, because the meter is what a scrape sees and the tally is only a
        // convenience for fixtures. Reflected over the enum so a cause added later
        // is covered without this fixture being edited.
        var causes = Enum.GetValues<RepoContextAnnBuildFaultCause>();

        var measurements = Observe(reporter =>
        {
            foreach (var cause in causes)
            {
                reporter.RecordFaulted(cause, Repo, TestSpace, RepoContextAnnBuildStepPhase.Ingesting);
            }
        });

        var faulted = measurements
            .Where(m => string.Equals(
                m.Progress, RepoContextAnnBuildSliceReporter.ProgressFaultedTag, StringComparison.Ordinal)
                && m.Value != 0)
            .ToArray();

        var expected = causes.Select(RepoContextAnnBuildSliceReporter.DescribeCause).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(causes, Is.Not.Empty,
                "positive control: the reflection must find members, or every assertion below "
                + "passes over an empty set and pins nothing");
            Assert.That(faulted, Is.Not.Empty,
                "positive control: the listener must have observed faulted measurements, or the "
                + "tag assertions below are made against nothing");
            Assert.That(faulted.Select(m => m.Cause), Is.EquivalentTo(expected),
                "every fault the counter records must name a cause. A faulted arm that rises "
                + "without one is exactly the reading run 12 produced: it says an investigation "
                + "is needed and nothing about where to point it");
            Assert.That(faulted, Has.None.Matches<SliceMeasurement>(m => string.IsNullOrEmpty(m.Cause)),
                "and none of them may carry an empty cause, which would scrape as a present "
                + "series with a blank label - the one shape that looks answered and is not");
        });
    }

    [Test]
    public void A_step_that_did_not_fault_carries_no_cause_tag_at_all()
    {
        // THE ADVERSARIAL ARM. A cause that were attached to every measurement -
        // or defaulted onto the healthy ones - would satisfy the fixture above
        // while telling a reader nothing, because a dimension that is always
        // present cannot distinguish anything. The healthy arms must carry no
        // cause dimension whatsoever, so 'cause' appearing at all is itself
        // evidence of a fault.
        var measurements = Observe(reporter =>
        {
            reporter.RecordSlice(
                RepoContextAnnBuildSliceOutcome.Advanced, Repo, TestSpace, RepoContextAnnBuildStepPhase.Ingesting);
            reporter.RecordSlice(
                RepoContextAnnBuildSliceOutcome.Idle, Repo, TestSpace, RepoContextAnnBuildStepPhase.Ingesting);
            reporter.RecordSlice(
                RepoContextAnnBuildSliceOutcome.Starved, Repo, TestSpace, RepoContextAnnBuildStepPhase.Ingesting);
            reporter.RecordSlice(
                RepoContextAnnBuildSliceOutcome.Churned, Repo, TestSpace, RepoContextAnnBuildStepPhase.Ingesting);
            reporter.RecordFaulted(
                RepoContextAnnBuildFaultCause.ScanPageStalled,
                Repo,
                TestSpace,
                RepoContextAnnBuildStepPhase.Ingesting);
        });

        var moved = measurements.Where(m => m.Value != 0).ToArray();
        var healthy = moved
            .Where(m => !string.Equals(
                m.Progress, RepoContextAnnBuildSliceReporter.ProgressFaultedTag, StringComparison.Ordinal))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(healthy, Has.Length.EqualTo(4),
                "positive control: all four non-faulted arms must have been recorded, or the "
                + "absence asserted below is an absence of measurements rather than an absence "
                + "of tags");
            Assert.That(
                moved.Count(m => !string.IsNullOrEmpty(m.Cause)), Is.EqualTo(1),
                "positive control: the single faulted step must genuinely have carried a cause, "
                + "or this fixture would pass identically against a build with no cause "
                + "dimension at all");
            Assert.That(healthy.Select(m => m.Cause), Is.All.Null,
                "a step that completed has no cause to report, and attaching one anyway would "
                + "double the cardinality of the four arms that are read most often while "
                + "destroying the property that makes the dimension useful: that its presence "
                + "means a fault");
        });
    }

    [Test]
    public void No_cause_is_minted_when_a_plane_is_primed()
    {
        // THE DELIBERATE NON-PRIMING, pinned so it is a decision rather than an
        // omission somebody later "fixes".
        //
        // Every enumerable outcome arm on this plane IS pre-minted, because an
        // absent series and a measured zero are byte-identical on a scrape and a
        // verdict resting on an unproven zero is worthless. The cause dimension is
        // deliberately the exception: pre-minting five causes would put five
        // permanently-zero series on every plane that never faults, and would assert
        // a partition of fault space that this classifier explicitly does not claim
        // to have (its 'unexpected' arm exists precisely because the space is open).
        //
        // The cost is that a zero on a cause is UNINTERPRETABLE rather than
        // innocent, and that is why the fixture below exists to say so out loud:
        // read the cause dimension only when progress=faulted is non-zero, and read
        // the absence of a cause series as "no fault of that kind has been recorded
        // yet", never as "that cause does not occur".
        //
        // Priming is per PLANE and no longer happens in the constructor, because a
        // constructor cannot know which repositories and spaces exist and a primed
        // series for a plane that does not exist claims a build nobody asked for.
        var measurements = Observe(reporter => reporter.EnsurePrimed(Repo, TestSpace));

        Assert.Multiple(() =>
        {
            // The known-positive control. Without it a listener that attached too
            // late, or matched the wrong instrument, would observe nothing at all
            // and the absence assertion would confirm the belief it was written to
            // test.
            Assert.That(measurements, Is.Not.Empty,
                "positive control: priming the plane must have minted SOMETHING, or this fixture "
                + "is a blind detector reporting an absence it could not have seen");
            Assert.That(
                measurements.Select(m => m.Progress).Distinct(),
                Is.EquivalentTo(
                    Enum.GetValues<RepoContextAnnBuildSliceOutcome>()
                        .Select(RepoContextAnnBuildSliceReporter.DescribeOutcome)),
                "positive control: the progress arms ARE pre-minted, and this fixture must be "
                + "able to see them, or its verdict on the cause dimension is worthless");
            Assert.That(measurements.Select(m => m.Cause), Is.All.Null,
                "and no cause may be minted, because the fault space is open rather than "
                + "enumerable: 'unexpected' exists exactly because a cause nobody has mapped can "
                + "occur, so a primed set would assert a completeness the classifier does not "
                + "have");
        });
    }

    [Test]
    public void Every_fault_cause_maps_to_a_distinct_bounded_tag()
    {
        // A closed tag set is what keeps an unrecognised value out of the meter as
        // unbounded-cardinality text. The count is asserted first so the reflection
        // can never go vacuously green over an empty set.
        var causes = Enum.GetValues<RepoContextAnnBuildFaultCause>();
        var tags = causes.Select(RepoContextAnnBuildSliceReporter.DescribeCause).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(causes, Is.Not.Empty,
                "positive control: the reflection must find members, or every assertion below "
                + "passes over an empty set");
            Assert.That(causes, Has.Length.EqualTo(6),
                "a member added without a tag of its own would fall onto 'unexpected' and be "
                + "silently merged with it, which is the one arm whose whole job is to be rare; "
                + "add the tag and update this count together");
            Assert.That(tags, Is.Unique,
                "two causes sharing a tag would be indistinguishable on the dashboard, which is "
                + "the blindness this dimension exists to remove");
            Assert.That(tags, Has.None.Null.And.None.Empty);
        });
    }

    [Test]
    public void An_unmapped_cause_value_fails_open_onto_the_arm_that_pages()
    {
        // A cast that lands outside the enum must not produce an unbounded tag, and
        // must not land on an arm with a benign explanation. 'unexpected' is the arm
        // that pages, and a fault nobody has classified is closer to one nobody
        // understands than to one already diagnosed.
        const RepoContextAnnBuildFaultCause Unmapped = (RepoContextAnnBuildFaultCause)9999;

        Assert.Multiple(() =>
        {
            Assert.That(Enum.IsDefined(Unmapped), Is.False,
                "positive control: the value must genuinely be outside the enum, or this fixture "
                + "is testing an ordinary mapped member");
            Assert.That(
                RepoContextAnnBuildSliceReporter.DescribeCause(Unmapped),
                Is.EqualTo(RepoContextAnnBuildSliceReporter.CauseUnexpectedTag));
        });
    }

    [Test]
    public void A_fault_cannot_be_recorded_through_the_outcome_entry_point()
    {
        // THE STRUCTURAL GUARANTEE, and the reason there are two entry points rather
        // than one with an optional argument. A single RecordSlice(outcome, cause =
        // default) would let a newly added fault path compile while emitting the
        // default value, and a default is precisely how the next reader is handed a
        // benign-looking number again. Here a fault cannot be counted without a
        // cause because no overload accepts one without.
        using var reporter = new RepoContextAnnBuildSliceReporter();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => reporter.RecordSlice(
                    RepoContextAnnBuildSliceOutcome.Faulted, Repo, TestSpace, RepoContextAnnBuildStepPhase.Ingesting),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(
                () => reporter.RecordSlice(
                    RepoContextAnnBuildSliceOutcome.Idle, Repo, TestSpace, RepoContextAnnBuildStepPhase.Ingesting),
                Throws.Nothing,
                "positive control: the other outcomes must still be accepted, or the refusal "
                + "above is indistinguishable from an entry point that rejects everything");
        });
    }

    [Test]
    public void The_tally_the_reporter_reads_back_agrees_with_the_causes_recorded()
    {
        // The snapshot is what the grain fixtures assert against, so it has to track
        // the meter rather than being a second, independently-wrong ledger.
        using var reporter = new RepoContextAnnBuildSliceReporter();

        reporter.RecordFaulted(
            RepoContextAnnBuildFaultCause.ScanPageStalled, Repo, TestSpace, RepoContextAnnBuildStepPhase.Ingesting);
        reporter.RecordFaulted(
            RepoContextAnnBuildFaultCause.ScanPageStalled, Repo, TestSpace, RepoContextAnnBuildStepPhase.Ingesting);
        reporter.RecordFaulted(
            RepoContextAnnBuildFaultCause.DependencyUnavailable,
            Repo,
            TestSpace,
            RepoContextAnnBuildStepPhase.Persisting);
        reporter.RecordFaulted(
            RepoContextAnnBuildFaultCause.ProjectionStale, Repo, TestSpace, RepoContextAnnBuildStepPhase.Opening);
        reporter.RecordFaulted(
            RepoContextAnnBuildFaultCause.PlaneRejected, Repo, TestSpace, RepoContextAnnBuildStepPhase.Training);
        reporter.RecordFaulted(
            RepoContextAnnBuildFaultCause.Saturated, Repo, TestSpace, RepoContextAnnBuildStepPhase.Opening);
        reporter.RecordFaulted(
            (RepoContextAnnBuildFaultCause)9999, Repo, TestSpace, RepoContextAnnBuildStepPhase.Reconciling);

        var slices = reporter.Read();

        Assert.Multiple(() =>
        {
            Assert.That(slices.Faulted, Is.EqualTo(7),
                "positive control: every recorded fault must reach the undifferentiated total, "
                + "so the per-cause split below is a partition of a number that is itself right");
            Assert.That(slices.FaultedByCause.ScanPageStalled, Is.EqualTo(2));
            Assert.That(slices.FaultedByCause.DependencyUnavailable, Is.EqualTo(1));
            Assert.That(slices.FaultedByCause.ProjectionStale, Is.EqualTo(1));
            Assert.That(slices.FaultedByCause.PlaneRejected, Is.EqualTo(1));
            Assert.That(slices.FaultedByCause.Saturated, Is.EqualTo(1),
                "the saturation arm has to be readable back off the snapshot, or the grain fixtures "
                + "that assert against the snapshot cannot tell a refusal from the arm that pages");
            Assert.That(slices.FaultedByCause.Unexpected, Is.EqualTo(1),
                "an unmapped value must land on the same arm the tag mapping resolves it to, or "
                + "the tally and the meter disagree about where a fault went");
            Assert.That(slices.FaultedByCause.Total, Is.EqualTo(slices.Faulted),
                "the per-cause arms must PARTITION the faulted total. A cause split that does "
                + "not sum to the total is the shape that makes a dashboard ratio quietly wrong");
            Assert.That(slices.FaultedByPhase.Ingesting, Is.EqualTo(2));
            Assert.That(slices.FaultedByPhase.Persisting, Is.EqualTo(1));
            Assert.That(slices.FaultedByPhase.Opening, Is.EqualTo(2));
            Assert.That(slices.FaultedByPhase.Training, Is.EqualTo(1));
            Assert.That(slices.FaultedByPhase.Reconciling, Is.EqualTo(1));
            Assert.That(slices.FaultedByPhase.Coordinating, Is.EqualTo(0));
            Assert.That(slices.FaultedByPhase.Total, Is.EqualTo(slices.Faulted),
                "and so must the per-phase arms. THE DENOMINATOR IS ASSERTED BEFORE ANY RATIO "
                + "is read off this instrument: an enumerated sample is not a population until "
                + "an independent measure agrees on its size, and the phase split is the arm "
                + "epic #2368 scores DoD-1b against");
        });
    }

    /// <summary>
    /// The classification table, asserted type by type.
    /// <para>
    /// <b>Two of these arms are subtypes of a later one and the ordering is
    /// load-bearing.</b> <c>ScanPageStalledException</c> derives from
    /// <see cref="TimeoutException"/>, so a classifier testing the timeout arm first
    /// would swallow every leaf-chain stall into <c>dependency-unavailable</c> -
    /// and a tree whose leaf cannot be paged in one grain call needs a different
    /// remedy from a cluster that has not settled. The two cases are adjacent in
    /// this table for that reason.
    /// </para>
    /// </summary>
    /// <param name="exception">The fault to classify.</param>
    /// <param name="expectedTag">The <c>cause</c> tag value it must be attributed to.</param>
    [TestCaseSource(nameof(ClassificationCases))]
    public void A_fault_is_attributed_to_the_cause_its_type_names(
        Exception exception, string expectedTag)
        => Assert.That(
            RepoContextAnnBuildSliceReporter.DescribeCause(
                RepoContextAnnIndexBuildGrain.ClassifyBuildFault(exception)),
            Is.EqualTo(expectedTag));

    private static IEnumerable<TestCaseData> ClassificationCases()
    {
        yield return new TestCaseData(
            new ScanPageStalledException("leaf page did not settle"),
            RepoContextAnnBuildSliceReporter.CauseScanPageStalledTag)
            .SetName("A_leaf_page_stall_is_not_folded_into_the_generic_timeout_arm");

        yield return new TestCaseData(
            new TimeoutException(
                "Response did not arrive on time for Request to shardroot/repo-context-vector-index/7 "
                + "IBPlusLeafGrain.GetEntriesAsync"),
            RepoContextAnnBuildSliceReporter.CauseDependencyUnavailableTag)
            .SetName("A_plain_grain_call_timeout_is_dependency_unavailable");

        yield return new TestCaseData(
            new LeafProjectionStaleException("checkpoint has fallen off the log"),
            RepoContextAnnBuildSliceReporter.CauseProjectionStaleTag)
            .SetName("A_stale_leaf_projection_has_its_own_arm");

        yield return new TestCaseData(
            new EmbeddingSpaceMismatchException("space dimension 768 does not match index dimension 384"),
            RepoContextAnnBuildSliceReporter.CausePlaneRejectedTag)
            .SetName("An_embedding_space_mismatch_is_plane_rejected");

        yield return new TestCaseData(
            new ArgumentOutOfRangeException("dimension"),
            RepoContextAnnBuildSliceReporter.CausePlaneRejectedTag)
            .SetName("A_rejected_argument_is_plane_rejected");

        yield return new TestCaseData(
            new IOException("the device is not ready"),
            RepoContextAnnBuildSliceReporter.CauseDependencyUnavailableTag)
            .SetName("A_storage_io_failure_is_dependency_unavailable");

        yield return new TestCaseData(
            new InvalidDataException("corrupt centroid block"),
            RepoContextAnnBuildSliceReporter.CauseUnexpectedTag)
            .SetName("An_unrecognised_type_falls_open_onto_the_arm_that_pages");

        yield return new TestCaseData(
            new InvalidOperationException(
                "build step failed",
                new ScanPageStalledException("leaf page did not settle")),
            RepoContextAnnBuildSliceReporter.CauseScanPageStalledTag)
            .SetName("A_wrapped_fault_is_classified_from_its_inner_chain");

        yield return new TestCaseData(
            new SiloUnavailableException("silo S1 is not available"),
            RepoContextAnnBuildSliceReporter.CauseDependencyUnavailableTag)
            .SetName("Silo_churn_is_matched_by_type_name_because_one_runtime_type_is_internal");

        // ISSUE #3286. The two instruments that book this one event disagreed:
        // repocontext.ann.index.load recorded outcome="refused" while this one
        // recorded cause="unexpected", the arm its own HELP text calls the only
        // value that should page. Measured live, refused read 61 against a faulted
        // 44 on the same plane over the same window.
        yield return new TestCaseData(
            new LatticeSaturatedException("WAL replay permits are withheld at the occupancy floor"),
            RepoContextAnnBuildSliceReporter.CauseSaturatedTag)
            .SetName("An_admission_refusal_is_saturated_rather_than_the_arm_that_pages");

        yield return new TestCaseData(
            new InvalidOperationException(
                "the open could not complete",
                new LatticeSaturatedException("WAL replay permits are withheld")),
            RepoContextAnnBuildSliceReporter.CauseSaturatedTag)
            .SetName("A_wrapped_admission_refusal_is_still_saturated");

        // THE OUTER-FIRST CASE. The classifier walks the inner-exception chain from
        // the OUTERMOST exception inwards, so a refusal that happens to wrap a
        // classifiable inner cause is attributed to the refusal. That is the right
        // attribution here - the open was turned away at admission and never got
        // far enough for the inner condition to be what stopped it - and a walk
        // reordered to deepest-first would quietly send an operator after an
        // embedding-space defect that is not there while the real condition is a
        // heap at its hard limit.
        //
        // Note what this case does NOT pin, because the distinction matters for
        // anyone maintaining it: the ARM ORDER inside the loop body. No arm below
        // the saturation arm matches InvalidOperationException, which
        // LatticeSaturatedException derives from, so moving the saturation arm down
        // the body changes nothing observable today. The reason it is written first
        // is stated at the site instead.
        yield return new TestCaseData(
            new LatticeSaturatedException(
                "WAL replay permits are withheld",
                new EmbeddingSpaceMismatchException("space dimension 768 does not match 384")),
            RepoContextAnnBuildSliceReporter.CauseSaturatedTag)
            .SetName("An_admission_refusal_outranks_an_inner_cause_it_merely_wrapped");
    }

    /// <summary>
    /// Stands in for the Orleans runtime exception of the same name, which cannot be
    /// constructed here because one of the two types the classifier matches is
    /// internal to Orleans. Matching by type NAME is what lets the classifier see it
    /// at all, and that match is exactly what this case pins - a classifier that
    /// switched to a type check would go green on every other case in this table and
    /// red only here.
    /// </summary>
    /// <param name="message">The exception message.</param>
    private sealed class SiloUnavailableException(string message) : Exception(message);
}
