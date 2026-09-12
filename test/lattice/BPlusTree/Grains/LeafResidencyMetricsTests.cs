using System.Diagnostics.Metrics;
using System.IO;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for <see cref="LeafResidencyMetrics"/>, the observable gauges that
/// report the <b>state</b> of the per-silo resident leaf working set (issue
/// #2788).
/// <para>
/// These exist because <c>orleans.lattice.leaf.residency.sheds</c> read zero on
/// a process logging 2,105 OOM events, and that zero was ambiguous between "the
/// working set is correctly quiescent under budget" and "registration never
/// happens, so the ledger is empty". Those have opposite remedies. Priming the
/// counter did not help and could not have: priming makes a counter's silence
/// readable, but a counter observes an <b>action</b> and the question is about
/// <b>state</b>.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafResidencyMetricsTests
{
    private static readonly string[] GaugeNames =
    [
        LatticeMetrics.LeafResidencyBudgetBytesName,
        LatticeMetrics.LeafResidencyResidentBytesName,
        LatticeMetrics.LeafResidencyRegistrationsName,
    ];

    /// <summary>
    /// Scrapes the three residency gauges once and returns the value observed
    /// for each instrument name that reported.
    /// </summary>
    private static Dictionary<string, long> Scrape()
    {
        LeafResidencyMetrics.EnsureRegistered();

        var observed = new Dictionary<string, long>(StringComparer.Ordinal);
        using var listener = MeterListening.StartForMeter(
            LatticeMetrics.Meter,
            GaugeNames,
            l => l.SetMeasurementEventCallback<long>(
                (instrument, value, _, _) => observed[instrument.Name] = value));

        listener.RecordObservableInstruments();
        return observed;
    }

    // ---------------------------------------------------------------
    // Projection. Exercised against an explicit, deterministic instance:
    // the shared working set's budget is environmental, so asserting a
    // value on it would either be untestable or would re-encode the
    // runtime's answer.
    // ---------------------------------------------------------------

    [Test]
    public void Budget_gauge_reports_the_resolved_budget_not_the_limit_it_was_derived_from()
    {
        const long heapLimit = 8L * 1024 * 1024 * 1024;
        var resolved = LeafResidentWorkingSet.ResolveBudgetBytes(heapLimit, 0L);

        var measured = LeafResidencyMetrics.MeasureBudgetBytes(
            new LeafResidentWorkingSet(resolved));

        Assert.Multiple(() =>
        {
            Assert.That(measured.Value, Is.EqualTo(resolved));

            // The load-bearing half. A gauge that published the heap limit
            // rather than the budget would look plausible on a dashboard and
            // would answer the question it exists to answer incorrectly, so the
            // "not the input" property is asserted independently of the value.
            Assert.That(
                measured.Value,
                Is.LessThan(heapLimit),
                "publishing the heap limit instead of the derived budget would misreport headroom by the divisor");
        });
    }

    [Test]
    public void Resident_bytes_gauge_tracks_registration_and_release()
    {
        var workingSet = new LeafResidentWorkingSet(64L * 1024 * 1024);
        Assert.That(LeafResidencyMetrics.MeasureResidentBytes(workingSet).Value, Is.Zero);

        var registration = workingSet.Register("tree", 4096L, snapshotBanked: true, shed: () => { });
        Assert.That(
            LeafResidencyMetrics.MeasureResidentBytes(workingSet).Value,
            Is.EqualTo(4096L),
            "a gauge that did not move on registration would report an empty ledger for a populated one");

        registration.Dispose();
        Assert.That(
            LeafResidencyMetrics.MeasureResidentBytes(workingSet).Value,
            Is.Zero,
            "a gauge that did not fall on release would drift upwards forever and read as a leak");
    }

    [Test]
    public void Registration_count_gauge_tracks_registration_and_release()
    {
        var workingSet = new LeafResidentWorkingSet(64L * 1024 * 1024);
        Assert.That(LeafResidencyMetrics.MeasureRegistrations(workingSet).Value, Is.Zero);

        var first = workingSet.Register("tree", 1024L, snapshotBanked: true, shed: () => { });
        var second = workingSet.Register("tree", 1024L, snapshotBanked: true, shed: () => { });

        Assert.That(
            LeafResidencyMetrics.MeasureRegistrations(workingSet).Value,
            Is.EqualTo(2),
            "this is the arm that separates an empty ledger from an under-budget one");

        first.Dispose();
        second.Dispose();

        Assert.That(LeafResidencyMetrics.MeasureRegistrations(workingSet).Value, Is.Zero);
    }

    // ---------------------------------------------------------------
    // Registration and publication.
    // ---------------------------------------------------------------

    [Test]
    public void EnsureRegistered_publishes_all_three_gauges_without_any_leaf_registering()
    {
        // Half of the control. Every projection test above would still pass if a
        // gauge were created lazily inside LeafResidentWorkingSet.Register,
        // because each of those tests registers a leaf. This is the only arm
        // that pins the property the gauges exist for: a scrape taken on an idle
        // silo must produce a present-and-zero reading rather than no series at
        // all, because an absent series is precisely what made the shed counter
        // unreadable in the first place.
        //
        // It is only half, and the boundary matters. Scrape() calls
        // EnsureRegistered itself, so this arm pins what EnsureRegistered does
        // and is blind to whether anything calls it. The companion arm
        // AddLattice_registers_the_residency_gauges_at_silo_build covers that,
        // and it has to be a source scan for the reason recorded there.
        var observed = Scrape();

        Assert.That(
            observed.Keys,
            Is.EquivalentTo(GaugeNames),
            "an absent series reads identically to 'the build does not carry the gauges'");
    }

    [Test]
    public void Each_residency_gauge_is_published_exactly_once()
    {
        LeafResidencyMetrics.EnsureRegistered();
        LeafResidencyMetrics.EnsureRegistered();

        var published = new List<string>();
        using var listener = MeterListening.StartForMeter(
            LatticeMetrics.Meter,
            GaugeNames,
            l => l.SetMeasurementEventCallback<long>(
                (instrument, _, _, _) => published.Add(instrument.Name)));

        listener.RecordObservableInstruments();

        Assert.Multiple(() =>
        {
            foreach (var name in GaugeNames)
            {
                Assert.That(
                    published.Count(n => string.Equals(n, name, StringComparison.Ordinal)),
                    Is.EqualTo(1),
                    $"{name} published more than once; instruments cannot be unregistered, so a duplicate series is permanent");
            }
        });
    }

    /// <summary>
    /// Pins the other half of eagerness: that <c>AddLattice</c> actually calls
    /// <c>EnsureRegistered</c>.
    /// </summary>
    /// <remarks>
    /// This is a source scan rather than a runtime assertion, and deliberately.
    /// Registration is process-wide and idempotent, so by the time any test can
    /// observe it, some earlier test in the same process has almost certainly
    /// already performed it - including, in this very fixture,
    /// <see cref="Scrape"/>, which calls <c>EnsureRegistered</c> itself so that
    /// the projection tests do not depend on run order. That makes a runtime
    /// assertion on "the host registered it" structurally unable to fail, which
    /// is a vacuous assertion rather than a weak one: deleting the
    /// <c>AddLattice</c> hook entirely would redden nothing at all.
    /// </remarks>
    [Test]
    public void AddLattice_registers_the_residency_gauges_at_silo_build()
    {
        var path = Path.Combine(
            HygieneRepository.FindRepoRoot(), "src", "lattice", "LatticeServiceCollectionExtensions.cs");

        Assert.That(File.Exists(path), Is.True, $"expected the registration seam at {path}");

        Assert.That(
            File.ReadAllText(path),
            Does.Contain("LeafResidencyMetrics.EnsureRegistered()"),
            "AddLattice no longer registers the residency gauges, so a silo that has not yet "
            + "activated a leaf publishes no residency series at all - the absent-versus-zero "
            + "ambiguity this change exists to remove, reintroduced inside its own fix");
    }

    [Test]
    public void The_gauges_observe_the_shared_working_set_the_activation_path_uses()
    {
        // The gauge callbacks read LeafResidentWorkingSet.Shared. The leaf
        // activation path resolves its working set from the activation's
        // services and falls back to Shared, and nothing in the library
        // registers that service, so production always uses Shared.
        //
        // That is an invariant, not a guarantee. Were the callbacks repointed at
        // some other instance, every other test here would still pass while the
        // deployed gauges described an object nobody uses - a silent wrong zero,
        // which is the exact failure class this whole change exists to remove.
        // So the coupling is pinned directly rather than assumed.
        var before = Scrape()[LatticeMetrics.LeafResidencyRegistrationsName];

        var registration = LeafResidentWorkingSet.Shared.Register(
            "residency-gauge-coupling", 1024L, snapshotBanked: true, shed: () => { });

        try
        {
            Assert.That(
                Scrape()[LatticeMetrics.LeafResidencyRegistrationsName],
                Is.EqualTo(before + 1),
                "the gauges do not observe the working set the activation path resolves");
        }
        finally
        {
            registration.Dispose();
        }
    }
}
