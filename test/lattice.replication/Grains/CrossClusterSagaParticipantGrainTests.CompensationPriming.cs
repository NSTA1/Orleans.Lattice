using System.Diagnostics.Metrics;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Issue #2918: the <c>cause</c> taxonomy on
/// <c>orleans_lattice_replication_saga_compensations_total</c> must be readable
/// at zero.
/// <para>
/// <b>What was wrong.</b> The counter carries a bounded two-member domain and
/// the only writes were the two increments, in two different methods. A cluster
/// that had never lost a coordinator published no
/// <c>cause="coordinator-loss"</c> series at all, which scrapes identically to a
/// build in which the fence-expiry path was deleted, and identically again to
/// one where the counter is unwired. The reading an operator wants - "no
/// coordinator has ever been lost" - was therefore unobtainable from the
/// absence, and the absence is the only thing a healthy cluster produces.
/// </para>
/// <para>
/// <b>Why prepare is the seam.</b> Both arms are reachable only from
/// <see cref="SagaPhase.Prepared"/>: <c>vote-abort</c> from <c>AbortAsync</c> and
/// <c>coordinator-loss</c> from the fence-expiry reminder. Prepare is therefore
/// exactly the population that can arm either, and it is the same execution path
/// as the armed value rather than a constructor, which would prove only that the
/// type loaded. The grain extends <c>TtlGrain</c> and declares no
/// <c>OnActivateAsync</c>, so its entry point is the seam available.
/// </para>
/// </summary>
public partial class CrossClusterSagaParticipantGrainTests
{
    /// <summary>
    /// The compensation causes the grain arms, read from the metrics class
    /// rather than duplicated as literals, so a renamed constant is a build
    /// break here instead of a silently uncovered arm.
    /// </summary>
    private static readonly string[] CompensationCauses =
    [
        LatticeReplicationMetrics.SagaCauseVoteAbort,
        LatticeReplicationMetrics.SagaCauseCoordinatorLoss,
    ];

    private static (MeterListener Listener, Dictionary<string, long> Totals, Dictionary<string, int> Counts)
        ListenForCompensationCauses()
    {
        var totals = new Dictionary<string, long>(StringComparer.Ordinal);
        var counts = new Dictionary<string, int>(StringComparer.Ordinal);
        var listener = MeterListening.StartForInstrument(
            LatticeReplicationMetrics.SagaCompensations,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                string? cause = null;

                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeReplicationMetrics.TagCause && tag.Value is string arm)
                    {
                        cause = arm;
                    }
                }

                if (cause is null)
                {
                    return;
                }

                lock (totals)
                {
                    totals[cause] = totals.TryGetValue(cause, out var running) ? running + value : value;
                    counts[cause] = counts.TryGetValue(cause, out var seen) ? seen + 1 : 1;
                }
            }));

        return (listener, totals, counts);
    }

    /// <summary>
    /// A successful prepare, which compensates nothing, must still publish both
    /// compensation causes at zero. Reverting the prime block at the top of
    /// <c>PrepareAsync</c> reddens this: both arms become absent rather than
    /// zero.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task A_prepare_primes_both_compensation_causes_at_zero()
    {
        var (grain, _, _) = CreateGrain([new RecordingSagaParticipant()]);
        var (listener, totals, _) = ListenForCompensationCauses();

        var response = await grain.PrepareAsync(Request());

        listener.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(response.Phase, Is.EqualTo(SagaPhase.Prepared),
                "the prepare must actually succeed, or this fixture characterises a path it never ran");

            Assert.That(totals.Keys, Is.EquivalentTo(CompensationCauses),
                "both declared compensation causes must be published, so that an absent series means the "
                + "build does not carry the instrument and nothing else (issue #2918).");

            foreach (var cause in CompensationCauses)
            {
                Assert.That(totals.GetValueOrDefault(cause, -1), Is.Zero,
                    $"the '{cause}' arm must be primed at zero, not incremented: nothing was compensated, so "
                    + "a non-zero total would mean this assertion passes for the wrong reason.");
            }
        });
    }

    /// <summary>
    /// A duplicate prepare, which returns at the idempotent re-attach guard,
    /// must still publish both causes at zero.
    /// <para>
    /// This is the arm that pins the prime's <em>position</em> rather than its
    /// existence. Moving the block below the guard leaves the test above green -
    /// a first prepare runs straight past it - while the re-attach path publishes
    /// nothing, which is exactly the unreachable-prime failure the fix exists to
    /// avoid. Only a scenario that takes the early return can tell the two
    /// placements apart.
    /// </para>
    /// <para>
    /// The observable is the measurement <em>count</em> per arm across a window
    /// spanning both prepares, not the running total: both prepares emit zero, so
    /// a total cannot distinguish one prime from two and the assertion would be
    /// satisfied by a prime that only the first prepare reaches. Counting is the
    /// only observable with a dose-response here. The window also starts before
    /// the first prepare deliberately - a listener attached midway through the
    /// scenario is a harness whose zeros are unreadable for the same reason the
    /// instrument's were.
    /// </para>
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task A_duplicate_prepare_that_returns_early_still_primes_both_causes()
    {
        var (grain, _, _) = CreateGrain([new RecordingSagaParticipant()]);
        var (listener, totals, counts) = ListenForCompensationCauses();

        var first = await grain.PrepareAsync(Request());
        var repeat = await grain.PrepareAsync(Request());

        listener.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(first.Phase, Is.EqualTo(SagaPhase.Prepared),
                "the first prepare must succeed, or the second is not a duplicate and this fixture is not "
                + "exercising the early-return path it exists to cover");

            Assert.That(repeat.Phase, Is.EqualTo(SagaPhase.Prepared),
                "the duplicate prepare must take the idempotent re-attach return");

            foreach (var cause in CompensationCauses)
            {
                Assert.That(counts.GetValueOrDefault(cause, 0), Is.EqualTo(2),
                    $"'{cause}' must be primed once per prepare, including the duplicate that returns at the "
                    + "re-attach guard. Exactly one means the prime sits below that guard and is unreachable "
                    + "on precisely the path whose absence it is meant to make readable.");

                Assert.That(totals.GetValueOrDefault(cause, -1), Is.Zero,
                    $"'{cause}' must still sum to zero: neither prepare compensated anything.");
            }
        });
    }

    /// <summary>
    /// The positive control: a real compensation must be reported as a one on
    /// its own cause by this same listener, with the other cause still at zero.
    /// <para>
    /// Without it, the two tests above are satisfied equally by a harness that
    /// observes nothing at all, because a primed zero and an unobserved
    /// measurement are the same assertion. Driving a genuine abort through the
    /// same listener is what makes those zeros measured zeros.
    /// </para>
    /// <para>
    /// The window covers the prepare as well as the abort, so both arms are in
    /// scope and the contrast between them is a measurement rather than an
    /// artefact of when the listener started: <c>vote-abort</c> reads one
    /// (a primed zero plus the compensation) while <c>coordinator-loss</c> reads
    /// the primed zero alone.
    /// </para>
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task A_real_compensation_is_reported_as_one_on_its_cause_by_this_same_harness()
    {
        var (grain, _, _) = CreateGrain([new RecordingSagaParticipant()]);

        var (listener, totals, _) = ListenForCompensationCauses();

        await grain.PrepareAsync(Request());
        await grain.AbortAsync(Request());

        listener.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(totals.GetValueOrDefault(LatticeReplicationMetrics.SagaCauseVoteAbort, -1), Is.EqualTo(1),
                "this harness must observe a real compensation as a one. If it reports zero here, the zeros "
                + "the priming tests assert are 'the harness saw nothing' and those tests are vacuous.");

            Assert.That(totals.GetValueOrDefault(LatticeReplicationMetrics.SagaCauseCoordinatorLoss, -1), Is.Zero,
                "the cause that did not occur must still read zero, which is what shows the harness "
                + "attributes a measurement to the arm that produced it rather than to both.");
        });
    }
}
