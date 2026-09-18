using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

// Per-clause regression tests for the WAL GC trim-stop reason instrument
// (issue #3155).
//
// The sibling file covers the boundary between the offset floor and the HLC
// eligibility predicate. This one covers the boundary INSIDE that predicate.
// WalGcTrimCore.ClassifyEntry refuses an entry by exactly one of three
// independent clauses - the consumer-cursor / TTL clause, the causal-stable
// clause, and the buffer-pin clause - and until this change all three were
// reported as a single not_eligible arm.
//
// That collapse had a measured cost rather than a theoretical one. Three trees
// in a live container stopped every scan on not_eligible, reclaimed nothing
// across an entire observation window, and the running system could not say
// whether it was looking at a parked consumer cursor, a stalled replication
// origin, or a held buffer pin. Those demand different investigations and have
// nothing in common beyond stopping the same scan.
//
// The load-bearing property is therefore DISCRIMINATION, exactly as it is in
// the sibling file: each test asserts its own arm advanced AND that the two
// sibling clause arms stayed at a measured zero, so a build that collapsed the
// classification back onto one arm cannot pass.
public sealed partial class LatticeWalGcTrimStopReasonTests
{
    private const string BufferingConsumer = "buffering-receiver";
    private const string Origin = "site-a";

    /// <summary>The three clause arms, which are the population these tests discriminate between.</summary>
    private static readonly string[] ClauseArms = ["cursor_floor", "causal_frontier", "block_pin"];

    /// <summary>
    /// A WAL entry carrying an explicit per-origin version vector, so the
    /// causal-stable clause has something to fail to dominate. The vector-less
    /// <c>Entry</c> helper cannot drive that clause at all: a null entry vector is
    /// the empty VC and is dominated by every non-null frontier by design.
    /// </summary>
    private static WalEntry VectorEntry(long offset, HybridLogicalClock ts, HybridLogicalClock originClock) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = $"k{offset}",
            Value = new byte[] { 1 },
            Timestamp = ts,
            OriginClusterId = Origin,
            VectorClock = Vector(originClock),
        },
    };

    private static VersionVector Vector(HybridLogicalClock clock)
    {
        var vector = new VersionVector();
        vector.Entries[Origin] = clock;
        return vector;
    }

    /// <summary>
    /// Builds a collector whose offset floor is deliberately generous (checkpoint
    /// offset 10, above every offset these tests seed) so the offset-floor arm can
    /// never fire and the only thing that can stop a scan is the clause under
    /// test.
    /// <para>
    /// Every parameter is optional and defaults to "this clause is not armed", so
    /// the three fixtures below differ from each other in exactly one input. A
    /// difference in the reported arm is therefore attributable to that input
    /// rather than to harness drift between three hand-built collectors.
    /// </para>
    /// </summary>
    private static async Task<LatticeWalGc> ClauseCollectorAsync(
        InMemoryWalStorageProvider provider,
        VersionVector? causalFrontier = null,
        HybridLogicalClock? blockedAt = null)
    {
        var registry = new InMemoryWalCursorRegistry();

        if (causalFrontier is null)
        {
            await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        }
        else
        {
            await registry.ReportCursorAsync(Tree, "shipper", Hlc(30), causalFrontier);
        }

        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));

        if (blockedAt is not null)
        {
            // A blocked-floor-only registration: cursor Zero keeps this consumer
            // out of the cursor floor entirely, so the only thing it contributes
            // is the buffer pin.
            await registry.ReportCursorAsync(Tree, BufferingConsumer, HybridLogicalClock.Zero, blockedAt);
        }

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(20),
        };
        var durableOffsets = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            [LeafConsumer] = 10,
        };

        return new LatticeWalGc(
            Services(provider, durablePins, durableOffsets), registry, Monitor());
    }

    /// <summary>
    /// Asserts that <paramref name="expected"/> is the only clause arm that
    /// advanced, and that the other two carry a measured zero rather than being
    /// absent. Absence and zero are different observations here, and priming is
    /// what makes the second one available.
    /// </summary>
    private static void AssertOnlyClauseArm(IReadOnlyList<Stop> stops, string expected)
    {
        Assert.That(Advanced(stops), Is.EqualTo(new[] { expected }),
            $"The scan was stopped by the {expected} clause, so that must be the only arm that advanced. "
            + "Reporting any other clause here sends the diagnosis to a subsystem that is not holding the WAL.");

        foreach (var arm in ClauseArms.Where(a => !string.Equals(a, expected, StringComparison.Ordinal)))
        {
            var series = stops.Where(s => string.Equals(s.Reason, arm, StringComparison.Ordinal)).ToList();

            Assert.That(series, Is.Not.Empty,
                $"The {arm} arm carries no series at all, so a reader cannot tell a clause that did not fire "
                + "from a silo that is not running WAL GC. Zero-priming is what separates the two.");
            Assert.That(series.Sum(static s => s.Value), Is.Zero,
                $"The {arm} arm advanced on a pass that was stopped by the {expected} clause. Two clauses "
                + "reported under one arm is the collapse this split exists to remove.");
        }
    }

    [Test]
    public async Task RunOnceAsync_scan_stopped_by_the_cursor_clause_reports_cursor_floor()
    {
        // Clause A. Neither the consumer cursor (20) nor a TTL ceiling (none
        // configured) accepts an entry stamped at 100, so the HLC clause refuses
        // the very first entry the scan examines.
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree, 0, new[] { Entry(0, Hlc(100)) }, CancellationToken.None);

        var sut = await ClauseCollectorAsync(provider);

        var (report, stops) = await RunAsync(sut);

        Assert.That(report.EntriesTrimmed, Is.Zero);
        AssertOnlyClauseArm(stops, "cursor_floor");
    }

    [Test]
    public async Task RunOnceAsync_scan_stopped_by_the_causal_clause_reports_causal_frontier()
    {
        // Clause B, and the case that is most easily mistaken for clause A. The
        // entry is stamped at 10 and the cursor floor is 20, so the HLC clause
        // accepts it outright - the consumer is comfortably ahead. What refuses
        // it is the reported per-origin frontier, which sits at 5 and therefore
        // does not dominate the entry's vector at 50. A reader sent to the
        // consumer cursor by a collapsed arm would find it healthy and clear the
        // wrong subsystem.
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree, 0, new[] { VectorEntry(0, Hlc(10), Hlc(50)) }, CancellationToken.None);

        var sut = await ClauseCollectorAsync(provider, causalFrontier: Vector(Hlc(5)));

        var (report, stops) = await RunAsync(sut);

        Assert.That(report.EntriesTrimmed, Is.Zero);
        AssertOnlyClauseArm(stops, "causal_frontier");
    }

    [Test]
    public async Task RunOnceAsync_scan_stopped_by_the_buffer_pin_clause_reports_block_pin()
    {
        // Clause C. The entry is stamped at 10, below the cursor floor of 20 and
        // carrying no vector, so clauses A and B both accept it. It is held only
        // because a buffering receiver published a pin at 5 and the clause is
        // at-or-above. This is a deliberate hold rather than a lag, which is why
        // it earns an arm of its own: the question it raises is whether the pin
        // is still live, not whether some cursor is moving.
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree, 0, new[] { Entry(0, Hlc(10)) }, CancellationToken.None);

        var sut = await ClauseCollectorAsync(provider, blockedAt: Hlc(5));

        var (report, stops) = await RunAsync(sut);

        Assert.That(report.EntriesTrimmed, Is.Zero);
        AssertOnlyClauseArm(stops, "block_pin");
    }

    [Test]
    public async Task RunOnceAsync_reports_a_different_arm_for_each_clause_that_can_refuse_an_entry()
    {
        // The property the split exists for, asserted directly rather than left
        // to be inferred from three tests passing. All three passes reclaim zero
        // and all three were reported identically before this change, so the
        // distinctness is the whole deliverable.
        var cursorHeld = new InMemoryWalStorageProvider();
        await cursorHeld.AppendBatchAsync(Tree, 0, new[] { Entry(0, Hlc(100)) }, CancellationToken.None);

        var causalHeld = new InMemoryWalStorageProvider();
        await causalHeld.AppendBatchAsync(
            Tree, 0, new[] { VectorEntry(0, Hlc(10), Hlc(50)) }, CancellationToken.None);

        var pinHeld = new InMemoryWalStorageProvider();
        await pinHeld.AppendBatchAsync(Tree, 0, new[] { Entry(0, Hlc(10)) }, CancellationToken.None);

        var (cursorReport, cursorStops) = await RunAsync(await ClauseCollectorAsync(cursorHeld));
        var (causalReport, causalStops) = await RunAsync(
            await ClauseCollectorAsync(causalHeld, causalFrontier: Vector(Hlc(5))));
        var (pinReport, pinStops) = await RunAsync(await ClauseCollectorAsync(pinHeld, blockedAt: Hlc(5)));

        Assert.Multiple(() =>
        {
            Assert.That(cursorReport.EntriesTrimmed, Is.Zero);
            Assert.That(causalReport.EntriesTrimmed, Is.Zero);
            Assert.That(pinReport.EntriesTrimmed, Is.Zero);
        });

        var reported = new[] { Advanced(cursorStops), Advanced(causalStops), Advanced(pinStops) }
            .Select(static arms => string.Join("+", arms))
            .ToList();

        Assert.That(reported.Distinct(StringComparer.Ordinal).Count(), Is.EqualTo(3),
            "Three passes that each reclaimed nothing for a different reason landed on fewer than three "
            + $"arms ({string.Join(", ", reported)}). That collapse is the defect this split removes: the "
            + "tree is observably stranded and its holder is unnameable.");
    }
}
