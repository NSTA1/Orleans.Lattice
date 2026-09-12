using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for issue 2585: a range-scan page fill that hits its hard
/// <see cref="LatticeOptions.MaxScanPageStallDuration"/> ceiling used to throw
/// away every row it had already read.
/// <para>
/// That made the ceiling a livelock rather than a bound. The retry the fault
/// invites re-walks the same leaves, hits the same ceiling, and discards the
/// same work, so a page that cannot fill in one attempt can never fill in any
/// number of them. The field shape was a stall reported after four leaves - four
/// leaf reads genuinely completed, then all four thrown away.
/// </para>
/// <para>
/// The fix banks what the walk had accumulated as an ordinary short page with
/// <see cref="EntriesPage.HasMore"/> set, which is a shape every caller already
/// handles (the cooperative <see cref="LatticeOptions.MaxScanPageDuration"/>
/// budget emits it today), so the caller resumes from the last row and the next
/// attempt starts strictly further along.
/// </para>
/// <para>
/// The bank is deliberately gated on having read <em>at least one row</em>. A
/// page banked empty carries no continuation a caller could advance past -
/// every cursor in <c>LatticeGrain</c> treats "no rows and no resume key" as the
/// end of the scan - so banking one would silently truncate the caller's scan
/// instead of curing the livelock. That gate is what the negative tests here
/// pin down.
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainScanPagePartialBankingTests
{
    private const string TreeId = "partial-bank-tree";
    private const string ShardKey = TreeId + "/0";

    /// <summary>
    /// A leaf chain that can serve only a bounded number of <em>productive</em>
    /// leaf reads per grain call before the next read parks forever.
    /// <para>
    /// A productive read is one that returns at least one row after the leaf
    /// has applied the caller's continuation token, which is what the shard
    /// pays for: the leaf filters at source, so a leaf entirely behind the
    /// continuation costs a round trip and no serialized values. Modelling the
    /// budget on productive reads is therefore what makes successive attempts
    /// comparable - each one gets the same allowance and spends it on new
    /// ground.
    /// </para>
    /// <para>
    /// The park is a task that never completes, which is the exact shape the
    /// cooperative budget is structurally unable to interrupt and the only
    /// shape the hard ceiling exists for.
    /// </para>
    /// </summary>
    private sealed class BudgetedChain
    {
        private readonly TaskCompletionSource<List<KeyValuePair<string, byte[]>>> _parkedEntries =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<List<string>> _parkedKeys =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private int _productiveReads;
        private int _sterileReads;

        public required ShardRootGrain Grain { get; init; }

        /// <summary>Productive leaf reads allowed before the next read parks.</summary>
        public int ProductiveReadBudget { get; set; } = int.MaxValue;

        /// <summary>
        /// Sterile leaf reads - ones whose rows are all filtered out by the
        /// caller's range - allowed before the next read parks. Separate from
        /// <see cref="ProductiveReadBudget"/> so a fixture can drive the walk
        /// into a stall having genuinely visited leaves and collected nothing,
        /// which is the case the empty-page gate exists for.
        /// </summary>
        public int SterileReadBudget { get; set; } = int.MaxValue;

        /// <summary>Productive reads served since the last <see cref="BeginAttempt"/>.</summary>
        public int ProductiveReads => _productiveReads;

        /// <summary>Whether a leaf read is still parked, unanswered.</summary>
        public bool IsParked => !_parkedEntries.Task.IsCompleted || !_parkedKeys.Task.IsCompleted;

        public void BeginAttempt()
        {
            _productiveReads = 0;
            _sterileReads = 0;
        }

        /// <summary>
        /// Releases both parks so an abandoned walk can drain without leaving an
        /// unobserved task fault behind.
        /// </summary>
        public void Drain()
        {
            _parkedEntries.TrySetResult([]);
            _parkedKeys.TrySetResult([]);
        }

        internal Task<List<KeyValuePair<string, byte[]>>> ReadEntries(
            List<KeyValuePair<string, byte[]>> visible)
        {
            if (visible.Count == 0)
            {
                if (_sterileReads >= SterileReadBudget)
                {
                    return _parkedEntries.Task;
                }

                _sterileReads++;
                return Task.FromResult(new List<KeyValuePair<string, byte[]>>());
            }

            if (_productiveReads >= ProductiveReadBudget)
            {
                return _parkedEntries.Task;
            }

            _productiveReads++;
            return Task.FromResult(visible);
        }

        internal Task<List<string>> ReadKeys(List<string> visible)
        {
            if (visible.Count == 0)
            {
                if (_sterileReads >= SterileReadBudget)
                {
                    return _parkedKeys.Task;
                }

                _sterileReads++;
                return Task.FromResult(new List<string>());
            }

            if (_productiveReads >= ProductiveReadBudget)
            {
                return _parkedKeys.Task;
            }

            _productiveReads++;
            return Task.FromResult(visible);
        }
    }

    /// <summary>
    /// Builds a chain of <paramref name="leafCount"/> single-row leaves whose
    /// keys ascend as <c>k0000</c>, <c>k0001</c>, and so on, wired to a
    /// <see cref="BudgetedChain"/>. The leaves honour the caller's
    /// <c>afterExclusive</c> continuation token so a resumed attempt sees only
    /// new ground, which is what makes monotonic progress observable rather
    /// than assumed.
    /// </summary>
    private static BudgetedChain CreateBudgetedChain(
        TimeSpan stallDuration,
        int leafCount = 16,
        int rowsPerLeaf = 1,
        Action<ShardRootState>? configureState = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var ids = new GrainId[leafCount];
        for (var i = 0; i < leafCount; i++)
        {
            ids[i] = GrainId.Create("leaf", $"leaf{i}");
        }

        state.State.RootNodeId = ids[0];
        state.State.RootIsLeaf = true;
        configureState?.Invoke(state.State);

        var factory = Substitute.For<IGrainFactory>();
        BudgetedChain? chain = null;

        for (var i = 0; i < leafCount; i++)
        {
            var index = i;
            var rows = new List<KeyValuePair<string, byte[]>>();
            for (var r = 0; r < rowsPerLeaf; r++)
            {
                rows.Add(new KeyValuePair<string, byte[]>(
                    $"k{((index * rowsPerLeaf) + r):D4}", [(byte)index]));
            }

            var leaf = Substitute.For<IBPlusLeafGrain>();

            leaf.GetEntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                    Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
                .Returns(call => chain!.ReadEntries(
                    Visible(rows, call.ArgAt<string?>(0), call.ArgAt<string?>(2))));

            leaf.GetKeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(),
                    Arg.Any<string?>(), Arg.Any<LatticePredicateNode?>())
                .Returns(call => chain!.ReadKeys(
                    Visible(rows, call.ArgAt<string?>(0), call.ArgAt<string?>(2))
                        .Select(e => e.Key).ToList()));

            leaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
            {
                LowKeyInclusive = rows[0].Key,
                HighKeyExclusive = null,
            }));
            leaf.GetNextSiblingAsync().Returns(Task.FromResult(
                index + 1 < leafCount ? (GrainId?)ids[index + 1] : null));
            leaf.GetPrevSiblingAsync().Returns(Task.FromResult(
                index > 0 ? (GrainId?)ids[index - 1] : null));
            factory.GetGrain<IBPlusLeafGrain>(ids[index]).Returns(leaf);
        }

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                MaxLeavesPerScanPage = 4096,
                // The cooperative budget is deliberately disabled: this fixture
                // asserts what only the hard ceiling can do.
                MaxScanPageDuration = TimeSpan.Zero,
                MaxScanPageStallDuration = stallDuration,
            },
            shardCount: 1,
            factory: factory);

        chain = new BudgetedChain
        {
            Grain = new ShardRootGrain(context, state, factory, optionsResolver,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
                TestMutationObservers.NoObservers()),
        };
        return chain;
    }

    private static List<KeyValuePair<string, byte[]>> Visible(
        List<KeyValuePair<string, byte[]>> rows,
        string? startInclusive,
        string? afterExclusive)
    {
        var visible = new List<KeyValuePair<string, byte[]>>();
        foreach (var row in rows)
        {
            if (startInclusive is not null
                && string.CompareOrdinal(row.Key, startInclusive) < 0)
            {
                continue;
            }

            if (afterExclusive is not null
                && string.CompareOrdinal(row.Key, afterExclusive) <= 0)
            {
                continue;
            }

            visible.Add(row);
        }

        return visible;
    }

    /// <summary>
    /// The headline behaviour, in the exact shape the field reported: four leaf
    /// reads completed and the fifth parked. Before issue 2585 this threw and
    /// the four rows were lost.
    /// </summary>
    [Test]
    public async Task A_ceiling_that_fires_after_four_leaves_banks_the_four_rows_it_read()
    {
        var chain = CreateBudgetedChain(TimeSpan.FromMilliseconds(250));
        chain.ProductiveReadBudget = 4;
        chain.BeginAttempt();

        var page = await chain.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Has.Count.EqualTo(4),
                "the four leaf reads that completed must be banked, not discarded");
            Assert.That(page.Entries.Select(e => e.Key),
                Is.EqualTo(new[] { "k0000", "k0001", "k0002", "k0003" }),
                "the banked rows must be the ones actually read, in key order");
            Assert.That(page.HasMore, Is.True,
                "a banked page is short, so a caller that stops here truncates the scan");
        });

        Assert.That(chain.IsParked, Is.True,
            "the ceiling must have banked a page while a leaf read was genuinely in flight - "
            + "the shape the cooperative budget cannot interrupt");

        chain.Drain();
        await Task.Yield();
    }

    /// <summary>
    /// The acceptance criterion for issue 2585, and the one that discriminates:
    /// it asserts a <em>climbing</em> banked count across successive attempts,
    /// not merely that no exception was thrown. A fix that returned an empty
    /// page every time would satisfy "no exception" and still be the livelock.
    /// <para>
    /// Each attempt gets the same four-productive-read allowance and resumes
    /// from the previous page's last key, so a shard that banks nothing makes a
    /// cumulative count that never moves, and one that banks its partial work
    /// makes a count that climbs 4, 8, 12.
    /// </para>
    /// </summary>
    [Test]
    public async Task Successive_attempts_bank_strictly_more_rows_than_their_predecessor()
    {
        var chain = CreateBudgetedChain(TimeSpan.FromMilliseconds(250), leafCount: 32);
        chain.ProductiveReadBudget = 4;

        var banked = new List<string>();
        var cumulative = new List<int>();
        string? continuation = null;

        for (var attempt = 0; attempt < 3; attempt++)
        {
            chain.BeginAttempt();
            var page = await chain.Grain.GetSortedEntriesBatchAsync(
                startInclusive: null, endExclusive: null, pageSize: 64,
                continuationToken: continuation);

            banked.AddRange(page.Entries.Select(e => e.Key));
            cumulative.Add(banked.Count);

            Assert.That(page.HasMore, Is.True, $"attempt {attempt} must report more to come");
            Assert.That(page.Entries, Is.Not.Empty,
                $"attempt {attempt} banked nothing, so the caller has no token to advance past "
                + "and the next attempt repeats this one exactly - the livelock");

            // This is what a caller does with a short page carrying no resume
            // boundary, in LatticeGrain.EntriesCursor and DrainEntrySlotsToBuffer alike.
            continuation = page.Entries[^1].Key;
        }

        Assert.Multiple(() =>
        {
            Assert.That(cumulative, Is.EqualTo(new[] { 4, 8, 12 }),
                "the banked total must climb by a full allowance per attempt");
            Assert.That(banked, Is.Unique,
                "a resumed attempt must start beyond the previous page, not re-serve it");
            Assert.That(banked, Is.Ordered.Using<string>(StringComparer.Ordinal),
                "monotonic progress means strictly forward, so the merged stream stays sorted");
        });

        chain.Drain();
        await Task.Yield();
    }

    /// <summary>
    /// The paired negative whose expected value is zero, and the reason it is
    /// here: without it, a "banked count climbed" assertion cannot tell a
    /// working bank from a check that fires unconditionally.
    /// <para>
    /// A ceiling that fires before any row has been read has nothing to bank,
    /// so it must still fault. Banking an empty page here would be strictly
    /// worse than the fault it replaced: a caller reading no rows and no resume
    /// boundary ends its scan, so a wedged prologue would silently return an
    /// empty result set instead of an error.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_ceiling_that_fires_before_any_row_is_read_banks_nothing_and_still_faults()
    {
        var chain = CreateBudgetedChain(TimeSpan.FromMilliseconds(250));
        chain.ProductiveReadBudget = 0;
        chain.BeginAttempt();

        var ex = Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await chain.Grain.GetSortedEntriesBatchAsync(
                startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.LeavesVisited, Is.Zero, "no leaf read completed, so nothing was read");
            Assert.That(ex.Phase, Is.EqualTo("leaf-walk"));
        });

        chain.Drain();
        await Task.Yield();
    }

    /// <summary>
    /// The other half of the empty-page gate, and the subtler one. Here leaves
    /// <em>were</em> read - the walk paid for four round trips - but every row
    /// they held was filtered out by the caller's range, so the page has no last
    /// key to resume from. Banking it would report "more to come" with no way to
    /// ask for more, and every cursor in <c>LatticeGrain</c> reads that as the
    /// end of the scan.
    /// <para>
    /// So the gate is rows read, not leaves visited. A fix that keyed on leaves
    /// would pass every other test in this fixture and silently truncate a real
    /// scan over a sparse range.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_ceiling_that_read_only_filtered_rows_banks_nothing_and_still_faults()
    {
        var chain = CreateBudgetedChain(TimeSpan.FromMilliseconds(250), leafCount: 16);
        // Every leaf's rows sort below this start key, so each read returns
        // empty: leaves genuinely visited, nothing collected. The fifth such
        // read parks, so the ceiling fires with LeavesVisited above zero and an
        // empty accumulator.
        chain.SterileReadBudget = 4;
        chain.BeginAttempt();

        var ex = Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await chain.Grain.GetSortedEntriesBatchAsync(
                startInclusive: "z", endExclusive: null, pageSize: 64, continuationToken: null));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.LeavesVisited, Is.EqualTo(4),
                "leaves were genuinely read - this is not the no-leaf-read case");
            Assert.That(ex.Phase, Is.EqualTo("leaf-walk"));
        });

        chain.Drain();
        await Task.Yield();
    }

    /// <summary>
    /// Parity for the keys page fill, which carries the same
    /// <see cref="KeysPage.HasMore"/> and <see cref="KeysPage.ResumeFromKey"/>
    /// contract and the same cursor on the calling side.
    /// </summary>
    [Test]
    public async Task A_keys_page_banks_the_keys_it_read_when_the_ceiling_fires()
    {
        var chain = CreateBudgetedChain(TimeSpan.FromMilliseconds(250));
        chain.ProductiveReadBudget = 3;
        chain.BeginAttempt();

        var page = await chain.Grain.GetSortedKeysBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Keys, Is.EqualTo(new[] { "k0000", "k0001", "k0002" }));
            Assert.That(page.HasMore, Is.True);
        });

        chain.Drain();
        await Task.Yield();
    }

    /// <summary>
    /// A banked page must carry the moved-away slots the walk collected on its
    /// way. A strongly consistent scan uses
    /// <see cref="EntriesPage.MovedAwaySlots"/> to re-fetch those slots from the
    /// split's new owner, so dropping them from a banked page would not raise an
    /// error anywhere - it would just lose rows, silently, exactly in the ranges
    /// an adaptive split is rebalancing.
    /// </summary>
    [Test]
    public async Task A_banked_page_reports_the_moved_away_slots_it_filtered()
    {
        // Slot every key to the same virtual slot so the first rows the walk
        // reads are filtered as moved away and reported rather than returned.
        var movedKey = "k0000";
        int movedSlot = 0;
        var chain = CreateBudgetedChain(
            TimeSpan.FromMilliseconds(250),
            leafCount: 16,
            configureState: s =>
            {
                s.MovedAwayVirtualShardCount = 64;
                movedSlot = ShardMap.GetVirtualSlot(movedKey, 64);
                s.MovedAwaySlots[movedSlot] = 1;
            });
        chain.ProductiveReadBudget = 4;
        chain.BeginAttempt();

        var page = await chain.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Not.Empty, "rows outside the moved slot must still bank");
            Assert.That(page.Entries.Select(e => e.Key), Does.Not.Contain(movedKey),
                "a moved-away row must be filtered, not returned by the old owner");
            Assert.That(page.MovedAwaySlots, Is.Not.Null.And.Contains(movedSlot),
                "a banked page that drops the moved slot loses those rows for good: the "
                + "orchestrator never learns to ask the new owner for them");
        });

        chain.Drain();
        await Task.Yield();
    }

    /// <summary>
    /// The instrumentation contract. Both arms are emitted from the one site a
    /// ceiling fire passes through, so <c>banked + discarded</c> is the count of
    /// ceiling fires and a reader never has to supply the denominator.
    /// <para>
    /// This is what makes a zero readable. A <c>banked</c> arm reading zero
    /// beside a non-zero <c>discarded</c> arm is a measured negative - ceilings
    /// fired and none of them found bankable work. The series being absent
    /// altogether while stalls are being counted is broken wiring, and the two
    /// are no longer confusable.
    /// </para>
    /// </summary>
    [Test]
    public async Task Both_outcome_arms_are_measured_so_a_zero_is_readable()
    {
        var outcomes = new List<string>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.ScanPageCeilingOutcomes,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagOutcome && tag.Value is string outcome)
                    {
                        outcomes.Add(outcome);
                    }
                }
            }));

        var banking = CreateBudgetedChain(TimeSpan.FromMilliseconds(250));
        banking.ProductiveReadBudget = 2;
        banking.BeginAttempt();
        await banking.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);

        var discarding = CreateBudgetedChain(TimeSpan.FromMilliseconds(250));
        discarding.ProductiveReadBudget = 0;
        discarding.BeginAttempt();
        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            await discarding.Grain.GetSortedEntriesBatchAsync(
                startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null));

        Assert.Multiple(() =>
        {
            Assert.That(outcomes.Count(o => o == "banked"), Is.EqualTo(1),
                "the ceiling fire that had rows to bank must be counted as banked");
            Assert.That(outcomes.Count(o => o == "discarded"), Is.EqualTo(1),
                "the complement must be emitted from the same site, or a zero banked arm "
                + "cannot be told apart from a series that was never wired up");
        });

        banking.Drain();
        discarding.Drain();
        await Task.Yield();
    }
}
