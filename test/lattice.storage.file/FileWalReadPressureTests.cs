using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// Regression tests for issue #2742: the read path bounded how many bytes a
/// page totalled, and nothing else. Two things it did not bound are what
/// actually failed in the field.
/// <list type="number">
/// <item><description><b>The largest contiguous block.</b> Decoding an entry
/// allocated one <c>byte[PayloadLength]</c> and then let the deserializer
/// allocate again from it. On a heap at 94.7% occupancy the contiguous
/// request fails long before total memory runs out, because a nearly-full
/// heap is a fragmented one.</description></item>
/// <item><description><b>The page's cost relative to the memory that
/// remains.</b> A 16 MiB ceiling is a sensible page and an unaffordable one,
/// and which it is depends on the machine, not the log.</description></item>
/// </list>
/// <para>
/// The fixture drives both through <see cref="IWalReadPressureGovernor"/>, so
/// every arm is deterministic: pressure and allocation failure are scripted
/// rather than provoked, and no test depends on timing, wall-clock, or the
/// real GC. That matters more than usual here, because the only alternative
/// way to observe this behaviour is to actually exhaust a heap.
/// </para>
/// </summary>
[TestFixture]
public sealed class FileWalReadPressureTests
{
    private const string TreeId = "tree-read-pressure";

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private string _root = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "lattice-file-wal-pressure", Guid.NewGuid().ToString("N"));
        System.IO.Directory.CreateDirectory(_root);
    }

    [TearDown]
    public void TearDown()
    {
        try
        {
            if (System.IO.Directory.Exists(_root))
            {
                System.IO.Directory.Delete(_root, recursive: true);
            }
        }
        catch (IOException)
        {
            // Best-effort cleanup; a leaked temp directory does not fail the test.
        }
    }

    /// <summary>
    /// A governor a test drives directly: a fixed budget, and an allocation
    /// ceiling above which every request fails the way a fragmented heap
    /// fails a large contiguous request.
    /// </summary>
    private sealed class ScriptedGovernor : IWalReadPressureGovernor
    {
        internal long? Budget { get; set; }

        /// <summary>Largest allocation that succeeds; <c>null</c> means all do.</summary>
        internal int? LargestAffordableAllocation { get; set; }

        /// <summary>
        /// Number of leading allocations to fail outright, modelling a heap
        /// that cannot serve a whole page's worth of live payloads at once
        /// but can serve a smaller one a moment later. A fixed size ceiling
        /// cannot express that, because the failure is about what the page
        /// totals rather than about any one entry in it.
        /// </summary>
        internal int AllocationFailuresToInject { get; set; }

        internal List<int> AllocationRequests { get; } = new();

        public long NarrowBudget(long configuredMaxBytes) => Budget ?? configuredMaxBytes;

        public byte[] Allocate(int byteCount)
        {
            AllocationRequests.Add(byteCount);
            if (AllocationFailuresToInject > 0)
            {
                AllocationFailuresToInject--;
                throw new OutOfMemoryException("scripted: no block available for the page being built.");
            }

            if (LargestAffordableAllocation is { } ceiling && byteCount > ceiling)
            {
                throw new OutOfMemoryException(
                    $"scripted: {byteCount} bytes exceeds the affordable ceiling of {ceiling}.");
            }

            return new byte[byteCount];
        }
    }

    private FileWalStorageProvider CreateProvider(long maxReadBatchBytes, IWalReadPressureGovernor governor)
    {
        var options = Options.Create(new FileWalStorageOptions
        {
            RootDirectory = _root,
            FlushToDisk = false,
            MaxReadBatchBytes = maxReadBatchBytes,
        });
        return new FileWalStorageProvider(options, _serializer, governor);
    }

    private static WalEntry Entry(long offset, int valueBytes)
    {
        var value = new byte[valueBytes];
        value.AsSpan().Fill((byte)(offset & 0xFF));
        return new WalEntry
        {
            Offset = offset,
            Mutation = new LatticeMutation
            {
                TreeId = TreeId,
                Kind = MutationKind.Set,
                Key = "k" + offset.ToString(System.Globalization.CultureInfo.InvariantCulture),
                Value = value,
                Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                OriginClusterId = "site-a",
            },
        };
    }

    private static async Task AppendAsync(FileWalStorageProvider sut, int count, int valueBytes)
    {
        for (var i = 0; i < count; i++)
        {
            await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(i, valueBytes) }, CancellationToken.None);
        }
    }

    private static async Task<List<WalEntry>> PageAsync(FileWalStorageProvider sut, long fromExclusive, int maxEntries)
    {
        var page = new List<WalEntry>();
        await foreach (var entry in sut.ReadAsync(TreeId, 0, fromExclusive, maxEntries, CancellationToken.None))
        {
            page.Add(entry);
        }

        return page;
    }

    // ---------------------------------------------------------------------
    // Clause 1: the configured ceiling is narrowed by occupancy.
    // ---------------------------------------------------------------------

    [Test]
    public void Budget_is_unchanged_while_the_heap_has_room_to_work_in()
    {
        var configured = 16L * 1024 * 1024;
        var narrowed = GcWalReadPressureGovernor.NarrowCore(
            configured,
            totalAvailableBytes: 12L * 1024 * 1024 * 1024,
            memoryLoadBytes: (long)(12L * 1024 * 1024 * 1024 * 0.5));

        Assert.That(narrowed, Is.EqualTo(configured),
            "Narrowing a healthy host's reads would slow recovery for no benefit.");
    }

    [Test]
    public void Budget_collapses_to_the_floor_at_the_occupancy_that_was_actually_observed()
    {
        // The live figure: 11.37 GiB of a 12 GiB cap, 94.73%.
        var total = 12L * 1024 * 1024 * 1024;
        var narrowed = GcWalReadPressureGovernor.NarrowCore(
            16L * 1024 * 1024,
            totalAvailableBytes: total,
            memoryLoadBytes: (long)(total * 0.9473d));

        Assert.That(narrowed, Is.EqualTo(GcWalReadPressureGovernor.MinimumBudgetBytes),
            "At the occupancy that produced 1,763 OOMs the read budget must already be at its floor.");
    }

    [Test]
    public void Budget_response_is_driven_by_occupancy_not_by_absolute_headroom()
    {
        // Identical absolute headroom, opposite verdicts. A rule written
        // against headroom cannot tell these apart, and that is precisely why
        // headroom is the wrong variable: hundreds of leaves replay
        // concurrently, so the headroom each one sees is shared and about to
        // be spent by the others, while occupancy already reflects that.
        const long headroom = 640L * 1024 * 1024;
        const long configured = 16L * 1024 * 1024;

        const long tightTotal = 12L * 1024 * 1024 * 1024;
        var tight = GcWalReadPressureGovernor.NarrowCore(
            configured, tightTotal, tightTotal - headroom);

        const long roomyTotal = 1L * 1024 * 1024 * 1024;
        var roomy = GcWalReadPressureGovernor.NarrowCore(
            configured, roomyTotal, roomyTotal - headroom);

        Assert.That(tight, Is.EqualTo(GcWalReadPressureGovernor.MinimumBudgetBytes),
            "94.8% of a 12 GiB heap is the failing state, whatever the headroom reads as.");
        Assert.That(roomy, Is.EqualTo(configured),
            "37.5% of a 1 GiB heap is healthy, and the same headroom figure describes it.");
    }

    [Test]
    public void Budget_is_monotonically_non_increasing_in_occupancy()
    {
        const long total = 12L * 1024 * 1024 * 1024;
        var configured = 16L * 1024 * 1024;
        var previous = long.MaxValue;
        for (var percent = 0; percent <= 100; percent++)
        {
            var narrowed = GcWalReadPressureGovernor.NarrowCore(
                configured, total, (long)(total * (percent / 100d)));
            Assert.That(narrowed, Is.LessThanOrEqualTo(previous),
                $"Budget rose as occupancy went from {percent - 1}% to {percent}%.");
            Assert.That(narrowed, Is.InRange(1L, configured));
            previous = narrowed;
        }
    }

    [Test]
    public void Absent_a_memory_signal_the_configured_ceiling_is_used_unchanged()
    {
        // A host with no container limit and no heap hard limit reports zero
        // here. Inventing pressure from a missing signal would be a pure
        // regression on every such host.
        var configured = 16L * 1024 * 1024;
        Assert.That(
            GcWalReadPressureGovernor.NarrowCore(configured, totalAvailableBytes: 0L, memoryLoadBytes: 0L),
            Is.EqualTo(configured));
    }

    [Test]
    public void Narrowing_never_widens_a_ceiling_smaller_than_the_floor()
    {
        const long total = 12L * 1024 * 1024 * 1024;
        var configured = 4096L;
        var narrowed = GcWalReadPressureGovernor.NarrowCore(configured, total, (long)(total * 0.99d));

        Assert.That(narrowed, Is.EqualTo(configured),
            "The floor is a floor for the budget, never a licence to exceed what the operator configured.");
    }

    [Test]
    public void The_governor_narrows_work_but_never_declines_it_at_any_occupancy()
    {
        // THE ANTI-LIVELOCK INVARIANT (issue #2742, and the #2737 worker's
        // correction: "a repair gate must not key on the fault it repairs").
        //
        // This governor DOES key on the fault it repairs - it reads heap
        // occupancy, which is permanently true for exactly the population
        // that needs it. That is only safe because it narrows work rather
        // than declining it. A gate that declines while its predicate holds
        // can never clear that predicate and is a livelock by construction; a
        // gate that narrows still makes forward progress, and forward
        // progress is what clears the predicate (activation completes ->
        // cursor registers -> the WAL GC's Zero block pin is skipped -> the
        // WAL trims -> occupancy falls -> the budget widens back).
        //
        // So the load-bearing property is not the taper. It is that the
        // output is positive at EVERY input, including the impossible ones:
        // occupancy at exactly the wall, past the wall, and with a garbage
        // negative signal. If any of these returned zero the deployment would
        // stop reading, stop activating, and stay stopped.
        const long total = 12L * 1024 * 1024 * 1024;
        var configured = 16L * 1024 * 1024;

        long[] pathological =
        [
            total,                  // exactly at the wall
            total + 1,              // reported load above the cap
            total * 4,              // wildly above the cap
            long.MaxValue / 2,      // absurd but representable
            -1L,                    // a nonsense signal
        ];

        foreach (var load in pathological)
        {
            var narrowed = GcWalReadPressureGovernor.NarrowCore(configured, total, load);
            Assert.That(narrowed, Is.GreaterThan(0L),
                $"A memory load of {load} against a {total}-byte cap yielded a non-positive budget. "
                + "A zero budget reads nothing, so it never advances a checkpoint, never lets an "
                + "activation complete, and never lowers the pressure that produced it.");
            Assert.That(narrowed, Is.LessThanOrEqualTo(configured));
        }
    }

    [Test]
    public async Task A_page_still_yields_an_entry_when_the_budget_is_smaller_than_that_entry()
    {
        // The second half of the anti-livelock invariant, at the shard rather
        // than in the governor. A positive budget is not enough on its own:
        // if a budget smaller than the next entry yielded an EMPTY page, the
        // replay loop would see slice.Count == 0, break, and report a
        // completed replay having consumed nothing - a silent stall that is
        // worse than the crash it replaced, because it looks like success.
        //
        // This is why FileWalStorageOptions' always-take-one escape hatch is
        // load-bearing and must not be "fixed". The fix bounds its
        // CONSEQUENCE (the entry is decoded from pooled chunks, never a
        // whole-entry contiguous buffer) rather than removing it.
        var governor = new ScriptedGovernor { Budget = 1L };
        using var provider = CreateProvider(maxReadBatchBytes: 16L * 1024 * 1024, governor: governor);

        await AppendAsync(provider, count: 2, valueBytes: 256 * 1024);

        var page = await PageAsync(provider, fromExclusive: -1, maxEntries: 256);

        Assert.That(page.Count, Is.EqualTo(1),
            "A one-byte budget must still yield exactly one entry: fewer stalls the replay, more "
            + "defeats the bound.");
    }

    [Test]
    public async Task A_narrowed_budget_shortens_the_page_the_shard_actually_reads()
    {
        var governor = new ScriptedGovernor();
        using var sut = CreateProvider(maxReadBatchBytes: 1024 * 1024, governor);
        await AppendAsync(sut, count: 16, valueBytes: 1024);

        var unpressured = await PageAsync(sut, -1, maxEntries: 16);
        Assert.That(unpressured, Has.Count.EqualTo(16));

        governor.Budget = 2048;
        var pressured = await PageAsync(sut, -1, maxEntries: 16);

        Assert.That(pressured, Has.Count.LessThan(16),
            "A narrowed budget that does not shorten the page is not narrowing anything.");
        Assert.That(pressured, Is.Not.Empty,
            "Narrowing must never produce an empty page: every reader treats that as end-of-stream.");
    }

    // ---------------------------------------------------------------------
    // Clause 2: an entry larger than the ceiling needs no contiguous buffer.
    // ---------------------------------------------------------------------

    [Test]
    public async Task An_entry_larger_than_the_budget_decodes_without_a_whole_entry_contiguous_buffer()
    {
        // The governor refuses any allocation above one chunk, which is how a
        // fragmented heap behaves: plenty of memory, no single block that big.
        // The decode path must still serve the entry, because it never asks
        // for one.
        var governor = new ScriptedGovernor
        {
            LargestAffordableAllocation = PooledPayloadSequence.ChunkBytes,
        };
        using var sut = CreateProvider(maxReadBatchBytes: 4096, governor);
        var big = 512 * 1024;
        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(0, big) }, CancellationToken.None);

        var page = await PageAsync(sut, -1, maxEntries: 4);

        Assert.That(page, Has.Count.EqualTo(1), "The always-take-one floor must still hold.");
        Assert.That(page[0].Mutation.Value!.Length, Is.EqualTo(big));
        Assert.That(governor.AllocationRequests, Is.Empty,
            "The decode path must not route payloads through whole-entry allocations at all.");
    }

    [Test]
    public async Task The_largest_contiguous_request_is_independent_of_the_budget()
    {
        // THE PROPERTY THAT MAKES THE FLOOR'S SIZE A CHEAP DECISION.
        //
        // The floor trades against time to recovery: a leaf becomes trimmable
        // by registering a cursor, and it registers only once a page carries
        // an entry in its own key range, so a narrower page means more pages
        // before a leaf's first registration. That argues for a wide floor.
        // The usual objection is that a wider budget is a bigger allocation
        // and therefore likelier to fail on a nearly-full heap - which is
        // exactly what the configured 16 MiB ceiling did.
        //
        // That objection does not apply here, and this test is why. The
        // budget bounds how many bytes a page may carry; it does not bound
        // any single allocation, because the page is staged as pooled
        // fixed-size chunks. The largest block the allocator is ever asked
        // for is one chunk, whatever the budget is. So widening the floor
        // adds pooled chunks, never a bigger block - and block size, not
        // total bytes, is what fails on a fragmented heap.
        //
        // If this ever stopped holding, the floor would silently become a
        // contiguous allocation of its own size and the decision to widen it
        // would turn from cheap into the original bug.
        var governor = new ScriptedGovernor
        {
            LargestAffordableAllocation = PooledPayloadSequence.ChunkBytes,
        };

        using (var seed = CreateProvider(maxReadBatchBytes: 16 * 1024 * 1024, governor))
        {
            await AppendAsync(seed, count: 24, valueBytes: 96 * 1024);
        }

        governor.AllocationRequests.Clear();

        foreach (var budget in new[] { 4096, 64 * 1024, 1024 * 1024, 16 * 1024 * 1024 })
        {
            using var sut = CreateProvider(maxReadBatchBytes: budget, governor);

            var page = await PageAsync(sut, -1, maxEntries: 24);

            Assert.That(page, Is.Not.Empty, $"Budget {budget} yielded nothing.");
            Assert.That(
                governor.AllocationRequests,
                Is.Empty,
                $"At a budget of {budget} bytes the decode path asked the allocator for a block of its "
                + "own, rather than staging into pooled chunks. Widening the floor would then widen the "
                + "contiguous request, which is the failure mode the configured ceiling already had.");
        }
    }

    [Test]
    public async Task Chunked_decode_round_trips_payloads_that_straddle_many_chunks()
    {
        // Segment boundaries are where a hand-rolled ReadOnlySequence goes
        // wrong, so span sizes either side of a chunk are pinned explicitly.
        var governor = new ScriptedGovernor();
        using var sut = CreateProvider(maxReadBatchBytes: 64L * 1024 * 1024, governor);
        var sizes = new[]
        {
            0,
            1,
            PooledPayloadSequence.ChunkBytes - 1,
            PooledPayloadSequence.ChunkBytes,
            PooledPayloadSequence.ChunkBytes + 1,
            (3 * PooledPayloadSequence.ChunkBytes) + 17,
        };

        for (var i = 0; i < sizes.Length; i++)
        {
            await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(i, sizes[i]) }, CancellationToken.None);
        }

        var page = await PageAsync(sut, -1, maxEntries: sizes.Length);

        Assert.That(page, Has.Count.EqualTo(sizes.Length));
        for (var i = 0; i < sizes.Length; i++)
        {
            Assert.That(page[i].Offset, Is.EqualTo(i));
            Assert.That(page[i].Mutation.Value!.Length, Is.EqualTo(sizes[i]), $"payload {i} round-tripped wrong");
            Assert.That(page[i].Mutation.Value!.All(b => b == (byte)(i & 0xFF)), Is.True,
                $"payload {i} content corrupted across chunk boundaries");
        }
    }

    // ---------------------------------------------------------------------
    // Clause 3: a page that cannot be allocated degrades instead of failing.
    // ---------------------------------------------------------------------

    [Test]
    public async Task A_page_that_cannot_be_allocated_is_retried_narrower_rather_than_abandoned()
    {
        // One failed allocation, then a heap that can serve anything. If the
        // read simply retried at the same width it would now succeed at 8 and
        // this would be indistinguishable from no retry at all, so the
        // narrowing is what the assertion below actually pins.
        var governor = new ScriptedGovernor { AllocationFailuresToInject = 1 };
        using var sut = CreateProvider(maxReadBatchBytes: 64L * 1024 * 1024, governor);
        await AppendAsync(sut, count: 8, valueBytes: 4096);
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);

        var page = await sut.ReadEncodedAsync(TreeId, 0, -1, 8, encoder, CancellationToken.None);

        Assert.That(page.Offsets.Length, Is.Not.Zero, "A degraded read must still make progress.");
        Assert.That(page.Offsets.Span[0], Is.EqualTo(0L), "A degraded read must resume, never skip.");
        Assert.That(page.Offsets.Length, Is.LessThan(8),
            "The page that failed must come back narrower; an unchanged width is not a retry.");
        Assert.That(
            page.EncodedEntries.Length,
            Is.EqualTo(page.Offsets.Length),
            "segments and offsets must stay parallel through a degraded read");
    }

    [Test]
    public async Task The_decode_path_degrades_on_its_own_rather_than_relying_on_the_encoded_path()
    {
        // The field stack trace is the decode path, and it allocates nothing
        // through the governor by design, so its retry cannot be reached by
        // scripting allocations. It is driven here through the decoder, which
        // is where the deserializer's own allocations actually fail.
        var directory = Path.Combine(_root, TreeId, "shard-0");
        var governor = new ScriptedGovernor();
        using (var seed = CreateProvider(maxReadBatchBytes: 64L * 1024 * 1024, governor))
        {
            await AppendAsync(seed, count: 8, valueBytes: 1024);
        }

        var options = new FileWalStorageOptions
        {
            RootDirectory = _root,
            FlushToDisk = false,
            MaxReadBatchBytes = 64L * 1024 * 1024,
        };
        using var shard = new FileWalShard(directory, options, TreeId, 0, governor);

        var failuresLeft = 1;
        var decoded = 0;
        var page = await shard.SnapshotDecodedAsync(
            -1L,
            8,
            64L * 1024 * 1024,
            _ =>
            {
                decoded++;
                if (failuresLeft > 0)
                {
                    failuresLeft--;
                    throw new OutOfMemoryException("scripted: deserializer could not allocate.");
                }

                return decoded;
            },
            CancellationToken.None);

        Assert.That(page.Offsets, Has.Length.LessThan(8),
            "A decode that ran out of memory must be retried over a narrower window.");
        Assert.That(page.Offsets[0], Is.EqualTo(0L), "A degraded decode must resume, never skip.");
        Assert.That(shard.ReadPressureDegradations, Is.EqualTo(1L),
            "Exactly one narrowing step must be recorded for one failure.");
    }

    [Test]
    public async Task A_decode_that_cannot_afford_one_entry_is_refused_rather_than_looping_forever()
    {
        var directory = Path.Combine(_root, TreeId, "shard-0");
        var governor = new ScriptedGovernor();
        using (var seed = CreateProvider(maxReadBatchBytes: 64L * 1024 * 1024, governor))
        {
            await AppendAsync(seed, count: 8, valueBytes: 1024);
        }

        var options = new FileWalStorageOptions
        {
            RootDirectory = _root,
            FlushToDisk = false,
            MaxReadBatchBytes = 64L * 1024 * 1024,
        };
        using var shard = new FileWalShard(directory, options, TreeId, 0, governor);

        Assert.ThrowsAsync<WalReadUnderPressureException>(async () => await shard.SnapshotDecodedAsync<int>(
            -1L,
            8,
            64L * 1024 * 1024,
            _ => throw new OutOfMemoryException("scripted: never affordable."),
            CancellationToken.None));
    }

    [Test]
    public async Task An_unaffordable_single_entry_is_refused_as_a_resource_verdict_not_a_corruption()
    {
        var governor = new ScriptedGovernor { LargestAffordableAllocation = 16 };
        using var sut = CreateProvider(maxReadBatchBytes: 64L * 1024 * 1024, governor);
        await sut.AppendBatchAsync(TreeId, 0, new[] { Entry(7, 4096) }, CancellationToken.None);
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);

        var ex = Assert.ThrowsAsync<WalReadUnderPressureException>(
            async () => await sut.ReadEncodedAsync(TreeId, 0, -1, 8, encoder, CancellationToken.None));

        Assert.That(ex!.TreeId, Is.EqualTo(TreeId));
        Assert.That(ex.Offset, Is.EqualTo(7L), "The caller needs the offset it could not get past.");
        Assert.That(ex.RequiredBytes, Is.GreaterThan(0L));
        Assert.That(ex.InnerException, Is.TypeOf<OutOfMemoryException>(),
            "The verdict must keep the underlying cause, not replace it.");
    }

    [Test]
    public async Task Degradation_is_only_used_when_an_allocation_actually_fails()
    {
        var governor = new ScriptedGovernor();
        using var sut = CreateProvider(maxReadBatchBytes: 64L * 1024 * 1024, governor);
        await AppendAsync(sut, count: 8, valueBytes: 1024);
        var encoder = new OrleansBinaryWalRecordEncoder(_serializer);

        var page = await sut.ReadEncodedAsync(TreeId, 0, -1, 8, encoder, CancellationToken.None);

        Assert.That(page.Offsets.Length, Is.EqualTo(8),
            "An affordable read must be served whole; degradation is a response to failure, not a policy.");
    }
}
