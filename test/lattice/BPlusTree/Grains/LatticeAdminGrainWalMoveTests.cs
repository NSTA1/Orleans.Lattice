using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit coverage for the administrative WAL placement surface on
/// <c>LatticeAdminGrain</c> - the read-only inspection methods and, above all,
/// the abort arms of the managed move saga.
/// <para>
/// The move is a fenced, resumable, multi-phase copy that ends in an
/// irreversible cutover, so almost all of its interesting behaviour is what it
/// does when a phase goes wrong: it must abort <em>before</em> flipping the
/// durable pin, release every fenced source so the partition resumes service on
/// the original provider without waiting out the quiesce lease, and never
/// expose a partially-applied batch. The cluster fixture in
/// <c>WalPlacementMoveIntegrationTests</c> exercises the happy path against
/// real in-memory providers; these tests drive the same coordinator against
/// scripted providers so each failure can be provoked deterministically.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeAdminGrainWalMoveTests
{
    private const string TreeId = "wal-move-tree";
    private const string SecondaryKey = "secondary";

    /// <summary>
    /// A minimal in-memory WAL partition: a set of retained offsets plus the
    /// hooks the move coordinator actually uses. Offsets are modelled directly
    /// (rather than through real records) because the copy phases reason purely
    /// about offset ranges, and a scripted <see cref="HighestOverrides"/> queue
    /// lets a test present the tail values that provoke each abort arm.
    /// </summary>
    private sealed class ScriptedWalProvider : IWalStorageProvider
    {
        private readonly SortedSet<long> _offsets = new();

        /// <summary>Highest-offset answers to serve before falling back to the real tail.</summary>
        public Queue<long> HighestOverrides { get; } = new();

        /// <summary>Trim floors requested on this provider, in call order.</summary>
        public List<long> Trims { get; } = new();

        /// <summary>Every offset handed to <see cref="AppendEncodedBatchAsync"/>, in call order.</summary>
        public List<long> AppendedOffsets { get; } = new();

        public void Seed(params long[] offsets)
        {
            foreach (var o in offsets) _offsets.Add(o);
        }

        public IReadOnlyCollection<long> Offsets => _offsets;

        public Task AppendBatchAsync(
            string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task AppendEncodedBatchAsync(
            string treeId, int shardIndex, ReadOnlyMemory<ArraySegment<byte>> encodedEntries,
            ReadOnlyMemory<long> offsets, IWalRecordEncoder encoder, CancellationToken cancellationToken)
        {
            foreach (var o in offsets.Span)
            {
                _offsets.Add(o);
                AppendedOffsets.Add(o);
            }
            return Task.CompletedTask;
        }

        public Task<WalShardEncodedPage> ReadEncodedAsync(
            string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries,
            IWalRecordEncoder encoder, CancellationToken cancellationToken)
        {
            var page = _offsets.Where(o => o > fromOffsetExclusive).Take(maxEntries).ToArray();
            var segments = page.Select(_ => new ArraySegment<byte>([1])).ToArray();
            return Task.FromResult(new WalShardEncodedPage
            {
                EncodedEntries = segments,
                Offsets = page,
            });
        }

        public async IAsyncEnumerable<WalEntry> ReadAsync(
            string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            yield break;
        }

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => Task.FromResult(HighestOverrides.Count > 0
                ? HighestOverrides.Dequeue()
                : (_offsets.Count == 0 ? -1 : _offsets.Max));

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => Task.FromResult(_offsets.Count == 0 ? -1 : _offsets.Min);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
        {
            Trims.Add(throughOffsetInclusive);
            _offsets.RemoveWhere(o => o <= throughOffsetInclusive);
            return Task.CompletedTask;
        }

        public Task<long> GetRetainedByteSizeAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => Task.FromResult(0L);
    }

    /// <summary>Catalog over a fixed key-to-provider map, so an unknown key is genuinely unresolvable.</summary>
    private sealed class ScriptedCatalog(Dictionary<string, IWalStorageProvider> providers) : IWalStorageProviderCatalog
    {
        public bool TryGet(string key, out IWalStorageProvider provider)
        {
            if (providers.TryGetValue(key, out var found))
            {
                provider = found;
                return true;
            }
            provider = null!;
            return false;
        }

        public IReadOnlyCollection<string> Keys => providers.Keys;
    }

    private sealed class Harness
    {
        public required LatticeAdminGrain Grain { get; init; }
        public required ScriptedWalProvider Source { get; init; }
        public required ScriptedWalProvider Target { get; init; }
        public required IWalShardGrain Wal { get; init; }
        public required ILatticeRegistry Registry { get; init; }

        /// <summary>Quiesce answers served in call order; the last one repeats.</summary>
        public List<Func<WalMoveQuiesceResult>> QuiesceScript { get; } = new();

        public int QuiesceCalls { get; set; }
        public int DeactivateCalls { get; set; }
    }

    private static Harness CreateHarness(
        WalPlacementPin? pin = null,
        int walPartitions = 2,
        bool registerSecondary = true,
        bool deactivateThrows = false)
    {
        pin ??= WalPlacementPin.Create();

        var source = new ScriptedWalProvider();
        var target = new ScriptedWalProvider();
        var providers = new Dictionary<string, IWalStorageProvider>(StringComparer.Ordinal)
        {
            [IWalStorageProviderCatalog.DefaultProviderKey] = source,
        };
        if (registerSecondary) providers[SecondaryKey] = target;
        var catalog = new ScriptedCatalog(providers);

        var factory = Substitute.For<IGrainFactory>();

        var lattice = Substitute.For<ILattice>();
        lattice.GetRoutingAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RoutingInfo>(
                new RoutingInfo(TreeId, ShardMap.CreateDefault(1, 1))));
        factory.GetGrain<ILattice>(TreeId).Returns(lattice);

        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetWalPlacementAsync(TreeId).Returns(_ => Task.FromResult(pin));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = 128,
                MaxInternalChildren = 128,
                ShardCount = 1,
                WalPartitions = walPartitions,
            }));
        registry.UpdateWalPlacementAsync(
                TreeId, Arg.Any<long>(), Arg.Any<IReadOnlyCollection<(int Partition, string ProviderKey)>>())
            .Returns(ci => Task.FromResult(pin.WithPartitions(
                (IReadOnlyCollection<(int, string)>)ci[2]!, pin.Version + 1)));
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var wal = Substitute.For<IWalShardGrain>();
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(wal);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions { WalPartitions = walPartitions });

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice-admin", LatticeConstants.AdminGrainKey));

        var harness = new Harness
        {
            Grain = new LatticeAdminGrain(
                context,
                factory,
                NullLogger<LatticeAdminGrain>.Instance,
                new LatticeOptionsResolver(factory, optionsMonitor, null, catalog),
                catalog,
                Substitute.For<IWalRecordEncoder>(),
                optionsMonitor),
            Source = source,
            Target = target,
            Wal = wal,
            Registry = registry,
        };

        wal.QuiesceForMoveAsync(Arg.Any<long>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var index = Math.Min(harness.QuiesceCalls, harness.QuiesceScript.Count - 1);
                harness.QuiesceCalls++;
                return Task.FromResult(harness.QuiesceScript[index]());
            });
        wal.DeactivateForMoveAsync(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                harness.DeactivateCalls++;
                return deactivateThrows
                    ? Task.FromException(new InvalidOperationException("silo unreachable"))
                    : Task.CompletedTask;
            });

        return harness;
    }

    private static WalMoveQuiesceResult Quiesced(long highest, long version = 0)
        => new(true, highest, version, IWalStorageProviderCatalog.DefaultProviderKey);

    private static WalMoveQuiesceResult NotQuiesced(long observedVersion)
        => new(false, -1, observedVersion, IWalStorageProviderCatalog.DefaultProviderKey);

    private static ILatticeAdmin Admin(Harness h) => h.Grain;

    private static IEnumerable<(int, string)> Move(int partition, string key) => [(partition, key)];

    // ---- read-only inspection surface

    [Test]
    public async Task AuditWalPlacementAsync_reports_every_partition_and_the_known_catalog_keys()
    {
        var pin = WalPlacementPin.Create().WithPartition(1, SecondaryKey, 7);
        var harness = CreateHarness(pin);

        var audit = await Admin(harness).AuditWalPlacementAsync(TreeId);

        Assert.Multiple(() =>
        {
            Assert.That(audit.TreeId, Is.EqualTo(TreeId));
            Assert.That(audit.Version, Is.EqualTo(7));
            Assert.That(audit.PartitionCount, Is.EqualTo(2));
            Assert.That(audit.Partitions, Has.Length.EqualTo(2));
            Assert.That(audit.Partitions[0].ProviderKey, Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
            Assert.That(audit.Partitions[1].ProviderKey, Is.EqualTo(SecondaryKey));
            Assert.That(audit.AllResolvableOnThisSilo, Is.True);
            Assert.That(audit.KnownProviderKeys, Is.EquivalentTo(
                new[] { IWalStorageProviderCatalog.DefaultProviderKey, SecondaryKey }));
        });
    }

    [Test]
    public async Task AuditWalPlacementAsync_flags_a_pin_naming_a_key_this_silo_cannot_resolve()
    {
        // The tree is pinned to a provider this silo never registered - the exact
        // misconfiguration the audit surface exists to make visible before a
        // partition activates and fails.
        var pin = WalPlacementPin.Create().WithPartition(0, SecondaryKey, 3);
        var harness = CreateHarness(pin, registerSecondary: false);

        var audit = await Admin(harness).AuditWalPlacementAsync(TreeId);

        Assert.Multiple(() =>
        {
            Assert.That(audit.AllResolvableOnThisSilo, Is.False);
            Assert.That(audit.Partitions[0].ResolvableOnThisSilo, Is.False);
            Assert.That(audit.Partitions[1].ResolvableOnThisSilo, Is.True);
            Assert.That(audit.KnownProviderKeys, Does.Not.Contain(SecondaryKey));
        });
    }

    [Test]
    public void AuditWalPlacementAsync_rejects_a_null_tree_id()
    {
        var harness = CreateHarness();
        Assert.That(async () => await Admin(harness).AuditWalPlacementAsync(null!),
            Throws.ArgumentNullException);
    }

    // ---- request validation

    [Test]
    public void ExecuteWalMoveAsync_rejects_a_partition_outside_the_tree_fan_out()
    {
        var harness = CreateHarness(walPartitions: 2);

        Assert.That(async () => await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(9, SecondaryKey)),
            Throws.InstanceOf<ArgumentOutOfRangeException>()
                .With.Message.Contains("2 WAL partition(s)"));
    }

    [Test]
    public void ExecuteWalMoveAsync_rejects_a_null_target_provider_key()
    {
        var harness = CreateHarness();

        Assert.That(async () => await Admin(harness).ExecuteWalMoveAsync(TreeId, [(0, null!)]),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ExecuteWalMoveAsync_rejects_a_target_key_this_silo_cannot_resolve()
    {
        var harness = CreateHarness(registerSecondary: false);

        Assert.That(async () => await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey)),
            Throws.InstanceOf<LatticeWalProviderMissingException>());
    }

    // ---- copy-phase abort arms

    [Test]
    public void A_move_aborts_when_the_source_refuses_to_fence_at_the_expected_version()
    {
        var harness = CreateHarness();
        harness.QuiesceScript.Add(() => NotQuiesced(observedVersion: 42));

        Assert.That(async () => await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey)),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains("42"));

        Assert.That(harness.Registry.ReceivedCalls().Any(c =>
                c.GetMethodInfo().Name == nameof(ILatticeRegistry.UpdateWalPlacementAsync)),
            Is.False, "an aborted move must never flip the durable pin");
    }

    [Test]
    public void A_move_aborts_when_the_target_already_holds_offsets_beyond_the_source()
    {
        var harness = CreateHarness();
        harness.Source.Seed(0, 1, 2, 3);
        harness.QuiesceScript.Add(() => Quiesced(highest: 3));
        // The target has diverged: it holds an offset the source never had.
        harness.Target.HighestOverrides.Enqueue(99);

        Assert.That(async () => await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey)),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains("not a clean prefix"));
    }

    [Test]
    public async Task A_move_reserves_the_target_trim_floor_when_the_source_has_trimmed_a_prefix()
    {
        var harness = CreateHarness();
        // The source's retained window starts at 5, so the target must reserve
        // floor 4 before the first append lands at 5 and stays contiguous.
        harness.Source.Seed(5, 6, 7);
        harness.QuiesceScript.Add(() => Quiesced(highest: 7));

        var receipt = await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Target.Trims, Is.EqualTo(new[] { 4L }));
            Assert.That(harness.Target.AppendedOffsets, Is.EqualTo(new[] { 5L, 6L, 7L }));
            Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.Moved));
            Assert.That(receipt.Moves[0].CopiedFromOffset, Is.EqualTo(5));
            Assert.That(receipt.Moves[0].CopiedThroughOffset, Is.EqualTo(7));
        });
    }

    [Test]
    public async Task A_move_copies_the_delta_when_appends_land_on_the_source_during_the_copy()
    {
        var harness = CreateHarness();
        harness.Source.Seed(0, 1, 2);
        harness.QuiesceScript.Add(() => Quiesced(highest: 2));
        harness.QuiesceScript.Add(() =>
        {
            // The convergence re-quiesce observes a tail that grew while the
            // first copy was in flight.
            harness.Source.Seed(3);
            return Quiesced(highest: 3);
        });
        harness.QuiesceScript.Add(() => Quiesced(highest: 3));

        var receipt = await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Target.AppendedOffsets, Is.EqualTo(new[] { 0L, 1L, 2L, 3L }),
                "the late append is carried across before the cutover");
            Assert.That(receipt.Moves[0].SourceHighestOffset, Is.EqualTo(3));
            Assert.That(receipt.Moves[0].TargetHighestOffset, Is.EqualTo(3));
            Assert.That(harness.QuiesceCalls, Is.EqualTo(3),
                "convergence loops until the source tail is stable");
        });
    }

    [Test]
    public void A_move_aborts_when_the_placement_changes_during_convergence()
    {
        var harness = CreateHarness();
        harness.Source.Seed(0, 1);
        harness.QuiesceScript.Add(() => Quiesced(highest: 1));
        harness.QuiesceScript.Add(() => NotQuiesced(observedVersion: 11));

        Assert.That(async () => await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey)),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains("during convergence"));

        Assert.That(harness.DeactivateCalls, Is.GreaterThan(0),
            "the fenced source is released so it resumes service immediately");
    }

    [Test]
    public void A_move_aborts_when_the_target_overshoots_the_source_after_the_copy()
    {
        var harness = CreateHarness();
        harness.Source.Seed(0, 1, 2);
        harness.QuiesceScript.Add(() => Quiesced(highest: 2));
        // Clean-prefix probe sees an empty target; the post-copy verify sees a
        // tail beyond the source.
        harness.Target.HighestOverrides.Enqueue(-1);
        harness.Target.HighestOverrides.Enqueue(50);

        Assert.That(async () => await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey)),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains("overshot"));
    }

    [Test]
    public void A_move_aborts_when_content_verification_finds_a_short_target()
    {
        var harness = CreateHarness();
        harness.Source.Seed(0, 1, 2);
        harness.QuiesceScript.Add(() => Quiesced(highest: 2));
        harness.Target.HighestOverrides.Enqueue(-1);
        harness.Target.HighestOverrides.Enqueue(1);

        Assert.That(
            async () => await Admin(harness).ExecuteWalMoveAsync(
                TreeId, Move(0, SecondaryKey), new WalMoveOptions { VerifyAfterCopy = true }),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains("failed verification"));
    }

    [Test]
    public void The_overshoot_guard_runs_even_when_content_verification_is_off()
    {
        var harness = CreateHarness();
        harness.Source.Seed(0, 1, 2);
        harness.QuiesceScript.Add(() => Quiesced(highest: 2));
        harness.Target.HighestOverrides.Enqueue(-1);
        harness.Target.HighestOverrides.Enqueue(50);

        Assert.That(
            async () => await Admin(harness).ExecuteWalMoveAsync(
                TreeId, Move(0, SecondaryKey), new WalMoveOptions { VerifyAfterCopy = false }),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("overshot"));
    }

    [Test]
    public async Task A_short_target_is_accepted_when_content_verification_is_off()
    {
        // Falsifies the verification test above: the same short tail passes once
        // VerifyAfterCopy is cleared, so the failure really is the verify arm.
        var harness = CreateHarness();
        harness.Source.Seed(0, 1, 2);
        harness.QuiesceScript.Add(() => Quiesced(highest: 2));
        harness.Target.HighestOverrides.Enqueue(-1);
        harness.Target.HighestOverrides.Enqueue(1);

        var receipt = await Admin(harness).ExecuteWalMoveAsync(
            TreeId, Move(0, SecondaryKey), new WalMoveOptions { VerifyAfterCopy = false });

        Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.Moved));
    }

    [Test]
    public void An_aborted_move_survives_a_source_that_cannot_be_deactivated()
    {
        // The failure lands mid-copy, so both the per-partition release and the
        // batch-level release run - and both fail. The original abort reason must
        // still be what surfaces.
        var harness = CreateHarness(deactivateThrows: true);
        harness.Source.Seed(0, 1);
        harness.QuiesceScript.Add(() => Quiesced(highest: 1));
        harness.QuiesceScript.Add(() => NotQuiesced(observedVersion: 5));

        Assert.That(async () => await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey)),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("during convergence"));

        Assert.That(harness.DeactivateCalls, Is.EqualTo(2),
            "release is attempted per-partition and again for the whole batch");
    }

    [Test]
    public void An_aborted_batch_releases_every_fenced_source()
    {
        var harness = CreateHarness(walPartitions: 3);
        harness.QuiesceScript.Add(() => NotQuiesced(observedVersion: 5));

        Assert.That(
            async () => await Admin(harness).ExecuteWalMoveAsync(
                TreeId,
                [(0, SecondaryKey), (1, SecondaryKey)],
                new WalMoveOptions { MaxConcurrentPartitionMoves = 1 }),
            Throws.InstanceOf<InvalidOperationException>());

        Assert.That(harness.DeactivateCalls, Is.GreaterThanOrEqualTo(2),
            "every partition fenced by the batch is released, not just the failed one");
    }

    // ---- cutover

    [Test]
    public async Task An_already_at_target_partition_completes_its_cutover_without_copying()
    {
        var pin = WalPlacementPin.Create().WithPartition(0, SecondaryKey, 4);
        var harness = CreateHarness(pin);

        var receipt = await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey));

        Assert.Multiple(() =>
        {
            Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.AlreadyAtTarget));
            Assert.That(receipt.Moves[0].Outcome, Is.EqualTo(WalMoveOutcome.AlreadyAtTarget));
            Assert.That(receipt.NewPlacementVersion, Is.EqualTo(4), "no flip, so no version bump");
            Assert.That(harness.QuiesceCalls, Is.Zero, "nothing to copy means nothing to fence");
            Assert.That(harness.DeactivateCalls, Is.EqualTo(1), "the cutover is still completed");
        });
    }

    [Test]
    public async Task A_flipped_move_tolerates_a_source_that_cannot_be_deactivated()
    {
        // The pin is already durable at this point, so a failed deactivation is
        // a repairable warning and must not fail the caller's move.
        var pin = WalPlacementPin.Create().WithPartition(0, SecondaryKey, 4);
        var harness = CreateHarness(pin, deactivateThrows: true);

        var receipt = await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey));

        Assert.Multiple(() =>
        {
            Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.AlreadyAtTarget));
            Assert.That(harness.DeactivateCalls, Is.EqualTo(1));
        });
    }

    // ---- reclaim

    [Test]
    public void ReclaimMovedWalSourceAsync_refuses_to_reclaim_the_live_placement()
    {
        var harness = CreateHarness();

        Assert.That(
            async () => await Admin(harness).ReclaimMovedWalSourceAsync(
                TreeId, 0, IWalStorageProviderCatalog.DefaultProviderKey),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains("live placement"));
    }

    [Test]
    public void ReclaimMovedWalSourceAsync_rejects_a_source_key_this_silo_cannot_resolve()
    {
        var pin = WalPlacementPin.Create().WithPartition(0, SecondaryKey, 2);
        var harness = CreateHarness(pin, registerSecondary: true);

        Assert.That(
            async () => await Admin(harness).ReclaimMovedWalSourceAsync(TreeId, 0, "never-registered"),
            Throws.InstanceOf<LatticeWalProviderMissingException>());
    }

    [Test]
    public void ReclaimMovedWalSourceAsync_rejects_a_partition_outside_the_tree_fan_out()
    {
        var harness = CreateHarness(walPartitions: 2);

        Assert.That(
            async () => await Admin(harness).ReclaimMovedWalSourceAsync(TreeId, -1, SecondaryKey),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task ReclaimMovedWalSourceAsync_trims_the_orphaned_source_through_its_tail()
    {
        var pin = WalPlacementPin.Create().WithPartition(0, SecondaryKey, 2);
        var harness = CreateHarness(pin);
        harness.Source.Seed(0, 1, 2, 3);

        var receipt = await Admin(harness).ReclaimMovedWalSourceAsync(
            TreeId, 0, IWalStorageProviderCatalog.DefaultProviderKey);

        Assert.Multiple(() =>
        {
            Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.SourceReclaimed));
            Assert.That(harness.Source.Trims, Is.EqualTo(new[] { 3L }));
            Assert.That(receipt.ToProviderKey, Is.EqualTo(SecondaryKey));
        });
    }

    [Test]
    public async Task ReclaimMovedWalSourceAsync_is_a_no_op_on_an_already_empty_source()
    {
        var pin = WalPlacementPin.Create().WithPartition(0, SecondaryKey, 2);
        var harness = CreateHarness(pin);

        var receipt = await Admin(harness).ReclaimMovedWalSourceAsync(
            TreeId, 0, IWalStorageProviderCatalog.DefaultProviderKey);

        Assert.Multiple(() =>
        {
            Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.NoOp));
            Assert.That(harness.Source.Trims, Is.Empty);
        });
    }
}
