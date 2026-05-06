using Microsoft.Extensions.Logging.Abstractions;
using MultiSiteManufacturing.Host;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Host.Lattice;
using MultiSiteManufacturing.Tests.Federation;

namespace MultiSiteManufacturing.Tests.Lattice;

/// <summary>
/// Covers <see cref="PartCrdtStore"/>: the LWW current-operator
/// register (in <c>mfg-part-operator</c>) and the OR-Set process-label
/// set (in <c>mfg-part-labels</c>, accessed through the package's
/// <see cref="Orleans.Lattice.OrSetAccessor"/>). Also covers
/// partition-awareness: writes during partition land in a silo-local
/// shadow key in each tree, cross-silo reads diverge, and
/// <see cref="PartCrdtStore.HealLocalShadowAsync"/> promotes the
/// shadow into the shared key — for the operator via a plain shared-key
/// Set, for labels via the OR-Set's natural causal merge — so LWW /
/// OR-Set convergence becomes visible from every silo.
/// </summary>
[TestFixture]
public class PartCrdtStoreTests
{
    private static readonly SiloIdentity SiloA = new("a", IsPrimary: true);
    private static readonly SiloIdentity SiloB = new("b", IsPrimary: false);

    private FederationTestClusterFixture _fixture = null!;
    private PartCrdtStore _store = null!;
    private PartCrdtStore _storeA = null!;
    private PartCrdtStore _storeB = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FederationTestClusterFixture();
        await _fixture.InitializeAsync();
        _store = new PartCrdtStore(_fixture.GrainFactory, SiloA);
        _storeA = new PartCrdtStore(_fixture.GrainFactory, SiloA);
        _storeB = new PartCrdtStore(_fixture.GrainFactory, SiloB);
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [TearDown]
    public async Task TearDown()
    {
        // Every partition test that flips the grain flag must leave it
        // cleared so a later test doesn't inherit a partitioned world.
        await _fixture.GrainFactory
            .GetGrain<IPartitionChaosGrain>(IPartitionChaosGrain.SingletonKey)
            .SetPartitionedAsync(false);
    }

    private Task SetPartitionedAsync(bool partitioned) =>
        _fixture.GrainFactory
            .GetGrain<IPartitionChaosGrain>(IPartitionChaosGrain.SingletonKey)
            .SetPartitionedAsync(partitioned);

    [Test]
    public async Task GetOperator_returns_null_when_never_assigned()
    {
        var serial = new PartSerialNumber("HPT-CRDT-2028-91000");
        var result = await _store.GetOperatorAsync(serial);
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task AssignOperator_then_GetOperator_round_trips()
    {
        var serial = new PartSerialNumber("HPT-CRDT-2028-91001");
        await _store.AssignOperatorAsync(serial, new OperatorId("operator:alice"));

        var result = await _store.GetOperatorAsync(serial);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.Value.Value, Is.EqualTo("operator:alice"));
    }

    [Test]
    public async Task AssignOperator_LWW_last_write_wins()
    {
        // Sequential writes: the later wall-clock timestamp dominates,
        // matching LWW register semantics.
        var serial = new PartSerialNumber("HPT-CRDT-2028-91002");
        await _store.AssignOperatorAsync(serial, new OperatorId("operator:alice"));
        await _store.AssignOperatorAsync(serial, new OperatorId("operator:bob"));

        var result = await _store.GetOperatorAsync(serial);

        Assert.That(result!.Value.Value, Is.EqualTo("operator:bob"));
    }

    [Test]
    public async Task AddLabel_is_idempotent_at_the_set_level()
    {
        // OR-Set adds produce a fresh causal dot every time, but live
        // element membership (the `Elements()` projection) is
        // unchanged — that's the user-observable idempotence the store
        // exposes through `GetLabelsAsync`.
        var serial = new PartSerialNumber("HPT-CRDT-2028-91003");
        await _store.AddLabelAsync(serial, "first-article");
        await _store.AddLabelAsync(serial, "first-article");
        await _store.AddLabelAsync(serial, "first-article");

        var labels = await _store.GetLabelsAsync(serial);

        Assert.That(labels, Is.EqualTo(new[] { "first-article" }));
    }

    [Test]
    public async Task AddLabel_accumulates_unique_labels_in_sorted_order()
    {
        var serial = new PartSerialNumber("HPT-CRDT-2028-91004");
        await _store.AddLabelAsync(serial, "heat-lot-17");
        await _store.AddLabelAsync(serial, "aged");
        await _store.AddLabelAsync(serial, "priority");

        var labels = await _store.GetLabelsAsync(serial);

        // PartCrdtStore.GetLabelsAsync sorts the OR-Set's live
        // elements through a `SortedSet<string>` before returning,
        // so the surface contract is lexicographic order regardless of
        // the OR-Set's internal base64-keyed enumeration.
        Assert.That(labels, Is.EqualTo(new[] { "aged", "heat-lot-17", "priority" }));
    }

    [Test]
    public async Task GetLabels_returns_empty_for_unseen_serial()
    {
        var serial = new PartSerialNumber("HPT-CRDT-2028-91005");
        var labels = await _store.GetLabelsAsync(serial);
        Assert.That(labels, Is.Empty);
    }

    [Test]
    public async Task GetLabels_does_not_leak_other_serials()
    {
        // OR-Set isolation: labels for serial X must not appear when
        // reading serial Y, even though both live in the same tree
        // (each serial gets its own OR-Set value under a distinct key).
        var sA = new PartSerialNumber("HPT-CRDT-2028-91006");
        var sB = new PartSerialNumber("HPT-CRDT-2028-91007");
        await _store.AddLabelAsync(sA, "only-a");
        await _store.AddLabelAsync(sB, "only-b");

        var labelsA = await _store.GetLabelsAsync(sA);
        var labelsB = await _store.GetLabelsAsync(sB);

        Assert.Multiple(() =>
        {
            Assert.That(labelsA, Is.EqualTo(new[] { "only-a" }));
            Assert.That(labelsB, Is.EqualTo(new[] { "only-b" }));
        });
    }

    [Test]
    public void AddLabel_rejects_slash_because_it_is_the_sub_key_separator()
    {
        var serial = new PartSerialNumber("HPT-CRDT-2028-91008");
        Assert.ThrowsAsync<ArgumentException>(() =>
            _store.AddLabelAsync(serial, "has/slash"));
    }

    [Test]
    public void AddLabel_rejects_empty_label()
    {
        var serial = new PartSerialNumber("HPT-CRDT-2028-91009");
        Assert.ThrowsAsync<ArgumentException>(() => _store.AddLabelAsync(serial, ""));
        Assert.ThrowsAsync<ArgumentException>(() => _store.AddLabelAsync(serial, "   "));
    }

    // ---------- partition-aware writes + heal ----------

    [Test]
    public async Task Partitioned_operator_writes_go_to_shadow_and_are_invisible_to_other_silo()
    {
        var serial = new PartSerialNumber("HPT-CRDT-2028-91010");

        // Baseline shared write so silo B has *something* to read.
        await _storeA.AssignOperatorAsync(serial, new OperatorId("operator:baseline"));

        await SetPartitionedAsync(true);
        try
        {
            // Silo A's partition-era write lands in shadow/a/… — silo B
            // should still see the pre-partition shared value because
            // its own shadow is empty and the shared register is
            // untouched.
            await _storeA.AssignOperatorAsync(serial, new OperatorId("operator:alice-partitioned"));

            var seenFromA = await _storeA.GetOperatorAsync(serial);
            var seenFromB = await _storeB.GetOperatorAsync(serial);

            Assert.Multiple(() =>
            {
                Assert.That(seenFromA!.Value.Value, Is.EqualTo("operator:alice-partitioned"));
                Assert.That(seenFromB!.Value.Value, Is.EqualTo("operator:baseline"));
            });
        }
        finally
        {
            await SetPartitionedAsync(false);
        }
    }

    [Test]
    public async Task Heal_promotes_operator_shadow_to_shared_so_all_silos_converge()
    {
        var serial = new PartSerialNumber("HPT-CRDT-2028-91011");

        await SetPartitionedAsync(true);
        try
        {
            await _storeA.AssignOperatorAsync(serial, new OperatorId("operator:alice-partitioned"));
        }
        finally
        {
            await SetPartitionedAsync(false);
        }

        var promoted = await _storeA.HealLocalShadowAsync();

        var seenFromA = await _storeA.GetOperatorAsync(serial);
        var seenFromB = await _storeB.GetOperatorAsync(serial);

        Assert.Multiple(() =>
        {
            Assert.That(promoted, Is.GreaterThanOrEqualTo(1));
            Assert.That(seenFromA!.Value.Value, Is.EqualTo("operator:alice-partitioned"));
            Assert.That(seenFromB!.Value.Value, Is.EqualTo("operator:alice-partitioned"));
        });
    }

    [Test]
    public async Task Heal_unions_labels_across_silos()
    {
        var serial = new PartSerialNumber("HPT-CRDT-2028-91012");

        // Pre-partition: a shared label that both silos can see.
        await _storeA.AddLabelAsync(serial, "baseline");

        await SetPartitionedAsync(true);
        try
        {
            // Each silo grows its own partition-era subset of the G-Set.
            await _storeA.AddLabelAsync(serial, "alice-only");
            await _storeB.AddLabelAsync(serial, "bob-only");

            var aDuringPartition = await _storeA.GetLabelsAsync(serial);
            var bDuringPartition = await _storeB.GetLabelsAsync(serial);

            Assert.Multiple(() =>
            {
                // Each silo sees its own shadow ∪ shared, not the other
                // silo's shadow.
                Assert.That(aDuringPartition, Is.EqualTo(new[] { "alice-only", "baseline" }));
                Assert.That(bDuringPartition, Is.EqualTo(new[] { "baseline", "bob-only" }));
            });
        }
        finally
        {
            await SetPartitionedAsync(false);
        }

        // Both silos drain their own shadow; G-Set union is commutative
        // so the final label set contains every element from either
        // partition era.
        await _storeA.HealLocalShadowAsync();
        await _storeB.HealLocalShadowAsync();

        var finalA = await _storeA.GetLabelsAsync(serial);
        var finalB = await _storeB.GetLabelsAsync(serial);
        var expected = new[] { "alice-only", "baseline", "bob-only" };

        Assert.Multiple(() =>
        {
            Assert.That(finalA, Is.EqualTo(expected));
            Assert.That(finalB, Is.EqualTo(expected));
        });
    }

    [Test]
    public async Task HealLocalShadowAsync_returns_zero_when_no_shadow_entries()
    {
        // Idempotent / safe to call when nothing has been written under
        // partition. No side effect on any shared key.
        var serial = new PartSerialNumber("HPT-CRDT-2028-91013");
        await _storeA.AssignOperatorAsync(serial, new OperatorId("operator:quiet"));

        var promoted = await _storeA.HealLocalShadowAsync();

        Assert.That(promoted, Is.EqualTo(0));
        var result = await _storeA.GetOperatorAsync(serial);
        Assert.That(result!.Value.Value, Is.EqualTo("operator:quiet"));
    }

    [Test]
    public async Task AssignOperator_raises_PartChanged_for_the_serial()
    {
        var serial = new PartSerialNumber("HPT-CRDT-2028-91020");
        var seen = new List<PartSerialNumber>();
        void Handler(PartSerialNumber s) => seen.Add(s);
        _store.PartChanged += Handler;
        try
        {
            await _store.AssignOperatorAsync(serial, new OperatorId("operator:eve"));
        }
        finally
        {
            _store.PartChanged -= Handler;
        }

        Assert.That(seen, Is.EqualTo(new[] { serial }), "serial not seen in PartChanged event");
    }

    [Test]
    public async Task AddLabel_raises_PartChanged_for_the_serial()
    {
        var serial = new PartSerialNumber("HPT-CRDT-2028-91021");
        var seen = new List<PartSerialNumber>();
        void Handler(PartSerialNumber s) => seen.Add(s);
        _store.PartChanged += Handler;
        try
        {
            await _store.AddLabelAsync(serial, "first-article");
            // Idempotent at the set level, but each call still raises -
            // the event is "the part's CRDT state may have changed", not
            // "an actual element was added", because the broadcaster
            // doesn't carry deltas, only "rebuild the summary for serial".
            await _store.AddLabelAsync(serial, "first-article");
        }
        finally
        {
            _store.PartChanged -= Handler;
        }

        Assert.That(seen, Is.EqualTo(new[] { serial, serial }), "serial not seen in PartChanged event");
    }

    [Test]
    public async Task PartChanged_subscriber_exception_does_not_propagate()
    {
        // A single bad subscriber must never break the mutation path
        // (which would fail the user-facing operator-assign / label-add
        // RPC) or the receiver-side replication apply (which would
        // surface as a 500 on the inbound gRPC Push and stall replication).
        var serial = new PartSerialNumber("HPT-CRDT-2028-91022");
        void BadHandler(PartSerialNumber _) => throw new InvalidOperationException("subscriber boom");
        _store.PartChanged += BadHandler;
        try
        {
            Assert.DoesNotThrowAsync(async () =>
                await _store.AssignOperatorAsync(serial, new OperatorId("operator:isolation")));

            // Assignment must have completed despite the bad subscriber.
            var result = await _store.GetOperatorAsync(serial);
            Assert.That(result!.Value.Value, Is.EqualTo("operator:isolation"));
        }
        finally
        {
            _store.PartChanged -= BadHandler;
        }
    }

    [Test]
    public async Task Heal_raises_PartChanged_once_per_distinct_serial_promoted()
    {
        // Two distinct serials touched under partition (one with a
        // shadow operator, one with a shadow label) must surface
        // exactly two PartChanged events on heal - one per serial -
        // even though three shadow entries promoted across the two
        // trees.
        var sX = new PartSerialNumber("HPT-CRDT-2028-91030");
        var sY = new PartSerialNumber("HPT-CRDT-2028-91031");

        await SetPartitionedAsync(true);
        try
        {
            await _storeA.AssignOperatorAsync(sX, new OperatorId("operator:px"));
            await _storeA.AddLabelAsync(sX, "px-shadow-label");
            await _storeA.AddLabelAsync(sY, "py-shadow-label");
        }
        finally
        {
            await SetPartitionedAsync(false);
        }

        var seen = new List<PartSerialNumber>();
        void Handler(PartSerialNumber s) => seen.Add(s);
        _storeA.PartChanged += Handler;
        try
        {
            var promoted = await _storeA.HealLocalShadowAsync();
            Assert.That(promoted, Is.GreaterThanOrEqualTo(3));
        }
        finally
        {
            _storeA.PartChanged -= Handler;
        }

        // Distinct-serial guarantee: every touched serial appears at
        // least once; no duplicates.
        Assert.Multiple(() =>
        {
            Assert.That(seen, Has.Count.EqualTo(2));
            Assert.That(seen, Does.Contain(sX));
            Assert.That(seen, Does.Contain(sY));
        });
    }
}

