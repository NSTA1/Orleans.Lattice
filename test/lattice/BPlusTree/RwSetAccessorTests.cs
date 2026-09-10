using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit coverage for <see cref="RwSetAccessor"/>: the remove-wins observed-remove
/// set value surface over an <see cref="ILattice"/> key, and the set-granularity
/// counterpart of <c>RwFlagAccessor</c>. Exercises reads (absent and seeded), the
/// add and remove delta minting, the causal-dot arithmetic that decides a new
/// dot's counter, the observed-remove cancellation that makes an add win locally,
/// membership and enumeration, the out-of-band merge, and the argument /
/// initialisation guards - all against a mocked lattice so no cluster is needed.
/// <para>
/// The dot-counter tests are the load-bearing ones. A replica's next counter must
/// exceed every dot it has already authored across <b>all three</b> of the
/// state's maps - adds, removes and tombstones - because a reused
/// <c>(replica, counter)</c> pair is indistinguishable from the earlier dot after
/// a merge. Reusing a counter would therefore let an add cancel a remove it never
/// observed, silently breaking remove-wins. Each map gets its own test so a
/// regression that drops one scan fails in isolation rather than being masked by
/// the other two.
/// </para>
/// </summary>
[TestFixture]
public class RwSetAccessorTests
{
    private static byte[] Bytes(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    private static string SlotKey(byte[] element) => Convert.ToBase64String(element);

    private static ILattice Empty(string key)
    {
        var lattice = Substitute.For<ILattice>();
        lattice.GetAsync(key, Arg.Any<CancellationToken>()).Returns(Task.FromResult<byte[]?>(null));
        return lattice;
    }

    private static ILattice Seeded(string key, RwSet state)
    {
        var lattice = Substitute.For<ILattice>();
        var bytes = JsonLatticeSerializer<RwSet>.Default.Serialize(state);
        lattice.GetAsync(key, Arg.Any<CancellationToken>()).Returns(Task.FromResult<byte[]?>(bytes));
        return lattice;
    }

    /// <summary>
    /// Captures the single delta the accessor applies through the durable
    /// (no-TTL) seam and returns it decoded.
    /// </summary>
    private static async Task<RwSetDelta> CaptureDeltaAsync(ILattice lattice, string key, Func<RwSetAccessor, Task> act)
    {
        byte[]? applied = null;
        lattice.ApplyCrdtDeltaAsync(key, LatticeMergeMode.RwSet, Arg.Do<byte[]>(b => applied = b), Arg.Any<CancellationToken>())
            .Returns(HybridLogicalClock.Zero);

        await act(lattice.RwSet(key));

        Assert.That(applied, Is.Not.Null, "The accessor must apply exactly one delta through the RwSet seam.");
        return JsonLatticeSerializer<RwSetDelta>.Default.Deserialize(applied!);
    }

    // --- reads ---

    [Test]
    public async Task GetAsync_absent_key_returns_an_empty_set()
    {
        var set = await Empty("k").RwSet("k").GetAsync();

        Assert.That(set.Count, Is.Zero);
    }

    [Test]
    public async Task GetAsync_seeded_key_returns_the_stored_members()
    {
        var seed = new RwSet();
        seed.Add(Bytes("apple"), "r1", 1);

        var set = await Seeded("k", seed).RwSet("k").GetAsync();

        Assert.That(set.Contains(Bytes("apple")), Is.True);
    }

    [Test]
    public void Accessor_exposes_the_lattice_and_key_it_is_bound_to()
    {
        // The accessor is a readonly record struct built by an extension
        // method, so these two projections are how a caller that was handed an
        // accessor recovers what it addresses - a wrapper that reported the
        // wrong key would write to a different CRDT entirely.
        var lattice = Empty("kk");
        var accessor = lattice.RwSet("kk");

        Assert.Multiple(() =>
        {
            Assert.That(accessor.Lattice, Is.SameAs(lattice));
            Assert.That(accessor.Key, Is.EqualTo("kk"));
        });
    }

    // --- add / remove delta minting ---

    [Test]
    public async Task AddAsync_mints_a_single_add_dot_for_the_element()
    {
        var lattice = Empty("k");

        var delta = await CaptureDeltaAsync(lattice, "k", a => a.AddAsync(Bytes("apple"), "r1"));

        Assert.Multiple(() =>
        {
            Assert.That(delta.Adds, Has.Count.EqualTo(1));
            Assert.That(delta.Adds[0].Element, Is.EqualTo(Bytes("apple")));
            Assert.That(delta.Adds[0].ReplicaId, Is.EqualTo("r1"));
            Assert.That(delta.Adds[0].Counter, Is.EqualTo(1), "The first dot a replica authors is counter 1.");
            Assert.That(delta.Removes, Is.Empty);
            Assert.That(delta.Tombstones, Is.Empty, "There are no observed removes to cancel on an empty set.");
        });
    }

    [Test]
    public async Task RemoveAsync_mints_a_single_remove_dot_and_cancels_nothing()
    {
        var lattice = Empty("k");

        var delta = await CaptureDeltaAsync(lattice, "k", a => a.RemoveAsync(Bytes("apple"), "r1"));

        Assert.Multiple(() =>
        {
            Assert.That(delta.Removes, Has.Count.EqualTo(1));
            Assert.That(delta.Removes[0].Element, Is.EqualTo(Bytes("apple")));
            Assert.That(delta.Removes[0].ReplicaId, Is.EqualTo("r1"));
            Assert.That(delta.Adds, Is.Empty);
            Assert.That(delta.Tombstones, Is.Empty,
                "A remove must never tombstone: tombstoning its own dot would make the "
                + "remove cancel itself and the element would stay a member.");
        });
    }

    [Test]
    public async Task AddAsync_cancels_every_remove_dot_it_has_observed()
    {
        // This is what makes an add win locally over removes it can see, while
        // leaving a concurrent unobserved remove intact. Two dots from two
        // replicas, because remove dots are compacted to the max per replica.
        var element = Bytes("apple");
        var seed = new RwSet();
        seed.Remove(element, "r1", 4);
        seed.Remove(element, "r2", 9);
        var lattice = Seeded("k", seed);

        var delta = await CaptureDeltaAsync(lattice, "k", a => a.AddAsync(element, "r3"));

        Assert.That(delta.Tombstones, Has.Count.EqualTo(2),
            "Every observed remove dot must be cancelled, or the add does not take effect.");
        Assert.Multiple(() =>
        {
            Assert.That(delta.Tombstones.Select(t => (t.ReplicaId, t.Counter)),
                Is.EquivalentTo(new[] { ("r1", 4L), ("r2", 9L) }),
                "The cancellation must name the exact observed dots - a tombstone that "
                + "invented a counter would also suppress a remove nobody has seen.");
            Assert.That(delta.Tombstones.Select(t => t.Element), Is.All.EqualTo(element),
                "Each cancelled dot must carry the element it belongs to, because the "
                + "delta is a flat list with no per-element grouping.");
        });
    }

    [Test]
    public async Task AddAsync_leaves_another_elements_removes_alone()
    {
        // Positive control for the cancellation above: without it, a regression
        // that cancelled every remove in the set - not just the addressed
        // element's - would still pass that test.
        var seed = new RwSet();
        seed.Remove(Bytes("pear"), "r1", 4);
        var lattice = Seeded("k", seed);

        var delta = await CaptureDeltaAsync(lattice, "k", a => a.AddAsync(Bytes("apple"), "r2"));

        Assert.That(delta.Tombstones, Is.Empty,
            "Adding 'apple' must not cancel a remove authored against 'pear'.");
    }

    // --- causal-dot counter arithmetic across all three maps ---

    [Test]
    public async Task AddAsync_mints_a_counter_above_the_replicas_highest_add_dot()
    {
        var element = Bytes("apple");
        var seed = new RwSet();
        seed.Add(element, "r1", 5);
        var lattice = Seeded("k", seed);

        var delta = await CaptureDeltaAsync(lattice, "k", a => a.AddAsync(element, "r1"));

        Assert.That(delta.Adds[0].Counter, Is.EqualTo(6));
    }

    [Test]
    public async Task AddAsync_mints_a_counter_above_the_replicas_highest_remove_dot()
    {
        // A replica that has only ever removed still has causal history. Minting
        // from the adds map alone would re-issue counter 1, which after a merge
        // is the same dot as the existing remove - so the add would be read as
        // cancelling a remove it never observed.
        var element = Bytes("apple");
        var seed = new RwSet();
        seed.Remove(element, "r1", 11);
        var lattice = Seeded("k", seed);

        var delta = await CaptureDeltaAsync(lattice, "k", a => a.AddAsync(element, "r1"));

        Assert.That(delta.Adds[0].Counter, Is.EqualTo(12),
            "The next dot must clear every dot this replica has authored in the removes map.");
    }

    [Test]
    public async Task AddAsync_mints_a_counter_above_the_replicas_highest_tombstone_dot()
    {
        // The tombstones map records remove dots an earlier add already
        // cancelled. They are still dots this replica authored, so the counter
        // has to clear them too - otherwise a later add reuses a counter that a
        // peer still holds live in its own removes map.
        var element = Bytes("apple");
        var seed = new RwSet();
        seed.Tombstones[SlotKey(element)] = [new OrSetDot { ReplicaId = "r1", Counter = 21 }];
        var lattice = Seeded("k", seed);

        var delta = await CaptureDeltaAsync(lattice, "k", a => a.AddAsync(element, "r1"));

        Assert.That(delta.Adds[0].Counter, Is.EqualTo(22),
            "The next dot must clear every dot this replica has authored in the tombstones map.");
    }

    [Test]
    public async Task AddAsync_ignores_dots_authored_by_other_replicas()
    {
        // Positive control for the three tests above: a counter derived from
        // the whole set rather than from this replica's own dots would pass all
        // of them and still be wrong, because two replicas advance
        // independently.
        var element = Bytes("apple");
        var seed = new RwSet();
        seed.Add(element, "other", 40);
        seed.Remove(element, "other", 41);
        seed.Tombstones[SlotKey(Bytes("pear"))] = [new OrSetDot { ReplicaId = "other", Counter = 42 }];
        var lattice = Seeded("k", seed);

        var delta = await CaptureDeltaAsync(lattice, "k", a => a.AddAsync(element, "r1"));

        Assert.That(delta.Adds[0].Counter, Is.EqualTo(1),
            "A replica's counter space is its own; another replica's dots must not advance it.");
    }

    [Test]
    public async Task RemoveAsync_mints_its_counter_from_the_same_causal_history()
    {
        // Removes and adds share one per-replica counter space, so a remove
        // must clear the replica's add dots as well as its own.
        var element = Bytes("apple");
        var seed = new RwSet();
        seed.Add(element, "r1", 7);
        var lattice = Seeded("k", seed);

        var delta = await CaptureDeltaAsync(lattice, "k", a => a.RemoveAsync(element, "r1"));

        Assert.That(delta.Removes[0].Counter, Is.EqualTo(8));
    }

    // --- TTL seam ---

    [Test]
    public async Task AddAsync_with_a_ttl_routes_through_the_ttl_carrying_seam()
    {
        var lattice = Empty("k");
        var ttl = TimeSpan.FromMinutes(5);

        await lattice.RwSet("k").AddAsync(Bytes("apple"), "r1", ttl);

        await lattice.Received(1).ApplyCrdtDeltaAsync(
            "k", LatticeMergeMode.RwSet, Arg.Any<byte[]>(), ttl, Arg.Any<CancellationToken>());
        await lattice.DidNotReceive().ApplyCrdtDeltaAsync(
            "k", LatticeMergeMode.RwSet, Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
    }

    // --- membership and enumeration ---

    [Test]
    public async Task ContainsAsync_reports_membership_of_the_decoded_set()
    {
        var seed = new RwSet();
        seed.Add(Bytes("apple"), "r1", 1);
        var accessor = Seeded("k", seed).RwSet("k");

        Assert.Multiple(async () =>
        {
            Assert.That(await accessor.ContainsAsync(Bytes("apple")), Is.True);
            Assert.That(await accessor.ContainsAsync(Bytes("pear")), Is.False);
        });
    }

    [Test]
    public async Task ToListAsync_returns_the_live_members()
    {
        var seed = new RwSet();
        seed.Add(Bytes("a"), "r1", 1);
        seed.Add(Bytes("b"), "r1", 2);
        seed.Remove(Bytes("b"), "r2", 1);

        var list = await Seeded("k", seed).RwSet("k").ToListAsync();

        Assert.That(list.Select(Convert.ToBase64String), Is.EquivalentTo(new[] { SlotKey(Bytes("a")) }),
            "A removed element must not enumerate - remove wins over the add it observed.");
    }

    [Test]
    public async Task ToListAsync_on_an_absent_key_returns_an_empty_list()
    {
        var list = await Empty("k").RwSet("k").ToListAsync();

        Assert.That(list, Is.Empty);
    }

    // --- out-of-band merge ---

    [Test]
    public async Task MergeAsync_flattens_all_three_dot_maps_of_the_supplied_state()
    {
        // The merge seam exists for replication consumers that already hold a
        // computed state. All three maps have to survive the flattening: losing
        // the tombstones alone would resurrect elements a peer had removed.
        var element = Bytes("apple");
        var other = new RwSet();
        other.Add(element, "r1", 1);
        other.Remove(Bytes("pear"), "r2", 2);
        other.Tombstones[SlotKey(Bytes("plum"))] = [new OrSetDot { ReplicaId = "r3", Counter = 3 }];
        var lattice = Empty("k");

        var delta = await CaptureDeltaAsync(lattice, "k", a => a.MergeAsync(other));

        Assert.Multiple(() =>
        {
            Assert.That(delta.Adds, Has.Count.EqualTo(1));
            Assert.That(delta.Adds[0].Element, Is.EqualTo(element));
            Assert.That(delta.Removes, Has.Count.EqualTo(1));
            Assert.That(delta.Removes[0].Element, Is.EqualTo(Bytes("pear")));
            Assert.That(delta.Tombstones, Has.Count.EqualTo(1));
            Assert.That(delta.Tombstones[0].ReplicaId, Is.EqualTo("r3"));
        });
    }

    [Test]
    public async Task MergeAsync_of_an_empty_state_produces_an_empty_delta()
    {
        var lattice = Empty("k");

        var delta = await CaptureDeltaAsync(lattice, "k", a => a.MergeAsync(new RwSet()));

        Assert.Multiple(() =>
        {
            Assert.That(delta.Adds, Is.Empty);
            Assert.That(delta.Removes, Is.Empty);
            Assert.That(delta.Tombstones, Is.Empty);
        });
    }

    // --- guards ---

    [Test]
    public void AddAsync_rejects_a_null_element()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.RwSet("k").AddAsync(null!, "r1"),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void AddAsync_rejects_an_empty_replica_id()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.RwSet("k").AddAsync(Bytes("a"), string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void AddAsync_with_a_ttl_rejects_a_null_element()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.RwSet("k").AddAsync(null!, "r1", TimeSpan.FromMinutes(1)),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void AddAsync_with_a_ttl_rejects_an_empty_replica_id()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.RwSet("k").AddAsync(Bytes("a"), string.Empty, TimeSpan.FromMinutes(1)),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void RemoveAsync_rejects_a_null_element()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.RwSet("k").RemoveAsync(null!, "r1"),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void RemoveAsync_rejects_an_empty_replica_id()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.RwSet("k").RemoveAsync(Bytes("a"), string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ContainsAsync_rejects_a_null_element()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.RwSet("k").ContainsAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void MergeAsync_rejects_a_null_state()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.RwSet("k").MergeAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void AddAsync_rejects_a_maxAttempts_below_one()
    {
        var lattice = Empty("k");
        Assert.That(async () => await lattice.RwSet("k").AddAsync(Bytes("a"), "r1", maxAttempts: 0),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void A_default_accessor_is_uninitialised_and_says_so()
    {
        // `default(RwSetAccessor)` carries a null lattice. Without the guard the
        // first hop would surface as a bare NullReferenceException, which does
        // not tell the caller they skipped ILattice.RwSet(key).
        Assert.That(async () => await default(RwSetAccessor).GetAsync(),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains("uninitialised"));
    }

    [Test]
    public void AddAsync_honours_a_cancelled_token()
    {
        var lattice = Empty("k");
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(async () => await lattice.RwSet("k").AddAsync(Bytes("a"), "r1", cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
