using System.Globalization;
using System.Text;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Randomized join-semilattice law suite over every state-based CRDT primitive.
/// <para>
/// Each primitive already carries one or two hand-picked commutativity /
/// associativity / idempotence tests, but a fixed example only pins the shape
/// its author thought of. The convergence defects actually found in this
/// repository were all shapes nobody had written an example for - concurrent
/// same-dot collisions resolved in opposite directions on two replicas
/// (<see href="https://github.com/NSTA1/Orleans.Lattice/pull/1705">#1705</see>),
/// RGA merge ordering
/// (<see href="https://github.com/NSTA1/Orleans.Lattice/pull/1679">#1679</see>),
/// and the aliasing follow-up
/// (<see href="https://github.com/NSTA1/Orleans.Lattice/pull/1709">#1709</see>).
/// This fixture searches for that class of defect instead of enumerating it: it
/// generates many random <em>reachable</em> states per primitive and asserts the
/// three axioms <see cref="ICrdt{TSelf}.MergeFrom"/> is documented to satisfy,
/// plus inflation (a merge never loses information).
/// </para>
/// <para>
/// Two design points make failures actionable and trustworthy:
/// </para>
/// <list type="bullet">
/// <item>States are built only through the public mutators (<c>Add</c>,
/// <c>Remove</c>, <c>Increment</c>, <c>Enable</c>, <c>Tick</c>, <c>Set</c>, ...)
/// from a small replica/element alphabet, so every generated state is one a real
/// replica could actually reach and concurrent dots collide often. Hand-built
/// states could otherwise fail a law for being unreachable rather than for a
/// real defect.</item>
/// <item>Comparison is over a <em>canonical projection of the whole internal
/// state</em> - every dot list and map rendered in sorted, order-independent
/// form - not just the observable value. Comparing only the observable
/// (<c>IsEnabled</c>, <c>Value</c>, <c>Elements</c>) would let two replicas
/// silently diverge in causal metadata and only differ on some later merge.</item>
/// </list>
/// <para>
/// The PRNG is seeded per-case and the seed plus the offending trial index are
/// reported on failure, so any discovered counterexample is deterministically
/// reproducible.
/// </para>
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class CrdtMergeLawTests
{
    private const int Trials = 300;
    private const int Seed = 0x1AE7;

    // Small alphabets so concurrent dots on the same replica/element collide
    // often. A wide alphabet would make almost every merge a trivial disjoint
    // union and never exercise the reconciliation branches.
    private static readonly string[] Replicas = ["r0", "r1", "r2"];
    private static readonly string[] Elements = ["a", "b", "c"];

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    private static string Element(Random random) => Elements[random.Next(Elements.Length)];

    private static string Replica(Random random) => Replicas[random.Next(Replicas.Length)];

    // ------------------------------------------------------------------
    // Canonical state projections - order-independent renderings of the FULL
    // internal state, so two structurally equal states compare equal whatever
    // order their dot lists happen to be in.
    // ------------------------------------------------------------------

    // Sorted but NOT de-duplicated: a duplicate dot is a real divergence (it
    // inflates the dot list and leaks into Count), so collapsing duplicates
    // here would hide precisely the de-duplication defects this suite exists
    // to find. Order independence needs a sort, not a Distinct.
    private static string Canon(IEnumerable<OrSetDot> dots) =>
        string.Join(",", dots
            .Select(d => $"{d.ReplicaId}:{d.Counter.ToString(CultureInfo.InvariantCulture)}")
            .OrderBy(s => s, StringComparer.Ordinal));

    private static string Canon(Dictionary<string, List<OrSetDot>> map) =>
        string.Join("|", map
            .Where(kv => kv.Value.Count > 0)
            .OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => $"{kv.Key}=>[{Canon(kv.Value)}]"));

    private static string Canon(Dictionary<string, long> map) =>
        string.Join(",", map
            .OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => $"{kv.Key}={kv.Value.ToString(CultureInfo.InvariantCulture)}"));

    private static string CanonState(GCounter value) => $"inc[{Canon(value.Increments)}]";

    private static string CanonState(PnCounter value) =>
        $"inc[{Canon(value.Increments)}] dec[{Canon(value.Decrements)}]";

    private static string CanonState(GSet value) =>
        $"el[{string.Join(",", value.Elements.OrderBy(e => e, StringComparer.Ordinal))}]";

    private static string CanonState(OrSet value) =>
        $"adds[{Canon(value.Adds)}] tomb[{Canon(value.Tombstones)}]";

    private static string CanonState(RwSet value) =>
        $"adds[{Canon(value.Adds)}] rem[{Canon(value.Removes)}] tomb[{Canon(value.Tombstones)}]";

    private static string CanonState(OrFlag value) =>
        $"en[{Canon(value.Enables)}] tomb[{Canon(value.Tombstones)}]";

    private static string CanonState(RwFlag value) =>
        $"en[{Canon(value.Enables)}] dis[{Canon(value.Disables)}] tomb[{Canon(value.Tombstones)}]";

    private static string CanonState(MvRegister value) =>
        "entries[" + string.Join(",", value.Entries
            .Select(e => $"{e.ReplicaId}:{e.Counter.ToString(CultureInfo.InvariantCulture)}:{Convert.ToBase64String(e.Value)}")
            .OrderBy(s => s, StringComparer.Ordinal)) +
        $"] ctx[{Canon(value.Context)}]";

    private static string CanonState(VersionVector value) =>
        "vv[" + string.Join(",", value.Entries
            .OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => $"{kv.Key}={kv.Value.WallClockTicks.ToString(CultureInfo.InvariantCulture)}.{kv.Value.Counter.ToString(CultureInfo.InvariantCulture)}")) +
        "]";

    // ------------------------------------------------------------------
    // Reachable-state generators - public mutators only.
    // ------------------------------------------------------------------

    // Hands out per-replica monotonic dot counters within a single generated
    // state, exactly as a real replica does. This matters for correctness of
    // the suite, not just realism: OrFlag.Enable / OrSet.Add / RwSet.Add
    // append the caller-supplied dot verbatim, so drawing a counter at random
    // lets one state mint the SAME (replica, counter) dot twice - a state no
    // replica can reach. Merge legitimately de-duplicates it, which then looks
    // like a commutativity failure and masks real defects behind noise.
    // Counters still start at 1 for every generated state, so two independently
    // generated states overlap heavily on dots - the concurrent same-dot shape
    // these laws most need to exercise.
    private sealed class DotMinter
    {
        private readonly Dictionary<string, long> _next = [];

        public long Next(string replicaId)
        {
            _next.TryGetValue(replicaId, out var counter);
            counter++;
            _next[replicaId] = counter;
            return counter;
        }
    }

    private static GCounter NewGCounter(Random random)
    {
        var value = new GCounter();
        var ops = random.Next(0, 6);
        for (var i = 0; i < ops; i++) value.Increment(Replica(random), random.Next(1, 5));
        return value;
    }

    private static PnCounter NewPnCounter(Random random)
    {
        var value = new PnCounter();
        var ops = random.Next(0, 6);
        for (var i = 0; i < ops; i++)
        {
            if (random.Next(2) == 0) value.Increment(Replica(random), random.Next(1, 5));
            else value.Decrement(Replica(random), random.Next(1, 5));
        }
        return value;
    }

    private static GSet NewGSet(Random random)
    {
        var value = new GSet();
        var ops = random.Next(0, 6);
        for (var i = 0; i < ops; i++) value.Add(Bytes(Element(random)));
        return value;
    }

    private static OrSet NewOrSet(Random random)
    {
        var value = new OrSet();
        var minter = new DotMinter();
        var ops = random.Next(0, 8);
        for (var i = 0; i < ops; i++)
        {
            if (random.Next(3) == 0) value.Remove(Bytes(Element(random)));
            else
            {
                var replica = Replica(random);
                value.Add(Bytes(Element(random)), replica, minter.Next(replica));
            }
        }
        return value;
    }

    private static RwSet NewRwSet(Random random)
    {
        var value = new RwSet();
        var minter = new DotMinter();
        var ops = random.Next(0, 8);
        for (var i = 0; i < ops; i++)
        {
            var replica = Replica(random);
            var counter = minter.Next(replica);
            if (random.Next(3) == 0) value.Remove(Bytes(Element(random)), replica, counter);
            else value.Add(Bytes(Element(random)), replica, counter);
        }
        return value;
    }

    private static OrFlag NewOrFlag(Random random)
    {
        var value = new OrFlag();
        var minter = new DotMinter();
        var ops = random.Next(0, 6);
        for (var i = 0; i < ops; i++)
        {
            if (random.Next(3) == 0) value.Disable();
            else
            {
                var replica = Replica(random);
                value.Enable(replica, minter.Next(replica));
            }
        }
        return value;
    }

    private static RwFlag NewRwFlag(Random random)
    {
        var value = new RwFlag();
        var minter = new DotMinter();
        var ops = random.Next(0, 6);
        for (var i = 0; i < ops; i++)
        {
            var replica = Replica(random);
            var counter = minter.Next(replica);
            if (random.Next(2) == 0) value.Disable(replica, counter);
            else value.Enable(replica, counter);
        }
        return value;
    }

    private static MvRegister NewMvRegister(Random random)
    {
        var value = new MvRegister();
        var ops = random.Next(0, 5);
        for (var i = 0; i < ops; i++) value.Set(Replica(random), Bytes(Element(random)));
        return value;
    }

    private static VersionVector NewVersionVector(Random random)
    {
        var value = new VersionVector();
        var ops = random.Next(0, 6);
        for (var i = 0; i < ops; i++) value.Tick(Replica(random));
        return value;
    }

    // ------------------------------------------------------------------
    // The law harness.
    // ------------------------------------------------------------------

    // Runs the three join-semilattice axioms plus inflation over `Trials`
    // random states. `clone` is used everywhere a state would otherwise be
    // mutated in place, so a law never observes a receiver another law dirtied.
    private static void AssertMergeLaws<T>(
        string name,
        Func<Random, T> generate,
        Func<T, T> clone,
        Action<T, T> mergeInto,
        Func<T, string> canon)
    {
        var random = new Random(Seed);

        for (var trial = 0; trial < Trials; trial++)
        {
            var a = generate(random);
            var b = generate(random);
            var c = generate(random);

            var where = $"{name} trial {trial.ToString(CultureInfo.InvariantCulture)} (seed {Seed.ToString(CultureInfo.InvariantCulture)})";

            // Commutativity: a . b == b . a
            var ab = clone(a);
            mergeInto(ab, b);
            var ba = clone(b);
            mergeInto(ba, a);
            Assert.That(canon(ba), Is.EqualTo(canon(ab)),
                $"{where}: merge is not commutative.\n  a = {canon(a)}\n  b = {canon(b)}");

            // Associativity: (a . b) . c == a . (b . c)
            var leftAssoc = clone(a);
            mergeInto(leftAssoc, b);
            mergeInto(leftAssoc, c);
            var bc = clone(b);
            mergeInto(bc, c);
            var rightAssoc = clone(a);
            mergeInto(rightAssoc, bc);
            Assert.That(canon(rightAssoc), Is.EqualTo(canon(leftAssoc)),
                $"{where}: merge is not associative.\n  a = {canon(a)}\n  b = {canon(b)}\n  c = {canon(c)}");

            // Idempotence: a . a == a
            var selfMerged = clone(a);
            mergeInto(selfMerged, clone(a));
            Assert.That(canon(selfMerged), Is.EqualTo(canon(a)),
                $"{where}: merge is not idempotent.\n  a = {canon(a)}");

            // Inflation: merging is a least-upper-bound, so re-merging an
            // operand already absorbed must be a no-op. A merge that changed
            // state here would be losing or re-adding information.
            var reabsorbed = clone(ab);
            mergeInto(reabsorbed, b);
            Assert.That(canon(reabsorbed), Is.EqualTo(canon(ab)),
                $"{where}: re-merging an absorbed operand changed the state.\n  a.b = {canon(ab)}\n  b = {canon(b)}");
        }
    }

    [Test]
    public void GCounter_merge_satisfies_the_join_semilattice_laws() =>
        AssertMergeLaws<GCounter>("GCounter", NewGCounter, v => v.Clone(), (x, y) => x.MergeFrom(y), CanonState);

    [Test]
    public void PnCounter_merge_satisfies_the_join_semilattice_laws() =>
        AssertMergeLaws<PnCounter>("PnCounter", NewPnCounter, v => v.Clone(), (x, y) => x.MergeFrom(y), CanonState);

    [Test]
    public void GSet_merge_satisfies_the_join_semilattice_laws() =>
        AssertMergeLaws<GSet>("GSet", NewGSet, v => v.Clone(), (x, y) => x.MergeFrom(y), CanonState);

    [Test]
    public void OrSet_merge_satisfies_the_join_semilattice_laws() =>
        AssertMergeLaws<OrSet>("OrSet", NewOrSet, v => v.Clone(), (x, y) => x.MergeFrom(y), CanonState);

    [Test]
    public void RwSet_merge_satisfies_the_join_semilattice_laws() =>
        AssertMergeLaws<RwSet>("RwSet", NewRwSet, v => v.Clone(), (x, y) => x.MergeFrom(y), CanonState);

    [Test]
    public void OrFlag_merge_satisfies_the_join_semilattice_laws() =>
        AssertMergeLaws<OrFlag>("OrFlag", NewOrFlag, v => v.Clone(), (x, y) => x.MergeFrom(y), CanonState);

    [Test]
    public void RwFlag_merge_satisfies_the_join_semilattice_laws() =>
        AssertMergeLaws<RwFlag>("RwFlag", NewRwFlag, v => v.Clone(), (x, y) => x.MergeFrom(y), CanonState);

    [Test]
    public void MvRegister_merge_satisfies_the_join_semilattice_laws() =>
        AssertMergeLaws<MvRegister>("MvRegister", NewMvRegister, v => v.Clone(), (x, y) => x.MergeFrom(y), CanonState);

    [Test]
    public void VersionVector_merge_satisfies_the_join_semilattice_laws() =>
        AssertMergeLaws<VersionVector>("VersionVector", NewVersionVector, v => v.Clone(), (x, y) => x.MergeFrom(y), CanonState);

    // ------------------------------------------------------------------
    // Replica convergence: the property the laws exist to deliver.
    // ------------------------------------------------------------------

    // Three replicas each receive the same three updates in a different order
    // (the delivery reordering a partition heal produces) and must all land on
    // the same state. This is the end the axioms are a means to, asserted
    // directly so a law that held only for the shapes above cannot hide here.
    private static void AssertReplicasConverge<T>(
        string name,
        Func<Random, T> generate,
        Func<T, T> clone,
        Action<T, T> mergeInto,
        Func<T, string> canon)
    {
        var random = new Random(Seed);

        for (var trial = 0; trial < Trials; trial++)
        {
            var updates = new[] { generate(random), generate(random), generate(random) };
            var orders = new[]
            {
                new[] { 0, 1, 2 },
                new[] { 2, 0, 1 },
                new[] { 1, 2, 0 },
            };

            string? reference = null;
            foreach (var order in orders)
            {
                var replica = clone(updates[order[0]]);
                mergeInto(replica, updates[order[1]]);
                mergeInto(replica, updates[order[2]]);

                var state = canon(replica);
                if (reference is null) reference = state;
                else
                {
                    Assert.That(state, Is.EqualTo(reference),
                        $"{name} trial {trial.ToString(CultureInfo.InvariantCulture)} (seed {Seed.ToString(CultureInfo.InvariantCulture)}): " +
                        $"replicas applying the same updates in order [{string.Join(",", order)}] did not converge.");
                }
            }
        }
    }

    [Test]
    public void GCounter_replicas_converge_under_reordered_delivery() =>
        AssertReplicasConverge<GCounter>("GCounter", NewGCounter, v => v.Clone(), (x, y) => x.MergeFrom(y), CanonState);

    [Test]
    public void PnCounter_replicas_converge_under_reordered_delivery() =>
        AssertReplicasConverge<PnCounter>("PnCounter", NewPnCounter, v => v.Clone(), (x, y) => x.MergeFrom(y), CanonState);

    [Test]
    public void GSet_replicas_converge_under_reordered_delivery() =>
        AssertReplicasConverge<GSet>("GSet", NewGSet, v => v.Clone(), (x, y) => x.MergeFrom(y), CanonState);

    [Test]
    public void OrSet_replicas_converge_under_reordered_delivery() =>
        AssertReplicasConverge<OrSet>("OrSet", NewOrSet, v => v.Clone(), (x, y) => x.MergeFrom(y), CanonState);

    [Test]
    public void RwSet_replicas_converge_under_reordered_delivery() =>
        AssertReplicasConverge<RwSet>("RwSet", NewRwSet, v => v.Clone(), (x, y) => x.MergeFrom(y), CanonState);

    [Test]
    public void OrFlag_replicas_converge_under_reordered_delivery() =>
        AssertReplicasConverge<OrFlag>("OrFlag", NewOrFlag, v => v.Clone(), (x, y) => x.MergeFrom(y), CanonState);

    [Test]
    public void RwFlag_replicas_converge_under_reordered_delivery() =>
        AssertReplicasConverge<RwFlag>("RwFlag", NewRwFlag, v => v.Clone(), (x, y) => x.MergeFrom(y), CanonState);

    [Test]
    public void MvRegister_replicas_converge_under_reordered_delivery() =>
        AssertReplicasConverge<MvRegister>("MvRegister", NewMvRegister, v => v.Clone(), (x, y) => x.MergeFrom(y), CanonState);

    [Test]
    public void VersionVector_replicas_converge_under_reordered_delivery() =>
        AssertReplicasConverge<VersionVector>("VersionVector", NewVersionVector, v => v.Clone(), (x, y) => x.MergeFrom(y), CanonState);
}
