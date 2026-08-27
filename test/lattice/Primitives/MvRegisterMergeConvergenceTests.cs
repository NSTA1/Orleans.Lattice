namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Convergence regressions for <see cref="MvRegister"/>. When two replicas
/// reuse one <c>(replicaId, counter)</c> dot but stamp different value bytes
/// under it, the dot is still present on both sides, so both
/// <see cref="MvRegister.MergeFrom(MvRegister)"/> and
/// <see cref="MvRegister.MergeDelta(MvRegisterDelta)"/> keep it - but they must
/// resolve the divergent value deterministically, or the merge stops being
/// commutative even though the type documents itself "commutative, associative,
/// and idempotent". The tie-break keeps the lexicographically greater value
/// bytes. A <c>replicaId</c> is a caller-supplied string with no minted-once
/// guarantee, so this collision is reachable rather than theoretical.
/// </summary>
[TestFixture]
public class MvRegisterMergeConvergenceTests
{
    private static byte[] Bytes(int k) => [(byte)k];

    private static string Hex(byte[] value) => Convert.ToHexString(value);

    private static string Observe(MvRegister register) =>
        string.Join(",", register.Values().Select(Hex).OrderBy(static s => s, StringComparer.Ordinal));

    private static MvRegister RegisterWith(string replicaId, byte[] value)
    {
        var register = new MvRegister();
        register.Set(replicaId, value);
        return register;
    }

    private static MvRegisterDelta DeltaFor(string replicaId, long counter, byte[] value) => new()
    {
        Entries = [new MvRegisterEntry { ReplicaId = replicaId, Counter = counter, Value = value }],
        Context = new Dictionary<string, long>(StringComparer.Ordinal) { [replicaId] = counter },
    };

    [Test]
    public void MergeFrom_is_commutative_under_a_same_dot_value_collision()
    {
        // Both registers start empty and write under "r1", so both mint the dot
        // (r1, 1) but with divergent bytes - the collision the tie-break must
        // resolve identically in either merge order.
        var low = Bytes(1);
        var high = Bytes(2);

        var ab = Observe(MvRegister.Merge(RegisterWith("r1", low), RegisterWith("r1", high)));
        var ba = Observe(MvRegister.Merge(RegisterWith("r1", high), RegisterWith("r1", low)));

        Assert.Multiple(() =>
        {
            Assert.That(ab, Is.EqualTo(ba), "MergeFrom must be commutative on a same-dot value collision");
            Assert.That(ab, Is.EqualTo(Hex(high)), "the deterministic winner is the greater value bytes");
        });
    }

    [Test]
    public void MergeDelta_agrees_with_MergeFrom_and_is_order_independent()
    {
        var low = Bytes(1);
        var high = Bytes(2);

        // Two deltas carrying the same dot (r1, 1) with divergent values.
        var deltaLow = DeltaFor("r1", 1, low);
        var deltaHigh = DeltaFor("r1", 1, high);

        var lowThenHigh = new MvRegister();
        lowThenHigh.MergeDelta(deltaLow);
        lowThenHigh.MergeDelta(deltaHigh);

        var highThenLow = new MvRegister();
        highThenLow.MergeDelta(deltaHigh);
        highThenLow.MergeDelta(deltaLow);

        var fullState = Observe(MvRegister.Merge(RegisterWith("r1", low), RegisterWith("r1", high)));

        Assert.Multiple(() =>
        {
            Assert.That(Observe(lowThenHigh), Is.EqualTo(Observe(highThenLow)),
                "MergeDelta must be order-independent on a same-dot value collision");
            Assert.That(Observe(lowThenHigh), Is.EqualTo(fullState),
                "a delta fold must converge to the same value a full-state merge produces");
            Assert.That(Observe(lowThenHigh), Is.EqualTo(Hex(high)),
                "the deterministic winner is the greater value bytes");
        });
    }
}
