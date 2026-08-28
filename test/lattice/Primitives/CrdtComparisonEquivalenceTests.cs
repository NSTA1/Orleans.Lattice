using System.Globalization;

namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Pins the two convergence-critical properties that the CRDT primitive
/// performance work in this change depends on:
/// <list type="number">
/// <item>the vectorized byte comparison that now backs
/// <c>Rga.CompareBytes</c> and <c>MvRegister.CompareValueBytes</c> agrees in
/// <em>sign</em> with the scalar loop it replaced, for every input shape; and</item>
/// <item><see cref="OrSetDot"/>'s counter-first equality override stays
/// consistent with its synthesized <c>GetHashCode</c>.</item>
/// </list>
/// Both are safety nets for optimizations whose correctness rests on an
/// equivalence rather than on a behaviour change, so a regression here would be
/// silent: dots would stop deduplicating, or two replicas would break a
/// same-dot value collision in opposite directions and never converge.
/// </summary>
[TestFixture]
public sealed class CrdtComparisonEquivalenceTests
{
    // The scalar loop exactly as it read before the vectorization, kept here as
    // the oracle the shipped implementation is compared against.
    private static int ScalarCompare(byte[] a, byte[] b)
    {
        var min = Math.Min(a.Length, b.Length);
        for (var i = 0; i < min; i++)
        {
            var c = a[i].CompareTo(b[i]);
            if (c != 0) return c;
        }
        return a.Length.CompareTo(b.Length);
    }

    private static int Sign(int value) => value < 0 ? -1 : value > 0 ? 1 : 0;

    [Test]
    public void Vectorized_byte_compare_agrees_in_sign_with_the_scalar_loop_over_random_pairs()
    {
        var random = new Random(20240613);
        for (var trial = 0; trial < 20_000; trial++)
        {
            // Short buffers drawn from a tiny alphabet so equal bytes, common
            // prefixes, and outright equality all occur often rather than
            // essentially never as they would with full-width random bytes.
            var left = new byte[random.Next(0, 9)];
            var right = new byte[random.Next(0, 9)];
            for (var i = 0; i < left.Length; i++) left[i] = (byte)random.Next(0, 3);
            for (var i = 0; i < right.Length; i++) right[i] = (byte)random.Next(0, 3);

            var expected = Sign(ScalarCompare(left, right));
            var actual = Sign(((ReadOnlySpan<byte>)left).SequenceCompareTo(right));

            Assert.That(
                actual,
                Is.EqualTo(expected),
                $"trial {trial.ToString(CultureInfo.InvariantCulture)}: " +
                $"[{string.Join(",", left)}] vs [{string.Join(",", right)}]");
        }
    }

    [Test]
    public void Vectorized_byte_compare_agrees_in_sign_with_the_scalar_loop_on_boundary_shapes()
    {
        (byte[] Left, byte[] Right)[] cases =
        [
            ([], []),
            ([], [0]),
            ([0], []),
            ([0], [0]),
            ([1], [2]),
            ([2], [1]),
            ([1, 2], [1, 2, 3]),          // proper prefix, shorter first
            ([1, 2, 3], [1, 2]),          // proper prefix, longer first
            ([1, 2, 3], [1, 2, 4]),       // equal length, differs in last byte
            ([0xFF], [0x00]),             // unsigned ordering, not signed
            ([0x00], [0xFF]),
            ([0x80, 0x00], [0x7F, 0xFF]), // high-bit byte must sort above
            (new byte[64], new byte[64]), // beyond one vector width, equal
        ];

        foreach (var (left, right) in cases)
        {
            var expected = Sign(ScalarCompare(left, right));
            var actual = Sign(((ReadOnlySpan<byte>)left).SequenceCompareTo(right));
            Assert.That(actual, Is.EqualTo(expected), $"[{string.Join(",", left)}] vs [{string.Join(",", right)}]");
        }
    }

    [Test]
    public void Vectorized_byte_compare_sorts_a_long_shared_prefix_on_its_first_difference()
    {
        // The shape the vector path is specifically for: a long common prefix
        // where the scalar loop must walk every byte before deciding.
        var left = new byte[1024];
        var right = new byte[1024];
        right[1023] = 1;

        Assert.That(Sign(((ReadOnlySpan<byte>)left).SequenceCompareTo(right)), Is.EqualTo(-1));
        Assert.That(Sign(((ReadOnlySpan<byte>)right).SequenceCompareTo(left)), Is.EqualTo(1));
        Assert.That(Sign(ScalarCompare(left, right)), Is.EqualTo(-1));
    }

    [Test]
    public void Dot_equality_matches_member_wise_comparison_over_random_pairs()
    {
        var random = new Random(20240614);
        string[] replicas = ["r", "r", "r-1", "r-2", "", "R"];

        for (var trial = 0; trial < 20_000; trial++)
        {
            var left = new OrSetDot { ReplicaId = replicas[random.Next(replicas.Length)], Counter = random.Next(0, 4) };
            var right = new OrSetDot { ReplicaId = replicas[random.Next(replicas.Length)], Counter = random.Next(0, 4) };

            var expected = string.Equals(left.ReplicaId, right.ReplicaId, StringComparison.Ordinal)
                && left.Counter == right.Counter;

            Assert.That(left.Equals(right), Is.EqualTo(expected), $"trial {trial.ToString(CultureInfo.InvariantCulture)}");
            Assert.That(left == right, Is.EqualTo(expected));
            Assert.That(left != right, Is.EqualTo(!expected));
        }
    }

    [Test]
    public void Dot_equality_is_reflexive_symmetric_and_hash_consistent()
    {
        var a = new OrSetDot { ReplicaId = "r-1", Counter = 7 };
        var b = new OrSetDot { ReplicaId = "r-1", Counter = 7 };
        var c = new OrSetDot { ReplicaId = "r-1", Counter = 8 };
        var d = new OrSetDot { ReplicaId = "r-2", Counter = 7 };

        Assert.That(a.Equals(a), Is.True, "reflexive");
        Assert.That(a.Equals(b) && b.Equals(a), Is.True, "symmetric");
        Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()), "equal dots must hash equal");
        Assert.That(a.Equals(c), Is.False, "counter discriminates");
        Assert.That(a.Equals(d), Is.False, "replica id discriminates");
    }

    [Test]
    public void Dot_equality_still_discriminates_on_replica_id_when_counters_collide()
    {
        // The exact case the reordering must not break: two replicas that both
        // authored counter 1. Counter-first short-circuits on equality here, so
        // the replica-id comparison is what has to decide.
        var set = new HashSet<OrSetDot>
        {
            new() { ReplicaId = "a", Counter = 1 },
            new() { ReplicaId = "b", Counter = 1 },
        };

        Assert.That(set, Has.Count.EqualTo(2));
        Assert.That(set.Contains(new OrSetDot { ReplicaId = "a", Counter = 1 }), Is.True);
        Assert.That(set.Contains(new OrSetDot { ReplicaId = "c", Counter = 1 }), Is.False);
    }

    [Test]
    public void Dot_equality_treats_a_null_replica_id_as_distinct_from_empty()
    {
        var withNull = new OrSetDot { ReplicaId = null!, Counter = 1 };
        var withEmpty = new OrSetDot { ReplicaId = string.Empty, Counter = 1 };

        Assert.That(withNull.Equals(withEmpty), Is.False);
        Assert.That(withNull.Equals(new OrSetDot { ReplicaId = null!, Counter = 1 }), Is.True);
    }
}
