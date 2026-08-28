using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Branch-coverage tests for the per-key dot fold in <see cref="OrSet"/> and
/// <see cref="RwSet"/> <c>MergeMap</c>, which picks between an allocation-free
/// linear membership scan and a HashSet-backed fold off the size of the
/// <em>incoming</em> dot list relative to the internal linear-scan threshold
/// (4). Both branches must produce the same pointwise union, so each pair below
/// drives the same merge either side of the threshold: a large incoming dot
/// list takes the set-backed fold, and a small delta folded into an already
/// long accumulated dot list takes the linear scan.
/// <para>
/// The dot-list shapes here are built directly rather than through
/// <c>Add</c> / <c>Remove</c> because a key only accumulates many concurrent
/// dots through replication, which these tests stand in for.
/// </para>
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class SetMergeDotFoldTests
{
    private const string Key = "ZWxlbQ==";

    private static List<OrSetDot> Dots(string replicaPrefix, int count)
    {
        var list = new List<OrSetDot>(count);
        for (var i = 0; i < count; i++)
            list.Add(new OrSetDot { ReplicaId = replicaPrefix + i, Counter = i });
        return list;
    }

    // ===== OrSet =====

    [Test]
    public void OrSet_MergeFrom_with_large_incoming_dot_list_unions_via_set()
    {
        var left = new OrSet { Adds = { [Key] = Dots("a-", 5) } };
        var right = new OrSet { Adds = { [Key] = Dots("b-", 5) } };

        left.MergeFrom(right);

        Assert.That(left.Adds[Key], Has.Count.EqualTo(10));
        Assert.That(left.Adds[Key], Is.SupersetOf(Dots("a-", 5)));
        Assert.That(left.Adds[Key], Is.SupersetOf(Dots("b-", 5)));
    }

    [Test]
    public void OrSet_MergeFrom_with_small_incoming_dot_list_into_long_history_unions_via_linear_scan()
    {
        var accumulated = Dots("a-", 5);
        var left = new OrSet { Adds = { [Key] = [.. accumulated] } };
        // One dot the target already holds plus two novel ones: the linear
        // branch must de-duplicate exactly as the set-backed fold does.
        var right = new OrSet { Adds = { [Key] = [accumulated[0], .. Dots("b-", 2)] } };

        left.MergeFrom(right);

        Assert.That(left.Adds[Key], Has.Count.EqualTo(7));
        Assert.That(left.Adds[Key], Is.SupersetOf(accumulated));
        Assert.That(left.Adds[Key], Is.SupersetOf(Dots("b-", 2)));
    }

    [Test]
    public void OrSet_MergeFrom_is_commutative_across_the_fold_threshold()
    {
        var accumulated = Dots("a-", 5);
        List<OrSetDot> delta = [accumulated[0], .. Dots("b-", 2)];

        var linear = new OrSet { Adds = { [Key] = [.. accumulated] } };
        linear.MergeFrom(new OrSet { Adds = { [Key] = [.. delta] } });

        // The mirrored merge folds the 5-dot side in, crossing the threshold
        // onto the set-backed branch: the union must be the same either way.
        var viaSet = new OrSet { Adds = { [Key] = [.. delta] } };
        viaSet.MergeFrom(new OrSet { Adds = { [Key] = [.. accumulated] } });

        Assert.That(viaSet.Adds[Key], Is.EquivalentTo(linear.Adds[Key]));
    }

    // ===== RwSet =====

    [Test]
    public void RwSet_MergeFrom_with_large_incoming_dot_list_unions_via_set()
    {
        var left = new RwSet
        {
            Adds = { [Key] = Dots("a-", 5) },
            Removes = { [Key] = Dots("ra-", 5) },
        };
        var right = new RwSet
        {
            Adds = { [Key] = Dots("b-", 5) },
            Removes = { [Key] = Dots("rb-", 5) },
        };

        left.MergeFrom(right);

        Assert.That(left.Adds[Key], Has.Count.EqualTo(10));
        Assert.That(left.Removes[Key], Has.Count.EqualTo(10));
    }

    [Test]
    public void RwSet_MergeFrom_with_small_incoming_dot_list_into_long_history_unions_via_linear_scan()
    {
        var accumulated = Dots("ra-", 5);
        var left = new RwSet { Removes = { [Key] = [.. accumulated] } };
        var right = new RwSet { Removes = { [Key] = [accumulated[0], .. Dots("rb-", 2)] } };

        left.MergeFrom(right);

        Assert.That(left.Removes[Key], Has.Count.EqualTo(7));
        Assert.That(left.Removes[Key], Is.SupersetOf(accumulated));
        Assert.That(left.Removes[Key], Is.SupersetOf(Dots("rb-", 2)));
    }

    [Test]
    public void RwSet_MergeFrom_is_commutative_across_the_fold_threshold()
    {
        var accumulated = Dots("ra-", 5);
        List<OrSetDot> delta = [accumulated[0], .. Dots("rb-", 2)];

        var linear = new RwSet { Removes = { [Key] = [.. accumulated] } };
        linear.MergeFrom(new RwSet { Removes = { [Key] = [.. delta] } });

        var viaSet = new RwSet { Removes = { [Key] = [.. delta] } };
        viaSet.MergeFrom(new RwSet { Removes = { [Key] = [.. accumulated] } });

        Assert.That(viaSet.Removes[Key], Is.EquivalentTo(linear.Removes[Key]));
    }
}
