using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Branch-coverage tests for <see cref="OrFlag"/> and <see cref="RwFlag"/>
/// exercising the HashSet-backed union / live-count fallbacks that only run
/// once a dot list grows past the internal linear-scan threshold (4). The
/// common-case linear paths are covered elsewhere; these drive the same
/// operations with larger dot sets so the set-based branches execute.
/// <para>
/// The union helpers pick their branch off the <em>incoming</em> side alone, so
/// the merge tests below size the incoming dot list either side of the
/// threshold and assert both branches produce the same union: a large incoming
/// list takes the set-based fold, and a small one folded into an already-large
/// target takes the allocation-free linear scan.
/// </para>
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class FlagLargeDotSetTests
{
    private static List<OrSetDot> Dots(string replicaPrefix, int count)
    {
        var list = new List<OrSetDot>(count);
        for (var i = 0; i < count; i++)
            list.Add(new OrSetDot { ReplicaId = replicaPrefix + i, Counter = i });
        return list;
    }

    // ===== OrFlag =====

    [Test]
    public void OrFlag_Disable_with_large_tombstone_set_tombstones_all_enables()
    {
        var flag = new OrFlag
        {
            Enables = Dots("enable-", 3),
            Tombstones = Dots("tomb-", 5),
        };

        var changed = flag.Disable();

        Assert.That(changed, Is.True);
        Assert.That(flag.IsEnabled, Is.False, "every enable dot must be tombstoned via the set-based path.");
    }

    [Test]
    public void OrFlag_IsEnabled_with_large_tombstone_set_counts_live_enables()
    {
        var enables = Dots("e-", 6);
        // Tombstone the first three enables, plus a couple of unrelated dots,
        // so the tombstone list exceeds the linear-scan threshold.
        var tombstones = new List<OrSetDot> { enables[0], enables[1], enables[2] };
        tombstones.AddRange(Dots("t-", 3));
        var flag = new OrFlag { Enables = enables, Tombstones = tombstones };

        Assert.That(flag.IsEnabled, Is.True, "three enable dots remain live under the set-based live count.");
    }

    [Test]
    public void OrFlag_MergeFrom_with_large_dot_lists_unions_via_set()
    {
        var left = new OrFlag { Enables = Dots("a-", 5) };
        var right = new OrFlag { Enables = Dots("b-", 5) };

        left.MergeFrom(right);

        Assert.That(left.Enables, Has.Count.EqualTo(10));
    }

    [Test]
    public void OrFlag_MergeDelta_with_large_delta_unions_via_set()
    {
        // The incoming side selects the branch: 5 delta dots exceed the
        // threshold, so the union goes through the HashSet fold.
        var flag = new OrFlag { Enables = Dots("a-", 5) };
        var delta = new OrFlagDelta { Enables = Dots("b-", 5), Disables = [] };

        flag.MergeDelta(delta);

        Assert.That(flag.Enables, Has.Count.EqualTo(10));
    }

    [Test]
    public void OrFlag_MergeDelta_with_small_delta_into_large_target_unions_via_linear_scan()
    {
        // The mirror case: an accumulated target past the threshold absorbing a
        // 1-2-dot delta stays on the allocation-free linear path and must union
        // identically, including de-duplicating a dot the target already holds.
        var enables = Dots("a-", 5);
        var flag = new OrFlag { Enables = [.. enables] };
        var delta = new OrFlagDelta { Enables = [enables[0], .. Dots("b-", 2)], Disables = [] };

        flag.MergeDelta(delta);

        Assert.That(flag.Enables, Has.Count.EqualTo(7));
        Assert.That(flag.Enables, Is.SupersetOf(enables));
        Assert.That(flag.Enables, Is.SupersetOf(Dots("b-", 2)));
    }

    // ===== RwFlag =====

    [Test]
    public void RwFlag_Enable_with_large_tombstone_set_tombstones_disables()
    {
        var flag = new RwFlag
        {
            Disables = Dots("disable-", 3),
            Tombstones = Dots("tomb-", 5),
        };

        var changed = flag.Enable("writer", 1);

        Assert.That(changed, Is.True, "prior disable dots must be tombstoned via the set-based path.");
        Assert.That(flag.IsEnabled, Is.True, "a fresh enable with all disables tombstoned leaves the flag enabled.");
    }

    [Test]
    public void RwFlag_IsEnabled_with_large_tombstone_set_counts_live_disables()
    {
        var disables = Dots("d-", 6);
        // Tombstone every disable dot plus extras so the flag is enabled and the
        // tombstone list crosses the linear-scan threshold.
        var tombstones = new List<OrSetDot>(disables);
        tombstones.AddRange(Dots("t-", 2));
        var flag = new RwFlag
        {
            Enables = Dots("e-", 1),
            Disables = disables,
            Tombstones = tombstones,
        };

        Assert.That(flag.IsEnabled, Is.True, "all disables tombstoned under the set-based live count leaves the enable winning.");
    }

    [Test]
    public void RwFlag_MergeFrom_with_large_dot_lists_unions_via_set()
    {
        var left = new RwFlag { Enables = Dots("a-", 5), Disables = Dots("da-", 5) };
        var right = new RwFlag { Enables = Dots("b-", 5), Disables = Dots("db-", 5) };

        left.MergeFrom(right);

        Assert.That(left.Enables, Has.Count.EqualTo(10));
        Assert.That(left.Disables, Has.Count.EqualTo(10));
    }

    [Test]
    public void RwFlag_MergeDelta_with_large_delta_unions_via_set()
    {
        // The incoming side selects the branch: 5 delta dots exceed the
        // threshold, so the union goes through the HashSet fold.
        var flag = new RwFlag { Enables = Dots("a-", 5), Disables = Dots("da-", 5) };
        var delta = new RwFlagDelta { Enables = Dots("b-", 5), Disables = Dots("db-", 5), Tombstones = [] };

        flag.MergeDelta(delta);

        Assert.That(flag.Enables, Has.Count.EqualTo(10));
        Assert.That(flag.Disables, Has.Count.EqualTo(10));
    }

    [Test]
    public void RwFlag_MergeDelta_with_small_delta_into_large_target_unions_via_linear_scan()
    {
        // The mirror case: an accumulated target past the threshold absorbing a
        // 1-2-dot delta stays on the allocation-free linear path and must union
        // identically, including de-duplicating dots the target already holds.
        var enables = Dots("a-", 5);
        var disables = Dots("da-", 5);
        var flag = new RwFlag { Enables = [.. enables], Disables = [.. disables] };
        var delta = new RwFlagDelta
        {
            Enables = [enables[0], .. Dots("b-", 2)],
            Disables = [disables[0], .. Dots("db-", 2)],
            Tombstones = [],
        };

        flag.MergeDelta(delta);

        Assert.That(flag.Enables, Has.Count.EqualTo(7));
        Assert.That(flag.Disables, Has.Count.EqualTo(7));
        Assert.That(flag.Enables, Is.SupersetOf(enables));
        Assert.That(flag.Disables, Is.SupersetOf(disables));
    }
}
