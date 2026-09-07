using System.Linq.Expressions;

namespace Orleans.Lattice.GrainIndex.Tests.Query;

/// <summary>
/// The three output-identical folds on the plan-and-execute path: reading a
/// comparison's constant side without invoking the expression compiler,
/// distributing <c>&amp;&amp;</c> over <c>||</c> in place when one side is a
/// single conjunction, and de-duplicating a union's grains through a span probe
/// over the raw entry key.
/// <para>
/// Each fold replaces a general mechanism with a cheaper one on the shapes that
/// dominate, so what these tests pin is that the cheap path and the general path
/// agree - including on the awkward roots (static, nested, boxed, null-valued,
/// property-with-a-getter) that decide which of the two runs.
/// </para>
/// </summary>
public sealed partial class GrainIndexQueryTests
{
    private const int SharedThreshold = 18;

    private static int StaticThreshold => 30;

    // ---- constant folding -------------------------------------------------

    [Test]
    public async Task Where_captured_field_of_a_nested_closure_reads_the_same_bound()
    {
        // Two nested captures put the bound two member reads deep inside the
        // display-class chain, which is the recursive case of the fold.
        var index = Populated();
        int outer = 18;

        async Task<List<string>> QueryAsync()
        {
            int inner = outer;
            return await KeysAsync(index.Index.Where(s => s.Age >= inner && s.Age < outer + 100));
        }

        Assert.That(await QueryAsync(), Is.EquivalentTo(new[] { "bob", "carol", "dave" }));
    }

    [Test]
    public async Task Where_static_field_constant_is_read_without_an_instance()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= SharedThreshold));

        Assert.That(keys, Is.EquivalentTo(new[] { "bob", "carol", "dave" }));
    }

    [Test]
    public async Task Where_static_property_constant_is_read_through_its_getter()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= StaticThreshold));

        Assert.That(keys, Is.EquivalentTo(new[] { "carol", "dave" }));
    }

    [Test]
    public async Task Where_instance_property_constant_is_read_through_its_getter()
    {
        var index = Populated();
        var bound = new Bound { Age = 30, Country = "GB" };

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= bound.Age));

        Assert.That(keys, Is.EquivalentTo(new[] { "carol", "dave" }));
    }

    [Test]
    public async Task Where_reference_constant_read_from_a_closure_matches_by_value()
    {
        var index = Populated();
        var bound = new Bound { Age = 0, Country = "GB" };

        var keys = await KeysAsync(index.Index.Where(s => s.Country == bound.Country));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "carol" }));
    }

    [Test]
    public async Task Where_null_constant_read_from_a_closure_is_routed_as_null()
    {
        // A null-valued captured bound must fold to null rather than fail the
        // read, because null is a legitimate value the range builder routes.
        var index = Populated();
        DateTimeOffset? absent = null;

        var keys = await KeysAsync(index.Index.Where(s => s.LastSeen == absent));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice" }));
    }

    [Test]
    public async Task Where_constant_side_the_fold_cannot_read_still_evaluates()
    {
        // A method call is outside the closed set the fold reads directly, so the
        // planner must fall back to compiling it. Same answer, longer road.
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Country == "gb".ToUpperInvariant()));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "carol" }));
    }

    [Test]
    public async Task Where_parameter_free_atom_folds_away_without_compiling()
    {
        // A constant-true conjunct contributes nothing and a constant-false one
        // kills the conjunction; both are decided by evaluating the atom, which
        // now goes through the same fold.
        var index = Populated();

        var kept = await KeysAsync(index.Index.Where(Conjoin(Expression.Constant(true), 40)));
        var killed = await KeysAsync(index.Index.Where(Conjoin(Expression.Constant(false), 0)));

        Assert.Multiple(() =>
        {
            Assert.That(kept, Is.EquivalentTo(new[] { "dave" }));
            Assert.That(killed, Is.Empty);
        });
    }

    [Test]
    public async Task Where_negated_parameter_free_atom_folds_to_its_complement()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index
            .Where(Conjoin(Expression.Not(Expression.Constant(false)), 40)));

        Assert.That(keys, Is.EquivalentTo(new[] { "dave" }));
    }

    /// <summary>
    /// <c>&lt;parameterFree&gt; &amp;&amp; s.Age &gt;= bound</c>, built by hand so
    /// the constant conjunct survives into the expression tree rather than being
    /// folded away by the C# compiler.
    /// </summary>
    private static Expression<Func<IndexedTestState, bool>> Conjoin(Expression parameterFree, int bound)
    {
        var parameter = Expression.Parameter(typeof(IndexedTestState), "s");
        var body = Expression.AndAlso(
            parameterFree,
            Expression.GreaterThanOrEqual(
                Expression.Property(parameter, nameof(IndexedTestState.Age)),
                Expression.Constant(bound)));

        return Expression.Lambda<Func<IndexedTestState, bool>>(body, parameter);
    }

    // ---- disjunctive normal form -----------------------------------------

    [Test]
    public async Task Where_and_chain_over_three_properties_keeps_every_conjunct()
    {
        // The '&&' chain is the shape distributed in place: each level appends
        // into the conjunction already accumulated rather than rebuilding it, so
        // losing or reordering an atom would show up here.
        var index = Populated();

        var keys = await KeysAsync(index.Index
            .Where(s => s.Age >= 18 && s.Country == "GB" && s.Status == TestStatus.Active));

        Assert.That(keys, Is.EquivalentTo(new[] { "carol" }));
    }

    [Test]
    public async Task Where_and_distributed_over_a_union_on_the_right_keeps_both_branches()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index
            .Where(s => s.Age >= 18 && (s.Country == "GB" || s.Country == "DE")));

        Assert.That(keys, Is.EquivalentTo(new[] { "carol", "dave" }));
    }

    [Test]
    public async Task Where_and_distributed_over_a_union_on_the_left_keeps_both_branches()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index
            .Where(s => (s.Country == "GB" || s.Country == "DE") && s.Age >= 18));

        Assert.That(keys, Is.EquivalentTo(new[] { "carol", "dave" }));
    }

    [Test]
    public async Task Where_union_distributed_over_a_union_produces_the_full_cross_product()
    {
        // Both sides are unions, so neither in-place shortcut applies and the
        // general cross product runs. Four conjunctions, one per pairing.
        var index = Populated();

        var keys = await KeysAsync(index.Index
            .Where(s => (s.Country == "GB" || s.Country == "DE")
                && (s.Age >= 30 || s.Status == TestStatus.Active)));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "carol", "dave" }));
    }

    [Test]
    public async Task Where_nested_and_chains_under_a_union_stay_independent()
    {
        // Distributing in place mutates one operand list, so a shared or aliased
        // conjunction would leak atoms across branches. Each branch here narrows
        // to a different single grain, which no cross-contamination survives.
        var index = Populated();

        var keys = await KeysAsync(index.Index
            .Where(s => (s.Age >= 40 && s.Country == "DE")
                || (s.Age >= 18 && s.Age < 20 && s.Country == "FR")));

        Assert.That(keys, Is.EquivalentTo(new[] { "bob", "dave" }));
    }

    [Test]
    public void Where_still_rejects_a_predicate_that_expands_past_the_conjunction_ceiling()
    {
        // The ceiling is checked before the in-place shortcuts, so an expansion
        // that would exceed it must still throw rather than be folded quietly.
        var index = Populated();

        var parameter = Expression.Parameter(typeof(IndexedTestState), "s");
        Expression Union(int offset) => Expression.OrElse(
            Expression.Equal(
                Expression.Property(parameter, nameof(IndexedTestState.Age)),
                Expression.Constant(offset)),
            Expression.Equal(
                Expression.Property(parameter, nameof(IndexedTestState.Age)),
                Expression.Constant(offset + 1)));

        // Seven nested binary unions distribute to 2^7 = 128 conjunctions.
        var body = Enumerable.Range(0, 7)
            .Select(i => Union(i * 2))
            .Aggregate(Expression.AndAlso);

        var predicate = Expression.Lambda<Func<IndexedTestState, bool>>(body, parameter);

        Assert.Throws<NotSupportedException>(() => index.Index.Where(predicate));
    }

    // ---- union de-duplication --------------------------------------------

    [Test]
    public async Task Union_reports_a_grain_matching_every_branch_exactly_once()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index
            .Where(s => s.Country == "GB" || s.Age >= 18 || s.Status == TestStatus.Active));

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EquivalentTo(new[] { "alice", "bob", "carol", "dave" }));
            Assert.That(keys, Has.Count.EqualTo(4));
        });
    }

    [Test]
    public async Task Union_does_not_confuse_a_grain_key_that_suffixes_another()
    {
        // The de-duplication set is probed with a span sliced out of the raw
        // entry key. "bob" suffixes "xbob", so a probe anchored anywhere but
        // immediately after the second separator would collapse the two into one
        // and drop a genuine result.
        var index = QueryTestIndex.Create(
            ("bob", QueryTestIndex.State(age: 20, country: "GB")),
            ("xbob", QueryTestIndex.State(age: 20, country: "FR")));

        var keys = await KeysAsync(index.Index.Where(s => s.Country == "GB" || s.Country == "FR"));

        Assert.That(keys, Is.EquivalentTo(new[] { "bob", "xbob" }));
    }

    [Test]
    public async Task Union_keeps_grain_keys_that_prefix_one_another_distinct()
    {
        var index = QueryTestIndex.Create(
            ("a", QueryTestIndex.State(age: 20, country: "GB")),
            ("ab", QueryTestIndex.State(age: 21, country: "GB")),
            ("abc", QueryTestIndex.State(age: 22, country: "FR")));

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= 20 || s.Country == "GB"));

        Assert.That(keys, Is.EquivalentTo(new[] { "a", "ab", "abc" }));
    }

    [Test]
    public async Task Union_reports_the_grain_key_the_de_duplication_set_interned()
    {
        // The key handed out by the union path is the instance the insert
        // created, so it has to carry the grain key verbatim - not an empty or
        // truncated string.
        var index = Populated();

        var matches = new List<string>();
        await foreach (string key in index.Index
            .Where(s => s.Country == "GB" || s.Country == "DE")
            .ToKeysAsync())
        {
            matches.Add(key);
        }

        Assert.That(matches, Is.EquivalentTo(new[] { "alice", "carol", "dave" }));
    }

    [Test]
    public async Task Union_carrying_payloads_still_reports_each_grain_once_with_its_entry()
    {
        // The payload branch keeps the materialised-match shape, so it needs its
        // own coverage: the span path is key-only by construction.
        var index = Populated();

        var matches = new List<GrainIndexMatch>();
        await foreach (var match in index.Index
            .Where(s => s.Country == "GB" || s.Age >= 30)
            .ToMatchesAsync())
        {
            matches.Add(match);
        }

        Assert.Multiple(() =>
        {
            Assert.That(matches.Select(m => m.GrainKey), Is.EquivalentTo(new[] { "alice", "carol", "dave" }));
            Assert.That(matches, Has.All.Property(nameof(GrainIndexMatch.Value)).Not.Empty);
        });
    }

    [Test]
    public async Task Union_under_stream_execution_de_duplicates_identically()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index
            .Where(s => s.Country == "GB" || s.Age >= 18)
            .WithExecution(GrainIndexQueryExecution.Stream));

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EquivalentTo(new[] { "alice", "bob", "carol", "dave" }));
            Assert.That(index.Tree.CursorsOpened, Is.Zero);
        });
    }

    [Test]
    public async Task Union_over_a_small_page_size_closes_every_cursor_it_opened()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index
            .Where(s => s.Country == "GB" || s.Age >= 18)
            .WithPageSize(1));

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EquivalentTo(new[] { "alice", "bob", "carol", "dave" }));
            Assert.That(index.Tree.OpenCursors, Is.Empty);
        });
    }

    [Test]
    public async Task Union_mixing_a_single_clause_branch_with_an_intersect_branch_shares_one_set()
    {
        // The two branches feed the same de-duplication set through different
        // entry points - one span-probed, one string-keyed - so a grain matching
        // both must still be reported once.
        var index = Populated();

        var keys = await KeysAsync(index.Index
            .Where(s => s.Country == "GB" || (s.Age >= 18 && s.Status == TestStatus.Active)));

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EquivalentTo(new[] { "alice", "carol" }));
            Assert.That(keys, Has.Count.EqualTo(2));
        });
    }

    private sealed class Bound
    {
        internal int Age { get; init; }

        internal string Country { get; init; } = string.Empty;
    }
}
