namespace Orleans.Lattice.GrainIndex.Tests.Query;

/// <summary>
/// Pins the single-conjunction fast path in the planner's disjunctive-normal-form
/// lowering. The fast path collects a pure '&amp;&amp;' chain (or its De Morgan dual)
/// straight into one conjunction instead of building a nested singleton pair per
/// atom and folding them away again; anything with a real disjunction under it
/// must still fall through to the general distribute/concatenate recursion.
/// </summary>
public sealed partial class GrainIndexQueryTests
{
    [Test]
    public async Task Where_long_conjunction_chain_lowers_to_one_conjunction()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(
            s => s.Age >= 18 && s.Age < 40 && s.Country == "GB" && s.Status == TestStatus.Active));

        Assert.That(keys, Is.EquivalentTo(new[] { "carol" }));
    }

    [Test]
    public async Task Where_negated_disjunction_lowers_through_de_morgan_to_one_conjunction()
    {
        // !(a || b) is (!a && !b): a pure conjunction under the inherited
        // polarity, so the fast path applies even though the node is an OrElse.
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => !(s.Country == "GB" || s.Age >= 41)));

        Assert.That(keys, Is.EquivalentTo(new[] { "bob" }));
    }

    [Test]
    public async Task Where_negated_conjunction_still_lowers_to_a_disjunction()
    {
        // !(a && b) is (!a || !b), a genuine union, so the fast path must
        // decline and the general recursion must produce two conjunctions.
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => !(s.Country == "GB" && s.Age < 18)));

        Assert.That(keys, Is.EquivalentTo(new[] { "bob", "carol", "dave" }));
    }

    [Test]
    public async Task Where_disjunction_nested_under_a_conjunction_falls_back_to_distribute()
    {
        // The disjunction is on the right of an '&&', so the fast path only
        // discovers it after collecting the left-hand atom - the partially
        // filled list must be discarded rather than leak into the result.
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(
            s => s.Age >= 18 && (s.Country == "GB" || s.Country == "DE")));

        Assert.That(keys, Is.EquivalentTo(new[] { "carol", "dave" }));
    }

    [Test]
    public async Task Where_disjunction_on_the_left_of_a_conjunction_falls_back_to_distribute()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(
            s => (s.Country == "GB" || s.Country == "DE") && s.Age >= 18));

        Assert.That(keys, Is.EquivalentTo(new[] { "carol", "dave" }));
    }

    [Test]
    public async Task Where_conjunction_and_its_reassociated_form_agree()
    {
        // Left- and right-associated '&&' chains reach the fast path through
        // different recursion shapes and must collect the same atom set.
        var index = Populated();

        var left = await KeysAsync(index.Index.Where(
            s => s.Age >= 18 && s.Age < 40 && s.Country == "GB"));
        var right = await KeysAsync(index.Index.Where(
            s => s.Age >= 18 && (s.Age < 40 && s.Country == "GB")));

        Assert.That(right, Is.EquivalentTo(left));
    }

    [Test]
    public async Task Where_single_atom_lowers_to_one_conjunction()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Country == "DE"));

        Assert.That(keys, Is.EquivalentTo(new[] { "dave" }));
    }
}
