using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>A source's answer: available with its bounded matches, or unavailable with a reason.</summary>
[TestFixture]
public sealed class LtSuggestionSetTests
{
    [Test]
    public void An_available_answer_carries_its_items_and_whether_more_matched()
    {
        var set = LtSuggestionSet.Of([new LtSuggestion("crm/orders", "Tree")], truncated: true);

        Assert.Multiple(() =>
        {
            Assert.That(set.IsAvailable, Is.True);
            Assert.That(set.UnavailableReason, Is.Null);
            Assert.That(set.Truncated, Is.True);
            Assert.That(set.Items.Single().Detail, Is.EqualTo("Tree"));
        });
    }

    [Test]
    public void No_matches_is_the_shared_empty_answer()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LtSuggestionSet.Of([]), Is.SameAs(LtSuggestionSet.Empty));
            Assert.That(LtSuggestionSet.Empty.IsAvailable, Is.True);
            Assert.That(LtSuggestionSet.Empty.Items, Is.Empty);
        });
    }

    [Test]
    public void An_unavailable_answer_names_why_and_has_no_items()
    {
        var set = LtSuggestionSet.Unavailable("No directory.");

        Assert.Multiple(() =>
        {
            Assert.That(set.IsAvailable, Is.False);
            Assert.That(set.UnavailableReason, Is.EqualTo("No directory."));
            Assert.That(set.Items, Is.Empty);
        });
    }

    [Test]
    public void Find_matches_a_value_exactly_and_case_sensitively()
    {
        var set = LtSuggestionSet.Of([new LtSuggestion("Orders"), new LtSuggestion("orders")]);

        Assert.Multiple(() =>
        {
            Assert.That(set.Find("orders")!.Value, Is.EqualTo("orders"));
            Assert.That(set.Find("ORDERS"), Is.Null);
        });
    }

    [Test]
    public void Arguments_are_validated()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => LtSuggestionSet.Of(null!), Throws.ArgumentNullException);
            Assert.That(() => LtSuggestionSet.Unavailable(" "), Throws.ArgumentException);
            Assert.That(() => LtSuggestionSet.Empty.Find(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void A_suggestion_is_a_value_with_an_optional_detail()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new LtSuggestion("eu-west").Detail, Is.Null);
            Assert.That(new LtSuggestion("eu-west", "This region"), Is.EqualTo(new LtSuggestion("eu-west", "This region")));
            Assert.That(Enum.GetNames<LtComboBoxMode>(), Is.EqualTo(new[] { "PickExisting", "Suggest" }));
        });
    }
}
