using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.Tests.UI.Suggestions;

/// <summary>Matching typed text: exact first, then prefix, then contains, bounded, and reusing the values.</summary>
[TestFixture]
public sealed class SuggestionMatcherTests
{
    private static readonly LtSuggestion[] Values =
    [
        new("billing/crm-export"),
        new("crm/customers"),
        new("crm/orders"),
        new("crm"),
        new("CRM/legacy"),
    ];

    [Test]
    public void The_exact_value_comes_first_then_prefixes_then_contains()
    {
        var set = SuggestionMatcher.Match(Values, "crm", 10);

        Assert.That(set.Items.Select(item => item.Value), Is.EqualTo(new[] { "crm", "crm/customers", "crm/orders", "CRM/legacy", "billing/crm-export" }));
    }

    [Test]
    public void The_answer_is_bounded_and_says_more_matched()
    {
        var set = SuggestionMatcher.Match(Values, "crm", 2);

        Assert.Multiple(() =>
        {
            Assert.That(set.Items.Select(item => item.Value), Is.EqualTo(new[] { "crm", "crm/customers" }));
            Assert.That(set.Truncated, Is.True);
        });
    }

    [Test]
    public void Empty_text_lists_the_first_values_in_order()
    {
        var set = SuggestionMatcher.Match(Values, string.Empty, 2);

        Assert.That(set.Items.Select(item => item.Value), Is.EqualTo(new[] { "billing/crm-export", "crm/customers" }));
    }

    [Test]
    public void The_values_own_instances_are_reused()
    {
        var set = SuggestionMatcher.Match(Values, "crm/orders", 1);

        Assert.That(set.Items.Single(), Is.SameAs(Values[2]));
    }

    [Test]
    public void Nothing_matching_is_the_empty_answer()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SuggestionMatcher.Match(Values, "zzz", 5), Is.SameAs(LtSuggestionSet.Empty));
            Assert.That(SuggestionMatcher.Match(Values, "crm", 0), Is.SameAs(LtSuggestionSet.Empty));
            Assert.That(SuggestionMatcher.Match([], "crm", 5), Is.SameAs(LtSuggestionSet.Empty));
        });
    }
}
