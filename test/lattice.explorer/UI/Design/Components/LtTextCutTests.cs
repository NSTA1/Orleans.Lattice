using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

/// <summary>
/// The clip helper keeps whole characters: a cut that would fall between the two halves
/// of a surrogate pair steps back before it, so a clipped preview never ends in a lone
/// surrogate that the page draws as the replacement character.
/// </summary>
[TestFixture]
public sealed class LtTextCutTests
{
    private const string Emoji = "\U0001F600";

    [TestCase("abcdef", 3, "abc")]
    [TestCase("abc", 3, "abc")]
    [TestCase("abc", 10, "abc")]
    [TestCase("abc", 0, "")]
    [TestCase("abc", -1, "")]
    [TestCase("", 2, "")]
    public void Prefix_keeps_at_most_the_length_asked_for(string text, int length, string expected) =>
        Assert.That(LtTextCut.Prefix(text, length).ToString(), Is.EqualTo(expected));

    [Test]
    public void Prefix_never_ends_in_the_first_half_of_a_surrogate_pair()
    {
        var text = "ab" + Emoji + "cd";

        Assert.Multiple(() =>
        {
            Assert.That(LtTextCut.Prefix(text, 3).ToString(), Is.EqualTo("ab"), "a cut between the halves keeps neither");
            Assert.That(LtTextCut.Prefix(text, 4).ToString(), Is.EqualTo("ab" + Emoji), "a cut after the pair keeps it whole");
            Assert.That(LtTextCut.Prefix(text, 2).ToString(), Is.EqualTo("ab"));
            Assert.That(LtTextCut.Prefix(Emoji, 1).ToString(), Is.Empty);
        });
    }

    [Test]
    public void Prefix_rejects_null_text() =>
        Assert.Throws<ArgumentNullException>(() => LtTextCut.Prefix(null!, 1));
}
