using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// The canonical-case rule every route segment and preference key is held to.
/// </summary>
[TestFixture]
public sealed class ExplorerRouteSlugTests
{
    [Test]
    public void IsCanonical_LowerCaseAscii_IsTrue()
    {
        Assert.That(ExplorerRouteSlug.IsCanonical("tag-indexes"), Is.True);
    }

    [Test]
    public void IsCanonical_DigitsHyphenUnderscoreAndDot_AreAllowed()
    {
        Assert.That(ExplorerRouteSlug.IsCanonical("shell.all-tenants_2"), Is.True);
    }

    [TestCase("Trees")]
    [TestCase("tagIndexes")]
    [TestCase("TAG")]
    public void IsCanonical_AnyUpperCase_IsFalse(string value)
    {
        Assert.That(ExplorerRouteSlug.IsCanonical(value), Is.False);
    }

    [TestCase("with space")]
    [TestCase("with/slash")]
    [TestCase("with?query")]
    [TestCase("with%20escape")]
    public void IsCanonical_NonSlugCharacter_IsFalse(string value)
    {
        Assert.That(ExplorerRouteSlug.IsCanonical(value), Is.False);
    }

    [Test]
    public void IsCanonical_Null_IsFalse()
    {
        Assert.That(ExplorerRouteSlug.IsCanonical(null), Is.False);
    }

    [Test]
    public void IsCanonical_Empty_IsFalse()
    {
        Assert.That(ExplorerRouteSlug.IsCanonical(string.Empty), Is.False);
    }

    [Test]
    public void Normalize_UpperCase_FoldsToLowerCase()
    {
        Assert.That(ExplorerRouteSlug.Normalize("TagIndexes"), Is.EqualTo("tagindexes"));
    }

    [Test]
    public void Normalize_NonSlugCharacters_BecomeHyphens()
    {
        Assert.That(ExplorerRouteSlug.Normalize("a b/c"), Is.EqualTo("a-b-c"));
    }

    [Test]
    public void Normalize_AlreadyCanonical_ReturnsTheSameInstance()
    {
        var value = string.Concat("tag", "-indexes");

        // The parse path runs this on every segment, so the canonical case must
        // not allocate a copy.
        Assert.That(ExplorerRouteSlug.Normalize(value), Is.SameAs(value));
    }

    [Test]
    public void Normalize_Null_ReturnsEmpty()
    {
        Assert.That(ExplorerRouteSlug.Normalize(null), Is.EqualTo(string.Empty));
    }

    [Test]
    public void Normalize_Empty_ReturnsEmpty()
    {
        Assert.That(ExplorerRouteSlug.Normalize(string.Empty), Is.EqualTo(string.Empty));
    }

    [Test]
    public void EnsureCanonical_CanonicalValue_DoesNotThrow()
    {
        Assert.That(() => ExplorerRouteSlug.EnsureCanonical("explore"), Throws.Nothing);
    }

    [Test]
    public void EnsureCanonical_UpperCaseValue_Throws()
    {
        Assert.That(
            () => ExplorerRouteSlug.EnsureCanonical("Explore"),
            Throws.ArgumentException.With.Message.Contains("lower-case"));
    }

    [Test]
    public void EnsureCanonical_Null_Throws()
    {
        Assert.That(() => ExplorerRouteSlug.EnsureCanonical(null), Throws.ArgumentException);
    }

    [Test]
    public void FromIdentifier_DottedPluginId_TakesTheLastSegment()
    {
        Assert.That(ExplorerRouteSlug.FromIdentifier("orleans.lattice.data"), Is.EqualTo("data"));
    }

    [Test]
    public void FromIdentifier_UndottedIdentifier_NormalizesTheWholeValue()
    {
        Assert.That(ExplorerRouteSlug.FromIdentifier("DeadLetter"), Is.EqualTo("deadletter"));
    }

    [Test]
    public void FromIdentifier_TrailingDot_FallsBackToTheWholeValue()
    {
        Assert.That(ExplorerRouteSlug.FromIdentifier("data."), Is.EqualTo("data."));
    }

    [Test]
    public void FromIdentifier_Null_ReturnsEmpty()
    {
        Assert.That(ExplorerRouteSlug.FromIdentifier(null), Is.EqualTo(string.Empty));
    }

    [Test]
    public void FromIdentifier_Empty_ReturnsEmpty()
    {
        Assert.That(ExplorerRouteSlug.FromIdentifier(string.Empty), Is.EqualTo(string.Empty));
    }
}
