using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// The extension point for surface state in the URL: an immutable, key-sorted
/// parameter set with value equality.
/// </summary>
[TestFixture]
public sealed class ExplorerRouteParametersTests
{
    [Test]
    public void Empty_HasNoEntries()
    {
        Assert.That(ExplorerRouteParameters.Empty.Count, Is.Zero);
    }

    [Test]
    public void Create_Null_ReturnsEmpty()
    {
        Assert.That(ExplorerRouteParameters.Create(null), Is.SameAs(ExplorerRouteParameters.Empty));
    }

    [Test]
    public void Create_EmptySequence_ReturnsEmpty()
    {
        Assert.That(ExplorerRouteParameters.Create([]), Is.SameAs(ExplorerRouteParameters.Empty));
    }

    [Test]
    public void Create_SortsByKey()
    {
        var parameters = ExplorerRouteParameters.Create(
        [
            new ExplorerRouteParameter("zeta", "1"),
            new ExplorerRouteParameter("alpha", "2"),
        ]);

        Assert.Multiple(() =>
        {
            Assert.That(parameters[0].Key, Is.EqualTo("alpha"));
            Assert.That(parameters[1].Key, Is.EqualTo("zeta"));
        });
    }

    [Test]
    public void Create_RepeatedKey_KeepsTheLastValue()
    {
        var parameters = ExplorerRouteParameters.Create(
        [
            new ExplorerRouteParameter("page", "1"),
            new ExplorerRouteParameter("page", "2"),
        ]);

        Assert.Multiple(() =>
        {
            Assert.That(parameters.Count, Is.EqualTo(1));
            Assert.That(parameters.GetValueOrEmpty("page"), Is.EqualTo("2"));
        });
    }

    [Test]
    public void Parameter_UpperCaseKey_Throws()
    {
        Assert.That(() => new ExplorerRouteParameter("Page", "1"), Throws.ArgumentException);
    }

    [Test]
    public void Parameter_NullValue_BecomesEmpty()
    {
        Assert.That(new ExplorerRouteParameter("page", null!).Value, Is.EqualTo(string.Empty));
    }

    [Test]
    public void With_NewKey_AddsIt()
    {
        var parameters = ExplorerRouteParameters.Empty.With("page", "3");

        Assert.That(parameters.GetValueOrEmpty("page"), Is.EqualTo("3"));
    }

    [Test]
    public void With_SameKeyAndValue_ReturnsTheSameInstance()
    {
        var parameters = ExplorerRouteParameters.Empty.With("page", "3");

        Assert.That(parameters.With("page", "3"), Is.SameAs(parameters));
    }

    [Test]
    public void With_EmptyValue_RemovesTheKey()
    {
        var parameters = ExplorerRouteParameters.Empty.With("page", "3").With("page", string.Empty);

        Assert.That(parameters.Count, Is.Zero);
    }

    [Test]
    public void With_NullValue_RemovesTheKey()
    {
        var parameters = ExplorerRouteParameters.Empty.With("page", "3").With("page", null);

        Assert.That(parameters.Count, Is.Zero);
    }

    [Test]
    public void With_UpperCaseKey_Throws()
    {
        Assert.That(() => ExplorerRouteParameters.Empty.With("Page", "3"), Throws.ArgumentException);
    }

    [Test]
    public void Without_AbsentKey_ReturnsTheSameInstance()
    {
        var parameters = ExplorerRouteParameters.Empty.With("page", "3");

        Assert.That(parameters.Without("other"), Is.SameAs(parameters));
    }

    [Test]
    public void Without_Null_ReturnsTheSameInstance()
    {
        var parameters = ExplorerRouteParameters.Empty.With("page", "3");

        Assert.That(parameters.Without(null), Is.SameAs(parameters));
    }

    [Test]
    public void Without_OnlyKey_ReturnsEmpty()
    {
        var parameters = ExplorerRouteParameters.Empty.With("page", "3");

        Assert.That(parameters.Without("page"), Is.SameAs(ExplorerRouteParameters.Empty));
    }

    [Test]
    public void Without_OneOfSeveral_KeepsTheRest()
    {
        var parameters = ExplorerRouteParameters.Empty
            .With("page", "3")
            .With("filter", "x")
            .Without("page");

        Assert.Multiple(() =>
        {
            Assert.That(parameters.Count, Is.EqualTo(1));
            Assert.That(parameters.GetValueOrEmpty("filter"), Is.EqualTo("x"));
        });
    }

    [Test]
    public void TryGetValue_AbsentKey_ReturnsFalseAndEmpty()
    {
        var found = ExplorerRouteParameters.Empty.TryGetValue("page", out var value);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.False);
            Assert.That(value, Is.EqualTo(string.Empty));
        });
    }

    [Test]
    public void TryGetValue_NullKey_ReturnsFalse()
    {
        Assert.That(ExplorerRouteParameters.Empty.TryGetValue(null, out _), Is.False);
    }

    [Test]
    public void GetValueOrEmpty_AbsentKey_ReturnsEmpty()
    {
        Assert.That(ExplorerRouteParameters.Empty.GetValueOrEmpty("page"), Is.EqualTo(string.Empty));
    }

    [Test]
    public void Equals_SameContentInDifferentOrder_IsTrue()
    {
        var left = ExplorerRouteParameters.Empty.With("a", "1").With("b", "2");
        var right = ExplorerRouteParameters.Empty.With("b", "2").With("a", "1");

        Assert.Multiple(() =>
        {
            Assert.That(left, Is.EqualTo(right));
            Assert.That(left.GetHashCode(), Is.EqualTo(right.GetHashCode()));
        });
    }

    [Test]
    public void Equals_DifferentValue_IsFalse()
    {
        var left = ExplorerRouteParameters.Empty.With("a", "1");
        var right = ExplorerRouteParameters.Empty.With("a", "2");

        Assert.That(left, Is.Not.EqualTo(right));
    }

    [Test]
    public void Equals_DifferentCount_IsFalse()
    {
        var left = ExplorerRouteParameters.Empty.With("a", "1");
        var right = left.With("b", "2");

        Assert.That(left, Is.Not.EqualTo(right));
    }

    [Test]
    public void Equals_Null_IsFalse()
    {
        Assert.That(ExplorerRouteParameters.Empty.Equals(null), Is.False);
    }

    [Test]
    public void Equals_NonParameterObject_IsFalse()
    {
        Assert.That(ExplorerRouteParameters.Empty.Equals("not a parameter set"), Is.False);
    }

    [Test]
    public void Enumerator_WalksEveryEntryInKeyOrder()
    {
        var parameters = ExplorerRouteParameters.Empty.With("b", "2").With("a", "1");

        var keys = new List<string>();
        foreach (var parameter in parameters)
        {
            keys.Add(parameter.Key);
        }

        Assert.That(keys, Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public void Enumerator_ThroughTheInterface_WalksEveryEntry()
    {
        IEnumerable<ExplorerRouteParameter> parameters =
            ExplorerRouteParameters.Empty.With("b", "2").With("a", "1");

        Assert.That(parameters.Select(static p => p.Key), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public void Enumerator_ThroughTheNonGenericInterface_WalksEveryEntry()
    {
        System.Collections.IEnumerable parameters = ExplorerRouteParameters.Empty.With("a", "1");

        var count = 0;
        foreach (var _ in parameters)
        {
            count++;
        }

        Assert.That(count, Is.EqualTo(1));
    }

    [Test]
    public void Indexer_ReturnsTheEntryAtThatPosition()
    {
        var parameters = ExplorerRouteParameters.Empty.With("a", "1");

        Assert.That(parameters[0].Value, Is.EqualTo("1"));
    }
}
