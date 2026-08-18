namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Symbols;

/// <summary>
/// Unit tests for <see cref="SymbolNaming"/>, the shared derivation of unqualified
/// names and test-subject names from the syntactic fully-qualified names the C#
/// extractor produces. Both the reverse-index writer and the graph reader key off
/// these, so the two must agree on every edge case pinned here.
/// </summary>
[TestFixture]
public sealed class SymbolNamingTests
{
    [TestCase("N.Foo", "Foo")]
    [TestCase("Foo", "Foo")]
    [TestCase("A.B.C.Widget", "Widget")]
    [TestCase("N.Foo<T>", "Foo")]
    [TestCase("N.Outer<T>.FooTests", "FooTests")]
    [TestCase("N.Map<TKey, TValue>", "Map")]
    public void SimpleName_returns_the_last_segment_without_generic_arity(string fqName, string expected)
        => Assert.That(SymbolNaming.SimpleName(fqName), Is.EqualTo(expected));

    [Test]
    public void SimpleName_null_throws()
        => Assert.That(() => SymbolNaming.SimpleName(null!), Throws.InstanceOf<ArgumentNullException>());

    [TestCase("N.FooTests", "Foo")]
    [TestCase("N.FooTest", "Foo")]
    [TestCase("Acme.Widgets.GadgetTests", "Gadget")]
    [TestCase("N.Outer.WidgetTests", "Widget")]
    public void TestSubject_strips_the_test_suffix(string fqName, string expected)
        => Assert.That(SymbolNaming.TestSubject(fqName), Is.EqualTo(expected));

    [TestCase("N.Foo")]
    [TestCase("N.Service")]
    [TestCase("N.Tests")]
    [TestCase("N.Test")]
    public void TestSubject_returns_null_for_a_non_test_or_empty_subject(string fqName)
        => Assert.That(SymbolNaming.TestSubject(fqName), Is.Null);
}
