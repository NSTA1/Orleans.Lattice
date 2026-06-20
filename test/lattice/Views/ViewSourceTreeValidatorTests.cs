using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>Unit tests for <see cref="ViewSourceTreeValidator"/>.</summary>
[TestFixture]
public class ViewSourceTreeValidatorTests
{
    [Test]
    public void ThrowIfViewTree_accepts_a_directly_writable_source()
    {
        Assert.That(() => ViewSourceTreeValidator.ThrowIfViewTree("people"), Throws.Nothing);
    }

    [Test]
    public void ThrowIfViewTree_rejects_a_view_tree_source()
    {
        Assert.That(
            () => ViewSourceTreeValidator.ThrowIfViewTree("view-adults"),
            Throws.InvalidOperationException.With.Message.Contains("view-adults"));
    }

    [Test]
    public void ThrowIfViewTree_rejects_the_bare_prefix()
    {
        Assert.That(
            () => ViewSourceTreeValidator.ThrowIfViewTree("view-"),
            Throws.InvalidOperationException);
    }

    [Test]
    public void ThrowIfViewTree_null_source_throws()
    {
        Assert.That(() => ViewSourceTreeValidator.ThrowIfViewTree(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ThrowIfViewTree_empty_source_throws()
    {
        Assert.That(() => ViewSourceTreeValidator.ThrowIfViewTree(string.Empty), Throws.ArgumentException);
    }
}
