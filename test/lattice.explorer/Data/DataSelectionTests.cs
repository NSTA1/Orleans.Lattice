using Orleans.Lattice.Explorer.Core.Data;

namespace Orleans.Lattice.Explorer.Tests.Data;

/// <summary>
/// Direct unit tests for the <see cref="DataSelection"/> session-store key naming,
/// so the shared Data / History selection key is covered on its own.
/// </summary>
[TestFixture]
public class DataSelectionTests
{
    [Test]
    public void SelectedKey_composes_prefixed_key()
    {
        Assert.That(DataSelection.SelectedKey("orders"), Is.EqualTo("data-selected-key:orders"));
    }

    [Test]
    public void SelectedKey_null_tree_throws()
    {
        Assert.That(() => DataSelection.SelectedKey(null!), Throws.ArgumentNullException);
    }
}
