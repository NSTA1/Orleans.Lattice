using Orleans.Lattice.Explorer.Backup;

namespace Orleans.Lattice.Explorer.Tests.Backup;

/// <summary>
/// The Backups plugin's scope vocabulary. The list scope is the bare tree id and
/// every operation scope carries a suffix, which is what lets the plugin gate
/// re-derive "at least one tree still grants me list access" from the keyed
/// store instead of remembering that one once did.
/// </summary>
[TestFixture]
public class BackupsPluginKeysTests
{
    [Test]
    public void ListScope_is_the_bare_tree_id()
    {
        Assert.That(BackupsPluginKeys.ListScope("tree-a"), Is.EqualTo("tree-a"));
    }

    [Test]
    public void Every_operation_scope_carries_its_own_suffix()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BackupsPluginKeys.CaptureScope("tree-a"), Is.EqualTo("tree-a/capture"));
            Assert.That(
                BackupsPluginKeys.CaptureIncrementalScope("tree-a"),
                Is.EqualTo("tree-a/capture-incremental"));
            Assert.That(BackupsPluginKeys.RestoreScope("tree-a"), Is.EqualTo("tree-a/restore"));
            Assert.That(BackupsPluginKeys.DeleteScope("tree-a"), Is.EqualTo("tree-a/delete"));
        });
    }

    [Test]
    public void IsListScope_accepts_a_list_scope()
    {
        Assert.That(BackupsPluginKeys.IsListScope(BackupsPluginKeys.ListScope("tree-a")), Is.True);
    }

    [Test]
    public void IsListScope_rejects_every_operation_scope()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BackupsPluginKeys.IsListScope(BackupsPluginKeys.CaptureScope("tree-a")), Is.False);
            Assert.That(
                BackupsPluginKeys.IsListScope(BackupsPluginKeys.CaptureIncrementalScope("tree-a")),
                Is.False,
                "the incremental suffix must not be read as the capture suffix, or the other way round");
            Assert.That(BackupsPluginKeys.IsListScope(BackupsPluginKeys.RestoreScope("tree-a")), Is.False);
            Assert.That(BackupsPluginKeys.IsListScope(BackupsPluginKeys.DeleteScope("tree-a")), Is.False);
        });
    }

    [Test]
    public void Null_arguments_throw()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => BackupsPluginKeys.ListScope(null!), Throws.ArgumentNullException);
            Assert.That(() => BackupsPluginKeys.CaptureScope(null!), Throws.ArgumentNullException);
            Assert.That(() => BackupsPluginKeys.CaptureIncrementalScope(null!), Throws.ArgumentNullException);
            Assert.That(() => BackupsPluginKeys.RestoreScope(null!), Throws.ArgumentNullException);
            Assert.That(() => BackupsPluginKeys.DeleteScope(null!), Throws.ArgumentNullException);
            Assert.That(() => BackupsPluginKeys.IsListScope(null!), Throws.ArgumentNullException);
        });
    }
}
