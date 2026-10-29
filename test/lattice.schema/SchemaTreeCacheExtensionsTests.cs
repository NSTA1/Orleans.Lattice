using System.Collections.Concurrent;

namespace Orleans.Lattice.Schema.Tests;

[TestFixture]
public sealed class SchemaTreeCacheExtensionsTests
{
    [Test]
    public void CacheIfSpaceAvailable_full_cache_refreshes_existing_entry_but_refuses_new_entry()
    {
        var cache = new ConcurrentDictionary<string, string?>(StringComparer.Ordinal);
        for (var i = 0; i < SchemaTreeCacheExtensions.MaxCachedTrees; i++)
        {
            cache.CacheIfSpaceAvailable("tree-" + i, null);
        }

        cache.CacheIfSpaceAvailable("tree-0", "updated");
        cache.CacheIfSpaceAvailable("overflow", "not cached");

        Assert.That(cache.Count, Is.EqualTo(SchemaTreeCacheExtensions.MaxCachedTrees));
        Assert.That(cache["tree-0"], Is.EqualTo("updated"));
        Assert.That(cache.ContainsKey("overflow"), Is.False);
    }

    [Test]
    public void CacheIfSpaceAvailable_nullable_value_type_preserves_null_and_default_value()
    {
        var cache = new ConcurrentDictionary<string, LatticeSchemaVersionConfig?>(StringComparer.Ordinal);

        cache.CacheIfSpaceAvailable("unversioned", null);
        cache.CacheIfSpaceAvailable("versioned", default(LatticeSchemaVersionConfig));

        Assert.That(cache.TryGetValue("unversioned", out var missing), Is.True);
        Assert.That(missing, Is.Null);
        Assert.That(cache["versioned"], Is.EqualTo(default(LatticeSchemaVersionConfig)));
    }

    [TestCase(SchemaConstants.PolicyTree)]
    [TestCase(SchemaConstants.VersionConfigTree)]
    public void InvalidateForMutation_matching_source_removes_only_governed_tree(string sourceTree)
    {
        var cache = new ConcurrentDictionary<string, string?>(StringComparer.Ordinal);
        cache.CacheIfSpaceAvailable("orders", null);
        cache.CacheIfSpaceAvailable("Orders", "other");

        cache.InvalidateForMutation(
            new LatticeMutation { TreeId = sourceTree, Key = "orders" }, sourceTree);

        Assert.That(cache.ContainsKey("orders"), Is.False);
        Assert.That(cache["Orders"], Is.EqualTo("other"));
    }

    [TestCase("sys-schema-version", "orders")]
    [TestCase("SYS-SCHEMA-POLICY", "orders")]
    [TestCase("sys-schema-policy", "")]
    [TestCase("sys-schema-policy", null)]
    public void InvalidateForMutation_unrelated_source_or_empty_key_preserves_entries(string treeId, string? key)
    {
        var cache = new ConcurrentDictionary<string, string?>(StringComparer.Ordinal);
        cache.CacheIfSpaceAvailable("orders", "cached");

        cache.InvalidateForMutation(
            new LatticeMutation { TreeId = treeId, Key = key! }, SchemaConstants.PolicyTree);

        Assert.That(cache["orders"], Is.EqualTo("cached"));
    }
}
