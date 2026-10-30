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
            new SchemaTreeCacheGenerations(),
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
            new SchemaTreeCacheGenerations(),
            new LatticeMutation { TreeId = treeId, Key = key! }, SchemaConstants.PolicyTree);

        Assert.That(cache["orders"], Is.EqualTo("cached"));
    }

    [Test]
    public void CacheIfCurrent_installs_value_when_no_invalidation_happened()
    {
        var cache = new ConcurrentDictionary<string, string?>(StringComparer.Ordinal);
        var generations = new SchemaTreeCacheGenerations();
        var captured = generations.Capture("orders");

        cache.CacheIfCurrent(generations, "orders", captured, "loaded");

        Assert.That(cache["orders"], Is.EqualTo("loaded"));
    }

    [Test]
    public void CacheIfCurrent_overtaken_by_invalidate_does_not_install_value()
    {
        var cache = new ConcurrentDictionary<string, string?>(StringComparer.Ordinal);
        var generations = new SchemaTreeCacheGenerations();
        var captured = generations.Capture("orders");

        cache.Invalidate(generations, "orders");
        cache.CacheIfCurrent(generations, "orders", captured, "stale");

        Assert.That(cache.ContainsKey("orders"), Is.False);
    }

    [Test]
    public void CacheIfCurrent_overtaken_by_matching_mutation_does_not_install_value()
    {
        var cache = new ConcurrentDictionary<string, string?>(StringComparer.Ordinal);
        var generations = new SchemaTreeCacheGenerations();
        var captured = generations.Capture("orders");

        cache.InvalidateForMutation(
            generations, new LatticeMutation { TreeId = SchemaConstants.PolicyTree, Key = "orders" }, SchemaConstants.PolicyTree);
        cache.CacheIfCurrent(generations, "orders", captured, "stale");

        Assert.That(cache.ContainsKey("orders"), Is.False);
    }

    [Test]
    public void Invalidate_removes_entry_and_advances_generation()
    {
        var cache = new ConcurrentDictionary<string, string?>(StringComparer.Ordinal);
        var generations = new SchemaTreeCacheGenerations();
        cache.CacheIfSpaceAvailable("orders", "cached");
        var captured = generations.Capture("orders");

        cache.Invalidate(generations, "orders");

        Assert.That(cache.ContainsKey("orders"), Is.False);
        Assert.That(generations.IsCurrent("orders", captured), Is.False);
    }

    [Test]
    public void InvalidateAll_clears_every_entry_and_overtakes_every_in_flight_load()
    {
        var cache = new ConcurrentDictionary<string, string?>(StringComparer.Ordinal);
        var generations = new SchemaTreeCacheGenerations();
        cache.CacheIfSpaceAvailable("orders", "cached");
        cache.CacheIfSpaceAvailable("invoices", null);
        var captured = Enumerable.Range(0, 200).Select(i => ("tree-" + i, generations.Capture("tree-" + i))).ToArray();

        cache.InvalidateAll(generations);
        foreach (var (treeId, generation) in captured)
        {
            cache.CacheIfCurrent(generations, treeId, generation, "stale");
        }

        Assert.That(cache, Is.Empty);
    }
}
