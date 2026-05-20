namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Pure-logic tests for the per-batch partition + manifest schema
/// helpers (<c>BuildBatchPartitionKey</c>, <c>BuildManifestPartitionKey</c>,
/// <c>BuildManifestRowKey</c>) and their supporting prefix constants
/// (the per-batch partition + manifest WAL schema). These helpers ship in commit 2a as schema scaffolding;
/// the behavioural rewrite that consumes them lands in 2b - 2d.
/// </summary>
public partial class AzureTableWalStorageProviderTests
{
    [Test]
    public void BatchPartitionPrefix_is_minimal_three_byte_marker()
    {
        // The marker prefixes every batch partition key. A longer
        // marker would add bytes to every row on the wire and at rest;
        // the test pins the minimal compact form (_b_) so a refactor
        // that lengthens the marker triggers an explicit review.
        Assert.That(AzureTableWalStorageProvider.BatchPartitionPrefix, Is.EqualTo("_b_"));
    }

    [Test]
    public void ManifestPartitionPrefix_is_minimal_three_byte_marker()
    {
        Assert.That(AzureTableWalStorageProvider.ManifestPartitionPrefix, Is.EqualTo("_m_"));
    }

    [Test]
    public void ManifestRowKeyPrefix_is_single_letter_M()
    {
        // Each manifest row key is M{start:D19}. Pin the prefix so a
        // change to multi-character prefixes (which would break the
        // M < TAIL ordering invariant the manifest range scan relies
        // on) is caught here.
        Assert.That(AzureTableWalStorageProvider.ManifestRowKeyPrefix, Is.EqualTo("M"));
    }

    [Test]
    public void TailRowKey_constant_is_TAIL_and_sorts_after_every_manifest_row()
    {
        // The shard's tail pointer lives in the manifest partition and
        // must sort strictly after every M{...} row, mirroring the
        // 'HEAD' > 'E' invariant in the per-batch partition. A
        // manifest range scan filters on RowKey lt 'TAIL' to exclude
        // the tail pointer from the batch enumeration.
        var maxStartOffsetRowKey = AzureTableWalStorageProvider.BuildManifestRowKey(long.MaxValue);

        Assert.Multiple(() =>
        {
            Assert.That(AzureTableWalStorageProvider.TailRowKey, Is.EqualTo("TAIL"));
            Assert.That(
                StringComparer.Ordinal.Compare(maxStartOffsetRowKey, AzureTableWalStorageProvider.TailRowKey),
                Is.LessThan(0));
        });
    }

    [Test]
    public void BuildBatchPartitionKey_formats_prefix_tree_shard_and_start_offset()
    {
        var pk = AzureTableWalStorageProvider.BuildBatchPartitionKey("my-tree", 7, 42L);

        Assert.That(pk, Is.EqualTo("_b_|my-tree|7|S0000000000000000042"));
    }

    [Test]
    public void BuildBatchPartitionKey_pads_zero_start_offset_correctly()
    {
        var pk = AzureTableWalStorageProvider.BuildBatchPartitionKey("tree", 0, 0L);

        Assert.That(pk, Is.EqualTo("_b_|tree|0|S0000000000000000000"));
    }

    [Test]
    public void BuildBatchPartitionKey_orders_partition_keys_lexicographically_by_start_offset()
    {
        // The D19 width makes batch partition keys sort
        // lexicographically iff their start offsets sort numerically;
        // ReadAsync's tail scan relies on this so it can stream batches
        // in commit-offset order with a single ascending range query.
        var keys = new[] { 0L, 1L, 9L, 10L, 99L, 100L, 1_000_000L, long.MaxValue }
            .Select(o => AzureTableWalStorageProvider.BuildBatchPartitionKey("tree", 0, o))
            .ToArray();

        var sorted = keys.OrderBy(k => k, StringComparer.Ordinal).ToArray();

        Assert.That(keys, Is.EqualTo(sorted));
    }

    [Test]
    public void BuildBatchPartitionKey_percent_encodes_disallowed_treeId_characters()
    {
        var pk = AzureTableWalStorageProvider.BuildBatchPartitionKey("a/b\\c#d?e", 0, 0L);

        Assert.That(pk, Is.EqualTo("_b_|a%2Fb%5Cc%23d%3Fe|0|S0000000000000000000"));
    }

    [Test]
    public void BuildBatchPartitionKey_throws_on_null_treeId()
    {
        Assert.That(
            () => AzureTableWalStorageProvider.BuildBatchPartitionKey(null!, 0, 0L),
            Throws.ArgumentNullException);
    }

    [Test]
    public void BuildBatchPartitionKey_throws_on_negative_start_offset()
    {
        Assert.That(
            () => AzureTableWalStorageProvider.BuildBatchPartitionKey("tree", 0, -1L),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void BuildBatchPartitionKey_distinguishes_distinct_start_offsets()
    {
        var a = AzureTableWalStorageProvider.BuildBatchPartitionKey("tree", 0, 0L);
        var b = AzureTableWalStorageProvider.BuildBatchPartitionKey("tree", 0, 1L);

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void BuildBatchPartitionKey_distinguishes_distinct_shards()
    {
        var s0 = AzureTableWalStorageProvider.BuildBatchPartitionKey("tree", 0, 42L);
        var s1 = AzureTableWalStorageProvider.BuildBatchPartitionKey("tree", 1, 42L);

        Assert.That(s0, Is.Not.EqualTo(s1));
    }

    [Test]
    public void BuildManifestPartitionKey_formats_prefix_tree_and_shard()
    {
        var pk = AzureTableWalStorageProvider.BuildManifestPartitionKey("my-tree", 7);

        Assert.That(pk, Is.EqualTo("_m_|my-tree|7"));
    }

    [Test]
    public void BuildManifestPartitionKey_percent_encodes_disallowed_treeId_characters()
    {
        var pk = AzureTableWalStorageProvider.BuildManifestPartitionKey("a/b\\c#d?e", 0);

        Assert.That(pk, Is.EqualTo("_m_|a%2Fb%5Cc%23d%3Fe|0"));
    }

    [Test]
    public void BuildManifestPartitionKey_throws_on_null_treeId()
    {
        Assert.That(
            () => AzureTableWalStorageProvider.BuildManifestPartitionKey(null!, 0),
            Throws.ArgumentNullException);
    }

    [Test]
    public void BuildManifestPartitionKey_distinguishes_distinct_shards()
    {
        var s0 = AzureTableWalStorageProvider.BuildManifestPartitionKey("tree", 0);
        var s1 = AzureTableWalStorageProvider.BuildManifestPartitionKey("tree", 1);

        Assert.That(s0, Is.Not.EqualTo(s1));
    }

    [Test]
    public void BuildManifestRowKey_pads_start_offsets_to_19_digits()
    {
        Assert.That(AzureTableWalStorageProvider.BuildManifestRowKey(42L), Is.EqualTo("M0000000000000000042"));
    }

    [Test]
    public void BuildManifestRowKey_pads_zero_start_offset_correctly()
    {
        Assert.That(AzureTableWalStorageProvider.BuildManifestRowKey(0L), Is.EqualTo("M0000000000000000000"));
    }

    [Test]
    public void BuildManifestRowKey_orders_keys_lexicographically()
    {
        var keys = new[] { 0L, 1L, 9L, 10L, 99L, 100L, 1_000_000L, long.MaxValue }
            .Select(AzureTableWalStorageProvider.BuildManifestRowKey)
            .ToArray();

        var sorted = keys.OrderBy(k => k, StringComparer.Ordinal).ToArray();

        Assert.That(keys, Is.EqualTo(sorted));
    }

    [Test]
    public void BuildManifestRowKey_sorts_strictly_before_TailRowKey()
    {
        // M < T under ordinal ordering so every committed-batch row
        // sorts before the tail pointer; the manifest range scan can
        // use 'RowKey lt TAIL' as a tight upper bound. The widest
        // possible manifest row key (M{long.MaxValue}) still sorts
        // strictly less than TAIL.
        var widest = AzureTableWalStorageProvider.BuildManifestRowKey(long.MaxValue);

        Assert.That(
            StringComparer.Ordinal.Compare(widest, AzureTableWalStorageProvider.TailRowKey),
            Is.LessThan(0));
    }

    [Test]
    public void BuildManifestRowKey_throws_on_negative_start_offset()
    {
        Assert.That(
            () => AzureTableWalStorageProvider.BuildManifestRowKey(-1L),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Batch_and_manifest_partition_keys_are_disjoint_namespaces()
    {
        // Two coexisting partition-key shapes in the per-batch
        // schema: manifest (2 separators) and batch (3 separators).
        // The byte-level prefix is the cheaper signal but the
        // separator count is the algebraic guarantee, so we assert
        // both here. A tree id that itself contains '|' characters
        // is percent-encoded by EncodePartitionSegment before
        // assembly, so '|' is always a structural separator.
        var manifest = AzureTableWalStorageProvider.BuildManifestPartitionKey("tree", 0);
        var batch = AzureTableWalStorageProvider.BuildBatchPartitionKey("tree", 0, 0L);

        Assert.Multiple(() =>
        {
            Assert.That(manifest, Does.StartWith("_m_|"));
            Assert.That(batch, Does.StartWith("_b_|"));

            // Separator counts make the namespaces algebraically disjoint.
            Assert.That(CountPipes(manifest), Is.EqualTo(2));
            Assert.That(CountPipes(batch), Is.EqualTo(3));

            // No shape is a prefix of another shape.
            Assert.That(batch, Does.Not.StartWith(manifest));
            Assert.That(manifest, Does.Not.StartWith(batch));
        });
    }

    private static int CountPipes(string s)
    {
        var c = 0;
        for (var i = 0; i < s.Length; i++)
        {
            if (s[i] == '|')
            {
                c++;
            }
        }
        return c;
    }
}
