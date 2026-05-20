using System.Globalization;
using System.Reflection;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Pure-logic tests for <see cref="AzureTableWalStorageProvider"/>'s
/// internal helpers. Behavioural end-to-end coverage against a live
/// Azure Tables endpoint or Azurite is gated separately under the
/// <c>AzureTableEmulator</c> category so it does not run on the
/// default dev loop.
/// </summary>
[TestFixture]
public partial class AzureTableWalStorageProviderTests
{
    [Test]
    public void BuildEntryRowKey_pads_offsets_to_19_digits()
    {
        Assert.That(AzureTableWalStorageProvider.BuildEntryRowKey(42L), Is.EqualTo("E0000000000000000042"));
    }

    [Test]
    public void BuildEntryRowKey_pads_zero_offset_correctly()
    {
        Assert.That(AzureTableWalStorageProvider.BuildEntryRowKey(0L), Is.EqualTo("E0000000000000000000"));
    }

    [Test]
    public void BuildEntryRowKey_orders_offsets_lexicographically()
    {
        var keys = new[] { 0L, 1L, 9L, 10L, 99L, 100L, 1_000_000L, long.MaxValue }
            .Select(AzureTableWalStorageProvider.BuildEntryRowKey)
            .ToArray();

        var sorted = keys.OrderBy(k => k, StringComparer.Ordinal).ToArray();

        Assert.That(keys, Is.EqualTo(sorted));
    }

    [Test]
    public void BuildEntryRowKey_sorts_strictly_before_HeadRowKey()
    {
        // 'E' (0x45) < 'H' (0x48) so every entry key compares as less
        // than the HEAD sentinel under ordinal ordering. This is the
        // invariant the ReadAsync filter ('RowKey lt HEAD') depends on.
        var maxOffsetKey = AzureTableWalStorageProvider.BuildEntryRowKey(long.MaxValue);

        Assert.That(StringComparer.Ordinal.Compare(maxOffsetKey, AzureTableWalStorageProvider.HeadRowKey), Is.LessThan(0));
    }

    [Test]
    public void BuildPartitionKey_leaves_safe_treeId_unchanged()
    {
        var pk = AzureTableWalStorageProvider.BuildPartitionKey("my-tree.v1_2024", 7);

        Assert.That(pk, Is.EqualTo("my-tree.v1_2024|7"));
    }

    [Test]
    public void BuildPartitionKey_percent_encodes_disallowed_characters()
    {
        var pk = AzureTableWalStorageProvider.BuildPartitionKey("a/b\\c#d?e", 0);

        // '/' '\' '#' '?' are all disallowed by Azure partition-key
        // rules. Encoded UTF-8 byte-wise as %XX with uppercase hex.
        Assert.That(pk, Is.EqualTo("a%2Fb%5Cc%23d%3Fe|0"));
    }

    [Test]
    public void BuildPartitionKey_percent_encodes_non_ascii_treeId()
    {
        // 'é' is 0xC3 0xA9 in UTF-8.
        var pk = AzureTableWalStorageProvider.BuildPartitionKey("café", 3);

        Assert.That(pk, Is.EqualTo("caf%C3%A9|3"));
    }

    [Test]
    public void BuildPartitionKey_throws_on_null_treeId()
    {
        Assert.That(
            () => AzureTableWalStorageProvider.BuildPartitionKey(null!, 0),
            Throws.ArgumentNullException);
    }

    [Test]
    public void BuildPartitionKey_round_trips_distinct_ids_to_distinct_partition_keys()
    {
        var inputs = new[]
        {
            "tree-a",
            "tree/a",
            "tree\\a",
            "tree#a",
            "tree?a",
            "tree a",
            "tree|a",
            "tree%a",
        };

        var partitionKeys = inputs
            .Select(t => AzureTableWalStorageProvider.BuildPartitionKey(t, 0))
            .ToArray();

        Assert.That(partitionKeys.Distinct().Count(), Is.EqualTo(inputs.Length));
    }

    [Test]
    public void BuildPartitionKey_separates_tree_from_shard_with_pipe()
    {
        var pk0 = AzureTableWalStorageProvider.BuildPartitionKey("tree", 0);
        var pk1 = AzureTableWalStorageProvider.BuildPartitionKey("tree", 1);

        Assert.Multiple(() =>
        {
            Assert.That(pk0, Does.EndWith("|0"));
            Assert.That(pk1, Does.EndWith("|1"));
            Assert.That(pk0, Is.Not.EqualTo(pk1));
        });
    }

    [Test]
    public void HeadRowKey_constant_is_HEAD()
    {
        // Defends the read-filter invariant: HEAD must match the
        // literal embedded in the OData filter. A typo here would
        // silently break GetHighestOffsetAsync.
        Assert.Multiple(() =>
        {
            Assert.That(AzureTableWalStorageProvider.HeadRowKey, Is.EqualTo("HEAD"));
            Assert.That(AzureTableWalStorageProvider.EntryRowKeyPrefix, Is.EqualTo("E"));
            Assert.That(
                StringComparer.Ordinal.Compare(
                    AzureTableWalStorageProvider.EntryRowKeyPrefix,
                    AzureTableWalStorageProvider.HeadRowKey),
                Is.LessThan(0));
        });
    }

    [Test]
    public void MaxEntriesPerBatch_uses_full_transaction_action_cap()
    {
        // Azure caps a transaction at 100 actions. The two-phase
        // per-batch schema drops the per-batch HEAD
        // sentinel from phase 1 so the full 100-action budget is
        // available for entries; reconciliation derives
        // endOffsetInclusive from a Top(1) DESC query over the batch
        // partition. Pin the constant so a future change on either
        // side trips this regression.
        Assert.That(AzureTableWalStorageProvider.MaxEntriesPerBatch, Is.EqualTo(100));
    }
}
