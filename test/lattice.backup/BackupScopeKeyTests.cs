namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit coverage for <see cref="BackupScopeKey"/>: the key is deterministic and
/// distinguishes scopes by kind, tree, and key / prefix, while treating a
/// null-vs-empty key/prefix identically, and every key it emits is safe to use
/// as a durable grain-storage key (no control characters or characters a
/// durable store such as Azure Table storage or Azure Cosmos DB forbids).
/// </summary>
public sealed class BackupScopeKeyTests
{
    // Characters Azure Table storage forbids in a partition/row key and Azure
    // Cosmos DB forbids in a document id. A scope key that reaches durable grain
    // storage must contain none of these (control characters are checked apart).
    private static readonly char[] ForbiddenPunctuation = { '/', '\\', '#', '?' };

    [Test]
    public void For_null_scope_throws()
    {
        Assert.That(() => BackupScopeKey.For(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void For_is_deterministic_for_the_same_scope()
    {
        var a = BackupScopeKey.For(BackupScopeSelector.WholeTree("orders"));
        var b = BackupScopeKey.For(BackupScopeSelector.WholeTree("orders"));

        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void For_distinguishes_kind_tree_and_selector()
    {
        var wholeTree = BackupScopeKey.For(BackupScopeSelector.WholeTree("orders"));
        var otherTree = BackupScopeKey.For(BackupScopeSelector.WholeTree("customers"));
        var prefix = BackupScopeKey.For(BackupScopeSelector.Prefix("orders", "a:"));
        var key = BackupScopeKey.For(BackupScopeSelector.Key("orders", "a:"));

        Assert.Multiple(() =>
        {
            Assert.That(wholeTree, Is.Not.EqualTo(otherTree));
            Assert.That(wholeTree, Is.Not.EqualTo(prefix));
            // Same tree and selector text but different kind must differ.
            Assert.That(prefix, Is.Not.EqualTo(key));
        });
    }

    [Test]
    public void For_whole_tree_key_is_durable_store_safe()
    {
        // Regression: the key doubles as the scheduler grain key, which a durable
        // provider (Azure Table storage) turns into a partition/row key. The
        // previous unit-separator delimiter (U+001F) was rejected with a 400
        // InvalidInput the first time a scheduler persisted its state.
        var key = BackupScopeKey.For(BackupScopeSelector.WholeTree("mfg-facts"));

        AssertDurableStoreSafe(key);
    }

    [Test]
    public void For_encodes_a_selector_carrying_forbidden_characters()
    {
        // A key or prefix is arbitrary user data and may carry any character,
        // including the unit separator, a forward slash, or the delimiter itself.
        var hostile = "a/b\\c#d?e\u001ff|g%h";
        var key = BackupScopeKey.For(BackupScopeSelector.Key("orders", hostile));

        AssertDurableStoreSafe(key);
    }

    [Test]
    public void For_encodes_a_tree_id_carrying_forbidden_characters()
    {
        var key = BackupScopeKey.For(BackupScopeSelector.WholeTree("teams/orders#eu"));

        AssertDurableStoreSafe(key);
    }

    [Test]
    public void For_distinguishes_selectors_that_collide_only_after_encoding()
    {
        // The delimiter and escape marker are themselves encoded inside a field,
        // so a selector whose text contains them can never masquerade as a
        // different scope once the fields are joined.
        var withDelimiter = BackupScopeKey.For(BackupScopeSelector.Key("orders", "a|1"));
        var withEscape = BackupScopeKey.For(BackupScopeSelector.Key("orders", "a%7C1"));
        var plain = BackupScopeKey.For(BackupScopeSelector.Key("orders", "a1"));

        Assert.Multiple(() =>
        {
            Assert.That(withDelimiter, Is.Not.EqualTo(plain));
            Assert.That(withEscape, Is.Not.EqualTo(plain));
            Assert.That(withDelimiter, Is.Not.EqualTo(withEscape));
        });
    }

    private static void AssertDurableStoreSafe(string key)
    {
        Assert.Multiple(() =>
        {
            foreach (var ch in key)
            {
                Assert.That(
                    ch is (>= '\u0000' and <= '\u001f') or (>= '\u007f' and <= '\u009f'),
                    Is.False,
                    $"key '{key}' contains control character U+{(int)ch:X4}");
                Assert.That(
                    ForbiddenPunctuation, Does.Not.Contain(ch),
                    $"key '{key}' contains durable-store-forbidden character '{ch}'");
            }
        });
    }
}
