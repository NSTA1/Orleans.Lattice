namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit tests for <see cref="ViewDigest.ContentEquals"/>, which compares the
/// drift fingerprint by value (entry count plus hash bytes) rather than by the
/// compiler-generated record-struct reference equality over the
/// <see cref="ViewDigest.Hash"/> array.
/// </summary>
[TestFixture]
public class ViewDigestTests
{
    [Test]
    public void ContentEquals_true_for_equal_count_and_equal_hash_bytes()
    {
        var a = new ViewDigest { Hash = [1, 2, 3], EntryCount = 7 };
        var b = new ViewDigest { Hash = [1, 2, 3], EntryCount = 7 };

        Assert.That(a.ContentEquals(b), Is.True);
    }

    [Test]
    public void ContentEquals_false_when_hash_bytes_differ()
    {
        var a = new ViewDigest { Hash = [1, 2, 3], EntryCount = 7 };
        var b = new ViewDigest { Hash = [1, 2, 4], EntryCount = 7 };

        Assert.That(a.ContentEquals(b), Is.False);
    }

    [Test]
    public void ContentEquals_false_when_entry_count_differs()
    {
        var a = new ViewDigest { Hash = [1, 2, 3], EntryCount = 7 };
        var b = new ViewDigest { Hash = [1, 2, 3], EntryCount = 8 };

        Assert.That(a.ContentEquals(b), Is.False);
    }

    [Test]
    public void ContentEquals_true_for_two_null_hashes_with_equal_count()
    {
        var a = new ViewDigest { Hash = null!, EntryCount = 0 };
        var b = new ViewDigest { Hash = null!, EntryCount = 0 };

        Assert.That(a.ContentEquals(b), Is.True);
    }

    [Test]
    public void ContentEquals_false_when_only_one_hash_is_null()
    {
        var a = new ViewDigest { Hash = null!, EntryCount = 1 };
        var b = new ViewDigest { Hash = [9], EntryCount = 1 };

        Assert.Multiple(() =>
        {
            Assert.That(a.ContentEquals(b), Is.False);
            Assert.That(b.ContentEquals(a), Is.False);
        });
    }
}
