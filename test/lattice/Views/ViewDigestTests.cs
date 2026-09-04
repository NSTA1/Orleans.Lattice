using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

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

    [Test]
    public void Equals_and_operator_true_for_equal_content_across_distinct_arrays()
    {
        // Distinct Hash instances with equal content: reference equality (the
        // pre-fix record-struct default) reports unequal, value equality equal.
        var a = new ViewDigest { Hash = [1, 2, 3], EntryCount = 7 };
        var b = new ViewDigest { Hash = [1, 2, 3], EntryCount = 7 };

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Hash, b.Hash), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Equals_false_when_hash_bytes_differ()
    {
        var a = new ViewDigest { Hash = [1, 2, 3], EntryCount = 7 };
        var b = new ViewDigest { Hash = [1, 2, 4], EntryCount = 7 };

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.False);
            Assert.That(a != b, Is.True);
        });
    }

    [Test]
    public void Serialization_round_trip_preserves_value_equality()
    {
        var digest = new ViewDigest { Hash = [4, 5, 6], EntryCount = 2 };

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<ViewDigest>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(digest));

        Assert.Multiple(() =>
        {
            Assert.That(decoded.Equals(digest), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(digest.GetHashCode()));
        });
    }
}
