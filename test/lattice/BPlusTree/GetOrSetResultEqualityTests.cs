using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Value-equality regression tests for <see cref="GetOrSetResult"/>. Its
/// <see cref="GetOrSetResult.ExistingValue"/> byte array was compared by
/// reference under the compiler-generated record equality, so two results
/// built from independently allocated but byte-identical payloads - including
/// a result and its post-serialization self - never compared equal.
/// </summary>
[TestFixture]
public sealed class GetOrSetResultEqualityTests
{
    private static SplitResult SampleSplit(string promotedKey = "m") => new()
    {
        PromotedKey = promotedKey,
        NewSiblingId = GrainId.Create("leaf", "sibling"),
        ChildIsLeaf = true,
    };

    [Test]
    public void Equal_when_existing_value_bytes_match_across_distinct_arrays()
    {
        var a = new GetOrSetResult { ExistingValue = [7, 8, 9] };
        var b = new GetOrSetResult { ExistingValue = [7, 8, 9] };

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.ExistingValue, b.ExistingValue), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_existing_value_bytes_differ()
    {
        var a = new GetOrSetResult { ExistingValue = [1, 2, 3] };
        var b = new GetOrSetResult { ExistingValue = [1, 2, 4] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Equal_when_both_split_values_match_and_existing_value_null()
    {
        var a = new GetOrSetResult { Split = SampleSplit() };
        var b = new GetOrSetResult { Split = SampleSplit() };

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_split_values_differ()
    {
        var a = new GetOrSetResult { Split = SampleSplit("m") };
        var b = new GetOrSetResult { Split = SampleSplit("q") };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_one_side_has_no_existing_value()
    {
        var a = new GetOrSetResult { ExistingValue = [1] };
        var b = new GetOrSetResult { ExistingValue = null };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Serialization_round_trip_preserves_value_equality()
    {
        var value = new GetOrSetResult { ExistingValue = [4, 5, 6] };

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<GetOrSetResult>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(value));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.ExistingValue, value.ExistingValue), Is.False);
            Assert.That(decoded.Equals(value), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(value.GetHashCode()));
        });
    }
}
