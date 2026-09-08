using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>
/// Guards value equality on the gRPC data-plane wire request DTOs whose
/// <see cref="byte"/> array payloads (<c>DataSetRequest.Value</c> and
/// <c>CrdtWriteRequest.Element</c>) the compiler-generated record equality would
/// otherwise compare with <see cref="EqualityComparer{T}.Default"/> (reference
/// equality). Two structurally identical requests carrying distinct-but-equal
/// arrays - the shape a request and its post-serialization self take - must compare
/// equal and share a hash code, and a difference in the byte content or any scalar
/// must compare unequal.
/// </summary>
[TestFixture]
public sealed class GrpcDataDtoEqualityTests
{
    [Test]
    public void DataSetRequest_equal_content_with_distinct_arrays_are_equal()
    {
        var a = new DataSetRequest { TreeId = "t", Key = "k", Value = new byte[] { 1, 2, 3 } };
        var b = new DataSetRequest { TreeId = "t", Key = "k", Value = new byte[] { 1, 2, 3 } };

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Value, b.Value), Is.False);
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void DataSetRequest_differing_value_content_is_not_equal()
    {
        var a = new DataSetRequest { TreeId = "t", Key = "k", Value = new byte[] { 1, 2, 3 } };
        var b = new DataSetRequest { TreeId = "t", Key = "k", Value = new byte[] { 1, 2, 4 } };

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void DataSetRequest_differing_scalar_is_not_equal()
    {
        var a = new DataSetRequest { TreeId = "t", Key = "k", Value = new byte[] { 1 } };
        var b = new DataSetRequest { TreeId = "t", Key = "other", Value = new byte[] { 1 } };

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void DataSetRequest_round_trip_compares_equal_by_value()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<DataSetRequest>>();
        var original = new DataSetRequest { TreeId = "tree-a", Key = "k1", Value = new byte[] { 1, 2, 3 } };

        var copy = serializer.Deserialize(serializer.SerializeToArray(original));

        Assert.That(copy, Is.EqualTo(original));
    }

    [Test]
    public void CrdtWriteRequest_equal_content_with_distinct_arrays_are_equal()
    {
        var a = new CrdtWriteRequest
        {
            TreeId = "t", Key = "k", Op = CrdtWriteOp.SetAdd, ReplicaId = "r",
            Amount = 3, Element = new byte[] { 9, 9 }, Field = "f", Index = 2,
        };
        var b = new CrdtWriteRequest
        {
            TreeId = "t", Key = "k", Op = CrdtWriteOp.SetAdd, ReplicaId = "r",
            Amount = 3, Element = new byte[] { 9, 9 }, Field = "f", Index = 2,
        };

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Element, b.Element), Is.False);
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void CrdtWriteRequest_differing_element_content_is_not_equal()
    {
        var a = new CrdtWriteRequest { TreeId = "t", Key = "k", Op = CrdtWriteOp.SetAdd, Element = new byte[] { 1 } };
        var b = new CrdtWriteRequest { TreeId = "t", Key = "k", Op = CrdtWriteOp.SetAdd, Element = new byte[] { 2 } };

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void CrdtWriteRequest_differing_scalar_is_not_equal()
    {
        var a = new CrdtWriteRequest { TreeId = "t", Key = "k", Op = CrdtWriteOp.CounterIncrement, Amount = 1 };
        var b = new CrdtWriteRequest { TreeId = "t", Key = "k", Op = CrdtWriteOp.CounterIncrement, Amount = 2 };

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void CrdtWriteRequest_round_trip_compares_equal_by_value()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<CrdtWriteRequest>>();
        var original = new CrdtWriteRequest
        {
            TreeId = "tree-a", Key = "k", Op = CrdtWriteOp.MaxRegisterSet, Element = new byte[] { 4, 5, 6 },
        };

        var copy = serializer.Deserialize(serializer.SerializeToArray(original));

        Assert.That(copy, Is.EqualTo(original));
    }

    [Test]
    public void CrdtMapField_equal_content_with_distinct_arrays_are_equal()
    {
        var a = new CrdtMapField { Field = "f", Values = [new byte[] { 1, 2 }, new byte[] { 3 }] };
        var b = new CrdtMapField { Field = "f", Values = [new byte[] { 1, 2 }, new byte[] { 3 }] };

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Values[0], b.Values[0]), Is.False);
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void CrdtMapField_differing_value_content_is_not_equal()
    {
        var a = new CrdtMapField { Field = "f", Values = [new byte[] { 1, 2 }] };
        var b = new CrdtMapField { Field = "f", Values = [new byte[] { 1, 9 }] };

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void CrdtMapField_differing_value_count_is_not_equal()
    {
        var a = new CrdtMapField { Field = "f", Values = [new byte[] { 1 }] };
        var b = new CrdtMapField { Field = "f", Values = [new byte[] { 1 }, new byte[] { 2 }] };

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void CrdtMapField_differing_field_is_not_equal()
    {
        var a = new CrdtMapField { Field = "f", Values = [new byte[] { 1 }] };
        var b = new CrdtMapField { Field = "other", Values = [new byte[] { 1 }] };

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void CrdtMapField_round_trip_compares_equal_by_value()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<CrdtMapField>>();
        var original = new CrdtMapField { Field = "f", Values = [new byte[] { 1, 2 }, new byte[] { 3, 4 }] };

        var copy = serializer.Deserialize(serializer.SerializeToArray(original));

        Assert.That(copy, Is.EqualTo(original));
    }

    private static CrdtReadResponse SampleReadResponse() => new()
    {
        CounterValue = 5,
        FlagValue = true,
        Elements = [new byte[] { 1, 2 }, new byte[] { 3 }],
        Vector = [new CrdtVectorEntry { ReplicaId = "r", Clock = "1:2" }],
        Map = [new CrdtMapField { Field = "f", Values = [new byte[] { 7 }] }],
    };

    [Test]
    public void CrdtReadResponse_equal_content_with_distinct_arrays_are_equal()
    {
        var a = SampleReadResponse();
        var b = SampleReadResponse();

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Elements[0], b.Elements[0]), Is.False);
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void CrdtReadResponse_differing_element_content_is_not_equal()
    {
        var a = SampleReadResponse();
        var b = a with { Elements = [new byte[] { 1, 2 }, new byte[] { 9 }] };

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void CrdtReadResponse_differing_map_value_content_is_not_equal()
    {
        var a = SampleReadResponse();
        var b = a with { Map = [new CrdtMapField { Field = "f", Values = [new byte[] { 8 }] }] };

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void CrdtReadResponse_differing_vector_is_not_equal()
    {
        var a = SampleReadResponse();
        var b = a with { Vector = [new CrdtVectorEntry { ReplicaId = "r", Clock = "9:9" }] };

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void CrdtReadResponse_differing_scalar_is_not_equal()
    {
        var a = SampleReadResponse();

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.Not.EqualTo(a with { CounterValue = 6 }));
            Assert.That(a, Is.Not.EqualTo(a with { FlagValue = false }));
        });
    }

    [Test]
    public void CrdtReadResponse_round_trip_compares_equal_by_value()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<CrdtReadResponse>>();
        var original = SampleReadResponse();

        var copy = serializer.Deserialize(serializer.SerializeToArray(original));

        Assert.That(copy, Is.EqualTo(original));
    }
}
