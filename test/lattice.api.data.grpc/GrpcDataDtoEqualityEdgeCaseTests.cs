namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>
/// Covers the edge arms of the hand-written value equality and hashing on
/// <see cref="CrdtMapField"/> and <see cref="CrdtReadResponse"/> that the
/// main equality fixture does not reach: the same-instance short circuit, the
/// length mismatch that decides before any element is compared, and the two
/// null-tolerant arms inside hashing.
/// </summary>
/// <remarks>
/// <para>
/// Both DTOs replace the compiler-generated record equality because their
/// payload lists would otherwise be compared by reference. The replacement is
/// hand-written, so each of its arms is a place a defect can hide, and the arms
/// below are precisely the ones a fixture built from "two populated, equal-length
/// samples" can never drive.
/// </para>
/// <para>
/// Every refusal here is paired with an accepting counterpart in the same test.
/// An implementation that rejected every comparison, or one that hashed every
/// payload to the same value, would satisfy exactly one half of each pair.
/// </para>
/// </remarks>
[TestFixture]
public sealed class GrpcDataDtoEqualityEdgeCaseTests
{
    private static CrdtReadResponse SampleReadResponse() => new()
    {
        CounterValue = 5,
        FlagValue = true,
        Elements = [new byte[] { 1, 2 }],
        Vector = [new CrdtVectorEntry { ReplicaId = "r", Clock = "1:1" }],
        Map = [new CrdtMapField { Field = "f", Values = [new byte[] { 3 }] }],
    };

    // ---- CrdtMapField ----------------------------------------------------

    [Test]
    public void CrdtMapField_compares_equal_to_itself_through_the_same_instance_shortcut()
    {
        var field = new CrdtMapField { Field = "f", Values = [new byte[] { 1, 2 }] };

        // The two sides are the same List<byte[]> reference, which the content
        // comparison short-circuits on before inspecting any element. The shared
        // instance below reaches the same arm without self-comparison, so the
        // shortcut is shown to be about the list, not about the field.
        var sharing = new CrdtMapField { Field = "f", Values = field.Values };

        Assert.Multiple(() =>
        {
            Assert.That(field.Equals(field), Is.True);
            Assert.That(field.Equals(sharing), Is.True);
            Assert.That(ReferenceEquals(field.Values, sharing.Values), Is.True);
            Assert.That(
                field.Equals(new CrdtMapField { Field = "other", Values = field.Values }),
                Is.False,
                "the shortcut is on the values only - a differing field name must still compare unequal");
        });
    }

    [Test]
    public void CrdtMapField_hashes_a_null_value_without_throwing()
    {
        // A decoded map field can carry a null slot; hashing must tolerate it
        // rather than fault, and must not collapse it onto an empty payload.
        var withNull = new CrdtMapField { Field = "f", Values = [null!] };
        var alsoWithNull = new CrdtMapField { Field = "f", Values = [null!] };
        var empty = new CrdtMapField { Field = "f", Values = [] };
        var populated = new CrdtMapField { Field = "f", Values = [new byte[] { 7 }] };

        Assert.Multiple(() =>
        {
            Assert.That(() => withNull.GetHashCode(), Throws.Nothing);
            Assert.That(
                withNull.GetHashCode(),
                Is.EqualTo(alsoWithNull.GetHashCode()),
                "hashing must stay deterministic across two structurally identical null-bearing fields");
            Assert.That(
                withNull.GetHashCode(),
                Is.Not.EqualTo(empty.GetHashCode()),
                "a null slot is not the same payload as no slot at all");
            Assert.That(withNull.GetHashCode(), Is.Not.EqualTo(populated.GetHashCode()));
        });
    }

    [Test]
    public void CrdtMapField_a_null_value_and_a_populated_value_are_not_equal()
    {
        var withNull = new CrdtMapField { Field = "f", Values = [null!] };
        var populated = new CrdtMapField { Field = "f", Values = [new byte[] { 7 }] };

        Assert.Multiple(() =>
        {
            Assert.That(withNull.Equals(populated), Is.False);
            Assert.That(populated.Equals(withNull), Is.False);
            Assert.That(
                withNull.Equals(new CrdtMapField { Field = "f", Values = [null!] }),
                Is.True,
                "two null slots in the same position are equal, so the rejection above is about content");
        });
    }

    // ---- CrdtReadResponse ------------------------------------------------

    [Test]
    public void CrdtReadResponse_differing_element_count_is_not_equal()
    {
        // Decided by the length test before any byte array is compared, which is
        // a different arm from the differing-content test in the main fixture.
        var one = SampleReadResponse();
        var two = one with { Elements = [new byte[] { 1, 2 }, new byte[] { 3, 4 }] };

        Assert.Multiple(() =>
        {
            Assert.That(one, Is.Not.EqualTo(two), "a prefix must not compare equal to the longer list");
            Assert.That(two, Is.Not.EqualTo(one));
            Assert.That(one, Is.Not.EqualTo(one with { Elements = [] }));
            Assert.That(
                one,
                Is.EqualTo(one with { Elements = [new byte[] { 1, 2 }] }),
                "the positive counterpart: an equal-length, equal-content list still compares equal");
        });
    }

    [Test]
    public void CrdtReadResponse_differing_vector_count_is_not_equal()
    {
        var one = SampleReadResponse();
        var two = one with
        {
            Vector =
            [
                new CrdtVectorEntry { ReplicaId = "r", Clock = "1:1" },
                new CrdtVectorEntry { ReplicaId = "s", Clock = "2:2" },
            ],
        };

        Assert.Multiple(() =>
        {
            Assert.That(one, Is.Not.EqualTo(two));
            Assert.That(two, Is.Not.EqualTo(one));
            Assert.That(one, Is.Not.EqualTo(one with { Vector = [] }));
            Assert.That(
                one,
                Is.EqualTo(one with { Vector = [new CrdtVectorEntry { ReplicaId = "r", Clock = "1:1" }] }),
                "the positive counterpart for the generic list comparison");
        });
    }

    [Test]
    public void CrdtReadResponse_differing_map_count_is_not_equal()
    {
        // The map list runs through the same generic length test as the vector,
        // so covering one does not cover the other's call site.
        var one = SampleReadResponse();
        var two = one with
        {
            Map =
            [
                new CrdtMapField { Field = "f", Values = [new byte[] { 3 }] },
                new CrdtMapField { Field = "g", Values = [new byte[] { 4 }] },
            ],
        };

        Assert.Multiple(() =>
        {
            Assert.That(one, Is.Not.EqualTo(two));
            Assert.That(one, Is.Not.EqualTo(one with { Map = [] }));
        });
    }

    [Test]
    public void CrdtReadResponse_hashes_a_null_element_without_throwing()
    {
        var withNull = SampleReadResponse() with { Elements = [null!] };
        var alsoWithNull = SampleReadResponse() with { Elements = [null!] };
        var empty = SampleReadResponse() with { Elements = [] };

        Assert.Multiple(() =>
        {
            Assert.That(() => withNull.GetHashCode(), Throws.Nothing);
            Assert.That(withNull.GetHashCode(), Is.EqualTo(alsoWithNull.GetHashCode()));
            Assert.That(
                withNull.GetHashCode(),
                Is.Not.EqualTo(empty.GetHashCode()),
                "a null element is not the same payload as no element at all");
        });
    }

    [Test]
    public void CrdtReadResponse_hashes_a_null_vector_or_map_list_without_throwing()
    {
        // Both list properties default to an empty list, but a decoded response
        // materialised by a serializer or a hand-built wire frame can present a
        // null list, and hashing must tolerate that rather than fault.
        var nullVector = SampleReadResponse() with { Vector = null! };
        var nullMap = SampleReadResponse() with { Map = null! };
        var populated = SampleReadResponse();

        Assert.Multiple(() =>
        {
            Assert.That(() => nullVector.GetHashCode(), Throws.Nothing);
            Assert.That(() => nullMap.GetHashCode(), Throws.Nothing);
            Assert.That(
                nullVector.GetHashCode(),
                Is.EqualTo((SampleReadResponse() with { Vector = null! }).GetHashCode()),
                "hashing must stay deterministic when a list is absent");
            Assert.That(
                nullVector.GetHashCode(),
                Is.Not.EqualTo(populated.GetHashCode()),
                "the positive counterpart: an absent list must not hash like a populated one");
            Assert.That(nullMap.GetHashCode(), Is.Not.EqualTo(populated.GetHashCode()));
        });
    }

    [Test]
    public void CrdtReadResponse_a_null_list_is_not_equal_to_a_populated_one()
    {
        var nullVector = SampleReadResponse() with { Vector = null! };
        var populated = SampleReadResponse();

        Assert.Multiple(() =>
        {
            Assert.That(nullVector, Is.Not.EqualTo(populated));
            Assert.That(populated, Is.Not.EqualTo(nullVector));
            Assert.That(
                nullVector,
                Is.EqualTo(SampleReadResponse() with { Vector = null! }),
                "two absent lists still compare equal, so the rejection above is about presence");
        });
    }
}
