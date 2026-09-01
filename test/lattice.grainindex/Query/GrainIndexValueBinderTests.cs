using Orleans.Lattice.GrainIndex.Query;

namespace Orleans.Lattice.GrainIndex.Tests.Query;

/// <summary>
/// The binder that brings a literal captured from a lambda back to the
/// property's declared type before encoding it. Getting this wrong would produce
/// a bound in the wrong encoding, which reads as a silently empty result rather
/// than as an error, so each conversion is pinned directly.
/// </summary>
[TestFixture]
public sealed class GrainIndexValueBinderTests
{
    [Test]
    public void A_matching_value_encodes_as_the_property_would()
    {
        var binder = GrainIndexValueBinder.Create(typeof(int));

        Assert.Multiple(() =>
        {
            Assert.That(binder.TryEncode(18, out string encoded), Is.True);
            Assert.That(encoded, Is.EqualTo(GrainIndexKeyEncoder.EncodeValue(18)));
        });
    }

    [Test]
    public void A_widened_literal_is_converted_to_the_property_type()
    {
        var binder = GrainIndexValueBinder.Create(typeof(double));

        Assert.Multiple(() =>
        {
            Assert.That(binder.TryEncode(1, out string encoded), Is.True);
            Assert.That(encoded, Is.EqualTo(GrainIndexKeyEncoder.EncodeValue(1.0)));
        });
    }

    [Test]
    public void A_value_for_a_nullable_property_encodes_as_the_present_form()
    {
        var binder = GrainIndexValueBinder.Create(typeof(int?));

        Assert.Multiple(() =>
        {
            Assert.That(binder.TryEncode(18, out string encoded), Is.True);
            Assert.That(encoded, Is.EqualTo(GrainIndexKeyEncoder.EncodeValue<int?>(18)));
        });
    }

    [Test]
    public void A_date_time_is_accepted_for_a_date_time_offset_property()
    {
        var binder = GrainIndexValueBinder.Create(typeof(DateTimeOffset));
        var moment = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        Assert.Multiple(() =>
        {
            Assert.That(binder.TryEncode(moment, out string encoded), Is.True);
            Assert.That(encoded, Is.EqualTo(GrainIndexKeyEncoder.EncodeValue(new DateTimeOffset(moment))));
        });
    }

    [Test]
    public void Null_encodes_to_the_null_slot_for_a_nullable_property()
    {
        var binder = GrainIndexValueBinder.Create(typeof(DateTimeOffset?));

        Assert.Multiple(() =>
        {
            Assert.That(binder.TryEncode(null, out string encoded), Is.True);
            Assert.That(encoded, Is.EqualTo(GrainIndexKeyEncoder.NullFlag.ToString()));
        });
    }

    [Test]
    public void Null_encodes_to_the_null_slot_for_a_reference_property()
    {
        var binder = GrainIndexValueBinder.Create(typeof(string));

        Assert.Multiple(() =>
        {
            Assert.That(binder.TryEncodeNull(out string encoded), Is.True);
            Assert.That(encoded, Is.EqualTo(GrainIndexKeyEncoder.NullFlag.ToString()));
        });
    }

    [Test]
    public void Null_is_rejected_for_a_property_that_cannot_hold_it()
    {
        var binder = GrainIndexValueBinder.Create(typeof(int));

        Assert.Multiple(() =>
        {
            Assert.That(binder.TryEncodeNull(out _), Is.False);
            Assert.That(binder.TryEncode(null, out _), Is.False);
        });
    }

    [Test]
    public void An_out_of_range_literal_is_rejected_rather_than_wrapped()
    {
        var binder = GrainIndexValueBinder.Create(typeof(int));

        Assert.That(binder.TryEncode(long.MaxValue, out _), Is.False);
    }

    [Test]
    public void An_unconvertible_literal_is_rejected()
    {
        var binder = GrainIndexValueBinder.Create(typeof(int));

        Assert.That(binder.TryEncode(Guid.NewGuid(), out _), Is.False);
    }

    [Test]
    public void A_string_property_encodes_a_string_literal()
    {
        var binder = GrainIndexValueBinder.Create(typeof(string));

        Assert.Multiple(() =>
        {
            Assert.That(binder.TryEncode("GB", out string encoded), Is.True);
            Assert.That(encoded, Is.EqualTo(GrainIndexKeyEncoder.EncodeValue("GB")));
        });
    }

    [Test]
    public void An_unordered_property_type_encodes_to_the_empty_component()
    {
        var binder = GrainIndexValueBinder.Create(typeof(Guid));

        Assert.Multiple(() =>
        {
            Assert.That(binder.TryEncode(Guid.Empty, out string encoded), Is.True);
            Assert.That(encoded, Is.Empty);
        });
    }
}
