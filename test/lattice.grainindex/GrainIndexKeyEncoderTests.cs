namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// The structural half of the key encoding: layout, range bounds, parsing, the
/// unordered fallback, and the argument guards. The ordering property itself
/// lives in <see cref="GrainIndexKeyEncoderOrderingTests"/>.
/// </summary>
[TestFixture]
public class GrainIndexKeyEncoderTests
{
    private const char Sep = GrainIndexKeyEncoder.Separator;

    [Test]
    public void Separator_is_the_lowest_code_unit_and_upper_bound_is_its_successor()
    {
        Assert.That(GrainIndexKeyEncoder.Separator, Is.EqualTo('\u0000'));
        Assert.That(GrainIndexKeyEncoder.RangeUpperBound, Is.EqualTo('\u0001'));
        Assert.That(GrainIndexKeyEncoder.NullFlag, Is.LessThan(GrainIndexKeyEncoder.PresentFlag));
    }

    [Test]
    public void EncodeKey_lays_the_property_then_value_then_grain_key_out_in_order()
    {
        var key = GrainIndexKeyEncoder.EncodeKey("Age", 17, "alice");

        Assert.That(key, Does.StartWith("Age" + Sep));
        Assert.That(key, Does.EndWith(Sep + "alice"));
        Assert.That(key.Split(Sep), Has.Length.EqualTo(3));
    }

    [Test]
    public void ComposeKey_from_a_pre_encoded_value_matches_the_typed_overload()
    {
        var encoded = GrainIndexKeyEncoder.EncodeValue(17);

        Assert.That(
            GrainIndexKeyEncoder.ComposeKey("Age", encoded, "alice"),
            Is.EqualTo(GrainIndexKeyEncoder.EncodeKey("Age", 17, "alice")));
    }

    [Test]
    public void ComposeKey_and_EncodeKey_agree_for_a_string_property()
    {
        // The two entry points are named apart precisely so a string-valued
        // property cannot bind to the pre-encoded overload by accident.
        Assert.That(
            GrainIndexKeyEncoder.ComposeKey("Country", GrainIndexKeyEncoder.EncodeValue("GB"), "alice"),
            Is.EqualTo(GrainIndexKeyEncoder.EncodeKey("Country", "GB", "alice")));
    }

    [Test]
    public void EncodeValue_never_emits_the_separator_even_for_a_string_carrying_it()
    {
        foreach (var value in new[] { "\u0000", "a\u0000b", "\u0001", "\u0000\u0001\u0000", string.Empty })
        {
            Assert.That(
                GrainIndexKeyEncoder.EncodeValue(value).IndexOf(Sep),
                Is.LessThan(0),
                $"value {System.Text.Json.JsonSerializer.Serialize(value)} leaked a separator");
        }
    }

    [Test]
    public void EncodeValue_is_injective_for_strings_that_differ_only_in_reserved_code_units()
    {
        string[] values = ["\u0000", "\u0001", "\u0001\u0001", "\u0000\u0000", "a\u0000", "a\u0001"];
        var encoded = values.Select(GrainIndexKeyEncoder.EncodeValue).ToArray();

        Assert.That(encoded.Distinct(StringComparer.Ordinal).Count(), Is.EqualTo(values.Length));
    }

    [Test]
    public void EncodeValue_distinguishes_null_from_the_empty_string()
    {
        Assert.That(
            GrainIndexKeyEncoder.EncodeValue<string?>(null),
            Is.Not.EqualTo(GrainIndexKeyEncoder.EncodeValue(string.Empty)));
    }

    [Test]
    public void EncodeValue_returns_an_empty_component_for_a_type_with_no_order_preserving_encoding()
    {
        Assert.That(GrainIndexKeyEncoder.EncodeValue(Guid.NewGuid()), Is.Empty);
        Assert.That(GrainIndexKeyEncoder.EncodeValue(TimeSpan.FromMinutes(3)), Is.Empty);
        Assert.That(GrainIndexKeyEncoder.EncodeValue(TestStatus.Active), Is.Empty);
        Assert.That(GrainIndexKeyEncoder.EncodeValue(12.5m), Is.Empty);
    }

    [Test]
    public void An_unordered_property_keeps_one_stable_key_per_grain_across_value_changes()
    {
        var before = GrainIndexKeyEncoder.EncodeKey("Id", Guid.NewGuid(), "alice");
        var after = GrainIndexKeyEncoder.EncodeKey("Id", Guid.NewGuid(), "alice");

        Assert.That(after, Is.EqualTo(before));
    }

    [Test]
    public void IsOrderPreserving_reports_the_v1_scope()
    {
        Type[] ordered =
        [
            typeof(bool), typeof(sbyte), typeof(byte), typeof(short), typeof(ushort), typeof(int),
            typeof(uint), typeof(long), typeof(ulong), typeof(char), typeof(float), typeof(double),
            typeof(DateTime), typeof(DateTimeOffset), typeof(string),
        ];

        foreach (var type in ordered)
        {
            Assert.That(GrainIndexKeyEncoder.IsOrderPreserving(type), Is.True, type.Name);
            if (type.IsValueType)
                Assert.That(GrainIndexKeyEncoder.IsOrderPreserving(typeof(Nullable<>).MakeGenericType(type)), Is.True, $"{type.Name}?");
        }

        foreach (var type in new[] { typeof(Guid), typeof(TimeSpan), typeof(decimal), typeof(TestStatus), typeof(object), typeof(byte[]) })
            Assert.That(GrainIndexKeyEncoder.IsOrderPreserving(type), Is.False, type.Name);
    }

    [Test]
    public void IsOrderPreserving_rejects_a_null_type()
    {
        Assert.That(() => GrainIndexKeyEncoder.IsOrderPreserving(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void The_property_range_contains_every_key_for_that_property_and_no_other()
    {
        var start = GrainIndexKeyEncoder.PropertyRangeStartInclusive("Age");
        var end = GrainIndexKeyEncoder.PropertyRangeEndExclusive("Age");

        string[] inside =
        [
            GrainIndexKeyEncoder.EncodeKey("Age", int.MinValue, string.Empty),
            GrainIndexKeyEncoder.EncodeKey("Age", 0, "alice"),
            GrainIndexKeyEncoder.EncodeKey("Age", int.MaxValue, "\uffff"),
        ];

        // 'AgeGroup' is the adversarial neighbour: a property whose name extends
        // this one's would land inside a range built on a printable separator.
        string[] outside =
        [
            GrainIndexKeyEncoder.EncodeKey("Ag", 0, "alice"),
            GrainIndexKeyEncoder.EncodeKey("AgeGroup", 0, "alice"),
            GrainIndexKeyEncoder.EncodeKey("Country", 0, "alice"),
        ];

        foreach (var key in inside)
            Assert.That(IsInRange(key, start, end), Is.True, key.Replace('\u0000', '.'));

        foreach (var key in outside)
            Assert.That(IsInRange(key, start, end), Is.False, key.Replace('\u0000', '.'));
    }

    [Test]
    public void The_exact_value_range_contains_only_the_keys_carrying_that_value()
    {
        var encoded = GrainIndexKeyEncoder.EncodeValue(17);
        var start = GrainIndexKeyEncoder.ValueRangeStartInclusive("Age", encoded);
        var end = GrainIndexKeyEncoder.ValueRangeEndExclusive("Age", encoded);

        Assert.That(IsInRange(GrainIndexKeyEncoder.EncodeKey("Age", 17, "alice"), start, end), Is.True);
        Assert.That(IsInRange(GrainIndexKeyEncoder.EncodeKey("Age", 17, string.Empty), start, end), Is.True);
        Assert.That(IsInRange(GrainIndexKeyEncoder.EncodeKey("Age", 16, "alice"), start, end), Is.False);
        Assert.That(IsInRange(GrainIndexKeyEncoder.EncodeKey("Age", 18, "alice"), start, end), Is.False);
    }

    [Test]
    public void A_string_value_range_excludes_values_that_merely_extend_it()
    {
        var encoded = GrainIndexKeyEncoder.EncodeValue("A");
        var start = GrainIndexKeyEncoder.ValueRangeStartInclusive("Country", encoded);
        var end = GrainIndexKeyEncoder.ValueRangeEndExclusive("Country", encoded);

        Assert.That(IsInRange(GrainIndexKeyEncoder.EncodeKey("Country", "A", "alice"), start, end), Is.True);
        Assert.That(IsInRange(GrainIndexKeyEncoder.EncodeKey("Country", "AB", "alice"), start, end), Is.False);
        Assert.That(IsInRange(GrainIndexKeyEncoder.EncodeKey("Country", "A\u0000", "alice"), start, end), Is.False);
        Assert.That(IsInRange(GrainIndexKeyEncoder.EncodeKey("Country", string.Empty, "alice"), start, end), Is.False);
    }

    [Test]
    public void A_greater_than_or_equal_scan_starts_at_the_value_range_start()
    {
        var start = GrainIndexKeyEncoder.ValueRangeStartInclusive("Age", GrainIndexKeyEncoder.EncodeValue(18));
        var end = GrainIndexKeyEncoder.PropertyRangeEndExclusive("Age");

        foreach (var age in new[] { 18, 19, 100, int.MaxValue })
            Assert.That(IsInRange(GrainIndexKeyEncoder.EncodeKey("Age", age, "alice"), start, end), Is.True, age.ToString());

        foreach (var age in new[] { int.MinValue, -1, 0, 17 })
            Assert.That(IsInRange(GrainIndexKeyEncoder.EncodeKey("Age", age, "alice"), start, end), Is.False, age.ToString());
    }

    [Test]
    public void A_strictly_greater_than_scan_starts_at_the_value_range_end()
    {
        var start = GrainIndexKeyEncoder.ValueRangeEndExclusive("Age", GrainIndexKeyEncoder.EncodeValue(18));
        var end = GrainIndexKeyEncoder.PropertyRangeEndExclusive("Age");

        Assert.That(IsInRange(GrainIndexKeyEncoder.EncodeKey("Age", 18, "alice"), start, end), Is.False);
        Assert.That(IsInRange(GrainIndexKeyEncoder.EncodeKey("Age", 19, "alice"), start, end), Is.True);
    }

    [Test]
    public void Null_valued_entries_sort_at_the_bottom_of_the_property_range()
    {
        var nullKey = GrainIndexKeyEncoder.EncodeKey<int?>("LastSeen", null, "alice");
        var start = GrainIndexKeyEncoder.PropertyRangeStartInclusive("LastSeen");

        Assert.That(IsInRange(nullKey, start, GrainIndexKeyEncoder.PropertyRangeEndExclusive("LastSeen")), Is.True);
        Assert.That(
            string.CompareOrdinal(nullKey, GrainIndexKeyEncoder.EncodeKey<int?>("LastSeen", int.MinValue, "alice")),
            Is.LessThan(0));
    }

    [Test]
    public void TryParseKey_round_trips_the_three_components()
    {
        var key = GrainIndexKeyEncoder.EncodeKey("Age", 17, "alice");

        Assert.That(GrainIndexKeyEncoder.TryParseKey(key, out var property, out var value, out var grainKey), Is.True);
        Assert.That(property, Is.EqualTo("Age"));
        Assert.That(value, Is.EqualTo(GrainIndexKeyEncoder.EncodeValue(17)));
        Assert.That(grainKey, Is.EqualTo("alice"));
    }

    [Test]
    public void TryParseKey_keeps_a_grain_key_that_itself_contains_a_separator_whole()
    {
        var key = GrainIndexKeyEncoder.EncodeKey("Age", 17, "a\u0000b");

        Assert.That(GrainIndexKeyEncoder.TryParseKey(key, out var property, out _, out var grainKey), Is.True);
        Assert.That(property, Is.EqualTo("Age"));
        Assert.That(grainKey, Is.EqualTo("a\u0000b"));
    }

    [Test]
    public void TryParseKey_round_trips_an_empty_grain_key_and_an_unordered_value()
    {
        var key = GrainIndexKeyEncoder.EncodeKey("Id", Guid.Empty, string.Empty);

        Assert.That(GrainIndexKeyEncoder.TryParseKey(key, out var property, out var value, out var grainKey), Is.True);
        Assert.That(property, Is.EqualTo("Id"));
        Assert.That(value, Is.Empty);
        Assert.That(grainKey, Is.Empty);
    }

    [Test]
    public void TryParseKey_rejects_a_key_that_is_not_an_entry_key()
    {
        foreach (var candidate in new[] { string.Empty, "Age", "Age\u0000", "\u0000\u0000x" })
        {
            Assert.That(
                GrainIndexKeyEncoder.TryParseKey(candidate, out var property, out var value, out var grainKey),
                Is.False,
                candidate.Replace('\u0000', '.'));
            Assert.That(property, Is.Empty);
            Assert.That(value, Is.Empty);
            Assert.That(grainKey, Is.Empty);
        }
    }

    [Test]
    public void TryParseKey_rejects_a_null_key()
    {
        Assert.That(
            () => GrainIndexKeyEncoder.TryParseKey(null!, out _, out _, out _),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Every_entry_point_rejects_a_null_or_empty_property_name()
    {
        Assert.That(() => GrainIndexKeyEncoder.ComposeKey(null!, "v", "g"), Throws.ArgumentNullException);
        Assert.That(() => GrainIndexKeyEncoder.ComposeKey(string.Empty, "v", "g"), Throws.ArgumentException);
        Assert.That(() => GrainIndexKeyEncoder.EncodeKey(null!, 1, "g"), Throws.ArgumentNullException);
        Assert.That(() => GrainIndexKeyEncoder.PropertyRangeStartInclusive(null!), Throws.ArgumentNullException);
        Assert.That(() => GrainIndexKeyEncoder.PropertyRangeEndExclusive(string.Empty), Throws.ArgumentException);
        Assert.That(() => GrainIndexKeyEncoder.ValueRangeStartInclusive(string.Empty, "v"), Throws.ArgumentException);
        Assert.That(() => GrainIndexKeyEncoder.ValueRangeEndExclusive(null!, "v"), Throws.ArgumentNullException);
    }

    [Test]
    public void Every_entry_point_rejects_a_property_name_carrying_a_reserved_character()
    {
        foreach (var name in new[] { "A\u0000ge", "A\u0001ge" })
        {
            Assert.That(() => GrainIndexKeyEncoder.ComposeKey(name, "v", "g"), Throws.ArgumentException, name.Replace('\u0000', '.'));
            Assert.That(() => GrainIndexKeyEncoder.EncodeKey(name, 1, "g"), Throws.ArgumentException, name.Replace('\u0000', '.'));
            Assert.That(() => GrainIndexKeyEncoder.PropertyRangeStartInclusive(name), Throws.ArgumentException);
        }
    }

    [Test]
    public void Every_entry_point_rejects_an_encoded_value_carrying_the_separator()
    {
        Assert.That(() => GrainIndexKeyEncoder.ComposeKey("Age", "a\u0000b", "g"), Throws.ArgumentException);
        Assert.That(() => GrainIndexKeyEncoder.ValueRangeStartInclusive("Age", "a\u0000b"), Throws.ArgumentException);
        Assert.That(() => GrainIndexKeyEncoder.ValueRangeEndExclusive("Age", "a\u0000b"), Throws.ArgumentException);
    }

    [Test]
    public void Every_entry_point_rejects_a_null_reference_argument()
    {
        Assert.That(() => GrainIndexKeyEncoder.ComposeKey("Age", null!, "g"), Throws.ArgumentNullException);
        Assert.That(() => GrainIndexKeyEncoder.ComposeKey("Age", "v", null!), Throws.ArgumentNullException);
        Assert.That(() => GrainIndexKeyEncoder.EncodeKey("Age", 1, null!), Throws.ArgumentNullException);
        Assert.That(() => GrainIndexKeyEncoder.ValueRangeStartInclusive("Age", null!), Throws.ArgumentNullException);
        Assert.That(() => GrainIndexKeyEncoder.ValueRangeEndExclusive("Age", null!), Throws.ArgumentNullException);
    }

    [Test]
    public void A_long_grain_key_still_encodes_correctly_after_the_stack_buffer_overflows()
    {
        var grainKey = new string('x', 4096);
        var key = GrainIndexKeyEncoder.EncodeKey("Country", new string('y', 4096), grainKey);

        Assert.That(GrainIndexKeyEncoder.TryParseKey(key, out var property, out _, out var parsed), Is.True);
        Assert.That(property, Is.EqualTo("Country"));
        Assert.That(parsed, Is.EqualTo(grainKey));
    }

    private static bool IsInRange(string key, string startInclusive, string endExclusive) =>
        string.CompareOrdinal(key, startInclusive) >= 0 && string.CompareOrdinal(key, endExclusive) < 0;
}
