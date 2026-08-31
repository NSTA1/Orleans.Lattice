namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexFingerprint"/>: the stable digest of a
/// declaration's drift-significant fields that the registry persists and every
/// later silo start compares against.
/// </summary>
[TestFixture]
public sealed class GrainIndexFingerprintTests
{
    private static GrainIndexFingerprint Compute(
        GrainIndexDescriptor descriptor,
        string keyCodecId = DescriptorFactory.DefaultKeyCodecId) =>
        GrainIndexFingerprint.Compute(descriptor, keyCodecId);

    private static GrainIndexFingerprint Baseline() => Compute(DescriptorFactory.Create());

    [Test]
    public void Compute_is_deterministic_for_the_same_declaration()
    {
        Assert.That(
            Compute(DescriptorFactory.Create()),
            Is.EqualTo(Compute(DescriptorFactory.Create())),
            "The fingerprint is the value a restarted process compares against a stored one, so "
            + "two equal declarations must hash identically.");
    }

    [Test]
    public void Compute_matches_a_pinned_value_so_the_canonical_encoding_cannot_drift_silently()
    {
        // A golden value. It changes only when the canonical byte encoding or
        // GrainIndexFingerprint.CurrentVersion changes - both of which
        // invalidate every stored fingerprint in every deployment, so the
        // change must be deliberate rather than incidental.
        Assert.That(
            Baseline().Value,
            Is.EqualTo("515791BCF2A11031AE661F23164A1D13"),
            "A change here invalidates every persisted fingerprint. If it is intended, bump "
            + $"{nameof(GrainIndexFingerprint)}.{nameof(GrainIndexFingerprint.CurrentVersion)} and "
            + "update this pin in the same change.");
    }

    [Test]
    public void Compute_renders_the_digest_as_uppercase_hexadecimal()
    {
        var value = Baseline().Value;

        Assert.Multiple(() =>
        {
            Assert.That(value, Has.Length.EqualTo(32),
                "XxHash128 produces 16 bytes, which is 32 hexadecimal characters.");
            Assert.That(value, Does.Match("^[0-9A-F]{32}$"),
                "The rendering must be uppercase hexadecimal so the stored value compares as text.");
        });
    }

    [Test]
    public void Compute_changes_when_the_tree_name_changes()
    {
        Assert.That(
            Compute(DescriptorFactory.Create(treeName: "__grainindex/elsewhere")),
            Is.Not.EqualTo(Baseline()),
            "Entries written under the old declaration live in the old tree, so the backing tree "
            + "is drift-significant.");
    }

    [Test]
    public void Compute_changes_when_the_grain_interface_type_changes()
    {
        Assert.That(
            Compute(DescriptorFactory.Create(grainInterfaceTypeName: "Other.IGrain")),
            Is.Not.EqualTo(Baseline()));
    }

    [Test]
    public void Compute_changes_when_the_state_type_changes()
    {
        Assert.That(
            Compute(DescriptorFactory.Create(stateTypeName: "Other.State")),
            Is.Not.EqualTo(Baseline()));
    }

    [Test]
    public void Compute_changes_when_the_key_codec_changes()
    {
        Assert.That(
            Compute(DescriptorFactory.Create(), keyCodecId: "Other.Codec"),
            Is.Not.EqualTo(Baseline()),
            "The codec fixes both the encoding and the ordering of every stored entry key.");
    }

    [Test]
    public void Compute_changes_when_a_projected_property_is_added()
    {
        Assert.That(
            Compute(DescriptorFactory.Create(properties:
            [
                new GrainIndexPropertyDescriptor("Age", "System.Int32"),
                new GrainIndexPropertyDescriptor("Country", "System.String"),
                new GrainIndexPropertyDescriptor("LastSeen", "System.DateTimeOffset"),
            ])),
            Is.Not.EqualTo(Baseline()));
    }

    [Test]
    public void Compute_changes_when_a_projected_property_is_removed()
    {
        Assert.That(
            Compute(DescriptorFactory.Create(properties:
                [new GrainIndexPropertyDescriptor("Age", "System.Int32")])),
            Is.Not.EqualTo(Baseline()));
    }

    [Test]
    public void Compute_changes_when_a_projected_property_is_renamed()
    {
        Assert.That(
            Compute(DescriptorFactory.Create(properties:
            [
                new GrainIndexPropertyDescriptor("Age", "System.Int32"),
                new GrainIndexPropertyDescriptor("Nation", "System.String"),
            ])),
            Is.Not.EqualTo(Baseline()));
    }

    [Test]
    public void Compute_changes_when_a_projected_property_is_retyped()
    {
        Assert.That(
            Compute(DescriptorFactory.Create(properties:
            [
                new GrainIndexPropertyDescriptor("Age", "System.Int64"),
                new GrainIndexPropertyDescriptor("Country", "System.String"),
            ])),
            Is.Not.EqualTo(Baseline()),
            "A property's declared type is part of the entry encoding, so retyping it is as "
            + "significant as replacing the property.");
    }

    [Test]
    public void Compute_changes_when_the_projected_properties_are_reordered()
    {
        Assert.That(
            Compute(DescriptorFactory.Create(properties:
            [
                new GrainIndexPropertyDescriptor("Country", "System.String"),
                new GrainIndexPropertyDescriptor("Age", "System.Int32"),
            ])),
            Is.Not.EqualTo(Baseline()),
            "The projected set is an ordered tuple in the entry encoding, so reordering it is a "
            + "breaking change and must move the fingerprint.");
    }

    [Test]
    public void Compute_ignores_the_replication_opt_in()
    {
        Assert.That(
            Compute(DescriptorFactory.Create(allowReplication: true)),
            Is.EqualTo(Baseline()),
            "AllowReplication is drift-safe: no stored entry's encoding depends on it, so flipping "
            + "it must leave the fingerprint equal and take the update-the-record branch.");
    }

    [Test]
    public void Compute_ignores_the_index_name_because_it_is_the_registry_key()
    {
        Assert.That(
            Compute(DescriptorFactory.Create(name: "renamed", treeName: "__grainindex/users")),
            Is.EqualTo(Baseline()),
            "A record is filed under the index name, so the name cannot drift within one record; "
            + "hashing it would only make the value depend on something that never varies.");
    }

    [Test]
    public void Compute_does_not_confuse_adjacent_fields_that_share_a_concatenation()
    {
        // Without a length prefix per field, ("ab", "c") and ("a", "bc") would
        // produce the same byte stream and therefore the same fingerprint.
        var first = Compute(DescriptorFactory.Create(properties:
            [new GrainIndexPropertyDescriptor("ab", "c")]));
        var second = Compute(DescriptorFactory.Create(properties:
            [new GrainIndexPropertyDescriptor("a", "bc")]));

        Assert.That(first, Is.Not.EqualTo(second),
            "Each field is fed length-prefixed so no two field sequences can run together into "
            + "the same byte stream.");
    }

    [Test]
    public void Compute_handles_a_property_name_longer_than_the_stack_feed_limit()
    {
        var longName = new string('n', 500);
        var descriptor = DescriptorFactory.Create(properties:
            [new GrainIndexPropertyDescriptor(longName, "System.String")]);

        Assert.Multiple(() =>
        {
            Assert.That(Compute(descriptor).Value, Has.Length.EqualTo(32),
                "A name past the stack-feed limit takes the pooled-buffer path and must still hash.");
            Assert.That(Compute(descriptor), Is.EqualTo(Compute(descriptor)),
                "The pooled-buffer path must be as deterministic as the stack path.");
        });
    }

    [Test]
    public void Compute_handles_an_empty_projected_property_set()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Compute(DescriptorFactory.Create(properties: [])).Value, Has.Length.EqualTo(32));
            Assert.That(
                Compute(DescriptorFactory.Create(properties: [])),
                Is.Not.EqualTo(Baseline()),
                "An empty projection is a different declaration from a populated one.");
        });
    }

    [Test]
    public void Compute_handles_an_empty_string_field()
    {
        Assert.That(
            Compute(DescriptorFactory.Create(stateTypeName: string.Empty)).Value,
            Has.Length.EqualTo(32),
            "An empty string feeds a zero byte count and no bytes, which must not fault.");
    }

    [Test]
    public void Compute_rejects_a_null_descriptor()
    {
        Assert.That(
            () => GrainIndexFingerprint.Compute(null!, DescriptorFactory.DefaultKeyCodecId),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Compute_rejects_a_null_key_codec_id()
    {
        Assert.That(
            () => GrainIndexFingerprint.Compute(DescriptorFactory.Create(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_rejects_a_null_value()
    {
        Assert.That(() => new GrainIndexFingerprint(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_round_trips_the_supplied_value()
    {
        Assert.That(new GrainIndexFingerprint("ABCD").Value, Is.EqualTo("ABCD"));
    }

    [Test]
    public void Default_value_is_empty_and_never_equals_a_computed_fingerprint()
    {
        var uninitialised = default(GrainIndexFingerprint);

        Assert.Multiple(() =>
        {
            Assert.That(uninitialised.Value, Is.Empty,
                "The default is the 'no fingerprint yet' sentinel.");
            Assert.That(uninitialised, Is.Not.EqualTo(Baseline()));
        });
    }

    [Test]
    public void Equality_compares_the_value_not_the_reference()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new GrainIndexFingerprint("ABCD"), Is.EqualTo(new GrainIndexFingerprint("ABCD")));
            Assert.That(new GrainIndexFingerprint("ABCD"), Is.Not.EqualTo(new GrainIndexFingerprint("DCBA")));
            Assert.That(
                new GrainIndexFingerprint("ABCD").GetHashCode(),
                Is.EqualTo(new GrainIndexFingerprint("ABCD").GetHashCode()));
        });
    }

    [Test]
    public void ToString_returns_the_hexadecimal_value()
    {
        Assert.That(new GrainIndexFingerprint("ABCD").ToString(), Is.EqualTo("ABCD"));
    }

    [Test]
    public void ToString_on_a_default_value_returns_an_empty_string()
    {
        Assert.That(default(GrainIndexFingerprint).ToString(), Is.Empty,
            "A default fingerprint must render as empty rather than faulting in a log line.");
    }

    [Test]
    public void Current_version_is_a_positive_stamp_folded_into_every_digest()
    {
        Assert.That(GrainIndexFingerprint.CurrentVersion, Is.GreaterThan(0),
            "The version stamp is the documented lever for invalidating every stored fingerprint "
            + "when the entry encoding or ordering scheme changes, so it must be incrementable.");
    }
}
