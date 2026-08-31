using Orleans.Lattice.GrainIndex.Registry;

namespace Orleans.Lattice.GrainIndex.Tests.Registry;

/// <summary>
/// Covers <see cref="GrainIndexRegistryRecord"/>: the durable record of truth
/// the registry persists per index.
/// </summary>
[TestFixture]
public sealed class GrainIndexRegistryRecordTests
{
    private static GrainIndexRegistryRecord Create(
        GrainIndexDescriptor? descriptor = null,
        string keyCodecId = DescriptorFactory.DefaultKeyCodecId,
        bool needsBackfill = false)
    {
        var effective = descriptor ?? DescriptorFactory.Create();
        return new GrainIndexRegistryRecord(
            effective,
            keyCodecId,
            GrainIndexFingerprint.Compute(effective, keyCodecId),
            needsBackfill);
    }

    [Test]
    public void The_record_round_trips_every_field_it_was_constructed_with()
    {
        var descriptor = DescriptorFactory.Create();
        var fingerprint = GrainIndexFingerprint.Compute(descriptor, "codec");
        var record = new GrainIndexRegistryRecord(descriptor, "codec", fingerprint, needsBackfill: true);

        Assert.Multiple(() =>
        {
            Assert.That(record.Descriptor, Is.SameAs(descriptor));
            Assert.That(record.KeyCodecId, Is.EqualTo("codec"));
            Assert.That(record.Fingerprint, Is.EqualTo(fingerprint));
            Assert.That(record.NeedsBackfill, Is.True);
        });
    }

    [Test]
    public void Needs_backfill_defaults_to_whatever_the_caller_states()
    {
        Assert.That(Create(needsBackfill: false).NeedsBackfill, Is.False);
    }

    [Test]
    public void A_null_descriptor_is_rejected()
    {
        Assert.That(
            () => new GrainIndexRegistryRecord(null!, "codec", default, false),
            Throws.ArgumentNullException);
    }

    [Test]
    public void A_null_key_codec_id_is_rejected()
    {
        Assert.That(
            () => new GrainIndexRegistryRecord(DescriptorFactory.Create(), null!, default, false),
            Throws.ArgumentNullException);
    }

    [Test]
    public void A_default_fingerprint_is_accepted_so_a_record_can_be_written_before_one_is_computed()
    {
        Assert.That(
            new GrainIndexRegistryRecord(DescriptorFactory.Create(), "codec", default, false).Fingerprint,
            Is.EqualTo(default(GrainIndexFingerprint)));
    }

    [Test]
    public void The_record_stays_internal_so_the_registry_shape_is_not_public_surface()
    {
        Assert.That(typeof(GrainIndexRegistryRecord).IsPublic, Is.False,
            "The registry is an entirely internal concern; no public type may leak its shape.");
    }

    [Test]
    public void The_record_carries_an_orleans_serialization_alias()
    {
        var alias = typeof(GrainIndexRegistryRecord)
            .GetCustomAttributes(typeof(AliasAttribute), inherit: false)
            .Cast<AliasAttribute>()
            .SingleOrDefault();

        Assert.That(alias?.Alias, Is.EqualTo(TypeAliases.GrainIndexRegistryRecord),
            "The record is persisted, so its wire identity must be a stable alias rather than "
            + "its CLR name.");
    }
}
