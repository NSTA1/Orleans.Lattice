using Orleans.Lattice.GrainIndex.Registry;

namespace Orleans.Lattice.GrainIndex.Tests.Registry;

/// <summary>
/// Covers <see cref="GrainIndexDriftDetector"/>: which declaration fields it
/// reports as changed, and the invariant that its breaking verdict agrees with
/// the persisted fingerprint.
/// </summary>
[TestFixture]
public sealed class GrainIndexDriftDetectorTests
{
    private static GrainIndexRegistryRecord Stored(
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

    private static GrainIndexDriftReport Detect(
        GrainIndexDescriptor current,
        string keyCodecId = DescriptorFactory.DefaultKeyCodecId,
        GrainIndexRegistryRecord? stored = null) =>
        GrainIndexDriftDetector.Detect(stored ?? Stored(), current, keyCodecId);

    [Test]
    public void An_identical_declaration_reports_no_drift()
    {
        Assert.That(Detect(DescriptorFactory.Create()).HasDrift, Is.False);
    }

    [Test]
    public void A_changed_tree_name_is_reported_as_breaking()
    {
        var report = Detect(DescriptorFactory.Create(treeName: "__grainindex/elsewhere"));

        Assert.Multiple(() =>
        {
            Assert.That(report.ChangedFields, Is.EqualTo(new[] { GrainIndexDefinitionField.TreeName }));
            Assert.That(report.HasBreakingChange(), Is.True);
        });
    }

    [Test]
    public void A_changed_grain_interface_type_is_reported_as_breaking()
    {
        var report = Detect(DescriptorFactory.Create(grainInterfaceTypeName: "Other.IGrain"));

        Assert.That(
            report.ChangedFields,
            Is.EqualTo(new[] { GrainIndexDefinitionField.GrainInterfaceType }));
    }

    [Test]
    public void A_changed_state_type_is_reported_as_breaking()
    {
        var report = Detect(DescriptorFactory.Create(stateTypeName: "Other.State"));

        Assert.That(report.ChangedFields, Is.EqualTo(new[] { GrainIndexDefinitionField.StateType }));
    }

    [Test]
    public void A_changed_key_codec_is_reported_as_breaking()
    {
        var report = Detect(DescriptorFactory.Create(), keyCodecId: "Other.Codec");

        Assert.Multiple(() =>
        {
            Assert.That(report.ChangedFields, Is.EqualTo(new[] { GrainIndexDefinitionField.KeyCodec }));
            Assert.That(report.HasBreakingChange(), Is.True);
        });
    }

    [Test]
    public void A_removed_projected_property_is_reported_as_breaking()
    {
        var report = Detect(DescriptorFactory.Create(properties:
            [new GrainIndexPropertyDescriptor("Age", "System.Int32")]));

        Assert.Multiple(() =>
        {
            Assert.That(report.ChangedFields, Is.EqualTo(new[] { GrainIndexDefinitionField.Properties }));
            Assert.That(report.HasBreakingChange(), Is.True);
        });
    }

    [Test]
    public void An_added_projected_property_is_reported_as_breaking()
    {
        var report = Detect(DescriptorFactory.Create(properties:
        [
            new GrainIndexPropertyDescriptor("Age", "System.Int32"),
            new GrainIndexPropertyDescriptor("Country", "System.String"),
            new GrainIndexPropertyDescriptor("LastSeen", "System.DateTimeOffset"),
        ]));

        Assert.That(report.ChangedFields, Is.EqualTo(new[] { GrainIndexDefinitionField.Properties }));
    }

    [Test]
    public void A_retyped_projected_property_is_reported_as_breaking()
    {
        var report = Detect(DescriptorFactory.Create(properties:
        [
            new GrainIndexPropertyDescriptor("Age", "System.Int64"),
            new GrainIndexPropertyDescriptor("Country", "System.String"),
        ]));

        Assert.That(report.ChangedFields, Is.EqualTo(new[] { GrainIndexDefinitionField.Properties }));
    }

    [Test]
    public void Reordered_projected_properties_are_reported_as_breaking()
    {
        var report = Detect(DescriptorFactory.Create(properties:
        [
            new GrainIndexPropertyDescriptor("Country", "System.String"),
            new GrainIndexPropertyDescriptor("Age", "System.Int32"),
        ]));

        Assert.Multiple(() =>
        {
            Assert.That(report.ChangedFields, Is.EqualTo(new[] { GrainIndexDefinitionField.Properties }),
                "The projected set is an ordered tuple in the entry encoding, so a reorder is not "
                + "an equivalent declaration.");
            Assert.That(report.HasBreakingChange(), Is.True);
        });
    }

    [Test]
    public void A_flipped_replication_opt_in_is_reported_as_drift_but_not_breaking()
    {
        var report = Detect(DescriptorFactory.Create(allowReplication: true));

        Assert.Multiple(() =>
        {
            Assert.That(
                report.ChangedFields,
                Is.EqualTo(new[] { GrainIndexDefinitionField.AllowReplication }));
            Assert.That(report.HasDrift, Is.True);
            Assert.That(report.HasBreakingChange(), Is.False,
                "No stored entry's encoding depends on the opt-in, so the reconciler must take "
                + "its update-the-record branch rather than rejecting start-up.");
        });
    }

    [Test]
    public void Several_simultaneous_changes_are_all_reported()
    {
        var report = Detect(
            DescriptorFactory.Create(
                treeName: "__grainindex/elsewhere",
                allowReplication: true,
                properties: [new GrainIndexPropertyDescriptor("Age", "System.Int32")]),
            keyCodecId: "Other.Codec");

        Assert.That(report.ChangedFields, Is.EqualTo(new[]
        {
            GrainIndexDefinitionField.TreeName,
            GrainIndexDefinitionField.KeyCodec,
            GrainIndexDefinitionField.Properties,
            GrainIndexDefinitionField.AllowReplication,
        }),
            "An operator fixing a drifted declaration needs every offending field named at once, "
            + "not the first one the detector happened to reach.");
    }

    [Test]
    public void A_changed_index_name_is_reported_even_though_it_is_the_registry_key()
    {
        var report = Detect(DescriptorFactory.Create(name: "renamed", treeName: "__grainindex/users"));

        Assert.That(report.ChangedFields, Is.EqualTo(new[] { GrainIndexDefinitionField.Name }),
            "The classification is total over the declaration; the name is reported for "
            + "completeness even though a record is filed under it.");
    }

    [Test]
    public void The_detector_and_the_fingerprint_always_agree_on_whether_a_change_is_breaking()
    {
        // The registry persists the fingerprint as its durable summary and uses
        // the detector for the field-level explanation. If the two ever
        // disagreed, a breaking change could be stored under an unchanged
        // fingerprint and go unnoticed on the next start.
        var stored = Stored();
        var cases = new (GrainIndexDescriptor Descriptor, string KeyCodecId)[]
        {
            (DescriptorFactory.Create(), DescriptorFactory.DefaultKeyCodecId),
            (DescriptorFactory.Create(treeName: "__grainindex/elsewhere"), DescriptorFactory.DefaultKeyCodecId),
            (DescriptorFactory.Create(grainInterfaceTypeName: "Other.IGrain"), DescriptorFactory.DefaultKeyCodecId),
            (DescriptorFactory.Create(stateTypeName: "Other.State"), DescriptorFactory.DefaultKeyCodecId),
            (DescriptorFactory.Create(), "Other.Codec"),
            (DescriptorFactory.Create(properties: []), DescriptorFactory.DefaultKeyCodecId),
            (DescriptorFactory.Create(allowReplication: true), DescriptorFactory.DefaultKeyCodecId),
        };

        Assert.Multiple(() =>
        {
            foreach (var (descriptor, keyCodecId) in cases)
            {
                var breaking = GrainIndexDriftDetector
                    .Detect(stored, descriptor, keyCodecId)
                    .HasBreakingChange();
                var fingerprintMoved =
                    GrainIndexFingerprint.Compute(descriptor, keyCodecId) != stored.Fingerprint;

                Assert.That(fingerprintMoved, Is.EqualTo(breaking),
                    $"The fingerprint must move for exactly the breaking changes. Descriptor "
                    + $"tree '{descriptor.TreeName}', codec '{keyCodecId}'.");
            }
        });
    }

    [Test]
    public void A_null_stored_record_is_rejected()
    {
        Assert.That(
            () => GrainIndexDriftDetector.Detect(null!, DescriptorFactory.Create(), "codec"),
            Throws.ArgumentNullException);
    }

    [Test]
    public void A_null_current_descriptor_is_rejected()
    {
        Assert.That(
            () => GrainIndexDriftDetector.Detect(Stored(), null!, "codec"),
            Throws.ArgumentNullException);
    }

    [Test]
    public void A_null_current_key_codec_id_is_rejected()
    {
        Assert.That(
            () => GrainIndexDriftDetector.Detect(Stored(), DescriptorFactory.Create(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Comparisons_are_ordinal_so_a_case_change_is_drift()
    {
        var report = Detect(DescriptorFactory.Create(treeName: "__grainindex/USERS"));

        Assert.That(report.ChangedFields, Is.EqualTo(new[] { GrainIndexDefinitionField.TreeName }),
            "Tree names are ordinal keys, so a case difference addresses a different tree.");
    }
}
