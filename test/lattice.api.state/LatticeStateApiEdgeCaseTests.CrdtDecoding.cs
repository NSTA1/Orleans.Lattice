using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// CRDT revision and current-state member decoding edge cases for state API records.
/// </summary>
public sealed partial class LatticeStateApiEdgeCaseTests
{
    [Test]
    public void Map_revision_clips_value_and_delta_previews_to_the_request_budget()
    {
        var value = MapRevision(new EntryRevision
        {
            Hlc = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
            Kind = HistoryRowKind.Set,
            SourceKey = "key",
            ValuePreview = [1, 2, 3, 4],
            ValueLength = 4,
            ValueTruncated = false,
            RetentionShape = HistoryRetentionMode.FullValue,
        }, previewBudget: 2);

        var delta = MapRevision(new EntryRevision
        {
            Hlc = new HybridLogicalClock { WallClockTicks = 2, Counter = 0 },
            Kind = HistoryRowKind.CrdtDelta,
            SourceKey = "key",
            Delta = [5, 6, 7],
            ValueLength = 3,
            ValueTruncated = false,
            Mode = LatticeMergeMode.OrSet,
            RetentionShape = HistoryRetentionMode.FullValue,
        }, previewBudget: 1);

        Assert.Multiple(() =>
        {
            Assert.That(value.ValuePreview, Is.EqualTo(new byte[] { 1, 2 }));
            Assert.That(value.Truncated, Is.True);
            Assert.That(delta.Delta, Is.EqualTo(new byte[] { 5 }));
            Assert.That(delta.Truncated, Is.True);
        });
    }

    [Test]
    public void Decode_member_changes_returns_empty_for_each_early_guard()
    {
        var registry = new CrdtShapeRegistry();
        var emptyDecoderRegistry = new CrdtProvenanceDecoderRegistry(Array.Empty<ICrdtProvenanceDecoder>());

        Assert.Multiple(() =>
        {
            Assert.That(DecodeMemberChanges(new EntryRevision { Mode = LatticeMergeMode.LwwRegister }, registry), Is.Empty);
            Assert.That(DecodeMemberChanges(new EntryRevision { Mode = LatticeMergeMode.OrSet }, shapeRegistry: null), Is.Empty);
            Assert.That(DecodeMemberChanges(new EntryRevision { Mode = LatticeMergeMode.OrSet, ValueTruncated = true }, registry), Is.Empty);
            Assert.That(DecodeMemberChanges(
                new EntryRevision { Mode = LatticeMergeMode.OrSet },
                registry,
                emptyDecoderRegistry), Is.Empty);
        });
    }

    [Test]
    public void Decode_member_changes_returns_empty_when_the_tree_shape_is_not_registered()
    {
        var changes = DecodeMemberChanges(new EntryRevision
        {
            Mode = LatticeMergeMode.OrMap,
            Kind = HistoryRowKind.CrdtDelta,
            Delta = [1],
        }, new CrdtShapeRegistry());

        Assert.That(changes, Is.Empty);
    }

    [Test]
    public void Decode_member_changes_decodes_set_revisions_with_retained_values()
    {
        var shape = CrdtShape.ForOrSet();
        var stateBytes = shape.SerializeState(new OrSet());

        var changes = DecodeMemberChanges(new EntryRevision
        {
            Mode = LatticeMergeMode.OrSet,
            Kind = HistoryRowKind.Set,
            ValuePreview = stateBytes,
        }, new CrdtShapeRegistry());

        Assert.That(changes, Is.Empty);
    }

    [Test]
    public void Decode_member_changes_swallows_corrupt_retained_state_bytes()
    {
        var registry = new CrdtShapeRegistry();
        registry.Register("tree", ThrowingShape(LatticeMergeMode.OrSet));

        var changes = DecodeMemberChanges(new EntryRevision
        {
            Mode = LatticeMergeMode.OrSet,
            Kind = HistoryRowKind.Set,
            ValuePreview = [1, 2, 3],
        }, registry);

        Assert.That(changes, Is.Empty);
    }

    [Test]
    public void Decode_member_changes_returns_empty_for_revisions_without_retained_member_bytes()
    {
        var registry = new CrdtShapeRegistry();

        Assert.Multiple(() =>
        {
            Assert.That(DecodeMemberChanges(new EntryRevision
            {
                Mode = LatticeMergeMode.OrSet,
                Kind = HistoryRowKind.CrdtDelta,
                Delta = null,
            }, registry), Is.Empty);
            Assert.That(DecodeMemberChanges(new EntryRevision
            {
                Mode = LatticeMergeMode.OrSet,
                Kind = HistoryRowKind.Set,
                ValuePreview = null,
            }, registry), Is.Empty);
        });
    }

    [Test]
    public void Current_member_decode_with_null_shape_reports_opaque_bytes()
    {
        var query = CreateQuery();

        var (decoded, members) = InvokeInstance<(bool Decoded, IReadOnlyList<CrdtMemberValue> Members)>(
            query,
            "DecodeCurrentStateMembers",
            "tree",
            new byte[] { 1, 2, 3 },
            null);

        Assert.Multiple(() =>
        {
            Assert.That(decoded, Is.False);
            Assert.That(members, Is.Empty);
        });
    }

    [TestCase(null, LatticeMergeMode.OrSet, TestName = "Current_member_decode_flags_raw_when_shape_registry_is_absent")]
    [TestCase("NotARealShape", LatticeMergeMode.OrSet, TestName = "Current_member_decode_flags_raw_when_shape_tag_has_no_decoder")]
    [TestCase("OrMap", LatticeMergeMode.OrMap, TestName = "Current_member_decode_flags_raw_when_tree_shape_is_not_registered")]
    [TestCase("OrSet", LatticeMergeMode.OrSet, TestName = "Current_member_decode_flags_raw_when_state_bytes_are_corrupt")]
    public void Current_member_decode_failures_flag_entries_as_raw(string? crdtShape, LatticeMergeMode mode)
    {
        var services = new ServiceCollection();
        if (crdtShape is not null)
        {
            var registry = new CrdtShapeRegistry();
            if (crdtShape == "OrSet")
            {
                registry.Register("tree", ThrowingShape(mode));
            }

            services.AddSingleton(registry);
        }

        var query = CreateQuery(services: services.BuildServiceProvider());
        var record = new EntryRecord
        {
            Key = "key",
            CrdtShape = crdtShape ?? LatticeMergeMode.OrSet.ToString(),
        };

        var decoded = InvokeInstance<EntryRecord>(
            query,
            "WithCurrentMembers",
            record,
            "tree",
            new byte[] { 1, 2, 3 });

        Assert.Multiple(() =>
        {
            Assert.That(decoded.Raw, Is.True);
            Assert.That(decoded.CurrentMembers, Is.Empty);
        });
    }
}
