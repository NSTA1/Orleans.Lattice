using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Tests for <see cref="ContentManifestPlanner"/> - the sender-side manifest
/// build, the receiver-side missing-set computation (including the
/// identical-content-newer-clock high-water-mark advance), and the
/// sender-side elided-index computation.
/// </summary>
[TestFixture]
public sealed class ContentManifestPlannerTests
{
    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static WalRecord Set(string key, byte[] value, HybridLogicalClock hlc) => new()
    {
        TreeId = "tree",
        Op = MutationKind.Set,
        Key = key,
        Value = value,
        Timestamp = hlc,
        OriginClusterId = "site-a",
    };

    // --- BuildManifest --------------------------------------------------

    [Test]
    public void BuildManifest_null_batch_throws()
    {
        Assert.Throws<ArgumentNullException>(() => ContentManifestPlanner.BuildManifest(null!));
    }

    [Test]
    public void BuildManifest_empty_batch_returns_empty()
    {
        var manifest = ContentManifestPlanner.BuildManifest(new List<WalRecord>());

        Assert.That(manifest, Is.Empty);
    }

    [Test]
    public void BuildManifest_includes_only_value_carrying_point_sets()
    {
        var batch = new List<WalRecord>
        {
            Set("a", new byte[] { 1 }, Hlc(10)),
            new() { TreeId = "tree", Op = MutationKind.Delete, Key = "b", Timestamp = Hlc(11) },
            new() { TreeId = "tree", Op = MutationKind.DeleteRange, Key = "c", EndExclusiveKey = "d", Timestamp = Hlc(12) },
            Set("e", new byte[] { 2 }, Hlc(13)),
        };

        var manifest = ContentManifestPlanner.BuildManifest(batch);

        Assert.Multiple(() =>
        {
            Assert.That(manifest, Has.Count.EqualTo(2));
            Assert.That(manifest[0].EntryIndex, Is.EqualTo(0));
            Assert.That(manifest[0].Key, Is.EqualTo("a"));
            Assert.That(manifest[1].EntryIndex, Is.EqualTo(3));
            Assert.That(manifest[1].Key, Is.EqualTo("e"));
        });
    }

    [Test]
    public void BuildManifest_skips_prepared_atomic_and_zero_clock_entries()
    {
        var batch = new List<WalRecord>
        {
            Set("a", new byte[] { 1 }, Hlc(10)) with { IsPrepared = true },
            Set("b", new byte[] { 1 }, Hlc(10)) with { AtomicBatchSize = 2 },
            Set("c", new byte[] { 1 }, HybridLogicalClock.Zero),
            Set("d", new byte[] { 1 }, Hlc(10)),
        };

        var manifest = ContentManifestPlanner.BuildManifest(batch);

        Assert.Multiple(() =>
        {
            Assert.That(manifest, Has.Count.EqualTo(1));
            Assert.That(manifest[0].Key, Is.EqualTo("d"));
            Assert.That(manifest[0].EntryIndex, Is.EqualTo(3));
        });
    }

    [Test]
    public void BuildManifest_hash_matches_content_hash_for_identical_values()
    {
        var batch = new List<WalRecord>
        {
            Set("a", new byte[] { 1, 2, 3 }, Hlc(10)),
            Set("b", new byte[] { 1, 2, 3 }, Hlc(11)),
        };

        var manifest = ContentManifestPlanner.BuildManifest(batch);

        // Identical Set values for different keys hash differently because
        // the key is part of the digest, but a re-set of the same key+value
        // hashes identically (proven in the missing-set tests below).
        Assert.That(manifest[0].ContentHash, Is.Not.EqualTo(manifest[1].ContentHash));
    }

    // --- ComputeMissingSet ----------------------------------------------

    private const string Sender = "site-a";

    private static ContentManifestRequest RequestFor(IReadOnlyList<ContentManifestEntry> manifest, string origin = Sender) =>
        new() { TreeName = "tree", OriginClusterId = origin, Entries = manifest };

    [Test]
    public void ComputeMissingSet_null_lookup_throws()
    {
        var request = new ContentManifestRequest { TreeName = "tree", OriginClusterId = Sender };

        Assert.Throws<ArgumentNullException>(() =>
            ContentManifestPlanner.ComputeMissingSet(in request, null!, HybridLogicalClock.Zero));
    }

    [Test]
    public void ComputeMissingSet_reports_unknown_keys_as_missing()
    {
        var manifest = ContentManifestPlanner.BuildManifest(new List<WalRecord> { Set("a", new byte[] { 1 }, Hlc(10)) });
        var request = RequestFor(manifest);

        var response = ContentManifestPlanner.ComputeMissingSet(in request, new Dictionary<string, ReceiverHeldContent>(), HybridLogicalClock.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(response.ExchangeSupported, Is.True);
            Assert.That(response.MissingEntryIndices, Is.EqualTo(new[] { 0 }));
            Assert.That(response.AdvancedHlc, Is.EqualTo(HybridLogicalClock.Zero));
        });
    }

    [Test]
    public void ComputeMissingSet_reports_different_hash_as_missing()
    {
        var manifest = ContentManifestPlanner.BuildManifest(new List<WalRecord> { Set("a", new byte[] { 9 }, Hlc(10)) });
        var request = RequestFor(manifest);
        var receiver = new Dictionary<string, ReceiverHeldContent>
        {
            ["a"] = new(manifest[0].ContentHash ^ 0x1UL, Sender, Hlc(10)),
        };

        var response = ContentManifestPlanner.ComputeMissingSet(in request, receiver, HybridLogicalClock.Zero);

        Assert.That(response.MissingEntryIndices, Is.EqualTo(new[] { 0 }));
    }

    [Test]
    public void ComputeMissingSet_reports_the_same_bytes_at_a_newer_version_as_missing()
    {
        // #4585: last-writer-wins orders by version, not by bytes. The receiver
        // holds V at 5; V at 20 is a different write and must ship, or a
        // concurrent write between them wins here alone.
        var manifest = ContentManifestPlanner.BuildManifest(new List<WalRecord> { Set("a", new byte[] { 1 }, Hlc(20)) });
        var request = RequestFor(manifest);
        var receiver = new Dictionary<string, ReceiverHeldContent>
        {
            ["a"] = new(manifest[0].ContentHash, Sender, Hlc(5)),
        };

        var response = ContentManifestPlanner.ComputeMissingSet(in request, receiver, HybridLogicalClock.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(response.MissingEntryIndices, Is.EqualTo(new[] { 0 }));
            Assert.That(response.AdvancedHlc, Is.EqualTo(HybridLogicalClock.Zero));
        });
    }

    [Test]
    public void ComputeMissingSet_reports_the_same_bytes_and_version_from_another_origin_as_missing()
    {
        var manifest = ContentManifestPlanner.BuildManifest(new List<WalRecord> { Set("a", new byte[] { 1 }, Hlc(20)) });
        var request = RequestFor(manifest);
        var receiver = new Dictionary<string, ReceiverHeldContent>
        {
            ["a"] = new(manifest[0].ContentHash, "site-b", Hlc(20)),
        };

        var response = ContentManifestPlanner.ComputeMissingSet(in request, receiver, HybridLogicalClock.Zero);

        Assert.That(response.MissingEntryIndices, Is.EqualTo(new[] { 0 }));
    }

    [Test]
    public void ComputeMissingSet_elides_exactly_the_held_write_and_advances_above_the_high_water_mark()
    {
        var manifest = ContentManifestPlanner.BuildManifest(new List<WalRecord> { Set("a", new byte[] { 1 }, Hlc(20)) });
        var request = RequestFor(manifest);
        var receiver = new Dictionary<string, ReceiverHeldContent>
        {
            ["a"] = new(manifest[0].ContentHash, Sender, Hlc(20)),
        };

        var response = ContentManifestPlanner.ComputeMissingSet(in request, receiver, Hlc(5));

        Assert.Multiple(() =>
        {
            Assert.That(response.MissingEntryIndices, Is.Empty, "the receiver holds exactly this write");
            Assert.That(response.AdvancedHlc, Is.EqualTo(Hlc(20)),
                "a held write above the receiver's mark advances it without shipping the payload");
        });
    }

    [Test]
    public void ComputeMissingSet_does_not_advance_when_the_high_water_mark_is_at_or_above_the_held_write()
    {
        var manifest = ContentManifestPlanner.BuildManifest(new List<WalRecord> { Set("a", new byte[] { 1 }, Hlc(20)) });
        var request = RequestFor(manifest);
        var receiver = new Dictionary<string, ReceiverHeldContent>
        {
            ["a"] = new(manifest[0].ContentHash, Sender, Hlc(20)),
        };

        var response = ContentManifestPlanner.ComputeMissingSet(in request, receiver, Hlc(20));

        Assert.Multiple(() =>
        {
            Assert.That(response.MissingEntryIndices, Is.Empty);
            Assert.That(response.AdvancedHlc, Is.EqualTo(HybridLogicalClock.Zero),
                "no advance when the receiver's mark is not strictly older");
        });
    }

    [Test]
    public void ComputeMissingSet_advanced_hlc_is_the_max_across_elided_entries()
    {
        var manifest = ContentManifestPlanner.BuildManifest(new List<WalRecord>
        {
            Set("a", new byte[] { 1 }, Hlc(30)),
            Set("b", new byte[] { 2 }, Hlc(40)),
        });
        var request = RequestFor(manifest);
        var receiver = new Dictionary<string, ReceiverHeldContent>
        {
            ["a"] = new(manifest[0].ContentHash, Sender, Hlc(30)),
            ["b"] = new(manifest[1].ContentHash, Sender, Hlc(40)),
        };

        var response = ContentManifestPlanner.ComputeMissingSet(in request, receiver, Hlc(5));

        Assert.Multiple(() =>
        {
            Assert.That(response.MissingEntryIndices, Is.Empty);
            Assert.That(response.AdvancedHlc, Is.EqualTo(Hlc(40)));
        });
    }

    // --- ComputeElidedIndices -------------------------------------------

    [Test]
    public void ComputeElidedIndices_null_manifest_throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            ContentManifestPlanner.ComputeElidedIndices(null!, Array.Empty<int>()));
    }

    [Test]
    public void ComputeElidedIndices_elides_every_manifested_entry_when_none_missing()
    {
        var manifest = new[]
        {
            new ContentManifestEntry { EntryIndex = 0 },
            new ContentManifestEntry { EntryIndex = 2 },
        };

        var elided = ContentManifestPlanner.ComputeElidedIndices(manifest, Array.Empty<int>());

        Assert.That(elided, Is.EquivalentTo(new[] { 0, 2 }));
    }

    [Test]
    public void ComputeElidedIndices_keeps_missing_entries()
    {
        var manifest = new[]
        {
            new ContentManifestEntry { EntryIndex = 0 },
            new ContentManifestEntry { EntryIndex = 1 },
            new ContentManifestEntry { EntryIndex = 3 },
        };

        var elided = ContentManifestPlanner.ComputeElidedIndices(manifest, new[] { 1 });

        Assert.That(elided, Is.EquivalentTo(new[] { 0, 3 }), "only index 1 was requested, so 0 and 3 are elided");
    }

    [Test]
    public void ComputeElidedIndices_elides_nothing_when_all_missing()
    {
        var manifest = new[]
        {
            new ContentManifestEntry { EntryIndex = 0 },
            new ContentManifestEntry { EntryIndex = 1 },
        };

        var elided = ContentManifestPlanner.ComputeElidedIndices(manifest, new[] { 0, 1 });

        Assert.That(elided, Is.Empty);
    }

    // --- End-to-end round-trip composition (duplicate-heavy workload) ---

    [Test]
    public void Round_trip_elides_duplicate_payloads_and_advances_hwm()
    {
        // Duplicate-heavy batch: three exact re-sends of writes the receiver
        // already holds (a redelivery after a lost acknowledgement), and key
        // "new", a write the receiver is missing.
        var batch = new List<WalRecord>
        {
            Set("dup1", new byte[] { 1 }, Hlc(100)),
            Set("dup2", new byte[] { 2 }, Hlc(101)),
            Set("new", new byte[] { 9 }, Hlc(102)),
            Set("dup3", new byte[] { 3 }, Hlc(103)),
        };
        var manifest = ContentManifestPlanner.BuildManifest(batch);
        var request = new ContentManifestRequest { TreeName = "tree", OriginClusterId = "site-a", Entries = manifest };

        var receiver = new Dictionary<string, ReceiverHeldContent>
        {
            ["dup1"] = new(manifest[0].ContentHash, "site-a", Hlc(100)),
            ["dup2"] = new(manifest[1].ContentHash, "site-a", Hlc(101)),
            // "new" not held
            ["dup3"] = new(manifest[3].ContentHash, "site-a", Hlc(103)),
        };

        var response = ContentManifestPlanner.ComputeMissingSet(in request, receiver, Hlc(3));
        var elided = ContentManifestPlanner.ComputeElidedIndices(manifest, response.MissingEntryIndices);

        // Sender simulates dropping elided indices and summing shipped bytes.
        long shippedBytes = 0;
        var shippedKeys = new List<string>();
        for (var i = 0; i < batch.Count; i++)
        {
            if (elided.Contains(i))
            {
                continue;
            }
            shippedBytes += batch[i].Value?.Length ?? 0;
            shippedKeys.Add(batch[i].Key);
        }

        Assert.Multiple(() =>
        {
            Assert.That(response.MissingEntryIndices, Is.EqualTo(new[] { 2 }), "only the genuinely-new key is shipped");
            Assert.That(elided, Is.EquivalentTo(new[] { 0, 1, 3 }), "the three duplicates are elided");
            Assert.That(shippedKeys, Is.EqualTo(new[] { "new" }));
            Assert.That(shippedBytes, Is.EqualTo(1), "only the 1-byte new value is shipped");
            Assert.That(response.AdvancedHlc, Is.EqualTo(Hlc(103)),
                "HWM advances to the newest elided write above it");
        });
    }

    [Test]
    public void ComputeElidedIndices_ignores_wire_indices_outside_the_manifest()
    {
        // The missing set arrives over the wire and is not trusted to be a
        // subset of the manifest, so a peer that names more (or out-of-range)
        // indices than the manifest holds must not corrupt the elision plan.
        var batch = new List<WalRecord>
        {
            Set("a", [1], Hlc(1)),
            Set("b", [2], Hlc(2)),
        };
        var manifest = ContentManifestPlanner.BuildManifest(batch);

        var elided = ContentManifestPlanner.ComputeElidedIndices(manifest, [0, 1, 7, -3, 99]);

        Assert.That(elided, Is.Empty);
    }

    [Test]
    public void ComputeElidedIndices_elides_every_entry_when_nothing_is_missing()
    {
        var batch = new List<WalRecord>
        {
            Set("a", [1], Hlc(1)),
            Set("b", [2], Hlc(2)),
            Set("c", [3], Hlc(3)),
        };
        var manifest = ContentManifestPlanner.BuildManifest(batch);

        var elided = ContentManifestPlanner.ComputeElidedIndices(manifest, []);

        Assert.That(elided, Is.EquivalentTo(new[] { 0, 1, 2 }));
    }
}