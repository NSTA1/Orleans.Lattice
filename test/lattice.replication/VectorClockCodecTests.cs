using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class VectorClockCodecTests
{
    private static VersionVector Vc(params (string id, HybridLogicalClock clock)[] entries)
    {
        var vc = new VersionVector();
        foreach (var (id, clock) in entries)
        {
            vc.Entries[id] = clock;
        }
        return vc;
    }

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    [Test]
    public void EncodeAbsolute_null_returns_empty_non_null_vector()
    {
        var encoded = VectorClockCodec.EncodeAbsolute(null);
        Assert.That(encoded, Is.Not.Null);
        Assert.That(encoded.Entries, Is.Empty);
    }

    [Test]
    public void EncodeAbsolute_returns_independent_copy()
    {
        var source = Vc(("a", Hlc(10)));
        var encoded = VectorClockCodec.EncodeAbsolute(source);

        Assert.That(encoded, Is.Not.SameAs(source));
        Assert.That(encoded.Entries, Has.Count.EqualTo(1));
        Assert.That(encoded.GetClock("a"), Is.EqualTo(Hlc(10)));

        // Mutating the source must not leak into the snapshot.
        source.Tick("b");
        Assert.That(encoded.Entries.ContainsKey("b"), Is.False);
    }

    [Test]
    public void EncodeDelta_null_current_returns_empty_delta()
    {
        var delta = VectorClockCodec.EncodeDelta(null, Vc(("a", Hlc(5))));
        Assert.That(delta.Entries, Is.Empty);
    }

    [Test]
    public void EncodeDelta_null_predecessor_returns_full_current()
    {
        var current = Vc(("a", Hlc(7)), ("b", Hlc(3)));
        var delta = VectorClockCodec.EncodeDelta(current, null);

        Assert.That(delta.GetClock("a"), Is.EqualTo(Hlc(7)));
        Assert.That(delta.GetClock("b"), Is.EqualTo(Hlc(3)));
        Assert.That(delta.Entries, Has.Count.EqualTo(2));
    }

    [Test]
    public void EncodeDelta_omits_unchanged_origins()
    {
        var predecessor = Vc(("a", Hlc(5)), ("b", Hlc(2)));
        var current = Vc(("a", Hlc(5)), ("b", Hlc(7)));
        var delta = VectorClockCodec.EncodeDelta(current, predecessor);

        Assert.That(delta.Entries.ContainsKey("a"), Is.False, "unchanged origin must be omitted");
        Assert.That(delta.GetClock("b"), Is.EqualTo(Hlc(7)));
    }

    [Test]
    public void EncodeDelta_omits_origins_whose_clock_regressed()
    {
        var predecessor = Vc(("a", Hlc(10)));
        var current = Vc(("a", Hlc(3)));
        var delta = VectorClockCodec.EncodeDelta(current, predecessor);

        Assert.That(delta.Entries, Is.Empty);
    }

    [Test]
    public void DecodeDelta_null_delta_reproduces_predecessor_independently()
    {
        var predecessor = Vc(("a", Hlc(5)));
        var decoded = VectorClockCodec.DecodeDelta(null, predecessor);

        Assert.That(decoded, Is.Not.SameAs(predecessor));
        Assert.That(decoded.GetClock("a"), Is.EqualTo(Hlc(5)));
    }

    [Test]
    public void DecodeDelta_null_predecessor_reduces_to_absolute_of_delta()
    {
        var delta = Vc(("a", Hlc(8)));
        var decoded = VectorClockCodec.DecodeDelta(delta, null);

        Assert.That(decoded.GetClock("a"), Is.EqualTo(Hlc(8)));
        Assert.That(decoded.Entries, Has.Count.EqualTo(1));
    }

    [Test]
    public void Round_trip_via_delta_reproduces_current_for_unchanged_origins()
    {
        var predecessor = Vc(("a", Hlc(5)), ("b", Hlc(2)));
        var current = Vc(("a", Hlc(5)), ("b", Hlc(7)), ("c", Hlc(1)));

        var delta = VectorClockCodec.EncodeDelta(current, predecessor);
        var decoded = VectorClockCodec.DecodeDelta(delta, predecessor);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.GetClock("a"), Is.EqualTo(Hlc(5)), "carried over from predecessor");
            Assert.That(decoded.GetClock("b"), Is.EqualTo(Hlc(7)), "advanced via delta");
            Assert.That(decoded.GetClock("c"), Is.EqualTo(Hlc(1)), "introduced via delta");
        });
    }

    [Test]
    public void Round_trip_via_absolute_is_self_contained_after_predecessor_trim()
    {
        // When the predecessor is trimmed by GC the producer must have
        // emitted an absolute frontier for the entry. A receiver
        // decodes the absolute encoding against a null predecessor and
        // recovers the frontier exactly.
        var current = Vc(("a", Hlc(9)), ("b", Hlc(4)));
        var absolute = VectorClockCodec.EncodeAbsolute(current);

        var decoded = VectorClockCodec.DecodeDelta(absolute, predecessor: null);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.GetClock("a"), Is.EqualTo(Hlc(9)));
            Assert.That(decoded.GetClock("b"), Is.EqualTo(Hlc(4)));
            Assert.That(decoded.Entries, Has.Count.EqualTo(2));
        });
    }

    [Test]
    public void Delta_decoded_against_missing_predecessor_loses_carry_over_entries()
    {
        // Documents the GC-safety contract from the codec remarks:
        // a delta-only entry whose predecessor has been trimmed is
        // collapsed to just the advanced origins. The producer must
        // therefore emit an absolute encoding at trim boundaries.
        var predecessor = Vc(("a", Hlc(5)), ("b", Hlc(2)));
        var current = Vc(("a", Hlc(5)), ("b", Hlc(7)));
        var delta = VectorClockCodec.EncodeDelta(current, predecessor);

        var decoded = VectorClockCodec.DecodeDelta(delta, predecessor: null);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.GetClock("b"), Is.EqualTo(Hlc(7)), "advanced origin survives");
            Assert.That(
                decoded.Entries.ContainsKey("a"),
                Is.False,
                "unchanged origin from trimmed predecessor is lost - expected by contract");
        });
    }

    [Test]
    public void Encode_absolute_and_decode_delta_agree_on_empty_inputs()
    {
        Assert.Multiple(() =>
        {
            Assert.That(VectorClockCodec.EncodeAbsolute(null).Entries, Is.Empty);
            Assert.That(VectorClockCodec.EncodeDelta(null, null).Entries, Is.Empty);
            Assert.That(VectorClockCodec.DecodeDelta(null, null).Entries, Is.Empty);
        });
    }

    [Test]
    public void Encoding_is_stable_across_repeated_calls()
    {
        var current = Vc(("a", Hlc(5)), ("b", Hlc(7)));
        var predecessor = Vc(("a", Hlc(5)));

        var d1 = VectorClockCodec.EncodeDelta(current, predecessor);
        var d2 = VectorClockCodec.EncodeDelta(current, predecessor);

        Assert.Multiple(() =>
        {
            Assert.That(d1.Entries, Is.EquivalentTo(d2.Entries));
            Assert.That(d1.GetClock("b"), Is.EqualTo(d2.GetClock("b")));
        });
    }

    // -- Gap (ii): codec input-immutability symmetry ------------------

    [Test]
    public void EncodeDelta_returns_independent_copy_so_post_call_source_advance_does_not_leak()
    {
        var current = Vc(("a", Hlc(5)));
        var predecessor = Vc(("a", Hlc(2)));
        var delta = VectorClockCodec.EncodeDelta(current, predecessor);

        Assert.That(delta, Is.Not.SameAs(current));

        // Mutating the source AFTER the encode must not leak into the
        // already-returned delta. Symmetric to
        // EncodeAbsolute_returns_independent_copy.
        current.Tick("b");
        current.Entries["a"] = Hlc(99);

        Assert.Multiple(() =>
        {
            Assert.That(delta.Entries.ContainsKey("b"), Is.False);
            Assert.That(delta.GetClock("a"), Is.EqualTo(Hlc(5)));
        });
    }

    [Test]
    public void EncodeDelta_does_not_mutate_predecessor()
    {
        var predecessor = Vc(("a", Hlc(2)));
        var current = Vc(("a", Hlc(5)), ("b", Hlc(7)));
        _ = VectorClockCodec.EncodeDelta(current, predecessor);

        Assert.Multiple(() =>
        {
            Assert.That(predecessor.Entries, Has.Count.EqualTo(1));
            Assert.That(predecessor.GetClock("a"), Is.EqualTo(Hlc(2)));
        });
    }

    [Test]
    public void DecodeDelta_returns_independent_copy_so_post_call_predecessor_advance_does_not_leak()
    {
        var predecessor = Vc(("a", Hlc(5)));
        var delta = Vc(("b", Hlc(3)));
        var decoded = VectorClockCodec.DecodeDelta(delta, predecessor);

        Assert.Multiple(() =>
        {
            Assert.That(decoded, Is.Not.SameAs(predecessor));
            Assert.That(decoded, Is.Not.SameAs(delta));
        });

        // Mutating either input AFTER the decode must not leak into
        // the result.
        predecessor.Tick("c");
        predecessor.Entries["a"] = Hlc(99);
        delta.Tick("d");

        Assert.Multiple(() =>
        {
            Assert.That(decoded.Entries.ContainsKey("c"), Is.False);
            Assert.That(decoded.Entries.ContainsKey("d"), Is.False);
            Assert.That(decoded.GetClock("a"), Is.EqualTo(Hlc(5)));
            Assert.That(decoded.GetClock("b"), Is.EqualTo(Hlc(3)));
        });
    }

    [Test]
    public void DecodeDelta_does_not_mutate_inputs()
    {
        var predecessor = Vc(("a", Hlc(5)));
        var delta = Vc(("b", Hlc(3)));
        _ = VectorClockCodec.DecodeDelta(delta, predecessor);

        Assert.Multiple(() =>
        {
            Assert.That(predecessor.Entries, Has.Count.EqualTo(1));
            Assert.That(predecessor.GetClock("a"), Is.EqualTo(Hlc(5)));
            Assert.That(delta.Entries, Has.Count.EqualTo(1));
            Assert.That(delta.GetClock("b"), Is.EqualTo(Hlc(3)));
        });
    }

    // -- Gap (iv): HLC-equal boundary in EncodeDelta ------------------

    [Test]
    public void EncodeDelta_omits_origins_whose_clock_is_exactly_equal_to_predecessor()
    {
        // Pin the boundary of the "strictly advances" rule: an origin
        // whose (WallClockTicks, Counter) is byte-identical to the
        // predecessor is not an advance and must be omitted from the
        // delta. Today the codec uses `clock > prior` which evaluates
        // false for equality; this test guards against any future
        // rewrite that flips the comparison to `>=`.
        var hlc = new HybridLogicalClock { WallClockTicks = 100, Counter = 7 };
        var predecessor = Vc(("a", hlc));
        var current = Vc(("a", hlc));

        var delta = VectorClockCodec.EncodeDelta(current, predecessor);

        Assert.That(delta.Entries, Is.Empty);
    }

    // -- Gap (v): many-origin scale ----------------------------------

    [Test]
    public void Round_trip_via_delta_handles_many_origins()
    {
        // Validate the codec on a non-trivial origin count to catch
        // any future rewrite that introduces O(n^2) work or relies on
        // dictionary iteration order. 12 origins exceeds typical
        // cluster sizes and exercises every branch of the merge loop.
        var predecessor = new VersionVector();
        var current = new VersionVector();
        for (var i = 0; i < 12; i++)
        {
            var id = $"site-{i:D2}";
            predecessor.Entries[id] = Hlc(i * 10);
            current.Entries[id] = Hlc(i * 10 + 5);
        }
        // Plus one origin only present on the current frontier.
        current.Entries["site-12"] = Hlc(7);

        var delta = VectorClockCodec.EncodeDelta(current, predecessor);
        var decoded = VectorClockCodec.DecodeDelta(delta, predecessor);

        Assert.Multiple(() =>
        {
            Assert.That(delta.Entries, Has.Count.EqualTo(13),
                "every origin advanced on current must appear in the delta");
            Assert.That(decoded.Entries, Has.Count.EqualTo(13));
            for (var i = 0; i < 12; i++)
            {
                var id = $"site-{i:D2}";
                Assert.That(decoded.GetClock(id), Is.EqualTo(Hlc(i * 10 + 5)),
                    $"{id} must round-trip to current value");
            }
            Assert.That(decoded.GetClock("site-12"), Is.EqualTo(Hlc(7)));
        });
    }
}
