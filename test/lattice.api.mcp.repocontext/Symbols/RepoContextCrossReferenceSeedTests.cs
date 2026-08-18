using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Symbols;

/// <summary>
/// Unit tests for the pure reverse-edge projection
/// (<see cref="RepoContextSymbolReconciler.SeedRecordEdges"/>) that backs the
/// cross-reference back-fill: given a stored <see cref="SymbolRecord"/>, it emits the
/// inbound referrer edges (keyed by referenced simple name, valued by the referring
/// symbol's fully-qualified name) and the test-linkage edges a fresh reconcile would
/// author, but seeded from the record's already-stored forward references rather than
/// a fresh extraction. These run without a silo, so the projection is proven directly.
/// </summary>
[TestFixture]
public sealed class RepoContextCrossReferenceSeedTests
{
    private static SymbolRecord TypeRecord(string fqName, params string[] references)
    {
        var record = new SymbolRecord
        {
            RepoId = "acme",
            FullyQualifiedName = fqName,
            Kind = SymbolKind.Type,
        };
        foreach (var name in references)
        {
            record.References.Add(Encoding.UTF8.GetBytes(name), name, counter: 0);
        }

        return record;
    }

    private static IReadOnlyDictionary<string, HashSet<string>> Referrers(SymbolRecord record)
    {
        var referrerAdds = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        var testAdds = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        RepoContextSymbolReconciler.SeedRecordEdges(record, referrerAdds, testAdds);
        return referrerAdds;
    }

    private static IReadOnlyDictionary<string, HashSet<string>> Tests(SymbolRecord record)
    {
        var referrerAdds = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        var testAdds = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        RepoContextSymbolReconciler.SeedRecordEdges(record, referrerAdds, testAdds);
        return testAdds;
    }

    [Test]
    public void Seed_projects_a_stored_reference_into_an_inbound_referrer_edge()
    {
        // A symbol N.A whose stored record already references "B" must project the
        // reverse edge B <- N.A, exactly what the incremental delta would have emitted
        // when N.A was first extracted. This is the projection the pre-existing index
        // never rebuilt on its own because N.A's content never changed.
        var referrers = Referrers(TypeRecord("N.A", "B"));

        Assert.Multiple(() =>
        {
            Assert.That(referrers, Does.ContainKey("B"),
                "the referenced simple name gains a reverse entry");
            Assert.That(referrers["B"], Is.EquivalentTo(new[] { "N.A" }),
                "the reverse edge records the referrer's fully-qualified name");
        });
    }

    [Test]
    public void Seed_projects_every_stored_reference_not_just_a_delta()
    {
        // The force-seed treats prior reverse state as empty, so all stored references
        // are emitted - this is the whole point versus the delta, which would emit
        // nothing for an unchanged record.
        var referrers = Referrers(TypeRecord("N.A", "B", "C", "D"));

        Assert.That(referrers.Keys, Is.EquivalentTo(new[] { "B", "C", "D" }));
    }

    [Test]
    public void Seed_projects_a_test_subject_edge_by_naming_convention()
    {
        // A test type projects the same subject linkage a fresh reconcile authors.
        var tests = Tests(TypeRecord("N.WidgetTests"));

        Assert.Multiple(() =>
        {
            Assert.That(tests, Does.ContainKey("Widget"));
            Assert.That(tests["Widget"], Is.EquivalentTo(new[] { "N.WidgetTests" }));
        });
    }

    [Test]
    public void Seed_emits_no_test_edge_for_a_non_test_type()
    {
        Assert.That(Tests(TypeRecord("N.Widget")), Is.Empty);
    }

    [Test]
    public void Seed_emits_no_test_edge_for_a_test_named_member_that_is_not_a_type()
    {
        // The test-subject linkage is a type-level relationship; a method whose name
        // ends in Tests must not create one.
        var method = new SymbolRecord
        {
            RepoId = "acme",
            FullyQualifiedName = "N.C.WidgetTests()",
            Kind = SymbolKind.Method,
        };

        Assert.That(Tests(method), Is.Empty);
    }

    [Test]
    public void Seed_of_a_record_with_no_references_emits_nothing()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Referrers(TypeRecord("N.Empty")), Is.Empty);
            Assert.That(Tests(TypeRecord("N.Empty")), Is.Empty);
        });
    }

    [Test]
    public void Seed_run_twice_onto_fresh_maps_produces_the_same_edges()
    {
        // Set semantics make the projection idempotent: re-seeding the same record onto
        // fresh maps yields the identical referrer set, so a re-driven back-fill pass
        // converges rather than accumulating.
        var record = TypeRecord("N.A", "B");
        var first = Referrers(record);
        var second = Referrers(record);

        Assert.That(second["B"], Is.EquivalentTo(first["B"]));
    }

    [Test]
    public void Seed_accumulates_referrers_from_multiple_records_under_one_referenced_name()
    {
        // Two distinct referrers of the same simple name both land under it, as the
        // shared add-map accumulates across records in the seed loop.
        var referrerAdds = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        var testAdds = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        RepoContextSymbolReconciler.SeedRecordEdges(TypeRecord("N.A", "B"), referrerAdds, testAdds);
        RepoContextSymbolReconciler.SeedRecordEdges(TypeRecord("N.C", "B"), referrerAdds, testAdds);

        Assert.That(referrerAdds["B"], Is.EquivalentTo(new[] { "N.A", "N.C" }));
    }

    [Test]
    public void StoredFileMeta_round_trips_the_cross_referenced_flag()
    {
        var seeded = new StoredFileMeta(
            "digest", "csharp", 10, 0, ["N.A"],
            SymbolsProcessed: true, ContentProcessed: true, TokenCount: 5, CrossReferenced: true);
        var unseeded = new StoredFileMeta("digest", "csharp", 10, 0, ["N.A"], SymbolsProcessed: true);

        Assert.Multiple(() =>
        {
            Assert.That(seeded.CrossReferenced, Is.True);
            Assert.That(unseeded.CrossReferenced, Is.False,
                "the flag defaults to false so a node written before the marker existed is a back-fill candidate");
        });
    }

    [Test]
    public void FileNode_cross_referenced_marker_is_read_as_stored_meta_reads_it()
    {
        // The bootstrap reads the marker as "any present value"; assert that shape so
        // the selection predicate and the stored-meta projection agree.
        var stamped = new FileNode
        {
            RepoId = "acme",
            Path = "a.cs",
            CrossReferenced = RepoContextValues.Lww("1", new HybridLogicalClock { WallClockTicks = 1 }),
        };
        var bare = new FileNode { RepoId = "acme", Path = "a.cs" };

        Assert.Multiple(() =>
        {
            Assert.That(RepoContextValues.ReadString(stamped.CrossReferenced) is not null, Is.True);
            Assert.That(RepoContextValues.ReadString(bare.CrossReferenced) is not null, Is.False);
        });
    }
}
