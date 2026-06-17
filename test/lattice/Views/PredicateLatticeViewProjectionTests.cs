using Orleans.Lattice.Tests.Predicates;

namespace Orleans.Lattice.Tests.Views;

/// <summary>Unit tests for the built-in <see cref="PredicateLatticeViewProjection"/>.</summary>
[TestFixture]
public class PredicateLatticeViewProjectionTests
{
    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static byte[] Encode(PredicatePerson person) =>
        JsonLatticeSerializer<PredicatePerson>.Default.Serialize(person);

    private static LatticePredicateNode AdultFilter() =>
        LatticePredicateTranslator.Translate<PredicatePerson>(p => p.Age >= 18);

    private static LatticeMutation Set(string key, byte[] value, HybridLogicalClock ts, long expires = 0) => new()
    {
        TreeId = "src",
        Kind = MutationKind.Set,
        Key = key,
        Value = value,
        Timestamp = ts,
        ExpiresAtTicks = expires,
        Category = MutationCategory.User,
    };

    [Test]
    public void Project_set_passing_filter_emits_upsert()
    {
        var projection = new PredicateLatticeViewProjection(AdultFilter());
        var value = Encode(new PredicatePerson("Alice", 30, true, 0.5, null, null));

        var writes = projection.Project(Set("k", value, Clock(5))).ToList();

        Assert.That(writes, Has.Count.EqualTo(1));
        Assert.That(writes[0].Kind, Is.EqualTo(ViewWriteKind.Upsert));
        Assert.That(writes[0].Key, Is.EqualTo("k"));
        Assert.That(writes[0].Value, Is.EqualTo(value));
        Assert.That(writes[0].Timestamp, Is.EqualTo(Clock(5)));
    }

    [Test]
    public void Project_set_failing_filter_emits_retraction_delete()
    {
        var projection = new PredicateLatticeViewProjection(AdultFilter());
        var value = Encode(new PredicatePerson("Bob", 12, true, 0.5, null, null));

        var writes = projection.Project(Set("k", value, Clock(5))).ToList();

        Assert.That(writes, Has.Count.EqualTo(1));
        Assert.That(writes[0].Kind, Is.EqualTo(ViewWriteKind.Delete));
        Assert.That(writes[0].Key, Is.EqualTo("k"));
    }

    [Test]
    public void Project_set_no_filter_is_key_preserving()
    {
        var projection = new PredicateLatticeViewProjection();
        var value = new byte[] { 7, 8, 9 };

        var writes = projection.Project(Set("k", value, Clock(1))).ToList();

        Assert.That(writes, Has.Count.EqualTo(1));
        Assert.That(writes[0].Key, Is.EqualTo("k"));
        Assert.That(writes[0].Value, Is.SameAs(value));
    }

    [Test]
    public void Project_set_with_key_remap_remaps_view_key()
    {
        var projection = new PredicateLatticeViewProjection(
            keySelector: src => $"view:{src}",
            keySelectorVersion: "v1");

        var writes = projection.Project(Set("k", [1], Clock(1))).ToList();

        Assert.That(writes[0].Key, Is.EqualTo("view:k"));
    }

    [Test]
    public void Project_set_with_value_selector_transforms_value()
    {
        var projection = new PredicateLatticeViewProjection(
            valueSelector: _ => [42],
            valueSelectorVersion: "v1");

        var writes = projection.Project(Set("k", [1], Clock(1))).ToList();

        Assert.That(writes[0].Value, Is.EqualTo(new byte[] { 42 }));
    }

    [Test]
    public void Project_set_with_value_selector_returning_null_emits_retraction_delete()
    {
        var projection = new PredicateLatticeViewProjection(
            valueSelector: _ => null,
            valueSelectorVersion: "v1");

        var writes = projection.Project(Set("k", [1], Clock(1))).ToList();

        Assert.That(writes, Has.Count.EqualTo(1));
        Assert.That(writes[0].Kind, Is.EqualTo(ViewWriteKind.Delete));
    }

    [Test]
    public void Project_delete_emits_delete_unconditionally()
    {
        var projection = new PredicateLatticeViewProjection(AdultFilter());
        var mutation = new LatticeMutation
        {
            TreeId = "src",
            Kind = MutationKind.Delete,
            Key = "k",
            Timestamp = Clock(9),
            Category = MutationCategory.User,
        };

        var writes = projection.Project(mutation).ToList();

        Assert.That(writes, Has.Count.EqualTo(1));
        Assert.That(writes[0].Kind, Is.EqualTo(ViewWriteKind.Delete));
        Assert.That(writes[0].Key, Is.EqualTo("k"));
    }

    [Test]
    public void Project_tombstone_emits_delete()
    {
        var projection = new PredicateLatticeViewProjection();
        var mutation = new LatticeMutation
        {
            TreeId = "src",
            Kind = MutationKind.Tombstone,
            Key = "k",
            Timestamp = Clock(9),
            Category = MutationCategory.User,
        };

        var writes = projection.Project(mutation).ToList();

        Assert.That(writes, Has.Count.EqualTo(1));
        Assert.That(writes[0].Kind, Is.EqualTo(ViewWriteKind.Delete));
    }

    [Test]
    public void Project_delete_range_with_matched_keys_emits_per_key_deletes()
    {
        var projection = new PredicateLatticeViewProjection();
        var mutation = new LatticeMutation
        {
            TreeId = "src",
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            MatchedKeys = ["a", "b", "c"],
            Timestamp = Clock(9),
            Category = MutationCategory.User,
        };

        var writes = projection.Project(mutation).ToList();

        Assert.That(writes.Select(w => w.Key), Is.EqualTo(new[] { "a", "b", "c" }));
        Assert.That(writes, Has.All.Matches<ViewWrite>(w => w.Kind == ViewWriteKind.Delete));
    }

    [Test]
    public void Project_set_attributes_source_key_on_upsert()
    {
        var projection = new PredicateLatticeViewProjection(
            keySelector: src => $"view:{src}",
            keySelectorVersion: "v1");

        var writes = projection.Project(Set("k", [1], Clock(1))).ToList();

        Assert.That(writes[0].Key, Is.EqualTo("view:k"));
        Assert.That(writes[0].SourceKey, Is.EqualTo("k"));
    }

    [Test]
    public void Project_delete_with_key_remap_recomputes_view_key_from_source_key()
    {
        // The re-key is a pure function of the source key, so a value-less delete
        // recomputes the same view key the matching upsert produced.
        var projection = new PredicateLatticeViewProjection(
            keySelector: src => $"view:{src}",
            keySelectorVersion: "v1");

        var mutation = new LatticeMutation
        {
            TreeId = "src",
            Kind = MutationKind.Delete,
            Key = "k",
            Timestamp = Clock(9),
            Category = MutationCategory.User,
        };

        var writes = projection.Project(mutation).ToList();

        Assert.That(writes, Has.Count.EqualTo(1));
        Assert.That(writes[0].Kind, Is.EqualTo(ViewWriteKind.Delete));
        Assert.That(writes[0].Key, Is.EqualTo("view:k"));
        Assert.That(writes[0].SourceKey, Is.EqualTo("k"));
    }

    [Test]
    public void Project_delete_range_with_matched_keys_and_remap_maps_each_key()
    {
        var projection = new PredicateLatticeViewProjection(
            keySelector: src => $"view:{src}",
            keySelectorVersion: "v1");

        var mutation = new LatticeMutation
        {
            TreeId = "src",
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            MatchedKeys = ["a", "b"],
            Timestamp = Clock(9),
            Category = MutationCategory.User,
        };

        var writes = projection.Project(mutation).ToList();

        Assert.That(writes.Select(w => w.Key), Is.EqualTo(new[] { "view:a", "view:b" }));
        Assert.That(writes.Select(w => w.SourceKey), Is.EqualTo(new[] { "a", "b" }));
        Assert.That(writes, Has.All.Matches<ViewWrite>(w => w.Kind == ViewWriteKind.Delete));
    }

    [Test]
    public void Project_delete_range_without_matched_keys_key_preserving_emits_range_delete()
    {
        var projection = new PredicateLatticeViewProjection();
        var mutation = new LatticeMutation
        {
            TreeId = "src",
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            Timestamp = Clock(9),
            Category = MutationCategory.User,
        };

        var writes = projection.Project(mutation).ToList();

        Assert.That(writes, Has.Count.EqualTo(1));
        Assert.That(writes[0].Kind, Is.EqualTo(ViewWriteKind.RangeDelete));
        Assert.That(writes[0].Key, Is.EqualTo("a"));
        Assert.That(writes[0].EndKey, Is.EqualTo("z"));
    }

    [Test]
    public void Project_delete_range_without_matched_keys_rekeyed_emits_range_reconcile()
    {
        var projection = new PredicateLatticeViewProjection(
            keySelector: src => $"view:{src}",
            keySelectorVersion: "v1");

        var mutation = new LatticeMutation
        {
            TreeId = "src",
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            Timestamp = Clock(9),
            Category = MutationCategory.User,
        };

        var writes = projection.Project(mutation).ToList();

        Assert.That(writes, Has.Count.EqualTo(1));
        Assert.That(writes[0].Kind, Is.EqualTo(ViewWriteKind.RangeReconcile));
        Assert.That(writes[0].Key, Is.EqualTo("a"));
        Assert.That(writes[0].EndKey, Is.EqualTo("z"));
    }

    [Test]
    public void Project_delete_range_without_matched_keys_or_end_emits_nothing()
    {
        var projection = new PredicateLatticeViewProjection();
        var mutation = new LatticeMutation
        {
            TreeId = "src",
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = null,
            Timestamp = Clock(9),
            Category = MutationCategory.User,
        };

        Assert.That(projection.Project(mutation), Is.Empty);
    }

    [Test]
    public void Project_tx_commit_emits_nothing()
    {
        var projection = new PredicateLatticeViewProjection();
        var mutation = new LatticeMutation
        {
            TreeId = "src",
            Kind = MutationKind.TxCommit,
            Key = "k",
            Timestamp = Clock(9),
            Category = MutationCategory.User,
        };

        Assert.That(projection.Project(mutation), Is.Empty);
    }

    [Test]
    public void ProjectionVersion_is_stable_across_instances_with_same_filter()
    {
        var a = new PredicateLatticeViewProjection(AdultFilter());
        var b = new PredicateLatticeViewProjection(AdultFilter());

        Assert.That(a.ProjectionVersion, Is.EqualTo(b.ProjectionVersion));
    }

    [Test]
    public void ProjectionVersion_changes_when_filter_changes()
    {
        var a = new PredicateLatticeViewProjection(AdultFilter());
        var b = new PredicateLatticeViewProjection(
            LatticePredicateTranslator.Translate<PredicatePerson>(p => p.Age >= 21));

        Assert.That(a.ProjectionVersion, Is.Not.EqualTo(b.ProjectionVersion));
    }

    [Test]
    public void ProjectionVersion_changes_when_selector_version_changes()
    {
        var a = new PredicateLatticeViewProjection(valueSelector: v => v, valueSelectorVersion: "v1");
        var b = new PredicateLatticeViewProjection(valueSelector: v => v, valueSelectorVersion: "v2");

        Assert.That(a.ProjectionVersion, Is.Not.EqualTo(b.ProjectionVersion));
    }

    [Test]
    public void Constructor_value_selector_without_version_throws()
    {
        Assert.That(
            () => new PredicateLatticeViewProjection(valueSelector: v => v),
            Throws.ArgumentException);
    }

    [Test]
    public void Constructor_key_selector_without_version_throws()
    {
        Assert.That(
            () => new PredicateLatticeViewProjection(keySelector: k => k),
            Throws.ArgumentException);
    }
}
