using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// The string-to-long mapping, whose whole job is to be collision-free by
/// construction rather than with high probability. A hash of any width would make
/// a collision return the wrong document silently and undiagnosably, so the tests
/// here are about identity and reuse, not about distribution.
/// </summary>
[TestFixture]
public sealed class VectorKeyDictionaryTests
{
    private const string Prefix = "keys/";

    private static VectorKeyDictionary Create(InMemoryVectorIndexStore store, int block = 8) =>
        new(store, Prefix, block);

    [Test]
    public void The_constructor_refuses_unusable_arguments()
    {
        var store = new InMemoryVectorIndexStore();

        Assert.Multiple(() =>
        {
            Assert.That(() => new VectorKeyDictionary(null!, Prefix, 8), Throws.ArgumentNullException);
            Assert.That(() => new VectorKeyDictionary(store, null!, 8), Throws.ArgumentNullException);
            Assert.That(() => new VectorKeyDictionary(store, Prefix, 0),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(() => new VectorKeyDictionary(store, Prefix, -1),
                Throws.TypeOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public async Task Distinct_identifiers_never_share_a_key()
    {
        var store = new InMemoryVectorIndexStore();
        var keys = Create(store);
        await keys.LoadAsync();

        var seen = new HashSet<long>();
        for (var i = 0; i < 5_000; i++)
        {
            var key = await keys.GetOrAddAsync($"id-{i}");
            Assert.That(seen.Add(key), Is.True, $"'id-{i}' was assigned a key already in use.");
        }

        Assert.That(keys.Count, Is.EqualTo(5_000));
    }

    [Test]
    public async Task Identifiers_that_would_collide_under_a_hash_are_kept_apart()
    {
        // Deliberately adversarial strings: near-identical, differing only in
        // ways a truncated or folded hash would erase.
        var store = new InMemoryVectorIndexStore();
        var keys = Create(store);
        await keys.LoadAsync();

        var ids = new[]
        {
            "a/b/c.cs", "a/b/C.cs", "a/b/c.cs ", " a/b/c.cs", "a\\b\\c.cs",
            "doc-0000000001", "doc-000000001", "doc-00000001",
            string.Empty.PadRight(200, 'x'), string.Empty.PadRight(200, 'x') + "y",
        };

        var assigned = new Dictionary<long, string>();
        foreach (var id in ids)
        {
            var key = await keys.GetOrAddAsync(id);
            Assert.That(assigned.TryAdd(key, id), Is.True,
                $"'{id}' collided with '{(assigned.TryGetValue(key, out var other) ? other : "?")}'.");
        }
    }

    [Test]
    public async Task An_identifier_keeps_the_key_it_was_first_given()
    {
        var store = new InMemoryVectorIndexStore();
        var keys = Create(store);
        await keys.LoadAsync();

        var first = await keys.GetOrAddAsync("stable");
        var second = await keys.GetOrAddAsync("stable");

        Assert.That(second, Is.EqualTo(first));
        Assert.That(keys.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task A_mapping_survives_a_reload()
    {
        var store = new InMemoryVectorIndexStore();
        var written = Create(store);
        await written.LoadAsync();

        var expected = new Dictionary<string, long>(StringComparer.Ordinal);
        for (var i = 0; i < 100; i++)
        {
            expected[$"id-{i}"] = await written.GetOrAddAsync($"id-{i}");
        }

        var reloaded = Create(store);
        await reloaded.LoadAsync();

        Assert.That(reloaded.Count, Is.EqualTo(100));
        foreach (var (id, key) in expected)
        {
            Assert.That(reloaded.TryGetKey(id, out var found), Is.True);
            Assert.That(found, Is.EqualTo(key));
            Assert.That(reloaded.TryGetId(key, out var back), Is.True);
            Assert.That(back, Is.EqualTo(id));
        }
    }

    [Test]
    public async Task A_reload_never_reissues_a_key_a_previous_process_handed_out()
    {
        // The reservation is durable before any identifier in the block is used,
        // so a process that dies mid-block resumes past the whole block. The
        // unused identifiers are burned, which is free in a 64-bit space and is
        // the only way to guarantee no reuse.
        var store = new InMemoryVectorIndexStore();
        var first = Create(store, block: 8);
        await first.LoadAsync();

        var used = new HashSet<long>();
        for (var i = 0; i < 3; i++)
        {
            used.Add(await first.GetOrAddAsync($"before-{i}"));
        }

        var second = Create(store, block: 8);
        await second.LoadAsync();

        for (var i = 0; i < 20; i++)
        {
            var key = await second.GetOrAddAsync($"after-{i}");
            Assert.That(used, Does.Not.Contain(key), "A key from an abandoned reservation was reissued.");
        }
    }

    [Test]
    public async Task A_lost_watermark_is_floored_by_the_mappings_that_survived_it()
    {
        var store = new InMemoryVectorIndexStore();
        var keys = Create(store);
        await keys.LoadAsync();

        var assigned = new List<long>();
        for (var i = 0; i < 20; i++)
        {
            assigned.Add(await keys.GetOrAddAsync($"id-{i}"));
        }

        store.Drop(VectorIndexStorageKeys.KeyWatermark(Prefix));

        var reloaded = Create(store);
        await reloaded.LoadAsync();
        var next = await reloaded.GetOrAddAsync("fresh");

        Assert.That(assigned, Does.Not.Contain(next),
            "A mapping that survived a lost watermark still proves its key is in use.");
        Assert.That(reloaded.NextKey, Is.GreaterThan(assigned.Max()));
    }

    [Test]
    public async Task Removing_an_identifier_retires_its_key_for_good()
    {
        var store = new InMemoryVectorIndexStore();
        var keys = Create(store);
        await keys.LoadAsync();

        var key = await keys.GetOrAddAsync("doomed");
        Assert.That(await keys.RemoveAsync("doomed"), Is.EqualTo(key));

        Assert.Multiple(() =>
        {
            Assert.That(keys.TryGetKey("doomed", out _), Is.False);
            Assert.That(keys.TryGetId(key, out _), Is.False);
            Assert.That(keys.Count, Is.Zero);
        });

        var reassigned = await keys.GetOrAddAsync("replacement");
        Assert.That(reassigned, Is.Not.EqualTo(key), "A retired key must never be recycled.");
    }

    [Test]
    public async Task Removing_an_unknown_identifier_reports_nothing_removed()
    {
        var store = new InMemoryVectorIndexStore();
        var keys = Create(store);
        await keys.LoadAsync();

        Assert.That(await keys.RemoveAsync("absent"), Is.Null);
    }

    [Test]
    public async Task A_removal_survives_a_reload()
    {
        var store = new InMemoryVectorIndexStore();
        var keys = Create(store);
        await keys.LoadAsync();
        await keys.GetOrAddAsync("kept");
        await keys.GetOrAddAsync("doomed");
        await keys.RemoveAsync("doomed");

        var reloaded = Create(store);
        await reloaded.LoadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(reloaded.Count, Is.EqualTo(1));
            Assert.That(reloaded.TryGetKey("kept", out _), Is.True);
            Assert.That(reloaded.TryGetKey("doomed", out _), Is.False);
        });
    }

    [Test]
    public async Task Clearing_drops_every_mapping_without_rewinding_the_counter()
    {
        var store = new InMemoryVectorIndexStore();
        var keys = Create(store);
        await keys.LoadAsync();

        var before = new List<long>();
        for (var i = 0; i < 30; i++)
        {
            before.Add(await keys.GetOrAddAsync($"id-{i}"));
        }

        await keys.ClearAsync();

        Assert.Multiple(() =>
        {
            Assert.That(keys.Count, Is.Zero);
            Assert.That(store.KeysWithPrefix(VectorIndexStorageKeys.KeyMapPrefix(Prefix)), Is.Empty);
        });

        var reloaded = Create(store);
        await reloaded.LoadAsync();
        for (var i = 0; i < 30; i++)
        {
            var key = await reloaded.GetOrAddAsync($"id-{i}");
            Assert.That(before, Does.Not.Contain(key),
                "A rebuild discards the mapping, not the record of which keys have been in circulation.");
        }
    }

    [Test]
    public async Task An_undecodable_mapping_record_is_dropped_rather_than_guessed_at()
    {
        var store = new InMemoryVectorIndexStore();
        var keys = Create(store);
        await keys.LoadAsync();
        await keys.GetOrAddAsync("good");
        await keys.GetOrAddAsync("damaged");

        store.Corrupt(
            VectorIndexStorageKeys.KeyMap(Prefix, "damaged"), VectorIndexPersistenceFormat.RecordHeaderSize);

        var reloaded = Create(store);
        await reloaded.LoadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(reloaded.Count, Is.EqualTo(1));
            Assert.That(reloaded.TryGetKey("good", out _), Is.True);
            Assert.That(reloaded.TryGetKey("damaged", out _), Is.False);
        });
    }

    [Test]
    public async Task Identifiers_exposes_exactly_what_is_mapped()
    {
        var store = new InMemoryVectorIndexStore();
        var keys = Create(store);
        await keys.LoadAsync();
        await keys.GetOrAddAsync("a");
        await keys.GetOrAddAsync("b");
        await keys.RemoveAsync("a");

        Assert.That(keys.Ids, Is.EquivalentTo(new[] { "b" }));
    }

    [Test]
    public async Task An_empty_or_null_identifier_is_refused()
    {
        var store = new InMemoryVectorIndexStore();
        var keys = Create(store);
        await keys.LoadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(async () => await keys.GetOrAddAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await keys.GetOrAddAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => keys.TryGetKey(null!, out _), Throws.ArgumentNullException);
            Assert.That(async () => await keys.RemoveAsync(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task Loading_an_empty_store_yields_an_empty_dictionary()
    {
        var store = new InMemoryVectorIndexStore();
        var keys = Create(store);

        await keys.LoadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(keys.Count, Is.Zero);
            Assert.That(keys.NextKey, Is.Zero);
            Assert.That(keys.TryGetId(0, out _), Is.False);
        });
    }

    [Test]
    public async Task A_reservation_covers_a_whole_block_of_assignments()
    {
        var store = new InMemoryVectorIndexStore();
        var keys = Create(store, block: 16);
        await keys.LoadAsync();

        var writesBefore = store.Writes;
        for (var i = 0; i < 16; i++)
        {
            await keys.GetOrAddAsync($"id-{i}");
        }

        // Sixteen mapping writes plus exactly one reservation write.
        Assert.That(store.Writes - writesBefore, Is.EqualTo(17));
    }
}
