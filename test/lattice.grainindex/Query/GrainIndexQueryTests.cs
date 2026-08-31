namespace Orleans.Lattice.GrainIndex.Tests.Query;

/// <summary>
/// The core query behaviour: a predicate over one projected property resolves to
/// exactly the grains whose indexed state satisfies it, in each of the result
/// shapes the surface offers.
/// </summary>
[TestFixture]
public sealed partial class GrainIndexQueryTests
{
    private static readonly DateTimeOffset Epoch = new(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Test]
    public async Task Where_single_property_range_returns_the_matching_grain_keys()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= 18));

        Assert.That(keys, Is.EquivalentTo(new[] { "bob", "carol", "dave" }));
    }

    [Test]
    public async Task Where_single_property_equality_returns_only_the_exact_match()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Country == "GB"));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "carol" }));
    }

    [Test]
    public async Task Where_bounded_range_intersects_both_bounds()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= 18 && s.Age < 40));

        Assert.That(keys, Is.EquivalentTo(new[] { "bob", "carol" }));
    }

    [Test]
    public async Task Where_strict_bounds_exclude_the_boundary_value()
    {
        var index = Populated();

        var above = await KeysAsync(index.Index.Where(s => s.Age > 18));
        var atOrBelow = await KeysAsync(index.Index.Where(s => s.Age <= 18));

        Assert.Multiple(() =>
        {
            Assert.That(above, Is.EquivalentTo(new[] { "carol", "dave" }));
            Assert.That(atOrBelow, Is.EquivalentTo(new[] { "alice", "bob" }));
        });
    }

    [Test]
    public async Task Where_reversed_operand_order_is_routed_identically()
    {
        var index = Populated();

        var natural = await KeysAsync(index.Index.Where(s => s.Age >= 18));
        var reversed = await KeysAsync(index.Index.Where(s => 18 <= s.Age));

        Assert.That(reversed, Is.EquivalentTo(natural));
    }

    [Test]
    public async Task Where_inequality_returns_every_other_grain()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Age != 18));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "carol", "dave" }));
    }

    [Test]
    public async Task Where_captured_local_is_evaluated_at_plan_time()
    {
        var index = Populated();
        int threshold = 40;

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= threshold));

        Assert.That(keys, Is.EquivalentTo(new[] { "dave" }));
    }

    [Test]
    public async Task Where_matching_nothing_yields_an_empty_result()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= 1000));

        Assert.That(keys, Is.Empty);
    }

    [Test]
    public async Task Where_contradiction_short_circuits_without_touching_the_tree()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Age < 10 && s.Age > 90));

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.Empty);
            Assert.That(index.Tree.CursorsOpened, Is.Zero);
        });
    }

    [Test]
    public async Task Where_over_an_empty_index_yields_nothing()
    {
        var index = QueryTestIndex.Create();

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= 0));

        Assert.That(keys, Is.Empty);
    }

    [Test]
    public async Task To_grains_resolves_each_matched_key_through_the_key_codec()
    {
        var index = Populated();

        var grains = new List<ITestStringKeyedGrain>();
        await foreach (var grain in index.Index.Where(s => s.Age >= 40).ToGrainsAsync())
        {
            grains.Add(grain);
        }

        Assert.That(grains, Is.EquivalentTo(new[] { index.GrainFor("dave") }));
    }

    [Test]
    public async Task To_grain_list_drains_the_scan()
    {
        var index = Populated();

        var grains = await index.Index.Where(s => s.Age >= 18).ToGrainListAsync();

        Assert.That(grains, Is.EquivalentTo(new[]
        {
            index.GrainFor("bob"),
            index.GrainFor("carol"),
            index.GrainFor("dave"),
        }));
    }

    [Test]
    public async Task To_grain_list_of_an_empty_result_is_an_empty_list()
    {
        var index = Populated();

        var grains = await index.Index.Where(s => s.Age >= 1000).ToGrainListAsync();

        Assert.That(grains, Is.Empty);
    }

    [Test]
    public async Task To_key_list_drains_the_scan()
    {
        var index = Populated();

        var keys = await index.Index.Where(s => s.Country == "GB").ToKeyListAsync();

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "carol" }));
    }

    [Test]
    public async Task To_key_list_of_an_empty_result_is_an_empty_list()
    {
        var index = Populated();

        var keys = await index.Index.Where(s => s.Country == "ZZ").ToKeyListAsync();

        Assert.That(keys, Is.Empty);
    }

    [Test]
    public async Task To_matches_carries_the_matched_entry_payload()
    {
        var index = Populated();

        var matches = new List<GrainIndexMatch>();
        await foreach (var match in index.Index.Where(s => s.Age >= 40).ToMatchesAsync())
        {
            matches.Add(match);
        }

        Assert.Multiple(() =>
        {
            Assert.That(matches, Has.Count.EqualTo(1));
            Assert.That(matches[0].GrainKey, Is.EqualTo("dave"));
            Assert.That(matches[0].PropertyName, Is.EqualTo("Age"));
            Assert.That(
                System.Text.Encoding.UTF8.GetString(matches[0].Value),
                Does.Contain("\"Age\":41"));
        });
    }

    [Test]
    public async Task To_keys_never_transfers_a_payload()
    {
        var index = Populated();

        var matches = new List<string>();
        await foreach (var key in index.Index.Where(s => s.Age >= 40).ToKeysAsync())
        {
            matches.Add(key);
        }

        Assert.That(matches, Is.EquivalentTo(new[] { "dave" }));
    }

    [Test]
    public async Task Any_reports_true_when_a_grain_matches()
    {
        var index = Populated();

        Assert.That(await index.Index.Where(s => s.Age >= 18).AnyAsync(), Is.True);
    }

    [Test]
    public async Task Any_reports_false_when_no_grain_matches()
    {
        var index = Populated();

        Assert.That(await index.Index.Where(s => s.Age >= 1000).AnyAsync(), Is.False);
    }

    [Test]
    public async Task Any_closes_the_cursor_it_abandoned()
    {
        var index = Populated();

        await index.Index.Where(s => s.Age >= 0).AnyAsync();

        Assert.That(index.Tree.OpenCursors, Is.Empty);
    }

    [Test]
    public void Index_exposes_its_name_and_projected_properties()
    {
        var index = Populated();

        Assert.Multiple(() =>
        {
            Assert.That(index.Index.Name, Is.EqualTo("Subjects"));
            Assert.That(index.Index.IndexedProperties, Is.EqualTo(new[] { "Age", "Country", "LastSeen", "Status" }));
        });
    }

    private static QueryTestIndex Populated() => QueryTestIndex.Create(
        ("alice", QueryTestIndex.State(age: 17, country: "GB", status: TestStatus.Active)),
        ("bob", QueryTestIndex.State(age: 18, country: "FR", lastSeen: Epoch, status: TestStatus.Retired)),
        ("carol", QueryTestIndex.State(age: 30, country: "GB", lastSeen: Epoch.AddDays(10), status: TestStatus.Active)),
        ("dave", QueryTestIndex.State(age: 41, country: "DE", lastSeen: Epoch.AddDays(20), status: TestStatus.Unknown)));

    private static async Task<List<string>> KeysAsync(IGrainIndexQuery<ITestStringKeyedGrain> query)
    {
        var keys = new List<string>();
        await foreach (string key in query.ToKeysAsync())
        {
            keys.Add(key);
        }

        return keys;
    }
}
