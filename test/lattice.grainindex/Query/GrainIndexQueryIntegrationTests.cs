using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.GrainIndex.Tests.Query;

/// <summary>
/// The query surface end to end: a real silo, a real lattice tree, entries
/// written by the real maintainer, and queries resolved through the real
/// <see cref="IGrainIndexProvider"/> registration.
/// <para>
/// These cover the acceptance shapes the unit tier proves against an in-memory
/// stand-in - single-property range, equality, string methods, conjunction
/// across properties, disjunction, an empty result, and the failure paths - so a
/// regression in the real scan, cursor, or push-down surface is caught here
/// rather than in a downstream package.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class GrainIndexQueryIntegrationTests
{
    private static readonly DateTimeOffset Epoch = new(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private const int BulkCount = 240;
    private const string BulkCountry = "BULK";

    private GrainIndexQueryClusterFixture _fixture = null!;
    private IGrainIndex<ITestStringKeyedGrain, IndexedTestState> _index = null!;
    private string[] _bulkKeys = [];

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new GrainIndexQueryClusterFixture();
        await _fixture.InitializeAsync();

        await SeedAsync(
            ("alice", State(17, "GB", null, TestStatus.Active)),
            ("bob", State(18, "FR", Epoch, TestStatus.Retired)),
            ("carol", State(30, "GB", Epoch.AddDays(10), TestStatus.Active)),
            ("dave", State(41, "DE", Epoch.AddDays(20), TestStatus.Unknown)));

        // The bulk rows share the tree with the four named subjects, so they are
        // seeded once here rather than inside a test body and are placed in a
        // value space no other test's predicate selects: every age is negative
        // and the country is one nothing else queries for.
        _bulkKeys = new string[BulkCount];
        var bulk = new (string Key, IndexedTestState State)[BulkCount];
        for (var i = 0; i < BulkCount; i++)
        {
            string key = "bulk-" + i.ToString("D4", System.Globalization.CultureInfo.InvariantCulture);
            _bulkKeys[i] = key;
            bulk[i] = (key, State(-1 - i, BulkCountry, null, TestStatus.Retired));
        }

        await SeedAsync(bulk);

        _index = _fixture.SiloServices
            .GetRequiredService<IGrainIndexProvider>()
            .GetIndex<ITestStringKeyedGrain, IndexedTestState>(GrainIndexQueryClusterFixture.DeclaredIndexName);
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task A_single_property_range_returns_exactly_the_matching_grains()
    {
        var keys = await _index.Where(s => s.Age >= 18).ToKeyListAsync();

        Assert.That(keys, Is.EquivalentTo(new[] { "bob", "carol", "dave" }));
    }

    [Test]
    public async Task An_equality_returns_exactly_the_matching_grains()
    {
        var keys = await _index.Where(s => s.Country == "GB").ToKeyListAsync();

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "carol" }));
    }

    [Test]
    public async Task A_string_method_returns_exactly_the_matching_grains()
    {
        var prefixed = await _index.Where(s => s.Country.StartsWith("G")).ToKeyListAsync();
        var contained = await _index.Where(s => s.Country.Contains("R")).ToKeyListAsync();

        Assert.Multiple(() =>
        {
            Assert.That(prefixed, Is.EquivalentTo(new[] { "alice", "carol" }));
            Assert.That(contained, Is.EquivalentTo(new[] { "bob" }));
        });
    }

    [Test]
    public async Task A_conjunction_across_properties_intersects_the_two_scans()
    {
        var keys = await _index.Where(s => s.Age >= 18 && s.Country == "GB").ToKeyListAsync();

        Assert.That(keys, Is.EquivalentTo(new[] { "carol" }));
    }

    [Test]
    public async Task A_disjunction_unions_the_scans_and_yields_each_grain_once()
    {
        var keys = await _index.Where(s => s.Age >= 18 || s.Country == "GB").ToKeyListAsync();

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "bob", "carol", "dave" }));
    }

    [Test]
    public async Task A_predicate_matching_nothing_returns_an_empty_result()
    {
        var keys = await _index.Where(s => s.Age >= 1000).ToKeyListAsync();

        Assert.That(keys, Is.Empty);
    }

    [Test]
    public async Task A_payload_predicate_over_an_unordered_type_matches_through_the_tree_push_down()
    {
        var keys = await _index.Where(s => s.Status == TestStatus.Active).ToKeyListAsync();

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "carol" }));
    }

    [Test]
    public async Task A_date_range_is_served_from_the_key_range()
    {
        var keys = await _index.Where(s => s.LastSeen >= Epoch.AddDays(10)).ToKeyListAsync();

        Assert.That(keys, Is.EquivalentTo(new[] { "carol", "dave" }));
    }

    [Test]
    public async Task Matched_grains_resolve_to_addressable_references()
    {
        var grains = await _index.Where(s => s.Age >= 40).ToGrainListAsync();

        Assert.Multiple(() =>
        {
            Assert.That(grains, Has.Count.EqualTo(1));
            Assert.That(grains[0].GetPrimaryKeyString(), Is.EqualTo("dave"));
        });
    }

    [Test]
    public async Task A_matched_entry_carries_its_projected_payload()
    {
        var matches = new List<GrainIndexMatch>();
        await foreach (var match in _index.Where(s => s.Age >= 40).ToMatchesAsync())
        {
            matches.Add(match);
        }

        Assert.Multiple(() =>
        {
            Assert.That(matches, Has.Count.EqualTo(1));
            Assert.That(matches[0].GrainKey, Is.EqualTo("dave"));
            Assert.That(System.Text.Encoding.UTF8.GetString(matches[0].Value), Does.Contain("\"Age\":41"));
        });
    }

    [Test]
    public void A_predicate_over_an_unprojected_property_fails_at_translation_time()
    {
        var exception = Assert.Throws<GrainIndexPropertyNotIndexedException>(
            () => _index.Where(s => s.Secret == "classified"));

        Assert.That(exception!.PropertyName, Is.EqualTo("Secret"));
    }

    [Test]
    public void An_unsupported_construct_fails_at_translation_time()
    {
        Assert.Throws<NotSupportedException>(() => _index.Where(s => s.Country.ToUpperInvariant() == "GB"));
    }

    [Test]
    public async Task Every_execution_mode_returns_the_same_rows()
    {
        var durable = await _index.Where(s => s.Age >= 18).ToKeyListAsync();
        var streamed = await _index.Where(s => s.Age >= 18)
            .WithExecution(GrainIndexQueryExecution.Stream)
            .ToKeyListAsync();
        var snapshot = await _index.Where(s => s.Age >= 18)
            .WithExecution(GrainIndexQueryExecution.SnapshotCursor)
            .ToKeyListAsync();

        Assert.Multiple(() =>
        {
            Assert.That(streamed, Is.EquivalentTo(durable));
            Assert.That(snapshot, Is.EquivalentTo(durable));
        });
    }

    [Test]
    public async Task A_large_result_set_pages_through_a_durable_cursor()
    {
        var seen = new List<string>(BulkCount);
        await foreach (string key in _index.Where(s => s.Country == BulkCountry).WithPageSize(16).ToKeysAsync())
        {
            seen.Add(key);
        }

        Assert.Multiple(() =>
        {
            Assert.That(seen, Is.EquivalentTo(_bulkKeys));
            Assert.That(seen.Distinct(StringComparer.Ordinal).Count(), Is.EqualTo(BulkCount));
        });
    }

    private async Task SeedAsync(params (string Key, IndexedTestState State)[] subjects)
    {
        var definition = GrainIndexQueryClusterFixture.Definition();
        var tree = _fixture.Cluster.GrainFactory.GetGrain<ILattice>(
            GrainIndexTreeNames.ForIndex(GrainIndexQueryClusterFixture.DeclaredIndexName));
        var maintainer = new GrainIndexMaintainer<ITestStringKeyedGrain, IndexedTestState>(definition, tree);

        foreach (var subject in subjects)
        {
            await maintainer.UpdateAsync(
                GrainIndexProjection.Empty(subject.Key),
                subject.Key,
                subject.State);
        }
    }

    private static IndexedTestState State(int age, string country, DateTimeOffset? lastSeen, TestStatus status) =>
        new()
        {
            Age = age,
            Country = country,
            LastSeen = lastSeen,
            Status = status,
        };
}
