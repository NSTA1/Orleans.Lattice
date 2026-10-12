using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>Every UI test belongs to exactly one browser shard with its required engines.</summary>
[TestFixture]
[Category("UI")]
public sealed class UiShardPartitionTests
{
    [Test]
    public void Every_fixture_method_is_selected_once_with_its_browser_engines()
    {
        using var document = JsonDocument.Parse(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "ui-test-shards.json")));
        var shards = document.RootElement.EnumerateArray().ToArray();
        Assert.That(shards, Has.Length.EqualTo(5));
        Assert.That(shards.Select(shard => shard.GetProperty("shard").GetString()).Distinct().Count(), Is.EqualTo(shards.Length));
        var fixtures = typeof(UiShardPartitionTests).Assembly.GetTypes()
            .Where(type => !type.IsAbstract && type.GetCustomAttributes<TestFixtureAttribute>().Any()).ToArray();
        Assert.That(fixtures, Is.Not.Empty);
        var counts = new Dictionary<string, int>();
        foreach (var fixture in fixtures)
        {
            var engines = fixture.GetCustomAttributes<TestFixtureAttribute>()
                .SelectMany(attribute => attribute.Arguments.OfType<string>())
                .Where(engine => engine is UiBrowsers.Chromium or UiBrowsers.Firefox or UiBrowsers.WebKit)
                .DefaultIfEmpty(UiBrowsers.Chromium).Distinct().ToArray();
            var methods = fixture.GetMethods().Where(method => method.GetCustomAttributes()
                .Any(attribute => attribute is TestAttribute or TestCaseAttribute or TestCaseSourceAttribute)).ToArray();
            Assert.That(methods, Is.Not.Empty, fixture.FullName);
            foreach (var method in methods)
            {
                var name = $"{fixture.FullName}.{method.Name}";
                var owners = shards.Where(shard => Matches(shard.GetProperty("filter").GetString()!, name)).ToArray();
                Assert.That(owners, Has.Length.EqualTo(1), name);
                var owner = owners.Single();
                var browsers = owner.GetProperty("browsers").EnumerateArray().Select(browser => browser.GetString()).ToArray();
                Assert.That(browsers, Is.SupersetOf(engines), name);
                var id = owner.GetProperty("shard").GetString()!;
                counts[id] = counts.GetValueOrDefault(id) + 1;
            }
        }

        Assert.That(counts.Count, Is.EqualTo(shards.Length), "No shard may select zero fixture methods.");
    }

    [TestCase("TestCategory=UI&(FullyQualifiedName~First|FullyQualifiedName~Second)&FullyQualifiedName!~Excluded", "First.Excluded", false)]
    [TestCase("TestCategory=UI&(FullyQualifiedName~First|FullyQualifiedName~Second)&FullyQualifiedName!~Excluded", "Second.Test", true)]
    [TestCase("TestCategory=UI&FullyQualifiedName~First|FullyQualifiedName~Second", "Second.Test", true)]
    public void Partition_filter_honours_grouping_and_exclusions(string filter, string name, bool expected) =>
        Assert.That(Matches(filter, name), Is.EqualTo(expected));

    private static bool Matches(string filter, string name)
    {
        var tokens = Regex.Split(filter, "([&|()])").Where(token => token.Length > 0).ToArray();
        var position = 0;
        var result = ReadOr();
        if (position != tokens.Length)
        {
            throw new InvalidOperationException($"Unparsed shard filter: {filter}");
        }

        return result;

        bool ReadOr()
        {
            var value = ReadAnd();
            while (position < tokens.Length && tokens[position] == "|")
            {
                position++;
                value |= ReadAnd();
            }

            return value;
        }

        bool ReadAnd()
        {
            var value = ReadAtom();
            while (position < tokens.Length && tokens[position] == "&")
            {
                position++;
                value &= ReadAtom();
            }

            return value;
        }

        bool ReadAtom()
        {
            var token = tokens[position++];
            if (token == "(")
            {
                var value = ReadOr();
                if (tokens[position++] != ")")
                {
                    throw new InvalidOperationException($"Unclosed shard filter group: {filter}");
                }

                return value;
            }

            if (token == "TestCategory=UI")
            {
                return true;
            }

            const string include = "FullyQualifiedName~";
            const string exclude = "FullyQualifiedName!~";
            if (token.StartsWith(include, StringComparison.Ordinal))
            {
                return name.Contains(token[include.Length..], StringComparison.OrdinalIgnoreCase);
            }

            if (token.StartsWith(exclude, StringComparison.Ordinal))
            {
                return !name.Contains(token[exclude.Length..], StringComparison.OrdinalIgnoreCase);
            }

            throw new InvalidOperationException($"Unsupported shard filter clause: {token}");
        }
    }
}
