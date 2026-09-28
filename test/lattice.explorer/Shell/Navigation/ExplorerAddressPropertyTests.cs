using System.Text;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.Shell.Navigation;

/// <summary>
/// Property-based round trips for the address grammar, over generated addresses
/// built from a hostile alphabet: every address formats to text that parses back
/// to an equal address, the canonical text is a fixed point, every route segment
/// is lower case, and the base-relative form parses the same.
/// </summary>
/// <remarks>
/// The generator is seeded, so a failure reproduces exactly; the failing seed and
/// address are in the message.
/// </remarks>
[TestFixture]
public sealed class ExplorerAddressPropertyTests
{
    private const int Cases = 600;

    // Letters of both cases, digits, every unreserved and reserved character the
    // grammar treats specially, white space, a percent sign, a multi-byte letter
    // and a character outside the basic plane.
    private static readonly string[] Alphabet =
    [
        "a", "b", "z", "A", "Z", "0", "9", "-", ".", "_", "~", "/", "?", "#", "&", "=", "%", "+", " ",
        ":", "@", "!", "*", "'", "(", ")", "[", "]", "\\", "\u00e9", "\u00df", "\u4e2d", "\U0001F600",
    ];

    private static readonly string[] Areas = ["data", "apps", "access", "tenancy", "not-found", "a1-b2"];

    private static readonly string[] Keys = ["key", "prefix", "at", "view", "x-y"];

    [Test]
    public void Every_address_round_trips_through_its_canonical_text()
    {
        ForAll(seed =>
        {
            var address = Generate(new Random(seed));
            var text = address.Format();

            Assert.That(ExplorerAddress.TryParse(text, out var parsed), Is.True, $"seed {seed}: '{text}' did not parse");
            Assert.That(parsed, Is.EqualTo(address), $"seed {seed}: '{text}' parsed to '{parsed}'");
        });
    }

    [Test]
    public void The_canonical_text_is_a_fixed_point()
    {
        ForAll(seed =>
        {
            var text = Generate(new Random(seed)).Format();
            Assert.That(ExplorerAddress.Parse(text).Format(), Is.EqualTo(text), $"seed {seed}");
        });
    }

    [Test]
    public void The_base_relative_form_parses_to_the_same_address()
    {
        ForAll(seed =>
        {
            var address = Generate(new Random(seed));
            Assert.That(ExplorerAddress.Parse(address.ToHref()), Is.EqualTo(address), $"seed {seed}: '{address.ToHref()}'");
        });
    }

    [Test]
    public void Every_route_segment_of_the_canonical_text_is_lower_case_ascii()
    {
        ForAll(seed =>
        {
            var text = Generate(new Random(seed)).Format();
            var path = text.Split('?')[0];

            foreach (var c in path)
            {
                Assert.That(
                    c is (>= 'a' and <= 'z') or (>= '0' and <= '9') or '-' or '.' or '_' or '~' or '/' or '%'
                        or (>= 'A' and <= 'F'),
                    Is.True,
                    $"seed {seed}: '{text}' carries '{c}' in its path");
            }

            foreach (var segment in path.Split('/', StringSplitOptions.RemoveEmptyEntries))
            {
                Assert.That(segment, Is.Not.EqualTo(".").And.Not.EqualTo(".."), $"seed {seed}: a dot segment would be removed by a browser");
            }
        });
    }

    [Test]
    public void Every_ancestor_of_an_address_round_trips_too()
    {
        ForAll(seed =>
        {
            for (var node = Generate(new Random(seed)); node is not null; node = node.Parent)
            {
                Assert.That(ExplorerAddress.Parse(node.Format()), Is.EqualTo(node), $"seed {seed}: ancestor '{node}'");
            }
        });
    }

    [Test]
    public void Parsing_arbitrary_text_never_throws_from_TryParse()
    {
        ForAll(seed =>
        {
            var random = new Random(seed);
            var text = Text(random, random.Next(0, 24), allowEmpty: true);
            Assert.That(() => ExplorerAddress.TryParse(text, out _), Throws.Nothing, $"seed {seed}: '{text}'");
        });
    }

    private static void ForAll(Action<int> property)
    {
        for (var seed = 1; seed <= Cases; seed++)
        {
            property(seed);
        }
    }

    private static ExplorerAddress Generate(Random random)
    {
        var tenant = random.Next(3) == 0 ? Text(random, random.Next(1, 8)) : null;
        var area = random.Next(6) == 0 ? null : Areas[random.Next(Areas.Length)];

        var path = new List<string>();
        if (area is not null)
        {
            for (var i = random.Next(0, 5); i > 0; i--)
            {
                path.Add(Text(random, random.Next(1, 10)));
            }
        }

        var query = new List<KeyValuePair<string, string>>();
        foreach (var key in Keys)
        {
            if (random.Next(3) == 0)
            {
                query.Add(new KeyValuePair<string, string>(key, Text(random, random.Next(0, 10), allowEmpty: true)));
            }
        }

        return ExplorerAddress.Create(tenant, area, path, query);
    }

    private static string Text(Random random, int length, bool allowEmpty = false)
    {
        var builder = new StringBuilder();
        for (var i = 0; i < length; i++)
        {
            builder.Append(Alphabet[random.Next(Alphabet.Length)]);
        }

        if (builder.Length == 0 && !allowEmpty)
        {
            builder.Append('x');
        }

        return builder.ToString();
    }
}
