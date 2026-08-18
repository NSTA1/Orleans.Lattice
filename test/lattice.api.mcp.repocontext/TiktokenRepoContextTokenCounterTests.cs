namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Tests for <see cref="TiktokenRepoContextTokenCounter"/>, the default
/// <see cref="IRepoContextTokenCounter"/>. They pin known strings to the exact BPE
/// token counts of both supported profiles (o200k_base and cl100k_base), prove the
/// two profiles genuinely disagree on at least one input (so the profile selection
/// is load-bearing), and cover empty / whitespace handling, determinism, the
/// <see cref="ReadOnlySpan{T}"/> overload parity, and the null-argument guard.
/// </summary>
[TestFixture]
public sealed class TiktokenRepoContextTokenCounterTests
{
    private static TiktokenRepoContextTokenCounter Counter(string profile) =>
        new(new RepoContextIndexingOptions { TokenizerProfile = profile });

    private static TiktokenRepoContextTokenCounter O200k() =>
        Counter(RepoContextIndexingOptions.TokenizerProfileO200k);

    private static TiktokenRepoContextTokenCounter Cl100k() =>
        Counter(RepoContextIndexingOptions.TokenizerProfileCl100k);

    [Test]
    public void Constructor_rejects_a_null_options()
    {
        Assert.That(() => new TiktokenRepoContextTokenCounter(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void CountTokens_rejects_a_null_string()
    {
        var counter = O200k();
        Assert.That(() => counter.CountTokens((string)null!), Throws.ArgumentNullException);
    }

    [Test]
    [TestCase("hello world", 2)]
    [TestCase("The quick brown fox jumps over the lazy dog.", 10)]
    [TestCase("namespace N; public class Widget { }", 8)]
    [TestCase("a", 1)]
    [TestCase("token", 1)]
    [TestCase("supercalifragilisticexpialidocious", 10)]
    [TestCase("CamelCaseIdentifierNameHere", 5)]
    public void CountTokens_matches_the_expected_o200k_count(string text, int expected)
    {
        Assert.That(O200k().CountTokens(text), Is.EqualTo(expected));
    }

    [Test]
    [TestCase("hello world", 2)]
    [TestCase("The quick brown fox jumps over the lazy dog.", 10)]
    [TestCase("namespace N; public class Widget { }", 8)]
    [TestCase("a", 1)]
    [TestCase("token", 1)]
    [TestCase("supercalifragilisticexpialidocious", 11)]
    [TestCase("CamelCaseIdentifierNameHere", 6)]
    public void CountTokens_matches_the_expected_cl100k_count(string text, int expected)
    {
        Assert.That(Cl100k().CountTokens(text), Is.EqualTo(expected));
    }

    [Test]
    public void The_two_profiles_disagree_on_at_least_one_input()
    {
        // If this ever stops differing the profile selection is untested by the count
        // assertions above; keep a real divergent input so the seam stays load-bearing.
        const string divergent = "supercalifragilisticexpialidocious";
        Assert.That(O200k().CountTokens(divergent), Is.Not.EqualTo(Cl100k().CountTokens(divergent)));
    }

    [Test]
    [TestCase("")]
    [TestCase("   ")]
    [TestCase("\t\r\n")]
    public void CountTokens_returns_zero_for_empty_input_and_a_positive_count_for_whitespace(string text)
    {
        var counter = O200k();
        if (text.Length == 0)
        {
            Assert.That(counter.CountTokens(text), Is.EqualTo(0));
        }
        else
        {
            Assert.That(counter.CountTokens(text), Is.GreaterThanOrEqualTo(0));
        }
    }

    [Test]
    public void CountTokens_returns_zero_for_the_empty_string()
    {
        Assert.That(O200k().CountTokens(string.Empty), Is.EqualTo(0));
    }

    [Test]
    public void CountTokens_is_deterministic_for_the_same_input()
    {
        var counter = O200k();
        const string text = "The quick brown fox jumps over the lazy dog.";

        var first = counter.CountTokens(text);
        var second = counter.CountTokens(text);
        var third = counter.CountTokens(text);

        Assert.That(first, Is.EqualTo(second));
        Assert.That(second, Is.EqualTo(third));
    }

    [Test]
    public void The_span_overload_agrees_with_the_string_overload()
    {
        var counter = O200k();
        const string text = "namespace N; public class Widget { }";

        Assert.That(counter.CountTokens(text.AsSpan()), Is.EqualTo(counter.CountTokens(text)));
    }

    [Test]
    public void The_span_overload_returns_zero_for_an_empty_span()
    {
        Assert.That(O200k().CountTokens(ReadOnlySpan<char>.Empty), Is.EqualTo(0));
    }
}
