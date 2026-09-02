namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Tests for the <see cref="RepoContextKeys.TryParse(string, out RepoContextKey)"/>
/// rejection paths and percent-decoding edge cases the sibling
/// <see cref="RepoContextKeysTests"/> does not reach: a key whose repository id
/// or memory components decode to empty, and the hexadecimal decoder's
/// lowercase and invalid-digit arms.
/// </summary>
/// <remarks>
/// The parser is the boundary every wire-supplied key crosses before it is used
/// to address a record, so its rejections are load-bearing: a key that decoded to
/// an empty repository id or an empty memory topic would address a different
/// range than the caller named, silently. Deterministic - pure parsing.
/// </remarks>
[TestFixture]
public sealed class RepoContextKeysParseRejectionTests
{
    private static bool TryParse(string key) => RepoContextKeys.TryParse(key, out _);

    [Test]
    public void Parse_rejects_a_key_whose_repo_id_decodes_to_empty()
        => Assert.That(TryParse("repo//file/src/app.cs"), Is.False,
            "An empty repository id would address a different range than the caller named.");

    [Test]
    public void Parse_rejects_a_memory_key_whose_topic_decodes_to_empty()
        => Assert.That(TryParse("repo/acme/mem//0001"), Is.False);

    [Test]
    public void Parse_rejects_a_memory_key_whose_id_decodes_to_empty()
        => Assert.That(TryParse("repo/acme/mem/decisions/"), Is.False);

    [Test]
    public void Parse_rejects_a_memory_key_with_more_than_two_components()
        => Assert.That(TryParse("repo/acme/mem/decisions/0001/extra"), Is.False,
            "A memory id is opaque and never carries an unescaped separator.");

    [Test]
    public void Parse_accepts_a_lowercase_percent_escape()
    {
        var parsed = RepoContextKeys.TryParse("repo/acme/mem/decisions/a%2fb", out var key);

        Assert.That(parsed, Is.True);
        Assert.That(key.Id, Is.EqualTo("a/b"),
            "Percent escapes are hexadecimal, so the lowercase form must decode identically to the uppercase one.");
    }

    [Test]
    public void Parse_decodes_a_lowercase_hex_digit_above_nine()
    {
        var parsed = RepoContextKeys.TryParse("repo/acme/mem/decisions/a%2bb", out var key);

        Assert.That(parsed, Is.True);
        Assert.That(key.Id, Is.EqualTo("a+b"),
            "A lowercase 'b' is the hex digit 11, so %2b decodes to '+' exactly as %2B does.");
    }

    [Test]
    public void Parse_accepts_an_uppercase_percent_escape()
    {
        var parsed = RepoContextKeys.TryParse("repo/acme/mem/decisions/a%2Fb", out var key);

        Assert.That(parsed, Is.True);
        Assert.That(key.Id, Is.EqualTo("a/b"));
    }

    [TestCase("repo/acme/mem/decisions/a%zzb", TestName = "non-hex escape digits")]
    [TestCase("repo/acme/mem/decisions/a%2gb", TestName = "escape with a non-hex second digit")]
    [TestCase("repo/acme/mem/decisions/ab%2", TestName = "escape truncated at the end")]
    public void Parse_leaves_a_malformed_percent_escape_verbatim(string key)
    {
        var parsed = RepoContextKeys.TryParse(key, out var parsedKey);

        Assert.That(parsed, Is.True,
            "A malformed escape is not a parse failure: the component decodes verbatim rather than "
            + "silently resolving to some other character.");
        Assert.That(parsedKey.Id, Does.Contain("%"),
            "The literal percent must survive, so the decoded id is never confused with an escaped one.");
    }
}
