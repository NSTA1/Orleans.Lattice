namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Symbols;

/// <summary>
/// Unit tests for <see cref="DeclaredSymbolNames"/>, the deterministic codec for the
/// newline-joined declared-symbol projection stamped onto a file node. The encoding
/// is ordinal-sorted and de-duplicated so an unchanged declared set round-trips to
/// the same bytes and never churns the last-writer-wins register.
/// </summary>
[TestFixture]
public sealed class DeclaredSymbolNamesTests
{
    [Test]
    public void Encode_is_sorted_deduplicated_and_drops_blanks()
    {
        var encoded = DeclaredSymbolNames.Encode(["N.B", "N.A", "N.B", "", "N.C"]);

        Assert.That(encoded, Is.EqualTo("N.A\nN.B\nN.C"),
            "names are ordinal-sorted, de-duplicated, and blanks are dropped");
    }

    [Test]
    public void Encode_is_order_insensitive()
    {
        var a = DeclaredSymbolNames.Encode(["N.A", "N.B", "N.C"]);
        var b = DeclaredSymbolNames.Encode(["N.C", "N.A", "N.B"]);

        Assert.That(a, Is.EqualTo(b), "the same set encodes identically regardless of input order");
    }

    [Test]
    public void Empty_input_encodes_to_empty_string()
        => Assert.That(DeclaredSymbolNames.Encode([]), Is.Empty);

    [Test]
    public void Round_trips_through_decode()
    {
        string[] names = ["Acme.Widgets", "Acme.Widgets.Gadget", "Acme.Widgets.Gadget.Run()"];

        var decoded = DeclaredSymbolNames.Decode(DeclaredSymbolNames.Encode(names));

        Assert.That(decoded, Is.EqualTo(names));
    }

    [Test]
    public void Decode_of_null_or_empty_yields_empty()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DeclaredSymbolNames.Decode(null), Is.Empty);
            Assert.That(DeclaredSymbolNames.Decode(string.Empty), Is.Empty);
        });
    }

    [Test]
    public void Encode_null_throws()
        => Assert.Throws<ArgumentNullException>(() => DeclaredSymbolNames.Encode(null!));
}
