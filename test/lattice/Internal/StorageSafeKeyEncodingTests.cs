using System.Text;

namespace Orleans.Lattice.Tests.Internal;

/// <summary>
/// Covers <see cref="StorageSafeKeyEncoding"/>, the percent-encoding shared by the
/// compound grain keys of state-persisting grains.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class StorageSafeKeyEncodingTests
{
    [Test]
    public void AppendEncoded_copies_safe_characters_verbatim()
    {
        Assert.That(Encode("tenant-a.b_c"), Is.EqualTo("tenant-a.b_c"));
    }

    [Test]
    public void AppendEncoded_escapes_the_delimiter_the_escape_marker_and_store_unsafe_characters()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Encode("a|b"), Is.EqualTo("a%007Cb"));
            Assert.That(Encode("%"), Is.EqualTo("%0025"));
            Assert.That(Encode("/\\#?"), Is.EqualTo("%002F%005C%0023%003F"));
            Assert.That(Encode("\u0001\u007f\u009f"), Is.EqualTo("%0001%007F%009F"));
        });
    }

    private static string Encode(string value)
    {
        var builder = new StringBuilder();
        StorageSafeKeyEncoding.AppendEncoded(builder, value);
        return builder.ToString();
    }
}
