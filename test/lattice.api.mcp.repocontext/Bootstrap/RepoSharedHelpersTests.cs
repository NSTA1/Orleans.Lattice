namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Covers <see cref="RepoBinaryContent"/> and <see cref="RepoFileEntrySequence"/>,
/// the helpers the repository sources and reconcilers share.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class RepoSharedHelpersTests
{
    [Test]
    public void IsProbablyBinary_flags_a_nul_byte_inside_the_sniff_window_only()
    {
        var text = "hello"u8.ToArray();
        var binary = new byte[] { 1, 0, 2 };
        var lateNul = new byte[9_000];
        Array.Fill(lateNul, (byte)'a');
        lateNul[8_500] = 0;

        Assert.Multiple(() =>
        {
            Assert.That(RepoBinaryContent.IsProbablyBinary(text), Is.False);
            Assert.That(RepoBinaryContent.IsProbablyBinary(binary), Is.True);
            Assert.That(RepoBinaryContent.IsProbablyBinary(lateNul), Is.False, "a NUL past the window is not sampled");
        });
    }

    [Test]
    public void Concat_yields_the_three_lists_in_order()
    {
        var a = new RepoFileEntry("a", "d1", 1, "cs");
        var b = new RepoFileEntry("b", "d2", 1, "cs");
        var c = new RepoFileEntry("c", "d3", 1, "cs");

        Assert.That(RepoFileEntrySequence.Concat([a], [], [b, c]), Is.EqualTo(new[] { a, b, c }));
    }
}
