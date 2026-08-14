namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for <see cref="RepoContextTextChunker"/>: the deterministic,
/// character-based windowing that turns a file into overlapping passages so content
/// deep in a large file is embedded and searchable, not just its leading window.
/// </summary>
[TestFixture]
public sealed class RepoContextTextChunkerTests
{
    [Test]
    public void Chunk_returns_the_whole_text_as_one_window_when_it_fits()
    {
        var text = new string('a', RepoContextTextChunker.DefaultWindowChars);

        var chunks = RepoContextTextChunker.Chunk(text);

        Assert.That(chunks, Is.EqualTo(new[] { text }),
            "Text within one window is a single passage - no splitting.");
    }

    [Test]
    public void Chunk_returns_an_empty_list_for_whitespace_only_text()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextTextChunker.Chunk(string.Empty), Is.Empty);
            Assert.That(RepoContextTextChunker.Chunk("   \n\t  "), Is.Empty,
                "A whitespace-only file has no embeddable passage.");
        });
    }

    [Test]
    public void Chunk_splits_into_overlapping_windows_that_cover_the_whole_text()
    {
        // 10-char windows stepping by 8 (2-char overlap) over 25 chars -> 0..10,
        // 8..18, 16..25 = three windows, each overlapping its neighbour by two.
        var text = new string('x', 25);

        var chunks = RepoContextTextChunker.Chunk(text, windowChars: 10, overlapChars: 2, maxChunks: 32);

        Assert.Multiple(() =>
        {
            Assert.That(chunks, Has.Count.EqualTo(3), "25 chars in 10-wide, 8-step windows is three passages.");
            Assert.That(chunks[0], Has.Length.EqualTo(10));
            Assert.That(chunks[1], Has.Length.EqualTo(10));
            Assert.That(chunks[2], Has.Length.EqualTo(9), "The final window is the remaining tail.");
        });
    }

    [Test]
    public void Chunk_caps_the_window_count_so_a_huge_file_cannot_dominate()
    {
        var text = new string('y', 10_000);

        var chunks = RepoContextTextChunker.Chunk(text, windowChars: 10, overlapChars: 0, maxChunks: 4);

        Assert.That(chunks, Has.Count.EqualTo(4),
            "The per-file cap bounds how many windows a single large file emits.");
    }

    [Test]
    public void Chunk_preserves_a_boundary_straddling_token_in_at_least_one_window()
    {
        // A marker placed near a window boundary is kept whole by the overlap: it
        // appears complete in the second window even though the first window's edge
        // cuts through where it starts.
        var text = new string('.', 9) + "NEEDLE" + new string('.', 20);

        var chunks = RepoContextTextChunker.Chunk(text, windowChars: 12, overlapChars: 6, maxChunks: 32);

        Assert.That(chunks.Any(c => c.Contains("NEEDLE", StringComparison.Ordinal)), Is.True,
            "The overlap keeps a boundary-straddling token intact in some window.");
    }

    [Test]
    public void Chunk_rejects_an_overlap_that_is_not_less_than_the_window()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => RepoContextTextChunker.Chunk("some text", windowChars: 10, overlapChars: 10, maxChunks: 4),
            "An overlap equal to the window would never advance.");
    }
}
