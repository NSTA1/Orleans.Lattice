using System.IO.Compression;
using System.Text.Json;

namespace Orleans.Lattice.Embedding.Onnx.Tests;

/// <summary>
/// Pins <see cref="WordPieceTokenizer"/> against token ids produced by the
/// reference HuggingFace tokenizer for <c>nomic-embed-text-v1</c>.
/// </summary>
/// <remarks>
/// <para>
/// This is the load-bearing gate of the whole image. The embedding vectors this
/// server returns are only interchangeable with previously stored ones while the
/// token stream matches the reference implementation exactly. A tokenizer that
/// merely looks plausible can silently mis-handle source text - swallowing
/// newlines and tabs, or dropping <c>=</c>, <c>&lt;</c>, <c>&gt;</c> and
/// backticks - and still emit correctly shaped, correctly normalized,
/// entirely wrong vectors. That defect is invisible to every structural check
/// (dimension, count, normalization), so it is caught here or not at all.
/// </para>
/// <para>
/// The fixture is generated from the pinned model revision's own tokenizer and
/// committed, so this runs with no Python, no network, and no model weights.
/// </para>
/// </remarks>
[TestFixture]
public sealed class WordPieceTokenizerGoldenTests
{
    private static readonly string FixtureDirectory =
        Path.Combine(TestContext.CurrentContext.TestDirectory, "Fixtures");

    private WordPieceTokenizer _tokenizer = null!;
    private IReadOnlyList<GoldenCase> _cases = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _tokenizer = WordPieceTokenizer.FromVocabularyFile(ExtractVocabulary());

        using var stream = File.OpenRead(Path.Combine(FixtureDirectory, "tokenizer-golden.json"));
        _cases = JsonSerializer.Deserialize<List<GoldenCase>>(
            stream,
            new JsonSerializerOptions(JsonSerializerDefaults.Web))
            ?? throw new InvalidOperationException("The golden tokenizer fixture is empty.");
    }

    /// <summary>
    /// Decompresses the committed vocabulary to a temporary file and returns its
    /// path. The vocabulary ships gzipped because it is model data rather than
    /// prose and carries characters the repository's text-hygiene gates reject in
    /// tracked text files.
    /// </summary>
    private static string ExtractVocabulary()
    {
        var destination = Path.Combine(
            TestContext.CurrentContext.WorkDirectory, "vocab-under-test.txt");

        using var compressed = File.OpenRead(Path.Combine(FixtureDirectory, "vocab.txt.gz"));
        using var decompressor = new GZipStream(compressed, CompressionMode.Decompress);
        using var output = File.Create(destination);
        decompressor.CopyTo(output);

        return destination;
    }

    [Test]
    public void Golden_fixture_is_loaded_and_non_trivial()
    {
        Assert.That(_cases, Is.Not.Empty);
        Assert.That(_cases.Count, Is.GreaterThanOrEqualTo(20));
    }

    [Test]
    public void Encode_matches_reference_token_ids_for_every_golden_case()
    {
        var failures = new List<string>();

        foreach (var golden in _cases)
        {
            var actual = _tokenizer.Encode(golden.Text, golden.MaxTokens);
            if (!actual.SequenceEqual(golden.Ids))
            {
                failures.Add(
                    $"text={Describe(golden.Text)} maxTokens={golden.MaxTokens}\n"
                    + $"  expected ({golden.Ids.Count}): {Preview(golden.Ids)}\n"
                    + $"  actual   ({actual.Length}): {Preview(actual)}");
            }
        }

        Assert.That(
            failures,
            Is.Empty,
            "The tokenizer diverged from the reference HuggingFace output. Vectors produced "
            + "from a divergent token stream are NOT interchangeable with already-stored "
            + "embeddings.\n" + string.Join("\n", failures));
    }

    [Test]
    public void Encode_applies_a_smaller_ceiling_after_a_larger_one()
    {
        // Regression guard: the tokenizer library's convenience overload reuses an
        // internal buffer and ignores a smaller ceiling once a larger one has been
        // requested, which would silently return untruncated ids to a caller that
        // asked for fewer. Order matters, so assert the descending direction.
        var longText = string.Join(" ", Enumerable.Repeat("token", 5000));

        var at512 = _tokenizer.Encode(longText, 512);
        var at128 = _tokenizer.Encode(longText, 128);
        var at64 = _tokenizer.Encode(longText, 64);

        Assert.Multiple(() =>
        {
            Assert.That(at512, Has.Length.EqualTo(512), "ceiling 512");
            Assert.That(at128, Has.Length.EqualTo(128), "ceiling 128 after 512");
            Assert.That(at64, Has.Length.EqualTo(64), "ceiling 64 after 128");
        });
    }

    [Test]
    public void Encode_terminates_a_truncated_sequence_with_the_separator_token()
    {
        // HuggingFace truncates then still terminates with [SEP] (id 102). A
        // tokenizer that simply cut the array would drop it and shift the vector.
        var longText = string.Join(" ", Enumerable.Repeat("token", 5000));

        var truncated = _tokenizer.Encode(longText, 256);

        Assert.That(truncated[^1], Is.EqualTo(102));
    }

    [Test]
    public void Encode_preserves_structural_characters_that_matter_in_source_code()
    {
        // Each of these was silently destroyed by a plausible-looking tokenizer
        // during development, so they are pinned explicitly and by name.
        var newline = _tokenizer.Encode("a\nb", 512);
        var tab = _tokenizer.Encode("a\tb", 512);
        var generics = _tokenizer.Encode("Task<byte[]>", 512);
        var assignment = _tokenizer.Encode("x=y;", 512);
        var backticks = _tokenizer.Encode("`publish`", 512);

        Assert.Multiple(() =>
        {
            // "a\nb" must be two words, never one fused "ab".
            Assert.That(newline, Has.Length.EqualTo(4), "newline must separate words");
            Assert.That(tab, Has.Length.EqualTo(4), "tab must separate words");

            // Angle brackets and square brackets are real tokens, not dropped.
            Assert.That(generics, Has.Length.EqualTo(8), "generic syntax must be tokenized");
            Assert.That(assignment, Has.Length.EqualTo(6), "'=' must be tokenized");
            Assert.That(backticks, Has.Length.EqualTo(5), "backticks must be tokenized");
        });
    }

    [Test]
    public void Encode_truncates_to_the_requested_ceiling()
    {
        var longText = string.Join(" ", Enumerable.Repeat("token", 5000));

        Assert.That(_tokenizer.Encode(longText, 512), Has.Length.EqualTo(512));
    }

    [Test]
    public void Encode_treats_null_as_empty()
    {
        var fromNull = _tokenizer.Encode(null, 512);
        var fromEmpty = _tokenizer.Encode(string.Empty, 512);

        Assert.That(fromNull, Is.EqualTo(fromEmpty));
    }

    [Test]
    public void Encode_rejects_a_non_positive_ceiling()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => _tokenizer.Encode("text", 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => _tokenizer.Encode("text", -1));
    }

    [Test]
    public void Encode_diverges_from_reference_for_an_unbroken_run_over_100_characters()
    {
        // KNOWN, DELIBERATELY PINNED DIVERGENCE.
        //
        // HuggingFace's WordPiece stage emits a single [UNK] (id 100) for any
        // basic token longer than max_input_chars_per_word (100), so 600 'a's
        // encode as [CLS] [UNK] [SEP]. FastBertTokenizer instead greedily
        // sub-words it. The two therefore disagree for unbroken runs over 100
        // characters - base64 blobs, minified assets, long hashes - and a chunk
        // containing one will not embed identically to the Onyx server's output.
        //
        // Ordinary source text is unaffected (a 48-chunk corpus of real
        // repository files matched at cosine 1.000000). This test exists so the
        // gap stays visible and cannot change silently; see the README's
        // "Known divergence" section.
        var unbroken = new string('a', 600);

        var actual = _tokenizer.Encode(unbroken, 512);

        Assert.That(
            actual,
            Is.Not.EqualTo(new long[] { 101, 100, 102 }),
            "FastBertTokenizer now matches the HuggingFace [UNK] rule for long unbroken "
            + "runs. That is an improvement: fold this case into the golden fixture and "
            + "delete this test along with the README's 'Known divergence' section.");
    }

    [Test]
    public void FromVocabularyFile_rejects_a_null_path() =>
        Assert.Throws<ArgumentNullException>(() => WordPieceTokenizer.FromVocabularyFile(null!));

    private static string Describe(string text) =>
        text.Length > 60
            ? $"\"{Escape(text[..60])}\"... ({text.Length} chars)"
            : $"\"{Escape(text)}\"";

    private static string Escape(string text) =>
        text.Replace("\n", "\\n", StringComparison.Ordinal)
            .Replace("\t", "\\t", StringComparison.Ordinal);

    private static string Preview(IReadOnlyList<long> ids) =>
        string.Join(",", ids.Take(16)) + (ids.Count > 16 ? ",..." : string.Empty);

    private sealed record GoldenCase(string Text, int MaxTokens, IReadOnlyList<long> Ids);
}
