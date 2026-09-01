namespace Orleans.Lattice.Embedding.Onnx.Tests;

/// <summary>
/// Covers the environment-driven startup configuration, including the
/// deliberate fallbacks that keep a misconfigured accelerator from producing a
/// dead container.
/// </summary>
[TestFixture]
public sealed class EmbedServerOptionsTests
{
    private string _modelPath = null!;
    private string _vocabPath = null!;

    [SetUp]
    public void SetUp()
    {
        _modelPath = Path.Combine(TestContext.CurrentContext.WorkDirectory, $"{Guid.NewGuid():N}.onnx");
        _vocabPath = Path.Combine(TestContext.CurrentContext.WorkDirectory, $"{Guid.NewGuid():N}.txt");
        File.WriteAllText(_modelPath, "model");
        File.WriteAllText(_vocabPath, "vocab");
    }

    [TearDown]
    public void TearDown()
    {
        File.Delete(_modelPath);
        File.Delete(_vocabPath);
    }

    [Test]
    public void FromEnvironment_applies_documented_defaults()
    {
        var options = EmbedServerOptions.FromEnvironment(Environment(new Dictionary<string, string>()));

        Assert.Multiple(() =>
        {
            Assert.That(options.Provider, Is.EqualTo(EmbedExecutionProvider.Cpu));
            Assert.That(options.Port, Is.EqualTo(EmbedServerOptions.DefaultPort));
            Assert.That(options.MaxContextLength, Is.EqualTo(EmbedServerOptions.DefaultMaxContextLength));
            Assert.That(options.IntraOpThreads, Is.EqualTo(0));
            Assert.That(options.DeviceId, Is.EqualTo(0));
        });
    }

    [Test]
    public void FromEnvironment_reads_every_configured_value()
    {
        var options = EmbedServerOptions.FromEnvironment(Environment(new Dictionary<string, string>
        {
            ["EMBED_PROVIDER"] = "cuda",
            ["EMBED_PORT"] = "9500",
            ["EMBED_INTRA_THREADS"] = "4",
            ["EMBED_DEVICE_ID"] = "1",
            ["EMBED_MAX_CONTEXT_LENGTH"] = "256",
        }));

        Assert.Multiple(() =>
        {
            Assert.That(options.Provider, Is.EqualTo(EmbedExecutionProvider.Cuda));
            Assert.That(options.Port, Is.EqualTo(9500));
            Assert.That(options.IntraOpThreads, Is.EqualTo(4));
            Assert.That(options.DeviceId, Is.EqualTo(1));
            Assert.That(options.MaxContextLength, Is.EqualTo(256));
        });
    }

    [Test]
    public void FromEnvironment_throws_when_the_model_path_is_unset()
    {
        var read = Environment(new Dictionary<string, string>());
        Func<string, string?> withoutModel = name =>
            name == "EMBED_MODEL_PATH" ? null : read(name);

        Assert.Throws<InvalidOperationException>(
            () => EmbedServerOptions.FromEnvironment(withoutModel));
    }

    [Test]
    public void FromEnvironment_throws_when_an_asset_path_does_not_exist()
    {
        Func<string, string?> read = name => name switch
        {
            "EMBED_MODEL_PATH" => Path.Combine(TestContext.CurrentContext.WorkDirectory, "missing.onnx"),
            "EMBED_VOCAB_PATH" => _vocabPath,
            _ => null,
        };

        Assert.Throws<InvalidOperationException>(() => EmbedServerOptions.FromEnvironment(read));
    }

    [Test]
    public void FromEnvironment_rejects_a_null_reader() =>
        Assert.Throws<ArgumentNullException>(() => EmbedServerOptions.FromEnvironment(null!));

    [TestCase("cuda", nameof(EmbedExecutionProvider.Cuda))]
    [TestCase("CUDA", nameof(EmbedExecutionProvider.Cuda))]
    [TestCase("gpu", nameof(EmbedExecutionProvider.Cuda))]
    [TestCase("nvidia", nameof(EmbedExecutionProvider.Cuda))]
    [TestCase(" dml ", nameof(EmbedExecutionProvider.DirectML))]
    [TestCase("directml", nameof(EmbedExecutionProvider.DirectML))]
    [TestCase("cpu", nameof(EmbedExecutionProvider.Cpu))]
    public void ParseProvider_maps_known_aliases(string value, string expected) =>
        Assert.That(EmbedServerOptions.ParseProvider(value).ToString(), Is.EqualTo(expected));

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    [TestCase("metal")]
    [TestCase("not-a-provider")]
    public void ParseProvider_falls_back_to_cpu_rather_than_failing(string? value)
    {
        // A dead container is strictly worse for the caller than a CPU one, so an
        // unknown accelerator must degrade rather than abort startup.
        Assert.That(EmbedServerOptions.ParseProvider(value), Is.EqualTo(EmbedExecutionProvider.Cpu));
    }

    [TestCase(null, EmbedServerOptions.DefaultPort)]
    [TestCase("", EmbedServerOptions.DefaultPort)]
    [TestCase("not-a-number", EmbedServerOptions.DefaultPort)]
    [TestCase("0", EmbedServerOptions.DefaultPort)]
    [TestCase("-5", EmbedServerOptions.DefaultPort)]
    [TestCase("9500", 9500)]
    public void ParsePositivePort_falls_back_for_unusable_input(string? value, int expected) =>
        Assert.That(EmbedServerOptions.ParsePositivePort(value), Is.EqualTo(expected));

    [Test]
    public void FromEnvironment_falls_back_for_unparseable_numeric_values()
    {
        var options = EmbedServerOptions.FromEnvironment(Environment(new Dictionary<string, string>
        {
            ["EMBED_PORT"] = "not-a-number",
            ["EMBED_MAX_CONTEXT_LENGTH"] = "-1",
            ["EMBED_INTRA_THREADS"] = "abc",
        }));

        Assert.Multiple(() =>
        {
            Assert.That(options.Port, Is.EqualTo(EmbedServerOptions.DefaultPort));
            Assert.That(
                options.MaxContextLength, Is.EqualTo(EmbedServerOptions.DefaultMaxContextLength));
            Assert.That(options.IntraOpThreads, Is.EqualTo(0));
        });
    }

    private Func<string, string?> Environment(Dictionary<string, string> values) => name =>
        name switch
        {
            "EMBED_MODEL_PATH" => _modelPath,
            "EMBED_VOCAB_PATH" => _vocabPath,
            _ => values.GetValueOrDefault(name),
        };
}
