using Orleans.Lattice.Benchmark.Microbench.Profiling;

namespace Orleans.Lattice.Benchmark.Microbench.Tests.Profiling;

/// <summary>
/// Unit tests for <see cref="ProfilerOptions"/> environment-variable parsing.
/// </summary>
[TestFixture]
public sealed class ProfilerOptionsTests
{
    [Test]
    public void ParseMode_returns_off_for_null_or_empty_or_whitespace()
    {
        Assert.That(ProfilerOptions.ParseMode(null), Is.EqualTo(ProfileMode.Off));
        Assert.That(ProfilerOptions.ParseMode(string.Empty), Is.EqualTo(ProfileMode.Off));
        Assert.That(ProfilerOptions.ParseMode("   "), Is.EqualTo(ProfileMode.Off));
    }

    [Test]
    public void ParseMode_recognises_canonical_values_case_insensitive()
    {
        Assert.That(ProfilerOptions.ParseMode("off"), Is.EqualTo(ProfileMode.Off));
        Assert.That(ProfilerOptions.ParseMode("OFF"), Is.EqualTo(ProfileMode.Off));
        Assert.That(ProfilerOptions.ParseMode("alloc"), Is.EqualTo(ProfileMode.Alloc));
        Assert.That(ProfilerOptions.ParseMode("Allocation"), Is.EqualTo(ProfileMode.Alloc));
        Assert.That(ProfilerOptions.ParseMode("cpu"), Is.EqualTo(ProfileMode.Cpu));
        Assert.That(ProfilerOptions.ParseMode("samples"), Is.EqualTo(ProfileMode.Cpu));
        Assert.That(ProfilerOptions.ParseMode("both"), Is.EqualTo(ProfileMode.Both));
        Assert.That(ProfilerOptions.ParseMode("all"), Is.EqualTo(ProfileMode.Both));
    }

    [Test]
    public void ParseMode_unknown_value_falls_back_to_off()
    {
        // The fallback also writes to stderr; we are only asserting the
        // returned value here.
        Assert.That(ProfilerOptions.ParseMode("garbage"), Is.EqualTo(ProfileMode.Off));
    }

    [Test]
    public void FromEnvironment_returns_disabled_when_var_is_unset()
    {
        var prior = Environment.GetEnvironmentVariable("BENCH_MICROBENCH_PROFILE");
        try
        {
            Environment.SetEnvironmentVariable("BENCH_MICROBENCH_PROFILE", null);
            var options = ProfilerOptions.FromEnvironment();
            Assert.That(options.IsEnabled, Is.False);
            Assert.That(options.Mode, Is.EqualTo(ProfileMode.Off));
            Assert.That(options.TopN, Is.EqualTo(ProfilerOptions.DefaultTopN));
            Assert.That(options.NetTraceOutputPath, Is.Null);
        }
        finally
        {
            Environment.SetEnvironmentVariable("BENCH_MICROBENCH_PROFILE", prior);
        }
    }

    [Test]
    public void FromEnvironment_resolves_all_three_knobs()
    {
        var priorMode = Environment.GetEnvironmentVariable("BENCH_MICROBENCH_PROFILE");
        var priorTopN = Environment.GetEnvironmentVariable("BENCH_MICROBENCH_PROFILE_TOPN");
        var priorNetTrace = Environment.GetEnvironmentVariable("BENCH_MICROBENCH_PROFILE_NETTRACE_PATH");
        try
        {
            Environment.SetEnvironmentVariable("BENCH_MICROBENCH_PROFILE", "both");
            Environment.SetEnvironmentVariable("BENCH_MICROBENCH_PROFILE_TOPN", "12");
            Environment.SetEnvironmentVariable("BENCH_MICROBENCH_PROFILE_NETTRACE_PATH", "/tmp/x.nettrace");

            var options = ProfilerOptions.FromEnvironment();
            Assert.That(options.Mode, Is.EqualTo(ProfileMode.Both));
            Assert.That(options.IsEnabled, Is.True);
            Assert.That(options.CapturesAllocations, Is.True);
            Assert.That(options.CapturesCpu, Is.True);
            Assert.That(options.TopN, Is.EqualTo(12));
            Assert.That(options.NetTraceOutputPath, Is.EqualTo("/tmp/x.nettrace"));
        }
        finally
        {
            Environment.SetEnvironmentVariable("BENCH_MICROBENCH_PROFILE", priorMode);
            Environment.SetEnvironmentVariable("BENCH_MICROBENCH_PROFILE_TOPN", priorTopN);
            Environment.SetEnvironmentVariable("BENCH_MICROBENCH_PROFILE_NETTRACE_PATH", priorNetTrace);
        }
    }

    [Test]
    public void FromEnvironment_falls_back_to_default_topn_when_value_is_non_positive_or_unparseable()
    {
        var priorMode = Environment.GetEnvironmentVariable("BENCH_MICROBENCH_PROFILE");
        var priorTopN = Environment.GetEnvironmentVariable("BENCH_MICROBENCH_PROFILE_TOPN");
        try
        {
            Environment.SetEnvironmentVariable("BENCH_MICROBENCH_PROFILE", "alloc");

            Environment.SetEnvironmentVariable("BENCH_MICROBENCH_PROFILE_TOPN", "-3");
            Assert.That(ProfilerOptions.FromEnvironment().TopN, Is.EqualTo(ProfilerOptions.DefaultTopN));

            Environment.SetEnvironmentVariable("BENCH_MICROBENCH_PROFILE_TOPN", "abc");
            Assert.That(ProfilerOptions.FromEnvironment().TopN, Is.EqualTo(ProfilerOptions.DefaultTopN));

            Environment.SetEnvironmentVariable("BENCH_MICROBENCH_PROFILE_TOPN", "0");
            Assert.That(ProfilerOptions.FromEnvironment().TopN, Is.EqualTo(ProfilerOptions.DefaultTopN));
        }
        finally
        {
            Environment.SetEnvironmentVariable("BENCH_MICROBENCH_PROFILE", priorMode);
            Environment.SetEnvironmentVariable("BENCH_MICROBENCH_PROFILE_TOPN", priorTopN);
        }
    }

    [Test]
    public void CapturesAllocations_and_CapturesCpu_reflect_mode()
    {
        Assert.That(new ProfilerOptions(ProfileMode.Off, 10, null).CapturesAllocations, Is.False);
        Assert.That(new ProfilerOptions(ProfileMode.Off, 10, null).CapturesCpu, Is.False);
        Assert.That(new ProfilerOptions(ProfileMode.Alloc, 10, null).CapturesAllocations, Is.True);
        Assert.That(new ProfilerOptions(ProfileMode.Alloc, 10, null).CapturesCpu, Is.False);
        Assert.That(new ProfilerOptions(ProfileMode.Cpu, 10, null).CapturesAllocations, Is.False);
        Assert.That(new ProfilerOptions(ProfileMode.Cpu, 10, null).CapturesCpu, Is.True);
        Assert.That(new ProfilerOptions(ProfileMode.Both, 10, null).CapturesAllocations, Is.True);
        Assert.That(new ProfilerOptions(ProfileMode.Both, 10, null).CapturesCpu, Is.True);
    }

    [Test]
    public void FilterNoiseFrames_defaults_to_true()
    {
        Assert.That(new ProfilerOptions(ProfileMode.Alloc, 10, null).FilterNoiseFrames, Is.True);
    }

    [Test]
    public void ParseFilterNoise_defaults_to_true_when_unset()
    {
        Assert.That(ProfilerOptions.ParseFilterNoise(null), Is.True);
        Assert.That(ProfilerOptions.ParseFilterNoise(string.Empty), Is.True);
        Assert.That(ProfilerOptions.ParseFilterNoise("   "), Is.True);
    }

    [Test]
    public void ParseFilterNoise_recognises_opt_out_values_case_insensitive()
    {
        Assert.That(ProfilerOptions.ParseFilterNoise("false"), Is.False);
        Assert.That(ProfilerOptions.ParseFilterNoise("FALSE"), Is.False);
        Assert.That(ProfilerOptions.ParseFilterNoise("0"), Is.False);
        Assert.That(ProfilerOptions.ParseFilterNoise("no"), Is.False);
        Assert.That(ProfilerOptions.ParseFilterNoise("off"), Is.False);
    }

    [Test]
    public void ParseFilterNoise_unknown_value_falls_back_to_true()
    {
        Assert.That(ProfilerOptions.ParseFilterNoise("true"), Is.True);
        Assert.That(ProfilerOptions.ParseFilterNoise("garbage"), Is.True);
    }

    [Test]
    public void FromEnvironment_resolves_filter_noise_opt_out()
    {
        var priorMode = Environment.GetEnvironmentVariable("BENCH_MICROBENCH_PROFILE");
        var priorFilter = Environment.GetEnvironmentVariable("BENCH_MICROBENCH_PROFILE_FILTER_NOISE");
        try
        {
            Environment.SetEnvironmentVariable("BENCH_MICROBENCH_PROFILE", "alloc");

            Environment.SetEnvironmentVariable("BENCH_MICROBENCH_PROFILE_FILTER_NOISE", "false");
            Assert.That(ProfilerOptions.FromEnvironment().FilterNoiseFrames, Is.False);

            Environment.SetEnvironmentVariable("BENCH_MICROBENCH_PROFILE_FILTER_NOISE", null);
            Assert.That(ProfilerOptions.FromEnvironment().FilterNoiseFrames, Is.True);
        }
        finally
        {
            Environment.SetEnvironmentVariable("BENCH_MICROBENCH_PROFILE", priorMode);
            Environment.SetEnvironmentVariable("BENCH_MICROBENCH_PROFILE_FILTER_NOISE", priorFilter);
        }
    }
}
