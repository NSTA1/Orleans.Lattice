namespace Orleans.Lattice.Embedding.Onnx.Tests;

/// <summary>
/// Covers the intra-op thread resolution and the provenance that travels with
/// it.
/// </summary>
/// <remarks>
/// The provenance half is tested as carefully as the number, because reporting a
/// derived value indistinguishably from a declared one is the specific trap this
/// bucket already walked into once on the repository-context host.
/// </remarks>
[TestFixture]
public sealed class IntraOpThreadResolutionTests
{
    [Test]
    public void A_declared_value_wins_over_the_grant_and_the_processor_count()
    {
        var resolved = EmbedServerOptions.ResolveIntraOpThreads("2", 4, 16);

        Assert.Multiple(() =>
        {
            Assert.That(resolved.Threads, Is.EqualTo(2));
            Assert.That(resolved.Source, Is.EqualTo(IntraOpThreadSource.Declared));
        });
    }

    [Test]
    public void A_declared_zero_is_honoured_as_an_escape_hatch()
    {
        var resolved = EmbedServerOptions.ResolveIntraOpThreads("0", 4, 16);

        Assert.Multiple(() =>
        {
            Assert.That(resolved.Threads, Is.EqualTo(EmbedServerOptions.LetRuntimeChoose));
            Assert.That(resolved.Source, Is.EqualTo(IntraOpThreadSource.Declared),
                "handing the decision back to the runtime is a choice an operator "
                + "may make, and it must be reported as a choice rather than as a "
                + "derivation.");
        });
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    [TestCase("-1")]
    [TestCase("many")]
    public void An_unusable_declaration_falls_through_to_the_grant(string? declared)
    {
        var resolved = EmbedServerOptions.ResolveIntraOpThreads(declared, 4, 16);

        Assert.Multiple(() =>
        {
            Assert.That(resolved.Threads, Is.EqualTo(4));
            Assert.That(resolved.Source, Is.EqualTo(IntraOpThreadSource.ContainerCpuGrant));
        });
    }

    [Test]
    public void The_grant_wins_over_an_overridden_processor_count()
    {
        var resolved = EmbedServerOptions.ResolveIntraOpThreads(null, 4, 16);

        Assert.Multiple(() =>
        {
            Assert.That(resolved.Threads, Is.EqualTo(4),
                "DOTNET_PROCESSOR_COUNT overrides Environment.ProcessorCount and wins "
                + "over the quota, so deriving from the processor count would restore "
                + "the 4x oversubscription this change exists to remove.");
            Assert.That(resolved.Source, Is.EqualTo(IntraOpThreadSource.ContainerCpuGrant));
        });
    }

    [Test]
    public void The_processor_count_is_used_when_no_quota_is_enforced()
    {
        var resolved = EmbedServerOptions.ResolveIntraOpThreads(null, null, 16);

        Assert.Multiple(() =>
        {
            Assert.That(resolved.Threads, Is.EqualTo(16));
            Assert.That(resolved.Source, Is.EqualTo(IntraOpThreadSource.ProcessorCount));
        });
    }

    [Test]
    public void A_resolved_count_is_never_below_one()
    {
        Assert.Multiple(() =>
        {
            Assert.That(EmbedServerOptions.ResolveIntraOpThreads(null, 0, 16).Threads, Is.EqualTo(1));
            Assert.That(EmbedServerOptions.ResolveIntraOpThreads(null, null, 0).Threads, Is.EqualTo(1));
        });
    }

    [Test]
    public void Every_provenance_renders_distinguishably()
    {
        var declared = new IntraOpThreadCount(4, IntraOpThreadSource.Declared).DescribeProvenance();
        var grant = new IntraOpThreadCount(4, IntraOpThreadSource.ContainerCpuGrant).DescribeProvenance();
        var processorCount = new IntraOpThreadCount(4, IntraOpThreadSource.ProcessorCount).DescribeProvenance();

        Assert.Multiple(() =>
        {
            Assert.That(declared, Does.Contain("DECLARED"));
            Assert.That(grant, Does.Contain("DERIVED"));
            Assert.That(processorCount, Does.Contain("DERIVED"));
            Assert.That(new[] { declared, grant, processorCount }, Is.Unique,
                "an identical count reached three different ways must not produce "
                + "three identical log lines.");
        });
    }

    [Test]
    public void Provenance_names_the_variable_that_overrides_it()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                new IntraOpThreadCount(4, IntraOpThreadSource.ContainerCpuGrant).DescribeProvenance(),
                Does.Contain(EmbedServerOptions.IntraOpThreadsKey));
            Assert.That(
                new IntraOpThreadCount(4, IntraOpThreadSource.ProcessorCount).DescribeProvenance(),
                Does.Contain(EmbedServerOptions.IntraOpThreadsKey));
        });
    }

    [Test]
    public void A_disagreement_between_the_grant_and_the_processor_count_is_reported()
    {
        var warning = EmbedServerOptions.DescribeProcessorCountDisagreement(4, 16);

        Assert.That(warning, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(warning, Does.Contain("4"));
            Assert.That(warning, Does.Contain("16"));
            Assert.That(warning, Does.Contain("DOTNET_PROCESSOR_COUNT"));
        });
    }

    [Test]
    public void An_agreeing_grant_and_processor_count_are_not_reported()
    {
        Assert.That(EmbedServerOptions.DescribeProcessorCountDisagreement(4, 4), Is.Null);
    }

    [Test]
    public void An_unenforced_grant_is_not_reported_as_a_disagreement()
    {
        Assert.That(EmbedServerOptions.DescribeProcessorCountDisagreement(null, 16), Is.Null,
            "an unlimited container that reports the host core count is agreeing "
            + "with reality, not disagreeing with it.");
    }
}
