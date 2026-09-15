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
    public void An_ABSENT_declaration_falls_through_to_the_grant(string? declared)
    {
        var resolved = EmbedServerOptions.ResolveIntraOpThreads(declared, 4, 16);

        Assert.Multiple(() =>
        {
            Assert.That(resolved.Threads, Is.EqualTo(4));
            Assert.That(resolved.Source, Is.EqualTo(IntraOpThreadSource.ContainerCpuGrant));
            Assert.That(
                resolved.DeclaredAuto,
                Is.False,
                "a value that merely failed to parse must not be reported as a deliberate "
                + "choice of the derivation. That conflation is the whole of issue #2863, and "
                + "it would be reintroduced here if the fallthrough set this flag.");
        });
    }

    [TestCase("-1")]
    [TestCase("many")]
    [TestCase("4.5")]
    [TestCase("99999999999999999999")]
    public void A_PRESENT_but_unusable_declaration_is_refused_rather_than_derived(string declared)
    {
        // Issue #2887. Absence and a typo are different facts about a deployment, and
        // deriving from both makes them indistinguishable: the operator who wrote
        // EMBED_INTRA_THREADS=-1 sees a server that starts, reports healthy, and runs on
        // a number they did not choose. A run taken in that state is misconfigured and
        // says so nowhere, which is the defect class this whole change addresses.
        var refusal = Assert.Throws<InvalidOperationException>(
            () => EmbedServerOptions.ResolveIntraOpThreads(declared, 4, 16));

        Assert.Multiple(() =>
        {
            Assert.That(
                refusal!.Message,
                Does.Contain("EMBED_INTRA_THREADS"),
                "the refusal must name the variable, or the operator has to guess which "
                + "of the container's settings the server is objecting to.");
            Assert.That(
                refusal.Message,
                Does.Contain(declared),
                "the refusal must quote the value it could not use. A typo is only obvious "
                + "once you can see what was actually read.");
            Assert.That(
                refusal.Message,
                Does.Contain("auto"),
                "diagnosing without a remedy leaves the operator to discover the escape "
                + "hatch themselves; 'auto' is the spelling that means 'derive on purpose'.");
        });
    }

    [Test]
    public void The_refusals_are_DISTINGUISHABLE_so_the_complaint_matches_the_mistake()
    {
        // A single generic complaint for every unusable value would pass the test above
        // while telling the operator nothing about which mistake they made.
        var negative = Assert.Throws<InvalidOperationException>(
            () => EmbedServerOptions.ResolveIntraOpThreads("-1", 4, 16))!.Message;
        var notANumber = Assert.Throws<InvalidOperationException>(
            () => EmbedServerOptions.ResolveIntraOpThreads("many", 4, 16))!.Message;
        var tooLarge = Assert.Throws<InvalidOperationException>(
            () => EmbedServerOptions.ResolveIntraOpThreads("99999999999999999999", 4, 16))!.Message;

        Assert.That(
            new[] { negative, notANumber, tooLarge },
            Is.Unique,
            "three different mistakes must not produce one indistinguishable complaint.");
    }

    [TestCase("auto")]
    [TestCase("AUTO")]
    [TestCase("  Auto  ")]
    public void The_auto_token_reaches_the_same_number_but_says_who_chose_it(string declared)
    {
        var resolved = EmbedServerOptions.ResolveIntraOpThreads(declared, 4, 16);

        Assert.Multiple(() =>
        {
            Assert.That(
                resolved.Threads,
                Is.EqualTo(4),
                "`auto` must reach the grant-derived count, not a second implementation of it.");
            Assert.That(resolved.Source, Is.EqualTo(IntraOpThreadSource.ContainerCpuGrant));
            Assert.That(
                resolved.DeclaredAuto,
                Is.True,
                "and it must be distinguishable from an omission. Without this the startup line "
                + "reads identically for a deliberate `auto`, an unset variable, and a typo, so "
                + "the deployment that migrated off the `0` sentinel is indistinguishable from "
                + "the one that never did.");
            Assert.That(
                resolved.DescribeProvenance(),
                Does.Contain("DECLARED"),
                "the operator-facing provenance must credit the declaration, or the observable "
                + "the runbook tells an operator to read after migrating says nothing.");
        });
    }

    [Test]
    public void The_auto_token_is_the_spelling_the_deployment_is_told_to_use()
        => Assert.That(
            EmbedServerOptions.AutoToken,
            Is.EqualTo("auto"),
            "the sample deployment's preflight, its .env.example and the container document "
            + "all name this literal, and the tuning preflight's ancestry guard is keyed to it. "
            + "Renaming it here without renaming it there would make every deployment that "
            + "writes the documented spelling refuse to start (issue #2887 turned the silent "
            + "derivation into a refusal), and would leave the ancestry guard asserting an "
            + "ancestor for a token no build requires.");

    [Test]
    public void A_near_miss_of_the_token_is_not_treated_as_the_token()
    {
        // Before issue #2887 this derived silently and the assertion was that it did not
        // set DeclaredAuto. Refusing is the stronger form of the same claim: a typo of the
        // token is now neither treated as the token nor quietly resolved to a number the
        // operator did not choose.
        var refusal = Assert.Throws<InvalidOperationException>(
            () => EmbedServerOptions.ResolveIntraOpThreads("atuo", 4, 16));

        Assert.That(
            refusal!.Message,
            Does.Contain("atuo"),
            "matching must be exact AND the near miss must be visible. A prefix or fuzzy "
            + "match would let a typo claim a deliberate choice, which is the "
            + "reassurance-in-the-broken-case failure this fixture's provenance assertions "
            + "exist to prevent.");
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
