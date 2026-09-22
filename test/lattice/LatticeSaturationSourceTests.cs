namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSaturationSource"/> and the
/// <see cref="LatticeSaturatedException.SaturationSource"/> attribution slot it
/// populates (issue #3294).
/// <para>
/// The discriminator exists because four independent admission seams raise one
/// exception type. That is right for the caller <em>contract</em> - all four
/// mean "this tree is back-pressured; the operation was refused" - and
/// insufficient for <em>policy</em>, because the seams disagree on whether an
/// automatic retry amplifies. Only the replay-permit seam refuses before the
/// caller has done any work. A retry policy keyed on the exception type alone
/// therefore cannot fix #3294 without simultaneously reintroducing #3348.
/// </para>
/// </summary>
[TestFixture]
public class LatticeSaturationSourceTests
{
    [Test]
    public void Source_constructor_records_the_refusing_seam_and_treeId()
    {
        var ex = new LatticeSaturatedException(
            "replay permit admission refused", "tree-a", LatticeSaturationSource.ReplayPermitAdmission);

        Assert.Multiple(() =>
        {
            Assert.That(ex.SaturationSource, Is.EqualTo(LatticeSaturationSource.ReplayPermitAdmission));
            Assert.That(ex.TreeId, Is.EqualTo("tree-a"));
            Assert.That(ex.Message, Is.EqualTo("replay permit admission refused"));
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    [Test]
    public void SourceAndInner_constructor_records_the_seam_and_preserves_the_inner_cause()
    {
        var inner = new TimeoutException("quiesce budget expired");
        var ex = new LatticeSaturatedException(
            "saga refused dispatch", "tree-b", LatticeSaturationSource.AtomicWriteSaga, inner);

        Assert.Multiple(() =>
        {
            Assert.That(ex.SaturationSource, Is.EqualTo(LatticeSaturationSource.AtomicWriteSaga));
            Assert.That(ex.TreeId, Is.EqualTo("tree-b"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
        });
    }

    /// <summary>
    /// Every pre-existing constructor must report
    /// <see cref="LatticeSaturationSource.Unspecified"/>, which is what an
    /// exception deserialised from a host predating the discriminator also
    /// carries. The default must be the conservative reading - three of the
    /// four seams amplify when retried - so an unattributed refusal is treated
    /// as not retryable rather than as retryable.
    /// </summary>
    [Test]
    public void Constructors_without_a_source_report_Unspecified()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new LatticeSaturatedException().SaturationSource,
                Is.EqualTo(LatticeSaturationSource.Unspecified));
            Assert.That(new LatticeSaturatedException("m").SaturationSource,
                Is.EqualTo(LatticeSaturationSource.Unspecified));
            Assert.That(new LatticeSaturatedException("m", new TimeoutException()).SaturationSource,
                Is.EqualTo(LatticeSaturationSource.Unspecified));
            Assert.That(new LatticeSaturatedException("m", "tree-c").SaturationSource,
                Is.EqualTo(LatticeSaturationSource.Unspecified));
            Assert.That(new LatticeSaturatedException("m", "tree-c", new TimeoutException()).SaturationSource,
                Is.EqualTo(LatticeSaturationSource.Unspecified));
        });
    }

    /// <summary>
    /// The enum's zero value must be <see cref="LatticeSaturationSource.Unspecified"/>,
    /// because that is the value a default-initialised field and a
    /// wire-absent member both take. If some attributed seam sat at zero
    /// instead, every legacy exception would silently claim to be that seam.
    /// </summary>
    [Test]
    public void Default_value_of_the_enum_is_Unspecified()
        => Assert.That(default(LatticeSaturationSource), Is.EqualTo(LatticeSaturationSource.Unspecified));

    /// <summary>
    /// Regression guard. <see cref="Exception"/> already declares a
    /// <c>Source</c> property (the application or object name that caused the
    /// error), so naming the discriminator <c>Source</c> compiles with only a
    /// CS0114 warning and silently <em>hides</em> the BCL member. Every
    /// consumer reading <c>ex.Source</c> for its documented meaning - loggers,
    /// diagnostics, and the framework itself - would then get an enum-derived
    /// value instead. The name is therefore load-bearing, and this asserts the
    /// two properties remain independent.
    /// </summary>
    [Test]
    public void SaturationSource_does_not_hide_the_BCL_Exception_Source()
    {
        var ex = new LatticeSaturatedException(
            "refused", "tree-d", LatticeSaturationSource.WalAdmission)
        {
            Source = "Orleans.Lattice.Tests",
        };

        Assert.Multiple(() =>
        {
            Assert.That(ex.Source, Is.EqualTo("Orleans.Lattice.Tests"),
                "Exception.Source must keep its BCL meaning and stay independently settable");
            Assert.That(ex.SaturationSource, Is.EqualTo(LatticeSaturationSource.WalAdmission),
                "the seam discriminator must be unaffected by Exception.Source");
        });
    }

    /// <summary>
    /// The five attributed seams must be distinct values, since the whole
    /// point is to tell them apart when choosing a retry policy.
    /// </summary>
    [Test]
    public void All_declared_sources_are_distinct()
    {
        var values = Enum.GetValues<LatticeSaturationSource>();
        Assert.That(values, Is.Unique);
        Assert.That(values, Has.Length.EqualTo(6),
            "Unspecified plus the five refusal seams; adding a sixth seam needs a retry-policy decision "
            + "in ShardActivationRetry.IsRetryableSaturation, so this count is deliberately pinned.");
    }

    /// <summary>
    /// The fan-out seam is raised above the routing layer, so retrying it
    /// below that layer would re-fan the whole batch across every shard of
    /// an already-saturated tree. It must therefore stay out of the
    /// in-library retry set, exactly like <see cref="LatticeSaturationSource.WalAdmission"/>.
    /// </summary>
    [Test]
    public void SetManyFanOut_is_a_distinct_declared_seam()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Enum.IsDefined(LatticeSaturationSource.SetManyFanOut), Is.True);
            Assert.That(LatticeSaturationSource.SetManyFanOut, Is.Not.EqualTo(LatticeSaturationSource.Unspecified));
            Assert.That(LatticeSaturationSource.SetManyFanOut, Is.Not.EqualTo(LatticeSaturationSource.WalAdmission));
        });
    }

    /// <summary>
    /// The fan-out refusal must round-trip its discriminator, because a
    /// caller deciding whether to back off branches on the property and
    /// not on the exception type.
    /// </summary>
    [Test]
    public void SetManyFanOut_round_trips_through_the_exception()
    {
        var ex = new LatticeSaturatedException(
            "fan-out budget elapsed", "tree-e", LatticeSaturationSource.SetManyFanOut);

        Assert.Multiple(() =>
        {
            Assert.That(ex.SaturationSource, Is.EqualTo(LatticeSaturationSource.SetManyFanOut));
            Assert.That(ex.TreeId, Is.EqualTo("tree-e"));
        });
    }
}
