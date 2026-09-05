namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the public <see cref="CompensationFailedException"/>: every
/// construction overload, the <see cref="CompensationFailedException.StepIndex"/>
/// attribution slot, the inheritance contract that keeps it deep-copyable
/// without a companion copier, and the stable Orleans serialization surface
/// (alias + <c>[Id]</c> member) the manifest relies on to surface the typed
/// exception across a grain boundary.
/// </summary>
[TestFixture]
public class CompensationFailedExceptionTests
{
    [Test]
    public void Parameterless_constructor_reports_an_unknown_step_index()
    {
        var ex = new CompensationFailedException();
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.Not.Null);
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.StepIndex, Is.EqualTo(-1));
        });
    }

    [Test]
    public void Message_constructor_preserves_the_message_and_reports_an_unknown_step_index()
    {
        var ex = new CompensationFailedException("compensation for step 2 faulted");
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("compensation for step 2 faulted"));
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.StepIndex, Is.EqualTo(-1));
        });
    }

    [Test]
    public void MessageAndInner_constructor_preserves_both_arguments()
    {
        var inner = new InvalidOperationException("effect handler threw");
        var ex = new CompensationFailedException("compensation faulted", inner);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("compensation faulted"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
            Assert.That(ex.StepIndex, Is.EqualTo(-1));
        });
    }

    [Test]
    public void StepIndex_constructor_preserves_the_faulted_step()
    {
        var ex = new CompensationFailedException("compensation for step 3 faulted", stepIndex: 3);
        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("compensation for step 3 faulted"));
            Assert.That(ex.InnerException, Is.Null);
            Assert.That(ex.StepIndex, Is.EqualTo(3));
        });
    }

    [Test]
    public void StepIndex_constructor_accepts_the_first_step()
    {
        var ex = new CompensationFailedException("step 0 compensation faulted", stepIndex: 0);
        Assert.That(ex.StepIndex, Is.EqualTo(0),
            "step 0 must be distinguishable from the unknown-step sentinel of -1");
    }

    [Test]
    public void Derives_directly_from_Exception_so_no_companion_copier_is_required()
    {
        Assert.That(typeof(CompensationFailedException).BaseType, Is.EqualTo(typeof(Exception)),
            "a [GenerateSerializer] exception deriving from a BCL exception subclass needs a "
            + "[RegisterCopier] IDeepCopier<T>; deriving directly from Exception avoids that");
    }

    [Test]
    public void Is_sealed_and_public()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(CompensationFailedException).IsSealed, Is.True);
            Assert.That(typeof(CompensationFailedException).IsPublic, Is.True);
        });
    }

    [Test]
    public void Carries_stable_Orleans_alias()
    {
        var aliasAttr = typeof(CompensationFailedException)
            .GetCustomAttributes(typeof(AliasAttribute), inherit: false)
            .Cast<AliasAttribute>()
            .SingleOrDefault();
        Assert.That(aliasAttr, Is.Not.Null);
        Assert.That(aliasAttr!.Alias, Is.EqualTo("ol.ecf"),
            "the alias value pins the Orleans wire format; a rename would break rolling-upgrade peers");
    }

    [Test]
    public void Carries_GenerateSerializer_attribute()
    {
        var attr = typeof(CompensationFailedException)
            .GetCustomAttributes(typeof(GenerateSerializerAttribute), inherit: false);
        Assert.That(attr, Is.Not.Empty);
    }

    [Test]
    public void StepIndex_carries_Id_zero()
    {
        var prop = typeof(CompensationFailedException).GetProperty(nameof(CompensationFailedException.StepIndex));
        Assert.That(prop, Is.Not.Null);
        var idAttr = prop!
            .GetCustomAttributes(typeof(IdAttribute), inherit: false)
            .Cast<IdAttribute>()
            .SingleOrDefault();
        Assert.That(idAttr, Is.Not.Null, "StepIndex must carry [Id]");
        Assert.That(idAttr!.Id, Is.EqualTo(0u));
    }
}
