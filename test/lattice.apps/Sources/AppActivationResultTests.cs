namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public class AppActivationResultTests
{
    [Test]
    public void Activated_exposes_the_assembly()
    {
        var assembly = typeof(AppActivationResultTests).Assembly;

        var result = AppActivationResult.Activated(assembly);

        Assert.That(result.IsActivated, Is.True);
        Assert.That(result.Assembly, Is.SameAs(assembly));
        Assert.That(result.Errors, Is.Empty);
    }

    [Test]
    public void Activated_null_assembly_throws()
    {
        Assert.Throws<ArgumentNullException>(() => AppActivationResult.Activated(null!));
    }

    [Test]
    public void Failed_copies_the_errors_and_exposes_no_assembly()
    {
        var errors = new List<AppManifestError> { new("load", "$", "Could not load.") };

        var result = AppActivationResult.Failed(errors);
        errors.Clear();

        Assert.That(result.IsActivated, Is.False);
        Assert.That(result.Assembly, Is.Null);
        Assert.That(result.Errors.Single().Code, Is.EqualTo("load"));
    }

    [Test]
    public void Failed_rejects_null_empty_or_null_containing_errors()
    {
        Assert.Throws<ArgumentNullException>(() => AppActivationResult.Failed(null!));
        Assert.Throws<ArgumentException>(() => AppActivationResult.Failed([]));
        Assert.Throws<ArgumentException>(() => AppActivationResult.Failed([null!]));
    }
}
