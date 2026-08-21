using Orleans.Lattice.Explorer.Access;

namespace Orleans.Lattice.Explorer.Tests.Access;

/// <summary>
/// Unit tests for the <see cref="AccessOperationResult"/> factories, the
/// <see cref="AccessOperationResult.IsSuccess"/> projection, and their argument
/// guards, so the Access-area mutation result type is fully covered on its own.
/// </summary>
[TestFixture]
public class AccessOperationResultTests
{
    [Test]
    public void Success_sets_succeeded_status_and_message()
    {
        var result = AccessOperationResult.Success("saved");

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(AccessOperationStatus.Succeeded));
            Assert.That(result.Message, Is.EqualTo("saved"));
            Assert.That(result.IsSuccess, Is.True);
        });
    }

    [Test]
    public void Denied_sets_denied_status_and_is_not_success()
    {
        var result = AccessOperationResult.Denied("nope");

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(AccessOperationStatus.Denied));
            Assert.That(result.Message, Is.EqualTo("nope"));
            Assert.That(result.IsSuccess, Is.False);
        });
    }

    [Test]
    public void Failure_sets_failed_status_and_is_not_success()
    {
        var result = AccessOperationResult.Failure("gone");

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(AccessOperationStatus.Failed));
            Assert.That(result.Message, Is.EqualTo("gone"));
            Assert.That(result.IsSuccess, Is.False);
        });
    }

    [Test]
    public void Success_null_message_throws()
    {
        Assert.That(() => AccessOperationResult.Success(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Denied_null_message_throws()
    {
        Assert.That(() => AccessOperationResult.Denied(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Failure_null_message_throws()
    {
        Assert.That(() => AccessOperationResult.Failure(null!), Throws.ArgumentNullException);
    }
}
