using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Serialization;
using Orleans.Serialization.Cloning;

namespace Orleans.Lattice.Api.Abstractions.Tests.Apps;

[TestFixture]
public sealed class AppBridgeExceptionTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void SetUp() => _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void TearDown() => _services.Dispose();

    private static IEnumerable<AppBridgeFailure> Failures() => Enum.GetValues<AppBridgeFailure>();

    [Test]
    public void Failure_code_set_is_closed_and_fails_closed_by_default()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Enum.GetNames<AppBridgeFailure>(),
                Is.EqualTo(new[] { "Denied", "NotFound", "Invalid", "TooLarge", "Conflict", "Unavailable" }));
            Assert.That(default(AppBridgeFailure), Is.EqualTo(AppBridgeFailure.Denied));
        });
    }

    [Test]
    public void Parameterless_ctor_reports_denied_with_its_fixed_message()
    {
        var ex = new AppBridgeException();

        Assert.Multiple(() =>
        {
            Assert.That(ex.Failure, Is.EqualTo(AppBridgeFailure.Denied));
            Assert.That(ex.Message, Is.EqualTo(AppBridgeException.DefaultMessage(AppBridgeFailure.Denied)));
            Assert.That(ex.InnerException, Is.Null);
        });
    }

    [TestCaseSource(nameof(Failures))]
    public void Failure_ctor_uses_the_fixed_message_for_its_code(AppBridgeFailure failure)
    {
        var ex = new AppBridgeException(failure);

        Assert.Multiple(() =>
        {
            Assert.That(ex.Failure, Is.EqualTo(failure));
            Assert.That(ex.Message, Is.EqualTo(AppBridgeException.DefaultMessage(failure)));
        });
    }

    [Test]
    public void Default_messages_are_distinct_and_non_empty()
    {
        var messages = Failures().Select(AppBridgeException.DefaultMessage).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(messages, Is.All.Not.Empty);
            Assert.That(messages, Is.Unique);
        });
    }

    [Test]
    public void Message_ctor_keeps_the_code_and_the_supplied_message()
    {
        var ex = new AppBridgeException(AppBridgeFailure.TooLarge, "value exceeds 64 KiB");

        Assert.Multiple(() =>
        {
            Assert.That(ex.Failure, Is.EqualTo(AppBridgeFailure.TooLarge));
            Assert.That(ex.Message, Is.EqualTo("value exceeds 64 KiB"));
        });
    }

    [Test]
    public void Message_ctor_accepts_an_empty_message()
    {
        Assert.That(new AppBridgeException(AppBridgeFailure.Invalid, string.Empty).Message, Is.Empty);
    }

    [Test]
    public void Message_ctor_rejects_a_null_message()
    {
        Assert.That(() => new AppBridgeException(AppBridgeFailure.Invalid, null!),
            Throws.ArgumentNullException.With.Property("ParamName").EqualTo("message"));
    }

    [TestCase(-1)]
    [TestCase(6)]
    [TestCase(int.MaxValue)]
    public void Undefined_codes_are_rejected_by_every_overload(int code)
    {
        var failure = (AppBridgeFailure)code;

        Assert.Multiple(() =>
        {
            Assert.That(() => new AppBridgeException(failure), Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(() => new AppBridgeException(failure, "text"), Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(() => AppBridgeException.DefaultMessage(failure), Throws.TypeOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void Exception_derives_directly_from_system_exception()
    {
        Assert.That(typeof(AppBridgeException).BaseType, Is.EqualTo(typeof(Exception)),
            "Deriving directly from System.Exception keeps a same-silo deep copy safe without a copier.");
    }

    [TestCaseSource(nameof(Failures))]
    public void Serializer_round_trip_preserves_code_and_message(AppBridgeFailure failure)
    {
        var serializer = _services.GetRequiredService<Serializer>();
        var original = new AppBridgeException(failure, $"sanitised {failure}");

        var copy = serializer.Deserialize<AppBridgeException>(serializer.SerializeToArray(original));

        Assert.Multiple(() =>
        {
            Assert.That(copy.Failure, Is.EqualTo(failure));
            Assert.That(copy.Message, Is.EqualTo(original.Message));
        });
    }

    [Test]
    public void Same_silo_deep_copy_preserves_code_and_message()
    {
        var copier = _services.GetRequiredService<DeepCopier<AppBridgeException>>();
        var original = new AppBridgeException(AppBridgeFailure.Conflict);

        var copy = copier.Copy(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Failure, Is.EqualTo(AppBridgeFailure.Conflict));
            Assert.That(copy.Message, Is.EqualTo(original.Message));
        });
    }
}
