using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Regression tests for structural equality of <see cref="RuntimeViewRegistration"/>.
/// The record carries a <c>byte[]? ProjectionProviderPayload</c> that the
/// compiler-generated record equality would compare by reference. Because a
/// re-issued <c>CreateAsync</c> produces a content-identical but distinct payload
/// array, reference equality made two logically identical registrations unequal,
/// defeating the idempotent re-registration dedup guard in the view registry
/// grain. Equality must compare the payload by content.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class RuntimeViewRegistrationEqualityRegressionTests
{
    private static RuntimeViewRegistration Registration(byte[]? payload) =>
        new()
        {
            ViewName = "runtime",
            SourceTreeId = "source-runtime",
            ProjectionTypeName = "Ns.Projection",
            ProjectionVersion = "v1",
            IsAggregation = true,
            Accumulative = false,
            ProjectionProviderKey = "provider",
            ProjectionProviderPayload = payload,
        };

    [Test]
    public void Registrations_with_distinct_but_equal_payload_arrays_are_equal()
    {
        var left = Registration([1, 2, 3]);
        var right = Registration([1, 2, 3]);

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(left.ProjectionProviderPayload, right.ProjectionProviderPayload), Is.False);
            Assert.That(left, Is.EqualTo(right));
            Assert.That(left.GetHashCode(), Is.EqualTo(right.GetHashCode()));
        });
    }

    [Test]
    public void Registrations_with_different_payload_content_are_not_equal()
    {
        var left = Registration([1, 2, 3]);
        var right = Registration([1, 2, 4]);

        Assert.That(left, Is.Not.EqualTo(right));
    }

    [Test]
    public void Registrations_with_null_payloads_are_equal()
    {
        var left = Registration(null);
        var right = Registration(null);

        Assert.Multiple(() =>
        {
            Assert.That(left, Is.EqualTo(right));
            Assert.That(left.GetHashCode(), Is.EqualTo(right.GetHashCode()));
        });
    }

    [Test]
    public void Registration_with_null_payload_differs_from_empty_payload()
    {
        var nullPayload = Registration(null);
        var emptyPayload = Registration([]);

        Assert.That(nullPayload, Is.Not.EqualTo(emptyPayload));
    }

    [Test]
    public void Registrations_differing_only_by_scalar_field_are_not_equal()
    {
        var left = Registration([9]);
        var right = left with { ProjectionVersion = "v2" };

        Assert.That(left, Is.Not.EqualTo(right));
    }
}
