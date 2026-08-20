using Orleans.Lattice.Api.Auth;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Exercises the static well-known instance on <see cref="DirectorySearchResult"/>.
/// The serialization fixture round-trips an uninitialised instance and never
/// triggers the static initializer, so the shared <c>Unavailable</c> value is
/// otherwise uncovered.
/// </summary>
[TestFixture]
public class DirectorySearchResultTests
{
    [Test]
    public void Unavailable_reports_no_configured_directory()
    {
        var result = DirectorySearchResult.Unavailable;

        Assert.That(result.Available, Is.False);
        Assert.That(result.Principals, Is.Empty);
        Assert.That(result.ContinuationToken, Is.Null);
    }
}
