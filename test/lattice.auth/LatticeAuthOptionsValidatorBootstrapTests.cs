using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Supplementary unit tests for <see cref="LatticeAuthOptionsValidator"/> covering
/// the bootstrap-administrator branches the sibling fixture does not reach: a null
/// set is rejected, a set containing a null-or-empty subject id is rejected, and a
/// populated set of real ids validates.
/// </summary>
[TestFixture]
public sealed class LatticeAuthOptionsValidatorBootstrapTests
{
    private static readonly LatticeAuthOptionsValidator Validator = new();

    [Test]
    public void Validate_null_bootstrap_administrators_fails_validation()
    {
        var options = new LatticeAuthOptions { BootstrapAdministrators = null! };

        var result = Validator.Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validate_empty_bootstrap_administrator_id_fails_validation()
    {
        var options = new LatticeAuthOptions
        {
            BootstrapAdministrators = new HashSet<string>(StringComparer.Ordinal) { string.Empty },
        };

        var result = Validator.Validate(null, options);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validate_populated_bootstrap_administrators_validates_successfully()
    {
        var options = new LatticeAuthOptions
        {
            BootstrapAdministrators = new HashSet<string>(StringComparer.Ordinal) { "root", "ops" },
        };

        var result = Validator.Validate(null, options);

        Assert.That(result.Succeeded, Is.True);
    }
}
