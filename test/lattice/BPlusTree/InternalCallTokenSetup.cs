using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Sets the internal call token in <see cref="RequestContext"/> before each test
/// so that integration tests can call internal Lattice grains directly.
/// </summary>
[SetUpFixture]
public class InternalCallTokenSetup
{
    [OneTimeSetUp]
    public void SetToken()
    {
        RequestContext.Set(LatticeConstants.InternalCallTokenKey, LatticeConstants.InternalCallTokenValue);
    }

    [OneTimeTearDown]
    public void ClearToken()
    {
        RequestContext.Set(LatticeConstants.InternalCallTokenKey, null);
    }
}
