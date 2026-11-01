using NUnit.Framework.Interfaces;

[assembly: Orleans.Lattice.Api.Backup.Tests.RedProofResetAttribute]

namespace Orleans.Lattice.Api.Backup.Tests;

// RED-PROOF: resets the red-proof switches around every test; removed with them.
[AttributeUsage(AttributeTargets.Assembly)]
internal sealed class RedProofResetAttribute : Attribute, ITestAction
{
    public ActionTargets Targets => ActionTargets.Test;

    public void BeforeTest(ITest test) => Orleans.Lattice.Backup.RedProof.Reset();

    public void AfterTest(ITest test) => Orleans.Lattice.Backup.RedProof.Reset();
}
