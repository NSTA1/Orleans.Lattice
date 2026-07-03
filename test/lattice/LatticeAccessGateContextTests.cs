namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the <see cref="LatticeAccessGateContext"/> ambient marker that
/// identifies a system-origin (infrastructure-authored) call so the future
/// access-gate enforcement point can skip authorization for it. Mirrors the
/// existing ambient-context tests (credential / maintenance / view scopes).
/// </summary>
[TestFixture]
public class LatticeAccessGateContextTests
{
    [Test]
    public void IsSystemOrigin_defaults_to_false()
    {
        Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.False);
    }

    [Test]
    public void EnterSystemOrigin_marks_the_scope_and_clears_on_dispose()
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.True);
        }

        Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.False);
    }

    [Test]
    public void EnterSystemOrigin_nested_scopes_restore_correctly()
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.True);
            using (LatticeAccessGateContext.EnterSystemOrigin())
            {
                Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.True);
            }

            Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.True);
        }

        Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.False);
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        var scope = LatticeAccessGateContext.EnterSystemOrigin();
        scope.Dispose();
        Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.False);

        // A second dispose must not re-apply the restore and clobber a marker
        // set after the first dispose returned.
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            scope.Dispose();
            Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.True);
        }
    }

    [Test]
    public async Task IsSystemOrigin_flows_across_async_await_boundary()
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Task.Yield();
            Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.True);
        }
    }
}
