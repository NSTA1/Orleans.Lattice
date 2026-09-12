using System.Reflection;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Pins the abandoned-drain exit code itself (issue #2401): the value, the one line
/// that assigns it, and the entry-point shape without which that assignment is
/// silently discarded.
/// </summary>
public sealed class RepoContextExitCodeTests
{
    [Test]
    public void The_abandoned_drain_code_is_distinct_from_every_band_that_already_means_something_else()
    {
        // Asserting the literal 70 against itself would be a tautology. What makes
        // the value correct is that it cannot be confused with a code an operator
        // already attributes to a different cause, so those are the properties under
        // test - and any replacement value must satisfy them too.
        var code = RepoContextExitCode.DrainAbandoned;

        Assert.Multiple(() =>
        {
            Assert.That(code, Is.Not.EqualTo(RepoContextExitCode.Success), "an abandoned drain is not a clean stop");
            Assert.That(
                code,
                Is.Not.AnyOf(0, 1, 2),
                "0, 1 and 2 are success, generic failure and shell misuse, so none of them identifies this cause");
            Assert.That(
                code,
                Is.Not.AnyOf(125, 126, 127),
                "125-127 are reserved by Docker and the shell for a daemon fault or an uninvokable command");
            Assert.That(
                code,
                Is.LessThan(128),
                "128 and above is the signal-derived band, which holds 137 (SIGKILL, the killed-mid-drain case of "
                + "issue #2389) and 143 (SIGTERM) - the neighbouring conditions this code exists to be told apart from");
            Assert.That(code, Is.InRange(1, 255), "the code must survive a POSIX wait status without truncation");
        });
    }

    [Test]
    public void Setting_the_process_exit_code_assigns_what_the_process_will_report()
    {
        // The production reporter is one assignment, and it is the single line the
        // whole signal reduces to. It is exercised directly here, restoring the
        // previous value, because no other test in this suite may touch the real
        // process exit code.
        var before = Environment.ExitCode;

        try
        {
            RepoContextExitCode.SetProcessExitCode(RepoContextExitCode.DrainAbandoned);

            Assert.That(Environment.ExitCode, Is.EqualTo(RepoContextExitCode.DrainAbandoned));
        }
        finally
        {
            Environment.ExitCode = before;
        }

        Assert.That(Environment.ExitCode, Is.EqualTo(before), "the test must not leave the NUnit host poisoned");
    }

    [Test]
    public void The_host_entry_point_returns_no_code_of_its_own_so_the_reported_exit_code_survives()
    {
        // This guard exists because the failure it prevents is SILENT. An entry
        // point that returns a code overrides Environment.ExitCode completely:
        // measured directly, a program that assigns 70 and then returns 0 exits 0,
        // with no warning and no diagnostic anywhere. Adding a `return` to
        // Program.cs would therefore restore the exact defect of issue #2401 while
        // every test above continued to pass, because they assert on the reporter
        // rather than on the process.
        //
        // The shape is read from the COMPILED entry point rather than from the
        // source. A top-level `await`-shaped program compiles to a synthesized
        // wrapper returning void that drives the async body; add any `return N` and
        // that wrapper returns int instead. So void here means "the process reports
        // whatever Environment.ExitCode holds", which is precisely the condition
        // being pinned.
        var entryPoint = typeof(RepoContextHostBuilder).Assembly.EntryPoint;

        Assert.That(
            entryPoint,
            Is.Not.Null,
            "the host assembly must be an executable, otherwise this guard is asserting nothing");

        Assert.That(
            entryPoint!.ReturnType,
            Is.EqualTo(typeof(void)),
            $"the entry point returns {entryPoint.ReturnType} - a returned exit code silently overrides "
            + $"{nameof(RepoContextExitCode)}.{nameof(RepoContextExitCode.SetProcessExitCode)}, so an abandoned "
            + "drain would go back to reporting success. Assign Environment.ExitCode instead of returning a code.");
    }

    [Test]
    public void The_entry_point_guard_would_catch_the_shape_it_exists_to_reject()
    {
        // A control on the guard above. Reflection assertions are exactly the kind
        // that pass forever against the wrong thing, so this demonstrates that the
        // predicate discriminates: a Task<int>-shaped entry point compiles to an
        // int-returning wrapper, which is the shape that would silently discard the
        // exit code, and the guard's assertion is false for it.
        var rejected = typeof(EntryPointShapes).GetMethod(
            nameof(EntryPointShapes.ReturnsACodeOfItsOwn),
            BindingFlags.Static | BindingFlags.NonPublic);

        var accepted = typeof(EntryPointShapes).GetMethod(
            nameof(EntryPointShapes.ReturnsNoCodeOfItsOwn),
            BindingFlags.Static | BindingFlags.NonPublic);

        Assert.Multiple(() =>
        {
            Assert.That(rejected, Is.Not.Null);
            Assert.That(accepted, Is.Not.Null);
            Assert.That(
                rejected!.ReturnType,
                Is.Not.EqualTo(typeof(void)),
                "the rejected shape must fail the guard's predicate");
            Assert.That(
                accepted!.ReturnType,
                Is.EqualTo(typeof(void)),
                "the accepted shape must pass the guard's predicate");
        });
    }

    /// <summary>
    /// The two compiled entry-point shapes, standing in for what
    /// <c>Program.cs</c> becomes with and without a returned exit code.
    /// </summary>
    private static class EntryPointShapes
    {
        internal static int ReturnsACodeOfItsOwn() => RepoContextExitCode.Success;

        internal static void ReturnsNoCodeOfItsOwn()
        {
        }
    }
}
