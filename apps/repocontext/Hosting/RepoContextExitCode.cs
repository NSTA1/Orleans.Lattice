namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The process exit codes this container reports, and the seam that assigns them.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists (issue #2401).</b> Issue #2397 made an abandoned drain loud
/// in the log and #2399 stopped it being reported as a success, but the process
/// still exited <c>0</c>. An orchestrator does not read logs; it reads the exit
/// code. So an abandoned drain and a clean stop were indistinguishable at the only
/// layer that acts automatically, and the loud log line was the sole evidence -
/// which reduces the whole improvement to log inspection by a human who already
/// suspects something is wrong.
/// </para>
/// <para>
/// <b>What a non-zero code does and does not buy.</b> It is an observability and
/// escalation signal, not a restart control. The sample compose file runs the
/// container under <c>restart: unless-stopped</c>, and Docker restarts on that
/// policy regardless of exit code, so nothing here suppresses a restart. What it
/// changes is what is <i>recorded</i>: <c>docker inspect</c> reports
/// <c>.State.ExitCode</c>, <c>docker ps -a</c> shows <c>Exited (70)</c> rather
/// than <c>Exited (0)</c>, and under Kubernetes the container terminates with
/// reason <c>Error</c> instead of <c>Completed</c>, which is what an alert can be
/// written against. Claiming more than that would be overclaiming.
/// </para>
/// <para>
/// <b>There is deliberately no way to turn this off.</b> A configuration knob
/// restoring <c>0</c> would be an exemption that removes the evidence rather than
/// the problem, which is the same trap as exempting an operator-invisible failure
/// counter from the operator's dashboard. An operator who does not want the signal
/// wants the drain to fit its budget instead.
/// </para>
/// </remarks>
public static class RepoContextExitCode
{
    /// <summary>The process stopped cleanly: the drain finished inside the host's shutdown budget.</summary>
    public const int Success = 0;

    /// <summary>
    /// The host's shutdown budget expired and the drain was abandoned part-way, so
    /// leaf activations were torn down without banking their projection checkpoints.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The value is <c>70</c>, which is <c>EX_SOFTWARE</c> in the BSD
    /// <c>sysexits.h</c> convention ("an internal software error has been
    /// detected"). That is the closest established meaning: the process failed to
    /// complete its own shutdown work within its own budget. The convention is not
    /// a standard any orchestrator interprets, so the value's real job is to be
    /// <b>distinct and documented</b> rather than semantically clever, and it was
    /// chosen to avoid every band that already means something else:
    /// </para>
    /// <list type="bullet">
    /// <item><description><c>0</c>, <c>1</c> and <c>2</c> - success, generic failure, and shell misuse, so none of them is distinguishable from an unrelated fault.</description></item>
    /// <item><description><c>125</c>, <c>126</c> and <c>127</c> - reserved by Docker and the shell for "the daemon itself failed", "the command could not be invoked", and "command not found".</description></item>
    /// <item><description><c>128</c> and above - signal-derived (<c>128 + N</c>), which is where the two codes an operator already associates with this container live: <c>137</c> for <c>SIGKILL</c> (the container killed mid-drain, issue #2389) and <c>143</c> for <c>SIGTERM</c>. Reusing that band would collide with exactly the neighbouring condition this code exists to separate itself from.</description></item>
    /// </list>
    /// </remarks>
    public const int DrainAbandoned = 70;

    /// <summary>
    /// Assigns <paramref name="code"/> to <see cref="Environment.ExitCode"/>, which
    /// is the process exit code this host actually reports.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the production reporter passed to <see cref="RepoContextDrainSignal"/>
    /// by <see cref="RepoContextHostBuilder"/>. It is a named method rather than a
    /// lambda so that the wiring is greppable and so that this assignment - the one
    /// line on which the whole signal depends - is directly testable.
    /// </para>
    /// <para>
    /// <b>It works only because the entry point does not return a code of its own.</b>
    /// <c>Environment.ExitCode</c> is honoured by an <c>async Task</c> entry point,
    /// whose compiled <c>Main</c> returns <see langword="void"/>. Adding an explicit
    /// <c>return</c> to <c>Program.cs</c> would change that compiled shape to return
    /// <see cref="int"/> and would <b>silently override</b> this assignment, so a
    /// drain abandoned at the budget would go back to reporting success.
    /// <c>RepoContextEntryPointShapeTests</c> pins the shape against precisely that.
    /// </para>
    /// </remarks>
    /// <param name="code">The exit code to report.</param>
    public static void SetProcessExitCode(int code) => Environment.ExitCode = code;
}
