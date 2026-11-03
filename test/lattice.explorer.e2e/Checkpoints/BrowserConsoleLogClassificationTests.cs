namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>
/// How <see cref="BrowserConsoleLog"/> classifies a console line, with no browser: the two lines an app's
/// sandboxed frame reported when the task board first loaded under a journey (U3, after #5595) are the
/// app's own policy at work, and fail nothing, while the Explorer's own violations and errors still do.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class BrowserConsoleLogClassificationTests
{
    [Test]
    public void An_unrecognised_policy_directive_from_the_frame_is_a_note()
    {
        Assert.That(
            BrowserConsoleLog.Classify("error", "Unrecognized Content-Security-Policy directive 'webrtc'.", "https://127.0.0.1:44051/_apps/frame/v1/frame.html:0:0"),
            Is.EqualTo(BrowserConsoleLog.ConsoleLine.Note));
    }

    [Test]
    public void A_refusal_inside_the_opaque_origin_frame_naming_a_frame_resource_is_a_note()
    {
        const string text = "Refused to connect to 'https://127.0.0.1:44051/_apps/frame/v1/lattice-app.css' because it violates the following Content Security Policy directive: \"connect-src 'none'\".";

        Assert.That(BrowserConsoleLog.Classify("error", text, ":11:0"), Is.EqualTo(BrowserConsoleLog.ConsoleLine.Note));
        Assert.That(BrowserConsoleLog.Classify("error", text, "blob:https://127.0.0.1:44051/abc:1:1"), Is.EqualTo(BrowserConsoleLog.ConsoleLine.Note));
    }

    [Test]
    public void A_violation_by_an_explorer_document_fails()
    {
        Assert.That(
            BrowserConsoleLog.Classify("error", "Refused to load the script 'https://evil.example/x.js' because it violates the following Content Security Policy directive: \"script-src 'self'\".", "https://127.0.0.1:44051/data:1:1"),
            Is.EqualTo(BrowserConsoleLog.ConsoleLine.PolicyViolation));
        Assert.That(
            BrowserConsoleLog.Classify("error", "Refused to connect to 'https://127.0.0.1:44051/_apps/frame/v1/x.css' because it violates the following Content Security Policy directive: \"connect-src 'self'\".", "https://127.0.0.1:44051/apps/task-board:3:1"),
            Is.EqualTo(BrowserConsoleLog.ConsoleLine.PolicyViolation),
            "an Explorer document's own violation fails even when it names a frame resource");
    }

    [Test]
    public void Console_errors_fail_and_warnings_are_notes()
    {
        Assert.That(BrowserConsoleLog.Classify("error", "deliberate console error", "https://127.0.0.1/:1:1"), Is.EqualTo(BrowserConsoleLog.ConsoleLine.Error));
        Assert.That(BrowserConsoleLog.Classify("warning", "slow", "https://127.0.0.1/:1:1"), Is.EqualTo(BrowserConsoleLog.ConsoleLine.Note));
        Assert.That(BrowserConsoleLog.Classify("log", "hello", "https://127.0.0.1/:1:1"), Is.EqualTo(BrowserConsoleLog.ConsoleLine.Ignored));
    }
}
