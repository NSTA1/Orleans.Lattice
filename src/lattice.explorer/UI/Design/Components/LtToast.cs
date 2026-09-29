namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>One notification in an <see cref="LtToastService"/>'s queue.</summary>
/// <param name="Id">The toast's id within its queue, used to dismiss it.</param>
/// <param name="Tone">What kind of news it carries.</param>
/// <param name="Message">The message, as plain text.</param>
internal sealed record LtToast(long Id, LtToastTone Tone, string Message);
