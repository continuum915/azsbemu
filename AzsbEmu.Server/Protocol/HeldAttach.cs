using System.Reflection;
using Amqp;
using Amqp.Listener;

namespace AzsbEmu.Server.Protocol;

/// <summary>
/// Lets the broker hold a receiver's attach unanswered, as Service Bus does while a session receiver waits for a
/// session. AMQPNetLite cannot take a detach for a link whose attach it hasn't answered: it fails the whole
/// connection. While held, the link is marked attached so a client that gives up closes only that link.
/// </summary>
internal static class HeldAttach
{
    private static readonly FieldInfo StateField =
        typeof(Link).GetField("state", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("Amqp.Link.state field not found.");
    private static readonly PropertyInfo ThisLockProperty =
        typeof(Link).GetProperty("ThisLock", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("Amqp.Link.ThisLock property not found.");
    private static readonly object Start = Enum.Parse(StateField.FieldType, "Start");
    private static readonly object Attached = Enum.Parse(StateField.FieldType, "Attached");

    public static void Hold(ListenerLink link)
    {
        lock (ThisLock(link))
        {
            if (StateField.GetValue(link)!.Equals(Start)) StateField.SetValue(link, Attached);
        }
    }

    /// <summary>
    /// Answers a held attach with <paramref name="answer"/>, unless the client detached it meanwhile.
    /// Returns false when the link is gone and nothing was answered.
    /// </summary>
    public static bool TryAnswer(ListenerLink link, Action answer)
    {
        lock (ThisLock(link))
        {
            if (!StateField.GetValue(link)!.Equals(Attached)) return false;
            StateField.SetValue(link, Start);
            answer(); // under the lock, so a detach can't arrive between restoring the state and answering
            return true;
        }
    }

    private static object ThisLock(Link link) => ThisLockProperty.GetValue(link)!;
}
