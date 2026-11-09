export const title = "Toron Doorbell Wake Level-Triggered State Machine";

export function state(vars: Record<string, any>): string {
  const dec = String(vars.decision ?? "NothingUnread");
  const rings = Number(vars.rings ?? 0);
  const head = Number(vars.head ?? 0);
  const cursor = Number(vars.cursor ?? 0);
  const pane = String(vars.pane_state ?? "Idle");

  if (dec === "NothingUnread") {
    return `Caught up (head=${head})`;
  }
  if (dec === "Unbound") {
    return "Binding dropped (wake blocked)";
  }
  if (dec === "Escalate") {
    return `Escalated: Agent deaf after ${rings} rings`;
  }
  if (dec === "Busy") {
    return `Mail waiting, pane ${pane.toLowerCase()} (suppressed)`;
  }
  if (dec === "Hold") {
    return `Mail waiting, backoff holding (ring ${rings} sent)`;
  }
  if (dec === "Ring") {
    return `Ringing doorbell (attempt ${rings + 1}, head=${head})`;
  }
  return `${dec} (${cursor}<${head}, rings=${rings})`;
}

export function step(
  action: string,
  _from: Record<string, any>,
  _to: Record<string, any>,
): string | null {
  if (action === "Done") return null;
  if (action === "NewMail") return "new mail arrives";
  if (action === "RingSent") return "send wake line to pane";
  if (action === "BackoffElapsed") return "backoff delay elapses";
  if (action === "AdvanceCursor") return "agent runs `toron next` (cursor advances)";
  if (action === "PaneStartsWorking") return "pane starts working";
  if (action === "PaneFinishesTurn") return "pane finishes turn (idle)";
  if (action === "BindingDrops") return "binding lost";
  if (action === "BindingRestores") return "binding verified";
  return action;
}

export function group(vars: Record<string, any>): string {
  const dec = String(vars.decision ?? "NothingUnread");
  if (dec === "NothingUnread") return "Settled";
  if (dec === "Ring") return "Active";
  if (dec === "Hold" || dec === "Busy") return "Waiting";
  if (dec === "Escalate") return "Escalated";
  return "Other";
}

export const colors: Record<string, string> = {
  Settled: "teal",
  Active: "blue",
  Waiting: "yellow",
  Escalated: "purple",
  Other: "tan",
};
