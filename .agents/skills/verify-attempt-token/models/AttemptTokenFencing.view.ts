export const title = "AttemptToken Fencing and Bead Reopen Revocation";

export function state(vars: Record<string, any>): string {
  const status = String(vars.bead_status ?? "Open");
  const rev = Number(vars.status_revision ?? 0);
  const attempt = String(vars.current_attempt ?? "None");
  const leases: any[] = Array.isArray(vars.active_leases) ? vars.active_leases : [];

  if (status === "Open") {
    return `Bead Open (rev ${rev})`;
  }
  if (status === "Closed") {
    return `Bead Closed (rev ${rev})`;
  }
  if (status === "InProgress") {
    if (leases.length === 0) {
      return `InProgress: ${attempt} (rev ${rev}, unleased)`;
    }
    const lease = leases[0];
    return `InProgress: ${attempt} holds lease (epoch ${lease.epoch})`;
  }
  return `${status} (rev ${rev})`;
}

export function step(
  action: string,
  _from: Record<string, any>,
  _to: Record<string, any>,
): string | null {
  if (action === "Done") return null;
  if (action.startsWith("Dispatch")) return "dispatch attempt";
  if (action.startsWith("AcquireLease")) return "acquire lease with AttemptToken";
  if (action.startsWith("ReleaseLease")) return "release lease";
  if (action === "Close") return "close bead (release leases)";
  if (action === "Reopen") return "reopen bead (rev++, fence prior leases)";
  return action;
}

export function group(vars: Record<string, any>): string {
  const status = String(vars.bead_status ?? "Open");
  if (status === "Open") return "Open";
  if (status === "InProgress") return "InProgress";
  if (status === "Closed") return "Closed";
  return "Other";
}

export const colors: Record<string, string> = {
  Open: "blue",
  InProgress: "teal",
  Closed: "purple",
};
