type StatusPillProps = {
  value: string;
};

export function StatusPill({ value }: StatusPillProps) {
  const normalized = value.toLowerCase();
  const tone =
    normalized === "enabled" || normalized === "filled" || normalized === "working"
      ? "border-lime-400/30 bg-lime-400/10 text-lime-300"
      : normalized === "paused" || normalized === "pending"
        ? "border-amber-400/30 bg-amber-400/10 text-amber-300"
        : "border-rose-400/30 bg-rose-400/10 text-rose-300";

  return <span className={`rounded-full border px-3 py-1 text-xs uppercase tracking-[0.25em] ${tone}`}>{value}</span>;
}
