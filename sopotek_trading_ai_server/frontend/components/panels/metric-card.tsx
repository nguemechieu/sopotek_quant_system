import { ReactNode } from "react";

type MetricCardProps = {
  label: string;
  value: string;
  hint?: string;
  tone?: "default" | "good" | "warn";
  footer?: ReactNode;
};

export function MetricCard({ label, value, hint, tone = "default", footer }: MetricCardProps) {
  const toneClass =
    tone === "good"
      ? "text-lime-300"
      : tone === "warn"
        ? "text-amber-300"
        : "text-sand";

  return (
    <section className="panel rounded-[24px] p-5">
      <p className="text-xs uppercase tracking-[0.3em] text-mist/45">{label}</p>
      <p className={`mt-3 text-3xl font-semibold tracking-tight ${toneClass}`}>{value}</p>
      {hint ? <p className="mt-2 text-sm text-mist/65">{hint}</p> : null}
      {footer ? <div className="mt-4 border-t border-white/10 pt-4 text-sm text-mist/70">{footer}</div> : null}
    </section>
  );
}
