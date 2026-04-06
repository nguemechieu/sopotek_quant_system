import { ReactNode } from "react";

type SectionCardProps = {
  eyebrow: string;
  title: string;
  children: ReactNode;
  rightSlot?: ReactNode;
  className?: string;
};

export function SectionCard({ eyebrow, title, children, rightSlot, className = "" }: SectionCardProps) {
  return (
    <section className={`panel rounded-[28px] p-6 ${className}`.trim()}>
      <div className="mb-5 flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
        <div>
          <p className="text-xs uppercase tracking-[0.32em] text-mist/45">{eyebrow}</p>
          <h2 className="mt-2 text-2xl font-semibold tracking-tight text-sand">{title}</h2>
        </div>
        {rightSlot}
      </div>
      {children}
    </section>
  );
}
