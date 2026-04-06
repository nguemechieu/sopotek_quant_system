"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const items = [
  { href: "/dashboard", label: "Control Panel" },
  { href: "/market", label: "Market View" },
  { href: "/strategies", label: "Strategies" },
  { href: "/orders", label: "Orders & Trades" },
  { href: "/risk", label: "Risk" }
];

export function Navigation() {
  const pathname = usePathname();

  return (
    <nav className="flex flex-wrap gap-2">
      {items.map((item) => {
        const active = pathname === item.href;
        return (
          <Link
            key={item.href}
            href={item.href}
            className={`rounded-full border px-4 py-2 text-sm transition ${
              active
                ? "border-amber-400/70 bg-amber-400/12 text-sand"
                : "border-white/10 bg-white/5 text-mist/75 hover:border-white/20 hover:text-mist"
            }`}
          >
            {item.label}
          </Link>
        );
      })}
    </nav>
  );
}
