import type { Metadata } from "next";
import type { ReactNode } from "react";

import { AppShell } from "@/components/layout/app-shell";
import "./globals.css";

export const metadata: Metadata = {
  title: "Sopotek Trading AI Platform",
  description: "Professional multi-user trading control plane backed by FastAPI, Kafka, and realtime market streams."
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body className="font-[var(--font-display)] text-mist">
        <AppShell>{children}</AppShell>
      </body>
    </html>
  );
}
