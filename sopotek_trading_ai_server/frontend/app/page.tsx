import { redirect } from "next/navigation";

import { readServerSession } from "@/lib/server-session";

export default async function HomePage() {
  redirect((await readServerSession()) ? "/dashboard" : "/login");
}
