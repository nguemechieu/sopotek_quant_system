import { AuthPage } from "@/components/auth/auth-page";
import { redirectIfAuthenticated } from "@/lib/server-session";

export default async function RegisterPage() {
  await redirectIfAuthenticated();
  return <AuthPage mode="register" />;
}
