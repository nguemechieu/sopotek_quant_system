import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{ts,tsx}",
    "./components/**/*.{ts,tsx}",
    "./lib/**/*.{ts,tsx}"
  ],
  theme: {
    extend: {
      colors: {
        ink: "#09111a",
        steel: "#122130",
        panel: "#0f1c28",
        line: "#294357",
        mist: "#d8e2ec",
        sand: "#f3dfc1",
        ember: "#f97316",
        lime: "#7ee787",
        rose: "#fb7185"
      },
      boxShadow: {
        glass: "0 24px 70px rgba(3, 10, 18, 0.38)"
      },
      backgroundImage: {
        grain: "radial-gradient(circle at top, rgba(249,115,22,0.14), transparent 36%), radial-gradient(circle at 80% 20%, rgba(126,231,135,0.10), transparent 26%), linear-gradient(180deg, #0b1620 0%, #09111a 48%, #060c12 100%)"
      }
    }
  },
  plugins: []
};

export default config;
