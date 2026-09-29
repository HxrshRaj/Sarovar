import "./globals.css";
import Banner from "../components/Banner";

export const metadata = { title: "Sarovar Analyst Console", description: "Ask questions of a synthetic UPI-style lakehouse (local model)" };

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>
        <Banner />
        <main>{children}</main>
      </body>
    </html>
  );
}
