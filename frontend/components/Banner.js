import Link from "next/link";

export default function Banner() {
  return (
    <header className="banner">
      <nav>
        <Link href="/">Ask</Link>
        <Link href="/health">Pipeline health</Link>
        <Link href="/catalog">Catalog</Link>
      </nav>
      <div className="tags">
        <span className="tag warn">Synthetic data</span>
        <span className="tag warn">Local model (Ollama · llama3.2:3b)</span>
        <span className="tag">Not production</span>
      </div>
    </header>
  );
}
