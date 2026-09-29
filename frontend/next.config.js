// /api/* is proxied to the FastAPI service. API_URL is read at build time (docker: http://api:8000).
module.exports = {
  output: "standalone",
  async rewrites() {
    return [{ source: "/api/:path*", destination: `${process.env.API_URL || "http://localhost:8000"}/api/:path*` }];
  },
};
