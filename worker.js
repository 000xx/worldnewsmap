/**
 * World News Map — Groq Proxy Worker
 * Deployed on Cloudflare Workers (free tier: 100k requests/day)
 *
 * This sits between the browser and Groq's API so the API key
 * never touches the client. The key is stored as a Worker secret.
 *
 * Setup:
 *   1. npx wrangler init newsmap-groq-proxy
 *   2. Replace the generated worker.js with this file
 *   3. npx wrangler secret put GROQ_API_KEY
 *   4. npx wrangler deploy
 *   5. Set your Worker URL in index.html GROQ_PROXY_URL
 */

const ALLOWED_ORIGINS = [
  'https://YOUR_GITHUB_USERNAME.github.io',
  'http://localhost:3000',
  'http://localhost:8080',
  'http://127.0.0.1:5500',
];

const GROQ_API_URL = 'https://api.groq.com/openai/v1/chat/completions';
const ALLOWED_MODEL = 'llama-3.3-70b-versatile';
const MAX_TOKENS = 60;

export default {
  async fetch(request, env) {
    // CORS preflight
    if (request.method === 'OPTIONS') {
      return handleCORS(request);
    }

    // Only POST
    if (request.method !== 'POST') {
      return new Response('Method not allowed', { status: 405 });
    }

    // Origin check
    const origin = request.headers.get('Origin') || '';
    const originAllowed = ALLOWED_ORIGINS.some(o => origin.startsWith(o));
    if (!originAllowed && origin !== '') {
      return new Response('Forbidden', { status: 403 });
    }

    // Parse request
    let body;
    try {
      body = await request.json();
    } catch {
      return new Response('Invalid JSON', { status: 400 });
    }

    // Validate — only allow our specific use case
    if (!body.messages || !Array.isArray(body.messages)) {
      return new Response('Invalid request', { status: 400 });
    }

    // Force safe parameters — prevent abuse
    const groqBody = {
      model: ALLOWED_MODEL,
      messages: body.messages.slice(0, 2), // max 2 messages (system + user)
      max_tokens: MAX_TOKENS,
      temperature: 0.3,
    };

    // Forward to Groq
    const groqResp = await fetch(GROQ_API_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${env.GROQ_API_KEY}`,
      },
      body: JSON.stringify(groqBody),
    });

    // Forward response (including rate limit headers)
    const respHeaders = new Headers({
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': originAllowed ? origin : ALLOWED_ORIGINS[0],
      'Access-Control-Allow-Methods': 'POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    });

    // Pass through rate limit info so the frontend can handle it
    const retryAfter = groqResp.headers.get('retry-after');
    if (retryAfter) respHeaders.set('Retry-After', retryAfter);

    const respBody = await groqResp.text();
    return new Response(respBody, {
      status: groqResp.status,
      headers: respHeaders,
    });
  },
};

function handleCORS(request) {
  const origin = request.headers.get('Origin') || '';
  const originAllowed = ALLOWED_ORIGINS.some(o => origin.startsWith(o));
  return new Response(null, {
    status: 204,
    headers: {
      'Access-Control-Allow-Origin': originAllowed ? origin : ALLOWED_ORIGINS[0],
      'Access-Control-Allow-Methods': 'POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
      'Access-Control-Max-Age': '86400',
    },
  });
}
