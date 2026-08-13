require('dotenv').config();
const http = require('http');
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const mysql = require('mysql2/promise');
const { createClient } = require('@supabase/supabase-js');
const { WebSocketServer, WebSocket } = require('ws');
const { Resend } = require('resend');

const PORT = process.env.PORT || 8081;
const ROOT = __dirname;
const DB_URL = process.env.DATABASE_URL || process.env.AIVEN_DB_URI;

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://ldtayomrkiutloswfnpl.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_SECRET_KEY || process.env.SUPABASE_PUBLISHABLE_KEY || process.env.SUPABASE_KEY || process.env.SUPABASE_ANON_KEY;

// --- RESEND TRANSACTIONAL NO-REPLY EMAIL ENGINE ---
const RESEND_API_KEY = process.env.RESEND_API_KEY || process.env.RESEND_KEY;
let resendClient = null;

if (RESEND_API_KEY && !RESEND_API_KEY.includes('re_123456789')) {
  resendClient = new Resend(RESEND_API_KEY);
  console.log('✅ Resend No-Reply Email Engine Initialized!');
}

// --- ASSET BUCKET CACHE (logo URLs loaded at startup, used in all emails) ---
// Maps local filename → public CDN URL once uploaded to Supabase Storage.
const cachedAssetUrls = {};

async function initAssetBucketCache() {
  if (!supabaseClient) return;

  const ASSETS = [
    { local: 'paykudi-logo.png',    storagePath: 'brand/paykudi-logo.png' },
    { local: 'paykudi-favicon.png', storagePath: 'brand/paykudi-favicon.png' },
  ];

  try {
    // Discover the first available bucket in the project.
    const { data: buckets, error: bucketsErr } = await supabaseClient.storage.listBuckets();
    if (bucketsErr || !buckets || buckets.length === 0) {
      console.warn('⚠️ [Asset Cache] No Supabase Storage buckets found — logo URLs will fall back to paykudi.co');
      return;
    }

    const bucket = buckets[0].name;
    console.log(`🪣 [Asset Cache] Using Supabase Storage bucket: "${bucket}"`);

    for (const asset of ASSETS) {
      const filePath = path.join(ROOT, asset.local);
      if (!fs.existsSync(filePath)) {
        console.warn(`⚠️ [Asset Cache] File not found locally: ${asset.local}`);
        continue;
      }

      const fileBuffer = fs.readFileSync(filePath);
      const contentType = asset.local.endsWith('.png') ? 'image/png' : 'image/jpeg';

      const { error: uploadErr } = await supabaseClient.storage
        .from(bucket)
        .upload(asset.storagePath, fileBuffer, {
          contentType,
          upsert: true,  // idempotent — re-uploads on every startup to keep fresh
          cacheControl: '31536000',  // 1-year CDN cache
        });

      if (uploadErr) {
        console.warn(`⚠️ [Asset Cache] Upload failed for ${asset.local}:`, uploadErr.message);
        continue;
      }

      const { data: urlData } = supabaseClient.storage
        .from(bucket)
        .getPublicUrl(asset.storagePath);

      if (urlData && urlData.publicUrl) {
        cachedAssetUrls[asset.local] = urlData.publicUrl;
        console.log(`✅ [Asset Cache] ${asset.local} → ${urlData.publicUrl}`);
      }
    }
  } catch (err) {
    console.error('❌ [Asset Cache] Startup asset push failed:', err.message);
  }
}

let dbClient = null;
let dbType = null; // 'mysql', 'pg', or 'supabase_sdk'
let dbConnected = false;
let supabaseClient = null;

// --- ZERO-EGRESS EVENT-DRIVEN RAM CACHE & HIGH-CONCURRENCY QUEUE ---
let cacheData = []; // Pure RAM storage loaded on startup and updated on submission

const submissionQueue = [];
let isFlushingQueue = false;

// IP Anti-Spam Rate Limiter
const ipRateMap = new Map();
const RATE_LIMIT_WINDOW_MS = 60 * 1000;
const MAX_SUBMITS_PER_IP = 25;

function isRateLimited(ip) {
  const now = Date.now();
  const userRate = ipRateMap.get(ip) || { count: 0, resetTime: now + RATE_LIMIT_WINDOW_MS };

  if (now > userRate.resetTime) {
    userRate.count = 1;
    userRate.resetTime = now + RATE_LIMIT_WINDOW_MS;
    ipRateMap.set(ip, userRate);
    return false;
  }

  userRate.count += 1;
  ipRateMap.set(ip, userRate);
  return userRate.count > MAX_SUBMITS_PER_IP;
}

// Resend No-Reply Thank You Email Dispatcher
async function sendThankYouEmail(toEmail) {
  if (!toEmail || toEmail === 'Anonymous User' || !toEmail.includes('@')) return;

  try {
    let emailHtml = fs.readFileSync(path.join(ROOT, 'thank-you-email.html'), 'utf8');
    const recipientName = toEmail.split('@')[0];
    emailHtml = emailHtml.replace(/\{\{recipient_name\}\}/g, recipientName);

    // Replace logo — handles both base64 data URI (template default) and filename src
    const logoUrl = cachedAssetUrls['paykudi-logo.png'] || 'https://paykudi.co/paykudi-logo.png';
    const outgoingEmailHtml = emailHtml
      .replace(/src="data:image\/png;base64,[^"]+"/g, `src="${logoUrl}"`)
      .replace(/src="paykudi-logo\.png"/g, `src="${logoUrl}"`);

    if (resendClient) {
      const response = await resendClient.emails.send({
        from: 'PayKudi <onboarding@resend.dev>',
        to: [toEmail],
        subject: 'Thank You for Your Feedback! - PayKudi',
        html: outgoingEmailHtml,
      });
      console.log(`✉️ [Resend No-Reply Email] Sent Thank You confirmation email to: ${toEmail}`, response);
    } else {
      console.log(`✉️ [Auto Email Prepared] Prepared computer Thank You email for: ${toEmail}.`);
    }
  } catch (err) {
    console.error('❌ Resend Email Dispatch Error:', err.message);
  }
}

// Website sign-up confirmation for waitlist, newsletter, community, and contact submissions.
async function sendWebsiteConfirmationEmail(toEmail, role, userMessage) {
  if (!toEmail || !toEmail.includes('@')) return;

  const WA_LINK = 'https://wa.me/2349000000000';
  const IG_LINK = 'https://www.instagram.com/usepaykudi?igsh=MXh5c3Z6NmJvNmVtOQ==';
  const SITE_LINK = 'https://paykudi.co';

  const confirmations = {
    'Update List': {
      subject: "You're on the list! - PayKudi",
      eyebrow: 'Early access confirmed',
      title: "You're on the list.",
      body: `<p style="margin:0 0 14px 0; font-family:Arial, Helvetica, sans-serif; font-weight:400; line-height:1.6; color:#2B2B2B; text-align:left;">No more guessing when PayKudi launches near you. You'll be one of the first to know, before it's public.</p><p style="margin:0; font-family:Arial, Helvetica, sans-serif; font-weight:400; line-height:1.6; color:#2B2B2B; text-align:left;">We're finishing the parts that matter most: making sure a payment stays locked in safely until a buyer confirms their order actually showed up. We'd rather take the time to get that right than rush it.</p>`,
      highlight_label: 'What you\'ll get first',
      highlight_text: 'Early access before the public launch, plus the occasional behind-the-scenes update. Never spam.',
      cta_label: 'Check us out',
      cta_href: SITE_LINK,
      cta_bg: 'linear-gradient(135deg, #0E5296 0%, #2EA043 100%)'
    },
    Newsletter: {
      subject: "You're subscribed! - PayKudi",
      eyebrow: 'Newsletter confirmed',
      title: "Good, you're in.",
      body: `<p style="margin:0 0 14px 0; font-family:Arial, Helvetica, sans-serif; font-weight:400; line-height:1.6; color:#2B2B2B; text-align:left;">Once a month, we'll send you real stories from the vendors, Instagram shoppers, and WhatsApp creators we're building PayKudi with, across Lagos, Abuja, Port Harcourt, and beyond.</p><p style="margin:0; font-family:Arial, Helvetica, sans-serif; font-weight:400; line-height:1.6; color:#2B2B2B; text-align:left;">No filler, no "10 tips" listicles. Just what's actually changing for people trying to buy and sell safely online.</p>`,
      highlight_label: 'One email a month, that\'s it',
      highlight_text: "We're building this with real sellers and buyers, so that's what you'll hear about. Not marketing noise.",
      cta_label: 'Check us out',
      cta_href: SITE_LINK,
      cta_bg: 'linear-gradient(135deg, #0E5296 0%, #2EA043 100%)'
    },
    Community: {
      subject: 'Welcome to the Community! - PayKudi',
      eyebrow: 'You\'re a founding member',
      title: 'Welcome to the fam. \uD83D\uDC9A',
      body: `<p style="margin:0 0 14px 0; font-family:Arial, Helvetica, sans-serif; font-weight:400; line-height:1.6; color:#2B2B2B; text-align:left;">You're now part of the small group helping shape PayKudi before anyone else sees it. Your feedback, frustrations, and "why doesn't it just do this" moments go directly into what we build next.</p><p style="margin:0; font-family:Arial, Helvetica, sans-serif; font-weight:400; line-height:1.6; color:#2B2B2B; text-align:left;">This isn't a mailing list you'll forget you joined. Expect real questions from us, early features to try first, and a direct line to the team.</p>`,
      highlight_label: 'As a founding member, you get',
      highlight_text: 'First access to new features, a say in what we prioritize, and a direct WhatsApp line to the team.',
      cta_label: 'Check us out',
      cta_href: SITE_LINK,
      cta_bg: 'linear-gradient(135deg, #0E5296 0%, #2EA043 100%)'
    },
    Contact: {
      subject: 'We received your message. - PayKudi',
      eyebrow: 'Message received',
      title: "Got it, we're on it.",
      body: `<p style="margin:0 0 14px 0; font-family:Arial, Helvetica, sans-serif; font-weight:400; line-height:1.6; color:#2B2B2B; text-align:left;">Your message just landed with our team, not a queue. We read every one of these ourselves.</p><p style="margin:0; font-family:Arial, Helvetica, sans-serif; font-weight:400; line-height:1.6; color:#2B2B2B; text-align:left;">We usually reply within one business day. If it's urgent, WhatsApp is genuinely the fastest way to reach us. A real person is on the other end.</p>`,
      highlight_label: 'What you told us',
      highlight_text: '{{userMessage}}',
      cta_label: 'Check us out',
      cta_href: SITE_LINK,
      cta_bg: 'linear-gradient(135deg, #0E5296 0%, #2EA043 100%)'
    }
  };

  const isContact = role && role.startsWith('Contact Inquiry');
  const conf = isContact ? confirmations.Contact : (confirmations[role] || confirmations.Community);

  // For contact, echo the user's message in the highlight block
  const safeMessage = userMessage ? String(userMessage).replace(/</g, '&lt;').replace(/>/g, '&gt;').slice(0, 500) : '(no message body)';
  const highlightText = isContact
    ? `"${safeMessage}"`
    : conf.highlight_text;

  // Contact Inquiry: show the echoed message block; other roles: remove it
  const userMessageBlock = ''; // slot kept empty — highlight block carries the message

  try {
    let emailHtml = fs.readFileSync(path.join(ROOT, 'website-signup-email.html'), 'utf8');
    const recipientName = toEmail.split('@')[0];
    emailHtml = emailHtml
      .replace(/\{\{recipient_name\}\}/g, recipientName)
      .replace(/\{\{confirmation_eyebrow\}\}/g, conf.eyebrow)
      .replace(/\{\{confirmation_title\}\}/g, conf.title)
      .replace(/\{\{confirmation_body\}\}/g, conf.body)
      .replace(/\{\{highlight_label\}\}/g, conf.highlight_label)
      .replace(/\{\{highlight_text\}\}/g, highlightText)
      .replace(/\{\{user_message_block\}\}/g, userMessageBlock)
      .replace(/\{\{cta_label\}\}/g, conf.cta_label)
      .replace(/\{\{cta_href\}\}/g, conf.cta_href)
      .replace(/\{\{cta_bg\}\}/g, conf.cta_bg);

    // Replace logo — handles both base64 data URI (template default) and filename src
    const logoUrl = cachedAssetUrls['paykudi-logo.png'] || 'https://paykudi.co/paykudi-logo.png';
    const outgoingEmailHtml = emailHtml
      .replace(/src="data:image\/png;base64,[^"]+"/g, `src="${logoUrl}"`)
      .replace(/src="paykudi-logo\.png"/g, `src="${logoUrl}"`);

    if (resendClient) {
      const response = await resendClient.emails.send({
        from: 'PayKudi <onboarding@resend.dev>',
        to: [toEmail],
        subject: conf.subject,
        html: outgoingEmailHtml,
      });
      console.log(`Website confirmation sent to ${toEmail}`, response);
    }
  } catch (err) {
    console.error('Website confirmation email error:', err.message);
  }
}

// Bulk Batch DB Insertion Engine with Automatic Retry Buffer
async function saveLocalBackup(batch) {
  try {
    const backupDir = path.join(ROOT, 'data');
    if (!fs.existsSync(backupDir)) {
      fs.mkdirSync(backupDir, { recursive: true });
    }
    const backupFile = path.join(backupDir, 'submissions_backup.json');
    let existing = [];
    if (fs.existsSync(backupFile)) {
      try {
        existing = JSON.parse(fs.readFileSync(backupFile, 'utf8') || '[]');
      } catch (e) {
        existing = [];
      }
    }
    batch.forEach(item => existing.push(item));
    fs.writeFileSync(backupFile, JSON.stringify(existing, null, 2), 'utf8');
  } catch (err) {
    console.error('⚠️ Local backup error:', err.message);
  }
}

async function flushSubmissionQueue() {
  if (submissionQueue.length === 0 || isFlushingQueue) return;
  isFlushingQueue = true;

  const batch = submissionQueue.splice(0, 50);
  let success = false;

  // Always append to local disk fail-safe backup
  saveLocalBackup(batch);

  if (supabaseClient) {
    const insertRows = batch.map(item => {
      const d = item.data;
      return {
        online_frequency: d.online_frequency || null,
        deal_platform: d.deal_platform || null,
        deal_value: d.deal_value || null,
        what_stopped: d.what_stopped || d.details || null,
        idea_feeling: d.idea_feeling || (d.role ? `Form: ${d.role}` : null),
        pay_stranger: d.pay_stranger || null,
        trust_amount: d.trust_amount || null,
        raise_dispute: d.raise_dispute || null,
        dispute_sla: d.dispute_sla || null,
        email: d.email || d.contact || null,
        country_code: d.country_code || '234',
        phone: d.phone || null,
        send_updates: d.send_updates === 'on' || d.send_updates === true || d.send_updates === 'true',
        raw_data: d
      };
    });

    let attempts = 0;
    const MAX_ATTEMPTS = 3;
    let lastErrorMsg = '';

    while (attempts < MAX_ATTEMPTS && !success) {
      attempts++;
      try {
        const { error } = await supabaseClient.from('survey_responses').insert(insertRows);
        if (error) {
          lastErrorMsg = error.message;
          const isTransient = error.message && (
            error.message.includes('fetch failed') ||
            error.message.includes('ECONNRESET') ||
            error.message.includes('ETIMEDOUT') ||
            error.message.includes('socket')
          );

          if (isTransient && attempts < MAX_ATTEMPTS) {
            // Wait 150ms and retry silently on a fresh connection
            await new Promise(r => setTimeout(r, 150 * attempts));
            continue;
          }
          console.error(`❌ Supabase API Insert Warning (Attempt ${attempts}/${MAX_ATTEMPTS}):`, error.message);
        } else {
          success = true;
          if (attempts > 1) {
            console.log(`⚡ [High-Concurrency Queue] Recovered after ${attempts} attempts & inserted ${batch.length} survey responses via Supabase API.`);
          } else {
            console.log(`⚡ [High-Concurrency Queue] Inserted ${batch.length} survey responses via Supabase API.`);
          }
        }
      } catch (err) {
        lastErrorMsg = err.message;
        const isTransient = err.message && (
          err.message.includes('fetch failed') ||
          err.message.includes('ECONNRESET') ||
          err.message.includes('ETIMEDOUT') ||
          err.message.includes('socket')
        );

        if (isTransient && attempts < MAX_ATTEMPTS) {
          await new Promise(r => setTimeout(r, 150 * attempts));
          continue;
        }
        console.error(`❌ Supabase Insert Exception (Attempt ${attempts}/${MAX_ATTEMPTS}):`, err.message);
      }
    }
  } else if (dbClient && dbConnected) {
    try {
      if (dbType === 'mysql') {
        const queryText = `
          INSERT INTO survey_responses (
            online_frequency, deal_platform, deal_value, what_stopped,
            idea_feeling, pay_stranger, trust_amount, raise_dispute,
            dispute_sla, email, country_code, phone, send_updates, raw_data
          ) VALUES ${batch.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)').join(', ')};
        `;
        const values = [];
        batch.forEach(item => {
          const d = item.data;
          values.push(
            d.online_frequency || null,
            d.deal_platform || null,
            d.deal_value || null,
            d.what_stopped || d.details || null,
            d.idea_feeling || (d.role ? `Form: ${d.role}` : null),
            d.pay_stranger || null,
            d.trust_amount || null,
            d.raise_dispute || null,
            d.dispute_sla || null,
            d.email || d.contact || null,
            d.country_code || '234',
            d.phone || null,
            d.send_updates === 'on' || d.send_updates === true || d.send_updates === 'true' ? 1 : 0,
            JSON.stringify(d)
          );
        });
        await dbClient.query(queryText, values);
        success = true;
        console.log(`⚡ [High-Concurrency Queue] Bulk inserted ${batch.length} responses into MySQL DB.`);
      } else if (dbType === 'pg') {
        const valueStrings = [];
        const values = [];
        let paramIdx = 1;

        batch.forEach(item => {
          const d = item.data;
          valueStrings.push(`($${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++})`);
          values.push(
            d.online_frequency || null,
            d.deal_platform || null,
            d.deal_value || null,
            d.what_stopped || d.details || null,
            d.idea_feeling || (d.role ? `Form: ${d.role}` : null),
            d.pay_stranger || null,
            d.trust_amount || null,
            d.raise_dispute || null,
            d.dispute_sla || null,
            d.email || d.contact || null,
            d.country_code || '234',
            d.phone || null,
            d.send_updates === 'on' || d.send_updates === true || d.send_updates === 'true',
            JSON.stringify(d)
          );
        });

        const queryText = `
          INSERT INTO survey_responses (
            online_frequency, deal_platform, deal_value, what_stopped,
            idea_feeling, pay_stranger, trust_amount, raise_dispute,
            dispute_sla, email, country_code, phone, send_updates, raw_data
          ) VALUES ${valueStrings.join(', ')};
        `;
        await dbClient.query(queryText, values);
        success = true;
        console.log(`⚡ [High-Concurrency Queue] Bulk inserted ${batch.length} responses into PostgreSQL DB.`);
      }
    } catch (err) {
      console.error('❌ Batch SQL Insert Error (Will retry automatically):', err.message);
    }
  }

  // Fail-Safe Buffer
  if (!success && (supabaseClient || (dbClient && dbConnected))) {
    submissionQueue.unshift(...batch);
  }

  isFlushingQueue = false;
  if (submissionQueue.length > 0 && success) {
    setImmediate(flushSubmissionQueue);
  }
}

// Auto-flush queue every 300ms
setInterval(flushSubmissionQueue, 300);

// Load all DB records into RAM cache ONCE on server startup
async function initStartupRAMCache() {
  if (supabaseClient) {
    try {
      const { data, error } = await supabaseClient
        .from('survey_responses')
        .select('*')
        .order('created_at', { ascending: false })
        .limit(1000);

      if (!error && data) {
        cacheData = data.map(item => ({
          id: item.id,
          timestamp: item.created_at || new Date().toISOString(),
          email: item.email || 'Anonymous User',
          phone: item.country_code ? `+${item.country_code} ${item.phone}` : (item.phone || 'N/A'),
          online_frequency: item.online_frequency || 'N/A',
          deal_platform: item.deal_platform || 'N/A',
          deal_value: item.deal_value || 'N/A',
          what_stopped: item.what_stopped || 'No friction feedback noted',
          idea_feeling: item.idea_feeling || 'Excited to try it',
          pay_stranger: item.pay_stranger || 'Yes',
          trust_amount: item.trust_amount || 'Under ₦10,000',
          raise_dispute: item.raise_dispute || 'Yes',
          dispute_sla: item.dispute_sla || 'Same day',
          send_updates: item.send_updates === true || item.send_updates === 1
        }));
        console.log(`🚀 [Zero-Egress RAM Cache] Loaded ${cacheData.length} responses into RAM on startup. Zero DB queries for GET requests!`);
      }
    } catch (err) {
      console.error('Supabase Startup Cache Init Error:', err.message);
    }
  } else if (dbClient && dbConnected) {
    try {
      let rows = [];
      if (dbType === 'mysql') {
        const [result] = await dbClient.query('SELECT * FROM survey_responses ORDER BY created_at DESC LIMIT 1000;');
        rows = result;
      } else if (dbType === 'pg') {
        const result = await dbClient.query('SELECT * FROM survey_responses ORDER BY created_at DESC LIMIT 1000;');
        rows = result.rows;
      }
      cacheData = rows.map(item => ({
        id: item.id,
        timestamp: item.created_at || new Date().toISOString(),
        email: item.email || 'Anonymous User',
        phone: item.country_code ? `+${item.country_code} ${item.phone}` : (item.phone || 'N/A'),
        online_frequency: item.online_frequency || 'N/A',
        deal_platform: item.deal_platform || 'N/A',
        deal_value: item.deal_value || 'N/A',
        what_stopped: item.what_stopped || 'No friction feedback noted',
        idea_feeling: item.idea_feeling || 'Excited to try it',
        pay_stranger: item.pay_stranger || 'Yes',
        trust_amount: item.trust_amount || 'Under ₦10,000',
        raise_dispute: item.raise_dispute || 'Yes',
        dispute_sla: item.dispute_sla || 'Same day',
        send_updates: item.send_updates === 1 || item.send_updates === true || item.send_updates === 'true'
      }));
      console.log(`🚀 [Zero-Egress RAM Cache] Loaded ${cacheData.length} responses into RAM on startup. Zero DB queries for GET requests!`);
    } catch (err) {
      console.error('DB Startup Cache Init Error:', err.message);
    }
  }
}

// Auto-detect connection method
async function initDatabase() {
  if (SUPABASE_URL && SUPABASE_KEY && !SUPABASE_KEY.includes('YOUR_ANON_KEY')) {
    try {
      supabaseClient = createClient(SUPABASE_URL, SUPABASE_KEY);
      dbConnected = true;
      dbType = 'supabase_sdk';
      console.log('✅ Connected to Supabase via Project URL & Key!');
      await initStartupRAMCache();
      await initAssetBucketCache(); // push brand assets to storage bucket after DB is ready
      return;
    } catch (e) {
      console.warn('⚠️ Supabase SDK init warning:', e.message);
    }
  }

  if (!DB_URL || DB_URL.includes('YOUR-PASSWORD') || DB_URL.includes('your-aiven-db-host')) {
    console.log('ℹ️  No database credentials set yet in .env');
    return;
  }

  try {
    if (DB_URL.startsWith('mysql://')) {
      dbType = 'mysql';
      const cleanUri = DB_URL.split('?')[0];
      dbClient = mysql.createPool({
        uri: cleanUri,
        ssl: { rejectUnauthorized: false },
        connectionLimit: 25,
        queueLimit: 1000
      });

      const initDbQuery = `
        CREATE TABLE IF NOT EXISTS survey_responses (
          id INT AUTO_INCREMENT PRIMARY KEY,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          online_frequency VARCHAR(100),
          deal_platform VARCHAR(100),
          deal_value VARCHAR(100),
          what_stopped TEXT,
          idea_feeling VARCHAR(100),
          pay_stranger VARCHAR(50),
          trust_amount VARCHAR(100),
          raise_dispute VARCHAR(50),
          dispute_sla VARCHAR(100),
          email VARCHAR(255),
          country_code VARCHAR(10),
          phone VARCHAR(50),
          send_updates BOOLEAN DEFAULT FALSE,
          raw_data JSON
        );
      `;
      await dbClient.query(initDbQuery);
      dbConnected = true;
      console.log('✅ Connected to MySQL DB & initialized "survey_responses" table.');
      await initStartupRAMCache();

    } else {
      dbType = 'pg';
      dbClient = new Pool({
        connectionString: DB_URL,
        ssl: { rejectUnauthorized: false },
        max: 25,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 5000
      });

      const initDbQuery = `
        CREATE TABLE IF NOT EXISTS survey_responses (
          id SERIAL PRIMARY KEY,
          created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
          online_frequency VARCHAR(100),
          deal_platform VARCHAR(100),
          deal_value VARCHAR(100),
          what_stopped TEXT,
          idea_feeling VARCHAR(100),
          pay_stranger VARCHAR(50),
          trust_amount VARCHAR(100),
          raise_dispute VARCHAR(50),
          dispute_sla VARCHAR(100),
          email VARCHAR(255),
          country_code VARCHAR(10),
          phone VARCHAR(50),
          send_updates BOOLEAN DEFAULT FALSE,
          raw_data JSONB
        );
      `;
      await dbClient.query(initDbQuery);
      dbConnected = true;
      console.log('✅ Connected to PostgreSQL DB & initialized "survey_responses" table.');
      await initStartupRAMCache();
    }
  } catch (err) {
    console.warn('⚠️ DB Connection Warning:', err.message);
  }
}

initDatabase();

const MIME = {
  '.html': 'text/html',
  '.css': 'text/css',
  '.js': 'application/javascript',
  '.json': 'application/json',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.ico': 'image/x-icon',
};

const server = http.createServer(async (req, res) => {
  // OWASP Security & CORS Headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'SAMEORIGIN');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');

  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  // API Endpoint: High-Concurrency Submit Handler
  if (req.method === 'POST' && req.url === '/api/submit') {
    const clientIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress || '127.0.0.1';

    if (isRateLimited(clientIp)) {
      res.writeHead(429, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: false, error: 'Too many requests. Please wait a moment.' }));
      return;
    }

    let body = '';
    let bodyLength = 0;
    const MAX_PAYLOAD_BYTES = 100 * 1024;

    req.on('data', chunk => {
      bodyLength += chunk.length;
      if (bodyLength > MAX_PAYLOAD_BYTES) {
        res.writeHead(413, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, error: 'Payload size limit exceeded' }));
        req.destroy();
        return;
      }
      body += chunk.toString();
    });

    req.on('end', async () => {
      try {
        const data = JSON.parse(body || '{}');
        const timestamp = new Date().toISOString();
        const recordId = 'rec-' + Date.now() + '-' + Math.floor(Math.random() * 1000);

        const userEmail = data.email || data.contact || 'Anonymous User';
        const formattedRecord = {
          id: recordId,
          timestamp: timestamp,
          email: userEmail,
          phone: data.country_code ? `+${data.country_code} ${data.phone || ''}` : (data.phone || 'N/A'),
          online_frequency: data.online_frequency || 'N/A',
          deal_platform: data.deal_platform || 'N/A',
          deal_value: data.deal_value || 'N/A',
          what_stopped: data.what_stopped || data.details || 'No friction feedback noted',
          idea_feeling: data.idea_feeling || (data.role ? `Form: ${data.role}` : 'Excited to try it'),
          pay_stranger: data.pay_stranger || 'N/A',
          trust_amount: data.trust_amount || 'N/A',
          raise_dispute: data.raise_dispute || 'N/A',
          dispute_sla: data.dispute_sla || 'N/A',
          send_updates: data.send_updates === 'on' || data.send_updates === true
        };

        // 1. Queue for DB Bulk Batch Insertion
        submissionQueue.push({ data, timestamp });

        // 2. Prepend directly to RAM cache & broadcast via WebSockets
        broadcastNewResponse(formattedRecord);

        // 3. Send the appropriate email confirmation when an email address was submitted.
        if (userEmail && userEmail !== 'Anonymous User' && userEmail.includes('@')) {
          if (data.role) {
            sendWebsiteConfirmationEmail(userEmail.trim(), data.role, data.details);
          } else {
            // Always send the survey thank-you first.
            sendThankYouEmail(userEmail.trim());

            // If the respondent opted in to community updates, also send the
            // Community welcome email — staggered by 3 s so it arrives as a
            // separate email rather than colliding with the thank-you.
            const optedIn = data.send_updates === 'on' || data.send_updates === true || data.send_updates === 'true';
            if (optedIn) {
              setTimeout(() => {
                sendWebsiteConfirmationEmail(userEmail.trim(), 'Community');
              }, 3000);
            }
          }
        }

        // 4. Return HTTP 200 OK immediately (< 5ms response)
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ success: true, dbQueued: true, emailQueued: !!data.email, id: recordId, timestamp }));
      } catch (err) {
        console.error('API Error saving response:', err);
        res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ success: false, error: err.message }));
      }
    });
    return;
  }

  // API Endpoint: View Stored Responses
  if (req.method === 'GET' && req.url === '/api/responses') {
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ success: true, cached: true, count: cacheData.length, data: cacheData }));
    return;
  }

  // Serve static files (with /admin, /survey, /thank-you, and /thank-you-email shortcuts)
  let rawUrl = decodeURIComponent(req.url).split('?')[0].toLowerCase();
  let requestPath = 'index.html';
  if (rawUrl === '/' || rawUrl === '/index.html' || rawUrl === '/index') {
    requestPath = 'index.html';
  } else if (rawUrl === '/survey' || rawUrl === '/surveyform' || rawUrl === '/surveyform.html') {
    requestPath = 'Surveyform.html';
  } else if (rawUrl === '/admin' || rawUrl === '/admin.html') {
    requestPath = 'admin.html';
  } else if (rawUrl === '/thank-you' || rawUrl === '/thank-you.html') {
    requestPath = 'index.html';
  } else if (rawUrl === '/thank-you-email' || rawUrl === '/thank-you-email.html') {
    requestPath = 'thank-you-email.html';
  } else {
    requestPath = decodeURIComponent(req.url).split('?')[0].replace(/^\//, '');
  }
  let filePath = path.join(ROOT, requestPath);
  filePath = filePath.split('?')[0];

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end('Not found: ' + req.url);
      return;
    }
    const ext = path.extname(filePath).toLowerCase();
    res.writeHead(200, {
      'Content-Type': MIME[ext] || 'text/html',
      'Cache-Control': 'no-cache',
    });
    res.end(data);
  });
});

server.keepAliveTimeout = 65000;
server.headersTimeout = 66000;

// WebSocket Real-Time Engine
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (request, socket, head) => {
  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit('connection', ws, request);
  });
});

wss.on('connection', (ws) => {
  console.log(`⚡ Admin WebSocket connected (Active Clients: ${wss.clients.size})`);
  ws.send(JSON.stringify({ type: 'INIT_DATA', count: cacheData.length, data: cacheData }));

  ws.on('close', () => {
    console.log(`🔌 Admin WebSocket disconnected (Active Clients: ${wss.clients.size})`);
  });
});

function broadcastNewResponse(newRecord) {
  cacheData.unshift(newRecord);

  const payload = JSON.stringify({
    type: 'NEW_RESPONSE',
    record: newRecord,
    totalCount: cacheData.length
  });

  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });
}

server.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    console.error(`\n❌ Error: Port ${PORT} is already in use by another process.`);
    console.error(`👉 Run 'npm run stop' or kill the process using port ${PORT} and try 'npm start' again.\n`);
    process.exit(1);
  } else {
    console.error('❌ Server error:', err);
  }
});

server.listen(PORT, () => {
  console.log(`\n🚀 PayKudi Server running at: http://localhost:${PORT}`);
  console.log(`⚡ Zero-Egress Event-Driven RAM Cache, Resend No-Reply Email & WebSockets Active on port ${PORT}\n`);
});
