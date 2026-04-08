// ============================================
// Prom3D + Mercato AI Ads Agent — Backend v5
// ============================================

const express = require('express');
const cors = require('cors');
const { GoogleAdsApi } = require('google-ads-api');
const { BetaAnalyticsDataClient } = require('@google-analytics/data');

const app = express();
app.use(cors());
app.use(express.json());

const CONFIG = {
  ADS_DEVELOPER_TOKEN:     'uMvPr7R4QQvCjYfGV25gcQ',
  ADS_CLIENT_ID:           '546482431037-pnc30oc3o04npp3k4oo1ifpf3is788c4.apps.googleusercontent.com',
  ADS_CLIENT_SECRET:       process.env.ADS_CLIENT_SECRET || '',
  ADS_REFRESH_TOKEN:       process.env.ADS_REFRESH_TOKEN || '',
  ADS_MCC_ID:              '5522488607',
  ADS_CUSTOMER_ID:         '2420492760',
  GA4_PROPERTY_ID:         '503012124',
  ADS_CUSTOMER_ID_MERCATO: '4433061490',
  GA4_PROPERTY_ID_MERCATO: '286553038',
};

const adsClient = new GoogleAdsApi({
  client_id:        CONFIG.ADS_CLIENT_ID,
  client_secret:    CONFIG.ADS_CLIENT_SECRET,
  developer_token:  CONFIG.ADS_DEVELOPER_TOKEN,
});

function makeCustomer(customerId) {
  return adsClient.Customer({
    customer_id:       customerId,
    login_customer_id: CONFIG.ADS_MCC_ID,
    refresh_token:     CONFIG.ADS_REFRESH_TOKEN,
  });
}

const ga4Credentials = {
  type:            'service_account',
  project_id:      process.env.GA4_PROJECT_ID      || 'prom3d-agent',
  private_key_id:  process.env.GA4_PRIVATE_KEY_ID  || '',
  private_key:     (process.env.GA4_PRIVATE_KEY || '').replace(/\\n/g, '\n'),
  client_email:    process.env.GA4_CLIENT_EMAIL     || '',
  client_id:       process.env.GA4_CLIENT_ID        || '',
  auth_uri:        'https://accounts.google.com/o/oauth2/auth',
  token_uri:       'https://oauth2.googleapis.com/token',
};

const ga4Client = new BetaAnalyticsDataClient({ credentials: ga4Credentials });

function parsePeriod(query) {
  const p = query.period || 'week';
  const today = new Date();
  const fmt = d => d.toISOString().split('T')[0];
  if (p === 'today')      return { startDate: 'today',     endDate: 'today' };
  if (p === 'yesterday')  return { startDate: 'yesterday', endDate: 'yesterday' };
  if (p === 'week')       return { startDate: '7daysAgo',  endDate: 'today' };
  if (p === 'month')      return { startDate: '30daysAgo', endDate: 'today' };
  if (p === 'last_month') {
    const first = new Date(today.getFullYear(), today.getMonth() - 1, 1);
    const last  = new Date(today.getFullYear(), today.getMonth(), 0);
    return { startDate: fmt(first), endDate: fmt(last) };
  }
  if (p === 'custom' && query.startDate && query.endDate)
    return { startDate: query.startDate, endDate: query.endDate };
  return { startDate: '7daysAgo', endDate: 'today' };
}

async function ga4Report(propertyId, metrics, dimensions, orderBys, limit, dateRange) {
  const range = dateRange || { startDate: '7daysAgo', endDate: 'today' };
  const params = { property: `properties/${propertyId}`, dateRanges: [range], metrics, dimensions };
  if (orderBys) params.orderBys = orderBys;
  if (limit)    params.limit    = limit;
  const [response] = await ga4Client.runReport(params);
  return response.rows || [];
}

// ── ROUTES ──────────────────────────────────────────────────

app.get('/', (req, res) => res.json({
  status: 'ok', message: 'Prom3D + Mercato Ads Agent Server running v5',
  shops: ['prom3d', 'mercato'],
}));

app.get('/debug', (req, res) => res.json({
  has_refresh_token:  !!process.env.ADS_REFRESH_TOKEN,
  has_client_secret:  !!process.env.ADS_CLIENT_SECRET,
  has_client_email:   !!process.env.GA4_CLIENT_EMAIL,
  has_private_key:    !!process.env.GA4_PRIVATE_KEY,
  client_email:       process.env.GA4_CLIENT_EMAIL || 'NOT SET',
  mcc_id:             CONFIG.ADS_MCC_ID,
}));

// ── GA4 Mercato ──────────────────────────────────────────────
app.get('/api/ga4/mercato', async (req, res) => {
  try {
    const dr = parsePeriod(req.query);
    const pid = CONFIG.GA4_PROPERTY_ID_MERCATO;
    const [ch, pg, tot] = await Promise.all([
      ga4Report(pid, [{name:'sessions'},{name:'activeUsers'},{name:'bounceRate'},{name:'averageSessionDuration'},{name:'conversions'},{name:'screenPageViewsPerSession'}], [{name:'sessionDefaultChannelGrouping'}], [{metric:{metricName:'sessions'},desc:true}], 10, dr),
      ga4Report(pid, [{name:'screenPageViews'},{name:'bounceRate'},{name:'averageSessionDuration'},{name:'conversions'}], [{name:'pagePath'}], [{metric:{metricName:'screenPageViews'},desc:true}], 10, dr),
      ga4Report(pid, [{name:'sessions'},{name:'totalUsers'},{name:'newUsers'},{name:'bounceRate'},{name:'averageSessionDuration'},{name:'screenPageViewsPerSession'},{name:'ecommercePurchases'},{name:'purchaseRevenue'},{name:'conversions'}], [], null, null, dr),
    ]);
    const t = tot[0]?.metricValues || [];
    const v = i => parseFloat(t[i]?.value || '0');
    res.json({
      success:true, shop:'mercato', period:dr,
      sessions:Math.round(v(0)), users:Math.round(v(1)), newUsers:Math.round(v(2)),
      bounceRate:parseFloat((v(3)*100).toFixed(1)), avgDuration:Math.round(v(4)),
      pagesPerSession:parseFloat(v(5).toFixed(1)), transactions:Math.round(v(6)),
      revenue:parseFloat(v(7).toFixed(0)), conversions:Math.round(v(8)),
      channelRows: ch.map(r=>({channel:r.dimensionValues[0].value, sessions:parseInt(r.metricValues[0].value), users:parseInt(r.metricValues[1].value), bounceRate:parseFloat((parseFloat(r.metricValues[2].value)*100).toFixed(1)), avgDuration:Math.round(parseFloat(r.metricValues[3].value)), conversions:parseInt(r.metricValues[4].value), pagesPerSession:parseFloat(parseFloat(r.metricValues[5].value).toFixed(1))})),
      topPages: pg.map(r=>({page:r.dimensionValues[0].value, views:parseInt(r.metricValues[0].value), bounceRate:parseFloat((parseFloat(r.metricValues[1].value)*100).toFixed(1)), avgDuration:Math.round(parseFloat(r.metricValues[2].value)), conversions:parseInt(r.metricValues[3].value)})),
    });
  } catch(err) { res.status(500).json({success:false, error:err.message}); }
});

// ── GA4 Prom3D ──────────────────────────────────────────────
app.get('/api/ga4/prom3d', async (req, res) => {
  try {
    const dr = parsePeriod(req.query);
    const pid = CONFIG.GA4_PROPERTY_ID;
    const [ch, pg, tot, ev] = await Promise.all([
      ga4Report(pid, [{name:'sessions'},{name:'activeUsers'},{name:'bounceRate'},{name:'averageSessionDuration'},{name:'conversions'},{name:'screenPageViewsPerSession'}], [{name:'sessionDefaultChannelGrouping'}], [{metric:{metricName:'sessions'},desc:true}], 10, dr),
      ga4Report(pid, [{name:'screenPageViews'},{name:'bounceRate'},{name:'averageSessionDuration'},{name:'conversions'}], [{name:'pagePath'}], [{metric:{metricName:'screenPageViews'},desc:true}], 10, dr),
      ga4Report(pid, [{name:'sessions'},{name:'totalUsers'},{name:'newUsers'},{name:'bounceRate'},{name:'averageSessionDuration'},{name:'screenPageViewsPerSession'},{name:'conversions'}], [], null, null, dr),
      ga4Report(pid, [{name:'eventCount'},{name:'conversions'}], [{name:'eventName'}], [{metric:{metricName:'eventCount'},desc:true}], 20, dr),
    ]);
    const t = tot[0]?.metricValues || [];
    const v = i => parseFloat(t[i]?.value || '0');
    const conversionEvents = ev.map(r=>({eventName:r.dimensionValues[0].value, eventCount:parseInt(r.metricValues[0].value), conversions:parseInt(r.metricValues[1].value)})).filter(e=>e.conversions>0||['generate_lead','form_submit','click','contact'].some(k=>e.eventName.includes(k)));
    res.json({
      success:true, shop:'prom3d', period:dr,
      sessions:Math.round(v(0)), users:Math.round(v(1)), newUsers:Math.round(v(2)),
      bounceRate:parseFloat((v(3)*100).toFixed(1)), avgDuration:Math.round(v(4)),
      pagesPerSession:parseFloat(v(5).toFixed(1)), conversions:Math.round(v(6)),
      conversionEvents,
      channelRows: ch.map(r=>({channel:r.dimensionValues[0].value, sessions:parseInt(r.metricValues[0].value), users:parseInt(r.metricValues[1].value), bounceRate:parseFloat((parseFloat(r.metricValues[2].value)*100).toFixed(1)), avgDuration:Math.round(parseFloat(r.metricValues[3].value)), conversions:parseInt(r.metricValues[4].value), pagesPerSession:parseFloat(parseFloat(r.metricValues[5].value).toFixed(1))})),
      topPages: pg.map(r=>({page:r.dimensionValues[0].value, views:parseInt(r.metricValues[0].value), bounceRate:parseFloat((parseFloat(r.metricValues[1].value)*100).toFixed(1)), avgDuration:Math.round(parseFloat(r.metricValues[2].value)), conversions:parseInt(r.metricValues[3].value)})),
    });
  } catch(err) { res.status(500).json({success:false, error:err.message}); }
});

// ── Prom3D Ads ──────────────────────────────────────────────
app.get('/api/ads/metrics', async (req, res) => {
  try {
    const c = makeCustomer(CONFIG.ADS_CUSTOMER_ID);
    const rows = await c.query(`SELECT campaign.id,campaign.name,campaign.status,metrics.impressions,metrics.clicks,metrics.ctr,metrics.conversions,metrics.cost_micros,metrics.average_cpc FROM campaign WHERE segments.date DURING LAST_30_DAYS ORDER BY metrics.impressions DESC LIMIT 10`);
    res.json({success:true, shop:'prom3d', data:rows.map(r=>({id:r.campaign.id, name:r.campaign.name, status:r.campaign.status, impressions:r.metrics.impressions, clicks:r.metrics.clicks, ctr:(r.metrics.ctr*100).toFixed(2), conversions:r.metrics.conversions, cost:(r.metrics.cost_micros/1e6).toFixed(2), avgCpc:(r.metrics.average_cpc/1e6).toFixed(2)}))});
  } catch(err) { res.status(500).json({success:false, error:err.message}); }
});

app.get('/api/ads/search-terms', async (req, res) => {
  try {
    const c = makeCustomer(CONFIG.ADS_CUSTOMER_ID);
    const rows = await c.query(`SELECT search_term_view.search_term,metrics.impressions,metrics.clicks,metrics.conversions,metrics.cost_micros FROM search_term_view WHERE segments.date DURING LAST_30_DAYS AND metrics.impressions>5 ORDER BY metrics.impressions DESC LIMIT 50`);
    const minus = ['безкоштовно','free','diy','скачати','thingiverse','stl','курс','навчання'];
    res.json({success:true, shop:'prom3d', data:rows.map(r=>({term:r.search_term_view.search_term, impressions:r.metrics.impressions, clicks:r.metrics.clicks, conversions:r.metrics.conversions, cost:(r.metrics.cost_micros/1e6).toFixed(2), isSuggestedMinus:minus.some(m=>r.search_term_view.search_term.toLowerCase().includes(m))}))});
  } catch(err) { res.status(500).json({success:false, error:err.message}); }
});


app.post('/api/ads/enable-campaign', async (req, res) => {
  try {
    const c = makeCustomer(CONFIG.ADS_CUSTOMER_ID);
    await c.campaigns.update([{resource_name:`customers/${CONFIG.ADS_CUSTOMER_ID}/campaigns/${req.body.campaignId}`,status:'ENABLED'}]);
    res.json({success:true, message:'Кампанію запущено'});
  } catch(err) { res.status(500).json({success:false, error:err.message}); }
});
app.post('/api/ads/pause-campaign', async (req, res) => {
  try {
    const c = makeCustomer(CONFIG.ADS_CUSTOMER_ID);
    await c.campaigns.update([{resource_name:`customers/${CONFIG.ADS_CUSTOMER_ID}/campaigns/${req.body.campaignId}`,status:'PAUSED'}]);
    res.json({success:true, message:`Кампанія призупинена`});
  } catch(err) { res.status(500).json({success:false, error:err.message}); }
});

app.post('/api/ads/add-negative-keywords', async (req, res) => {
  try {
    const c = makeCustomer(CONFIG.ADS_CUSTOMER_ID);
    await c.campaignCriteria.create(req.body.keywords.map(kw=>({campaign:`customers/${CONFIG.ADS_CUSTOMER_ID}/campaigns/${req.body.campaignId}`,keyword:{text:kw,match_type:'BROAD'}})));
    res.json({success:true, message:`Додано ${req.body.keywords.length} мінус-слів`});
  } catch(err) { res.status(500).json({success:false, error:err.message}); }
});

// ── Mercato Ads ──────────────────────────────────────────────
app.get('/api/mercato/ads/metrics', async (req, res) => {
  try {
    const c = makeCustomer(CONFIG.ADS_CUSTOMER_ID_MERCATO);
    const rows = await c.query(`SELECT campaign.id,campaign.name,campaign.status,metrics.impressions,metrics.clicks,metrics.ctr,metrics.conversions,metrics.cost_micros,metrics.average_cpc FROM campaign WHERE segments.date DURING LAST_30_DAYS ORDER BY metrics.impressions DESC LIMIT 10`);
    res.json({success:true, shop:'mercato', data:rows.map(r=>({id:r.campaign.id, name:r.campaign.name, status:r.campaign.status, impressions:r.metrics.impressions, clicks:r.metrics.clicks, ctr:(r.metrics.ctr*100).toFixed(2), conversions:r.metrics.conversions, cost:(r.metrics.cost_micros/1e6).toFixed(2), avgCpc:(r.metrics.average_cpc/1e6).toFixed(2)}))});
  } catch(err) { res.status(500).json({success:false, error:err.message}); }
});

app.get('/api/mercato/ads/search-terms', async (req, res) => {
  try {
    const c = makeCustomer(CONFIG.ADS_CUSTOMER_ID_MERCATO);
    const rows = await c.query(`SELECT search_term_view.search_term,metrics.impressions,metrics.clicks,metrics.conversions,metrics.cost_micros FROM search_term_view WHERE segments.date DURING LAST_30_DAYS AND metrics.impressions>5 ORDER BY metrics.impressions DESC LIMIT 50`);
    const minus = ['безкоштовно','free','diy','скачати'];
    res.json({success:true, shop:'mercato', data:rows.map(r=>({term:r.search_term_view.search_term, impressions:r.metrics.impressions, clicks:r.metrics.clicks, conversions:r.metrics.conversions, cost:(r.metrics.cost_micros/1e6).toFixed(2), isSuggestedMinus:minus.some(m=>r.search_term_view.search_term.toLowerCase().includes(m))}))});
  } catch(err) { res.status(500).json({success:false, error:err.message}); }
});


app.post('/api/mercato/ads/enable-campaign', async (req, res) => {
  try {
    const c = makeCustomer(CONFIG.ADS_CUSTOMER_ID_MERCATO);
    await c.campaigns.update([{resource_name:`customers/${CONFIG.ADS_CUSTOMER_ID_MERCATO}/campaigns/${req.body.campaignId}`,status:'ENABLED'}]);
    res.json({success:true, message:'Кампанію запущено'});
  } catch(err) { res.status(500).json({success:false, error:err.message}); }
});
app.post('/api/mercato/ads/pause-campaign', async (req, res) => {
  try {
    const c = makeCustomer(CONFIG.ADS_CUSTOMER_ID_MERCATO);
    await c.campaigns.update([{resource_name:`customers/${CONFIG.ADS_CUSTOMER_ID_MERCATO}/campaigns/${req.body.campaignId}`,status:'PAUSED'}]);
    res.json({success:true, message:`Кампанія призупинена`});
  } catch(err) { res.status(500).json({success:false, error:err.message}); }
});

app.post('/api/mercato/ads/add-negative-keywords', async (req, res) => {
  try {
    const c = makeCustomer(CONFIG.ADS_CUSTOMER_ID_MERCATO);
    await c.campaignCriteria.create(req.body.keywords.map(kw=>({campaign:`customers/${CONFIG.ADS_CUSTOMER_ID_MERCATO}/campaigns/${req.body.campaignId}`,keyword:{text:kw,match_type:'BROAD'}})));
    res.json({success:true, message:`Додано ${req.body.keywords.length} мінус-слів`});
  } catch(err) { res.status(500).json({success:false, error:err.message}); }
});

// ── GA4 legacy ──────────────────────────────────────────────
app.get('/api/ga4/metrics', async (req,res) => {
  try {
    const rows = await ga4Report(CONFIG.GA4_PROPERTY_ID,[{name:'sessions'},{name:'activeUsers'},{name:'bounceRate'},{name:'averageSessionDuration'},{name:'conversions'},{name:'screenPageViewsPerSession'}],[{name:'sessionDefaultChannelGrouping'}],null,null,parsePeriod(req.query));
    res.json({success:true,shop:'prom3d',data:rows.map(r=>({channel:r.dimensionValues[0].value,sessions:r.metricValues[0].value,users:r.metricValues[1].value,bounceRate:(parseFloat(r.metricValues[2].value)*100).toFixed(1)+'%',avgDuration:Math.round(parseFloat(r.metricValues[3].value))+'с',conversions:r.metricValues[4].value,pagesPerSession:parseFloat(r.metricValues[5].value).toFixed(1)}))});
  } catch(err){res.status(500).json({success:false,error:err.message});}
});

app.get('/api/ga4/pages', async (req,res) => {
  try {
    const rows = await ga4Report(CONFIG.GA4_PROPERTY_ID,[{name:'screenPageViews'},{name:'bounceRate'},{name:'averageSessionDuration'},{name:'conversions'}],[{name:'pagePath'}],[{metric:{metricName:'screenPageViews'},desc:true}],10,parsePeriod(req.query));
    res.json({success:true,shop:'prom3d',data:rows.map(r=>({page:r.dimensionValues[0].value,views:r.metricValues[0].value,bounceRate:(parseFloat(r.metricValues[1].value)*100).toFixed(1)+'%',avgDuration:Math.round(parseFloat(r.metricValues[2].value))+'с',conversions:r.metricValues[3].value}))});
  } catch(err){res.status(500).json({success:false,error:err.message});}
});

app.get('/api/mercato/ga4/metrics', async (req,res) => {
  try {
    const rows = await ga4Report(CONFIG.GA4_PROPERTY_ID_MERCATO,[{name:'sessions'},{name:'activeUsers'},{name:'bounceRate'},{name:'averageSessionDuration'},{name:'conversions'},{name:'screenPageViewsPerSession'}],[{name:'sessionDefaultChannelGrouping'}],null,null,parsePeriod(req.query));
    res.json({success:true,shop:'mercato',data:rows.map(r=>({channel:r.dimensionValues[0].value,sessions:r.metricValues[0].value,users:r.metricValues[1].value,bounceRate:(parseFloat(r.metricValues[2].value)*100).toFixed(1)+'%',avgDuration:Math.round(parseFloat(r.metricValues[3].value))+'с',conversions:r.metricValues[4].value,pagesPerSession:parseFloat(r.metricValues[5].value).toFixed(1)}))});
  } catch(err){res.status(500).json({success:false,error:err.message});}
});

app.get('/api/mercato/ga4/pages', async (req,res) => {
  try {
    const rows = await ga4Report(CONFIG.GA4_PROPERTY_ID_MERCATO,[{name:'screenPageViews'},{name:'bounceRate'},{name:'averageSessionDuration'},{name:'conversions'}],[{name:'pagePath'}],[{metric:{metricName:'screenPageViews'},desc:true}],10,parsePeriod(req.query));
    res.json({success:true,shop:'mercato',data:rows.map(r=>({page:r.dimensionValues[0].value,views:r.metricValues[0].value,bounceRate:(parseFloat(r.metricValues[1].value)*100).toFixed(1)+'%',avgDuration:Math.round(parseFloat(r.metricValues[2].value))+'с',conversions:r.metricValues[3].value}))});
  } catch(err){res.status(500).json({success:false,error:err.message});}
});


// ── Fetch landing page content for AI analysis ───────────────
app.get('/api/fetch-page', async (req, res) => {
  const url = req.query.url;
  if (!url) return res.status(400).json({success:false,error:'url required'});
  try {
    const r = await fetch(url, {
      signal: AbortSignal.timeout(8000),
      headers: {'User-Agent':'Mozilla/5.0 (compatible; AdsAgent/1.0)'}
    });
    const html = await r.text();
    // Extract text content
    const text = html
      .replace(/<script[^>]*>[\s\S]*?<\/script>/gi,'')
      .replace(/<style[^>]*>[\s\S]*?<\/style>/gi,'')
      .replace(/<[^>]+>/g,' ')
      .replace(/\s+/g,' ')
      .trim()
      .substring(0, 3000); // first 3000 chars
    res.json({success:true, url, text});
  } catch(err) {
    res.status(500).json({success:false, error:err.message});
  }
});


// ── Proxy: читати вміст сайту для ШІ аналізу ─────────────────
app.get('/api/fetch-page', async (req, res) => {
  const url = req.query.url;
  if (!url) return res.status(400).json({success:false, error:'url required'});
  try {
    const resp = await fetch(url, {
      signal: AbortSignal.timeout(8000),
      headers: {'User-Agent':'Mozilla/5.0 (compatible; AdsIntelBot/1.0)'}
    });
    const html = await resp.text();
    // Витягуємо текстовий контент без HTML тегів
    const text = html
      .replace(/<script[\s\S]*?<\/script>/gi, '')
      .replace(/<style[\s\S]*?<\/style>/gi, '')
      .replace(/<[^>]+>/g, ' ')
      .replace(/\s+/g, ' ')
      .substring(0, 3000)
      .trim();
    res.json({success:true, url, text});
  } catch(err) {
    res.status(500).json({success:false, error:err.message});
  }
});
// ── Start ────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Prom3D Ads Agent v5 — port ${PORT}`));
module.exports = app;

// ═══════════════════════════════════════════════════════════
// CAMPAIGN MANAGER — повне управління з дашборду
// ═══════════════════════════════════════════════════════════

// Допоміжна функція для обох бізнесів
function getCustomerId(biz) {
  return biz === 'mercato' ? CONFIG.ADS_CUSTOMER_ID_MERCATO : CONFIG.ADS_CUSTOMER_ID;
}

// ── Отримати всі оголошення кампанії ─────────────────────
async function getAds(biz) {
  const c = makeCustomer(getCustomerId(biz));
  return await c.query(`
    SELECT ad_group_ad.ad.id, ad_group_ad.ad.responsive_search_ad.headlines,
      ad_group_ad.ad.responsive_search_ad.descriptions,
      ad_group_ad.status, ad_group.name, campaign.name, campaign.id,
      metrics.impressions, metrics.clicks, metrics.ctr, metrics.conversions
    FROM ad_group_ad
    WHERE segments.date DURING LAST_30_DAYS
      AND ad_group_ad.ad.type = 'RESPONSIVE_SEARCH_AD'
      AND campaign.status != 'REMOVED'
    ORDER BY metrics.impressions DESC LIMIT 20`);
}

app.get('/api/:biz/ads/list', async (req, res) => {
  const biz = req.params.biz;
  if (!['mercato','prom3d'].includes(biz)) return res.status(400).json({success:false,error:'invalid biz'});
  try {
    const rows = await getAds(biz);
    const data = rows.map(r => ({
      adId:         r.ad_group_ad?.ad?.id,
      campaignId:   r.campaign?.id,
      campaignName: r.campaign?.name,
      adGroupName:  r.ad_group?.name,
      status:       r.ad_group_ad?.status,
      headlines: (r.ad_group_ad?.ad?.responsive_search_ad?.headlines||[])
        .map(h=>({text:h.text, pinned:h.pinnedField})),
      descriptions: (r.ad_group_ad?.ad?.responsive_search_ad?.descriptions||[])
        .map(d=>({text:d.text, pinned:d.pinnedField})),
      impressions:  r.metrics?.impressions,
      clicks:       r.metrics?.clicks,
      ctr:          (r.metrics?.ctr*100).toFixed(2),
      conversions:  r.metrics?.conversions,
    }));
    res.json({success:true, data});
  } catch(err) { res.status(500).json({success:false, error:err.message}); }
});

// ── Оновити заголовки та описи RSA ───────────────────────
app.post('/api/:biz/ads/update-ad', async (req, res) => {
  const biz = req.params.biz;
  const { adId, campaignId, adGroupId, headlines, descriptions } = req.body;
  if (!adId || !headlines?.length) return res.status(400).json({success:false,error:'adId and headlines required'});
  try {
    const customerId = getCustomerId(biz);
    const c = makeCustomer(customerId);
    await c.adGroupAds.update([{
      resource_name: `customers/${customerId}/adGroupAds/${adGroupId}~${adId}`,
      ad: {
        responsive_search_ad: {
          headlines:    headlines.map((t,i)=>({text:t,pinned_field:null})),
          descriptions: descriptions.map((t,i)=>({text:t,pinned_field:null})),
        }
      }
    }]);
    res.json({success:true, message:'Оголошення оновлено'});
  } catch(err) { res.status(500).json({success:false, error:err.message}); }
});

// ── Отримати ключові слова з деталями ────────────────────
app.get('/api/:biz/ads/keywords-full', async (req, res) => {
  const biz = req.params.biz;
  try {
    const c = makeCustomer(getCustomerId(biz));
    const rows = await c.query(`
      SELECT ad_group_criterion.criterion_id, ad_group_criterion.keyword.text,
        ad_group_criterion.keyword.match_type, ad_group_criterion.status,
        ad_group_criterion.cpc_bid_micros, ad_group_criterion.quality_info.quality_score,
        ad_group_criterion.quality_info.creative_quality_score,
        ad_group_criterion.quality_info.post_click_quality_score,
        ad_group_criterion.quality_info.search_predicted_ctr,
        campaign.id, campaign.name, ad_group.id, ad_group.name,
        metrics.impressions, metrics.clicks, metrics.ctr, metrics.conversions,
        metrics.cost_micros, metrics.average_cpc
      FROM keyword_view
      WHERE segments.date DURING LAST_30_DAYS
        AND campaign.status != 'REMOVED'
      ORDER BY metrics.impressions DESC LIMIT 50`);
    const data = rows.map(r=>({
      criterionId:  r.ad_group_criterion?.criterion_id,
      keyword:      r.ad_group_criterion?.keyword?.text,
      matchType:    r.ad_group_criterion?.keyword?.match_type,
      status:       r.ad_group_criterion?.status,
      bidMicros:    r.ad_group_criterion?.cpc_bid_micros,
      bid:          (r.ad_group_criterion?.cpc_bid_micros/1e6).toFixed(2),
      qs:           r.ad_group_criterion?.quality_info?.quality_score,
      qsAd:         r.ad_group_criterion?.quality_info?.creative_quality_score,
      qsLP:         r.ad_group_criterion?.quality_info?.post_click_quality_score,
      qsCTR:        r.ad_group_criterion?.quality_info?.search_predicted_ctr,
      campaignId:   r.campaign?.id,
      campaignName: r.campaign?.name,
      adGroupId:    r.ad_group?.id,
      adGroupName:  r.ad_group?.name,
      impressions:  r.metrics?.impressions,
      clicks:       r.metrics?.clicks,
      ctr:          (r.metrics?.ctr*100).toFixed(2),
      conversions:  r.metrics?.conversions,
      cost:         (r.metrics?.cost_micros/1e6).toFixed(2),
      avgCpc:       (r.metrics?.average_cpc/1e6).toFixed(2),
    }));
    res.json({success:true, data});
  } catch(err) { res.status(500).json({success:false, error:err.message}); }
});

// ── Змінити ставку ключового слова ───────────────────────
app.post('/api/:biz/ads/update-bid', async (req, res) => {
  const biz = req.params.biz;
  const { criterionId, adGroupId, campaignId, newBidUah } = req.body;
  if (!criterionId || !newBidUah) return res.status(400).json({success:false,error:'criterionId and newBidUah required'});
  try {
    const customerId = getCustomerId(biz);
    const c = makeCustomer(customerId);
    await c.adGroupCriteria.update([{
      resource_name: `customers/${customerId}/adGroupCriteria/${adGroupId}~${criterionId}`,
      cpc_bid_micros: Math.round(parseFloat(newBidUah) * 1e6),
    }]);
    res.json({success:true, message:`Ставку змінено → ₴${newBidUah}`});
  } catch(err) { res.status(500).json({success:false, error:err.message}); }
});

// ── Пауза / активація ключового слова ────────────────────
app.post('/api/:biz/ads/toggle-keyword', async (req, res) => {
  const biz = req.params.biz;
  const { criterionId, adGroupId, status } = req.body; // status: 'ENABLED' | 'PAUSED'
  try {
    const customerId = getCustomerId(biz);
    const c = makeCustomer(customerId);
    await c.adGroupCriteria.update([{
      resource_name: `customers/${customerId}/adGroupCriteria/${adGroupId}~${criterionId}`,
      status,
    }]);
    res.json({success:true, message:`Ключове слово ${status==='PAUSED'?'призупинено':'активовано'}`});
  } catch(err) { res.status(500).json({success:false, error:err.message}); }
});

// ── Додати нові ключові слова ─────────────────────────────
app.post('/api/:biz/ads/add-keywords', async (req, res) => {
  const biz = req.params.biz;
  const { adGroupId, keywords } = req.body;
  // keywords: [{text, matchType, bidUah}]
  if (!adGroupId || !keywords?.length) return res.status(400).json({success:false,error:'adGroupId and keywords required'});
  try {
    const customerId = getCustomerId(biz);
    const c = makeCustomer(customerId);
    const ops = keywords.map(k=>({
      ad_group:      `customers/${customerId}/adGroups/${adGroupId}`,
      status:        'ENABLED',
      keyword:       { text: k.text, match_type: k.matchType||'BROAD' },
      cpc_bid_micros: k.bidUah ? Math.round(parseFloat(k.bidUah)*1e6) : undefined,
    }));
    await c.adGroupCriteria.create(ops);
    res.json({success:true, message:`Додано ${keywords.length} ключових слів`});
  } catch(err) { res.status(500).json({success:false, error:err.message}); }
});

// ── Додати мінус-слова ────────────────────────────────────
app.post('/api/:biz/ads/add-negatives', async (req, res) => {
  const biz = req.params.biz;
  const { campaignId, keywords } = req.body;
  if (!campaignId || !keywords?.length) return res.status(400).json({success:false,error:'campaignId and keywords required'});
  try {
    const customerId = getCustomerId(biz);
    const c = makeCustomer(customerId);
    await c.campaignCriteria.create(
      keywords.map(kw=>({
        campaign: `customers/${customerId}/campaigns/${campaignId}`,
        keyword:  { text: kw, match_type: 'BROAD' },
      }))
    );
    res.json({success:true, message:`Додано ${keywords.length} мінус-слів`});
  } catch(err) { res.status(500).json({success:false, error:err.message}); }
});

// ── Увімкнути кампанію ────────────────────────────────────
app.post('/api/:biz/ads/enable-campaign', async (req, res) => {
  const biz = req.params.biz;
  const { campaignId } = req.body;
  try {
    const customerId = getCustomerId(biz);
    const c = makeCustomer(customerId);
    await c.campaigns.update([{
      resource_name: `customers/${customerId}/campaigns/${campaignId}`,
      status: 'ENABLED',
    }]);
    res.json({success:true, message:'Кампанію запущено'});
  } catch(err) { res.status(500).json({success:false, error:err.message}); }
});

// ── Змінити бюджет кампанії ───────────────────────────────
app.post('/api/:biz/ads/update-budget', async (req, res) => {
  const biz = req.params.biz;
  const { campaignId, budgetAmountMicros } = req.body;
  if (!campaignId || !budgetAmountMicros) return res.status(400).json({success:false,error:'campaignId and budget required'});
  try {
    const customerId = getCustomerId(biz);
    const c = makeCustomer(customerId);
    // Спочатку отримуємо поточний budget resource name
    const rows = await c.query(`
      SELECT campaign.id, campaign_budget.resource_name, campaign_budget.amount_micros
      FROM campaign WHERE campaign.id = ${campaignId}`);
    const budgetRN = rows[0]?.campaign_budget?.resource_name;
    if (!budgetRN) throw new Error('Budget not found');
    await c.campaignBudgets.update([{
      resource_name: budgetRN,
      amount_micros: Math.round(budgetAmountMicros),
    }]);
    res.json({success:true, message:`Бюджет оновлено → ₴${(budgetAmountMicros/1e6).toFixed(0)}/день`});
  } catch(err) { res.status(500).json({success:false, error:err.message}); }
});
