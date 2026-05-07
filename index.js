const axios = require('axios');
const fs = require('fs');
const { XMLParser } = require('fast-xml-parser');

const CONFIG = {
    CSV_FIRME_URL: 'https://serv-10.carrefour.com.ar:446/DYN/routes/GCP_DYN_DownloadExport',
    XML_MARKETPLACE_URL: 'https://www.carrefour.com.ar/XMLData/test-dy.xml',
    OUTPUT_FILE: 'feed_unificado.csv'
};

// Helper: lee un campo en cualquier capitalización (VTEX es inconsistente)
function pick(obj, ...keys) {
    for (const k of keys) {
        if (obj && obj[k] !== undefined && obj[k] !== null) return obj[k];
    }
    return undefined;
}

// Helper: extrae cuotas sin interés REALES de un commertialOffer.
// FUENTE ÚNICA DE VERDAD: Teasers / PromotionTeasers.
// Estos arrays los llena el motor de promociones de VTEX SOLO con las promos
// activas y publicadas que se muestran como ribbon en el PDP. Es lo que el
// front efectivamente promociona en la card.
//
// NO usamos Installments ni PaymentOptions porque esos exponen TODOS los medios
// de pago configurados (incluso los que no se promocionan visualmente),
// generando falsos positivos como el caso del Yoga Ball.
function extractCuotasFromCommertialOffer(commertialOffer) {
    let maxCuotas = 0;

    // Unimos Teasers y PromotionTeasers (VTEX usa ambos según el tipo de promo)
    const teasers = [
        ...(commertialOffer.Teasers || []),
        ...(commertialOffer.PromotionTeasers || [])
    ];

    if (teasers.length === 0) return 0;

    for (const teaser of teasers) {
        // Los effects pueden venir con distintas claves según versión de VTEX
        const effects = teaser.Effects || teaser.effects || teaser['<Effects>'] || {};
        const params = effects.Parameters || effects.parameters || [];

        // Indicador 1: parámetro explícito de cuotas sin interés
        for (const p of params) {
            const name = pick(p, 'Name', 'name') || '';
            const value = pick(p, 'Value', 'value');

            // Solo aceptamos el parámetro estricto que indica cuotas SIN interés.
            // Evitamos NumberOfDues / TotalNumberOfDues porque también aparecen
            // en promos de cuotas fijas CON interés.
            if (name === 'NumberOfDuesWithoutInterest') {
                const n = parseInt(value, 10);
                if (!isNaN(n) && n > 1 && n > maxCuotas) {
                    maxCuotas = n;
                }
            }
        }

        // Indicador 2: el nombre del teaser menciona explícitamente "sin interés"
        // (fallback para casos donde Carrefour configuró la promo sin parámetros estructurados)
        const teaserName = pick(teaser, 'Name', 'name', 'TeaserName', '<Name>') || '';
        if (/sin\s*inter[eé]s|sem\s*juros/i.test(teaserName)) {
            // Buscamos un número de cuotas en el nombre: "3 cuotas sin interés", "12 sem juros", etc.
            const m = teaserName.match(/(\d+)\s*(cuotas?|vezes?|x)/i);
            if (m) {
                const n = parseInt(m[1], 10);
                if (!isNaN(n) && n > 1 && n > maxCuotas) {
                    maxCuotas = n;
                }
            }
        }
    }

    return maxCuotas;
}

// Helper: pega a la API de VTEX con reintentos + backoff
async function fetchVtexBatch(apiUrl, attempt = 1) {
    const MAX_ATTEMPTS = 3;
    try {
        const res = await axios.get(apiUrl, { timeout: 15000 });
        return res.data;
    } catch (e) {
        const status = e.response?.status || 'NO_RESPONSE';
        if (attempt < MAX_ATTEMPTS) {
            const wait = 500 * Math.pow(2, attempt); // 1s, 2s, 4s
            console.log(`   ↻ Reintento ${attempt}/${MAX_ATTEMPTS - 1} (status: ${status}) en ${wait}ms...`);
            await new Promise(r => setTimeout(r, wait));
            return fetchVtexBatch(apiUrl, attempt + 1);
        }
        throw e;
    }
}

// 👑 FUNCIÓN GOD-TIER (V4): Búsqueda Omnibus
// Fixes: itera TODOS los sellers, matchea SKU exacto, suma Teasers, capitalización defensiva
async function getRealInstallments(mktpItems) {
    console.log('🕵️‍♂️ Iniciando escaneo profundo de cuotas reales en VTEX (Búsqueda Omnibus V4)...');
    const realInstallmentsMap = {};
    const skuIds = [];

    // 1. Extraer los ID de SKU
    for (const item of mktpItems) {
        const match = item.link.match(/idsku=(\d+)/);
        if (match) skuIds.push(match[1]);
    }

    let lotesFallidos = 0;

    // 2. Agrupar en lotes de 40 para no saturar el servidor
    const chunkSize = 40;
    for (let i = 0; i < skuIds.length; i += chunkSize) {
        const chunk = skuIds.slice(i, i + chunkSize);
        const chunkSet = new Set(chunk); // Para matchear el SKU exacto que pedimos
        const queryParams = chunk.map(id => `fq=skuId:${id}`).join('&');
        const apiUrl = `https://www.carrefour.com.ar/api/catalog_system/pub/products/search?${queryParams}`;

        try {
            const data = await fetchVtexBatch(apiUrl);

            for (const product of data) {
                if (!product.items || product.items.length === 0) continue;

                // FIX #2: en lugar de items[0], buscamos el item cuyo SKU pedimos
                for (const item of product.items) {
                    if (!chunkSet.has(item.itemId)) continue; // Solo procesamos los SKUs que pedimos

                    const sellers = item.sellers || [];
                    let maxCuotasSinInteres = 0;

                    // FIX #1: iteramos TODOS los sellers, no solo el [0]
                    for (const seller of sellers) {
                        const commertialOffer = seller.commertialOffer || {};
                        const cuotas = extractCuotasFromCommertialOffer(commertialOffer);
                        if (cuotas > maxCuotasSinInteres) {
                            maxCuotasSinInteres = cuotas;
                        }
                    }

                    if (maxCuotasSinInteres > 1) {
                        realInstallmentsMap[item.itemId] = maxCuotasSinInteres;
                    }
                }
            }
        } catch (e) {
            lotesFallidos++;
            const status = e.response?.status || 'NO_RESPONSE';
            console.log(`⚠️ Lote fallido (status: ${status}) | SKUs: ${chunk.slice(0, 5).join(',')}${chunk.length > 5 ? '...' : ''} (+${chunk.length - 5} más)`);
        }

        await new Promise(r => setTimeout(r, 300));
    }

    console.log(`✅ Escaneo completo. ¡Se descubrieron cuotas reales en ${Object.keys(realInstallmentsMap).length} productos!`);
    if (lotesFallidos > 0) {
        console.log(`⚠️ Atención: ${lotesFallidos} lote(s) fallaron tras todos los reintentos.`);
    }
    return realInstallmentsMap;
}

async function run() {
    console.log('🚀 Iniciando unificación...');

    try {
        console.log('📥 Descargando XML de Marketplace...');
        const xmlRes = await axios.get(`${CONFIG.XML_MARKETPLACE_URL}?nocache=${Date.now()}`);
        const parser = new XMLParser({ ignoreAttributes: false, removeNSPrefix: true });
        const jsonObj = parser.parse(xmlRes.data);
        const mktpItems = jsonObj.DY.channel.item;
        console.log(`✅ ${mktpItems.length} productos de marketplace listos.`);

        // Ejecutamos el escáner de cuotas reales
        const realInstallmentsMap = await getRealInstallments(mktpItems);

        const outputStream = fs.createWriteStream(CONFIG.OUTPUT_FILE, { encoding: 'utf8' });
        // BOM para que Excel/Dynamic Yield interpreten correctamente UTF-8 (evita mojibake tipo "interÃ©s")
        outputStream.write('\uFEFF');

        console.log('📥 Procesando CSV de Firme...');
        const csvRes = await axios({
            method: 'get',
            url: CONFIG.CSV_FIRME_URL,
            responseType: 'stream'
        });

        let headers = [];
        let isFirstLine = true;
        let remainder = '';
        let fileSeparator = ';';

        for await (const chunk of csvRes.data) {
            const lines = (remainder + chunk.toString()).split(/\r?\n/);
            remainder = lines.pop();

            for (let line of lines) {
                if (isFirstLine) {
                    if (line.includes('\t')) fileSeparator = '\t';
                    else if (line.includes(';')) fileSeparator = ';';
                    else if (line.includes(',')) fileSeparator = ',';

                    headers = line.split(fileSeparator).map(h => h.trim());
                    outputStream.write(line + '\n');
                    isFirstLine = false;
                } else {
                    outputStream.write(line + '\n');
                }
            }
        }

        console.log('➕ Agregando productos de Marketplace...');
        for (const item of mktpItems) {
            const row = buildMktpRow(item, headers, fileSeparator, realInstallmentsMap);
            outputStream.write(row + '\n');
        }

        outputStream.end();
        console.log('✨ Proceso finalizado con éxito.');

    } catch (err) {
        console.error('❌ Error crítico:', err.message);
        process.exit(1);
    }
}

function parseArsPrice(p) {
    if (!p) return "0.00";
    const cleaned = String(p).replace(/ARS\s*/gi, '').replace(/\./g, '').replace(',', '.').replace(/[^0-9.-]+/g, '');
    return parseFloat(cleaned).toFixed(2);
}

function buildMktpRow(item, headers, fileSeparator, realInstallmentsMap) {
    const price = parseArsPrice(item.sale_price || item.price);
    const inStock = item.availability === 'in stock' ? 'true' : 'false';
    const brand = item.brand || '';

    let ribbonValue = '';
    const match = item.link.match(/idsku=(\d+)/);
    if (match) {
        const skuId = match[1];
        const cuotasReales = realInstallmentsMap[skuId];
        if (cuotasReales) {
            ribbonValue = `${cuotasReales} Cuotas sin interés`;
        }
    }

    return headers.map(h => {
        switch (h) {
            case 'sku': return item.id;
            case 'group_id': return item.id;
            case 'name': return `"${item.title.replace(/"/g, '""')}"`;
            case 'url': return item.link;
            case 'image_url': return item.image_link;
            case 'categories': return `"${(item.product_type || 'Marketplace').replace(/ > /g, '|')}"`;
            case 'ribbons': return ribbonValue ? `"${ribbonValue}"` : '';
            case 'keywords': return `"${brand}"`;
            case 'price': return price;
            case 'in_stock': return inStock;
            default:
                if (h.startsWith('lng:carrefourar')) {
                    if (h.endsWith(':price')) return price;
                    if (h.endsWith(':in_stock')) return inStock;
                }
                return '';
        }
    }).join(fileSeparator);
}

run();
